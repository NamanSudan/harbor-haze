"""Historical AIS (NOAA MarineCadastre): national daily files -> San Pedro Bay positions.

Each day's national file is downloaded, read by Spark, cut to the study bounding box,
normalised to the canonical schema and written to lake/raw/ais/date=YYYY-MM-DD.
The national file is deleted after its batch, so disk use stays bounded.
Reruns skip days that are already in the lake, meaning the day's partition has readable parquet
files with at least one row (use --force to rebuild them).
"""

from __future__ import annotations

import argparse
import struct
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import pandas_udf

from harbor_haze.common import (
    LAKE,
    STAGING,
    BBox,
    batched,
    clear_dir,
    download,
    load_config,
    log,
    record,
    setup_logging,
    study_bbox,
    window_days,
)
from harbor_haze.features import AIS_SCHEMA
from harbor_haze.spark import get_spark

OUT = LAKE / "raw" / "ais"


def day_url(src: dict, d: date) -> tuple[str, str]:
    """(url, kind) for one day; kind is 'csv' (zipped CSV) or 'parquet' (GeoParquet)."""
    kind = "parquet" if d.year >= src["parquet_from_year"] else "csv"
    url = src[f"{kind}_url"].format(year=d.year, month=d.month, day=d.day)
    return url, kind


def done(d: date) -> bool:
    """True if the day's partition has parquet files that can be read and hold at least one row.

    Spark writes no _SUCCESS marker per partition with dynamic partition overwrite, so the data
    itself is checked. Only the parquet footers are read, which is fast and needs no Spark.
    """
    parts = list((OUT / f"date={d.isoformat()}").glob("part-*.parquet"))
    try:
        return bool(parts) and sum(pq.read_metadata(p).num_rows for p in parts) > 0
    except (OSError, pa.ArrowInvalid):
        return False


@pandas_udf(T.DoubleType())
def wkb_x(geom: pd.Series) -> pd.Series:
    return _wkb_coord(geom, 5)


@pandas_udf(T.DoubleType())
def wkb_y(geom: pd.Series) -> pd.Series:
    return _wkb_coord(geom, 13)


def _wkb_coord(geom: pd.Series, offset: int) -> pd.Series:
    """Decode x (offset 5) or y (offset 13) from little-endian 2D WKB points (21 bytes each)."""
    if geom.isna().any() or not (geom.map(len) == 21).all():
        # slow path for nulls or unexpected geometry sizes
        return geom.map(lambda b: _point_coord(b, offset)).astype("float64")
    raw = np.frombuffer(b"".join(geom.tolist()), dtype=np.uint8).reshape(-1, 21)
    if not (raw[:, 0] == 1).all():
        return geom.map(lambda b: _point_coord(b, offset)).astype("float64")
    return pd.Series(raw[:, offset : offset + 8].copy().view("<f8").ravel())


def _point_coord(b: bytes | None, offset: int) -> float | None:
    if b is None or len(b) < 21:
        return None
    order = "<" if b[0] == 1 else ">"
    return struct.unpack_from(order + "d", b, offset)[0]


def read_csv_days(spark: SparkSession, paths: list[str]) -> DataFrame:
    df = spark.read.option("header", True).csv(paths)
    return df.select(
        F.col("MMSI").cast("long").alias("mmsi"),
        F.to_timestamp("BaseDateTime").alias("ts"),
        F.col("LAT").cast("double").alias("lat"),
        F.col("LON").cast("double").alias("lon"),
        F.col("SOG").cast("double").alias("sog"),
        F.col("COG").cast("double").alias("cog"),
        F.col("Heading").cast("int").alias("heading"),
        F.col("Status").cast("int").alias("status"),
        F.col("VesselType").cast("int").alias("vessel_type"),
        F.col("Length").cast("double").alias("length"),
        F.col("Draft").cast("double").alias("draft"),
        F.col("VesselName").alias("vessel_name"),
        F.lit("marinecadastre_csv").alias("source"),
    )


def read_parquet_days(spark: SparkSession, paths: list[str]) -> DataFrame:
    df = spark.read.parquet(*paths)
    return df.select(
        F.col("mmsi").cast("long").alias("mmsi"),
        F.col("base_date_time").cast("timestamp").alias("ts"),
        wkb_y("geometry").alias("lat"),
        wkb_x("geometry").alias("lon"),
        F.col("sog").cast("double").alias("sog"),
        F.col("cog").cast("double").alias("cog"),
        F.col("heading").cast("int").alias("heading"),
        F.col("status").cast("int").alias("status"),
        F.col("vessel_type").cast("int").alias("vessel_type"),
        F.col("length").cast("double").alias("length"),
        F.col("draft").cast("double").alias("draft"),
        F.col("vessel_name"),
        F.lit("marinecadastre_parquet").alias("source"),
    )


def clip(df: DataFrame, bbox: BBox) -> DataFrame:
    """Keep the study area and drop obvious bad records (sentinel speeds, missing ids)."""
    return (
        df.where(F.col("lat").between(bbox.min_lat, bbox.max_lat))
        .where(F.col("lon").between(bbox.min_lon, bbox.max_lon))
        .where(F.col("mmsi").isNotNull() & F.col("ts").isNotNull())
        .withColumn("sog", F.when(F.col("sog").between(0, 60), F.col("sog")))
        .withColumn("date", F.to_date("ts"))
        .select(*AIS_SCHEMA.fieldNames(), "date")
        .dropDuplicates(["mmsi", "ts"])
    )


def stage(url: str, kind: str, d: date) -> tuple[str | None, int]:
    """Download one day into staging; unzip CSVs. Returns (local path or None if missing, bytes downloaded)."""
    target = STAGING / "ais" / url.rsplit("/", 1)[-1]
    size = download(url, target)
    if size is None:
        return None, 0
    if kind == "csv":
        out_dir = STAGING / "ais" / d.isoformat()
        with zipfile.ZipFile(target) as z:
            z.extractall(out_dir)
        target.unlink()
        csvs = sorted(out_dir.rglob("*.csv"))
        return str(csvs[0]) if csvs else None, size
    return str(target), size


def run(window: str, force: bool = False) -> None:
    cfg = load_config()
    src = cfg["sources"]["ais_history"]
    bbox = study_bbox(cfg)
    days = [d for d in window_days(cfg, window) if force or not done(d)]
    log.info("AIS history: %d day(s) to load for window %r", len(days), window)
    if not days:
        return
    spark = get_spark("ais-history")
    for batch in batched(days, src["days_per_batch"]):
        clear_dir(STAGING / "ais")
        staged: dict[str, list[str]] = {"csv": [], "parquet": []}
        downloaded = 0
        # the source hosts are slow per connection, so the batch downloads in parallel
        with ThreadPoolExecutor(max_workers=len(batch)) as pool:
            results = list(pool.map(lambda d: (d, *day_url(src, d), *stage(*day_url(src, d), d)), batch))
        for d, url, kind, path, size in results:
            if path is None:
                log.warning("AIS %s: no file at %s", d, url)
                record("ais_history", day=d, status="missing", url=url)
                continue
            staged[kind].append(path)
            downloaded += size
        frames = []
        if staged["csv"]:
            frames.append(read_csv_days(spark, staged["csv"]))
        if staged["parquet"]:
            frames.append(read_parquet_days(spark, staged["parquet"]))
        if not frames:
            continue
        national = frames[0] if len(frames) == 1 else frames[0].unionByName(frames[1])
        rows_in = national.count()  # national rows read, reported in the manifest
        clipped = clip(national, bbox).where(F.col("date").isin([d.isoformat() for d in batch]))
        clipped.repartition("date").write.mode("overwrite").partitionBy("date").parquet(str(OUT))
        kept = spark.read.parquet(str(OUT)).where(F.col("date").isin([d.isoformat() for d in batch])).count()

        log.info("AIS %s..%s: %s national rows -> %s kept", batch[0], batch[-1], f"{rows_in:,}", f"{kept:,}")
        record(
            "ais_history", first=batch[0], last=batch[-1], bytes_downloaded=downloaded, rows_in=rows_in, rows_kept=kept
        )
    clear_dir(STAGING / "ais")
    spark.stop()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--window", default="dev", help="window name from config/sources.yaml")
    p.add_argument("--force", action="store_true", help="reload days that are already in the lake")
    a = p.parse_args()
    setup_logging()
    run(a.window, a.force)


if __name__ == "__main__":
    main()
