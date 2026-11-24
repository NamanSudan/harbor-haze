"""EPA AQS pre-generated hourly files (PM2.5, NO2, wind) -> monitors near the port.

Each national file (one per parameter and year) is read by Spark, cut to California monitors
within `monitor_radius_km` of the port and to the window's days, and written to
lake/raw/epa_hourly/param=<name>/date=<yyyy-mm-dd> (UTC day). A rerun replaces only the days of its
window, so a small window never wipes days loaded earlier.
"""

from __future__ import annotations

import argparse
import shutil
import zipfile

from pyspark.sql import Column
from pyspark.sql import functions as F

from harbor_haze.common import DATA_DIR, LAKE, download, load_config, log, record, setup_logging, window_days
from harbor_haze.spark import get_spark

OUT = LAKE / "raw" / "epa_hourly"
CACHE = DATA_DIR / "cache" / "epa"


def haversine_col(lat: Column, lon: Column, lat0: float, lon0: float) -> Column:
    r = 6371.0088
    dlat = F.radians(lat - F.lit(lat0))
    dlon = F.radians(lon - F.lit(lon0))
    a = F.sin(dlat / 2) ** 2 + F.cos(F.radians(F.lit(lat0))) * F.cos(F.radians(lat)) * F.sin(dlon / 2) ** 2
    return 2 * r * F.asin(F.sqrt(a))


def run(window: str) -> None:
    cfg = load_config()
    src = cfg["sources"]["epa_hourly"]
    center = cfg["study_area"]["center"]
    radius = cfg["study_area"]["monitor_radius_km"]
    days = window_days(cfg, window)
    day_strs = [d.isoformat() for d in days]
    years = sorted({d.year for d in days})
    spark = get_spark("epa-hourly")
    for code, name in src["params"].items():
        for year in years:
            url = src["url"].format(param=code, year=year)
            zpath = CACHE / url.rsplit("/", 1)[-1]
            size = download(url, zpath)
            if size is None:
                log.warning("EPA %s %s: not published at %s", name, year, url)
                record("epa_hourly", param=name, year=year, status="missing", url=url)
                continue
            csv_dir = CACHE / zpath.stem
            if not csv_dir.exists():  # extract to a temp dir and rename, so a crash never leaves half a CSV
                tmp_dir = csv_dir.with_name(csv_dir.name + ".part")
                shutil.rmtree(tmp_dir, ignore_errors=True)
                with zipfile.ZipFile(zpath) as z:
                    z.extractall(tmp_dir)
                tmp_dir.rename(csv_dir)
            df = spark.read.option("header", True).csv(str(csv_dir))
            rows_in = df.count()
            lat, lon = F.col("Latitude").cast("double"), F.col("Longitude").cast("double")
            out = (
                df.where(F.col("State Code") == src["state_code"])
                .withColumn("distance_km", haversine_col(lat, lon, center["lat"], center["lon"]))
                .where(F.col("distance_km") <= radius)
                .where(F.col("Date GMT").isin(day_strs))
                .select(
                    F.concat_ws("-", "State Code", "County Code", "Site Num").alias("site_id"),
                    F.col("POC").cast("int").alias("poc"),
                    F.col("Parameter Code").alias("parameter_code"),
                    F.col("Parameter Name").alias("parameter_name"),
                    lat.alias("lat"),
                    lon.alias("lon"),
                    F.round("distance_km", 2).alias("distance_km"),
                    F.to_timestamp(F.concat_ws(" ", "Date GMT", "Time GMT"), "yyyy-MM-dd HH:mm").alias("ts_utc"),
                    F.to_timestamp(F.concat_ws(" ", "Date Local", "Time Local"), "yyyy-MM-dd HH:mm").alias("ts_local"),
                    F.col("Sample Measurement").cast("double").alias("value"),
                    F.col("Units of Measure").alias("units"),
                    F.col("Qualifier").alias("qualifier"),
                    F.col("Method Name").alias("method"),
                    F.col("County Name").alias("county"),
                )
                .dropDuplicates(["site_id", "poc", "parameter_code", "ts_utc"])
                .withColumn("param", F.lit(name))
                .withColumn("date", F.to_date("ts_utc"))
                .cache()
            )
            n_kept, n_sites = out.count(), out.select("site_id").distinct().count()
            if n_kept:
                # dynamic partition overwrite: only the (param, date) partitions present in `out` are replaced
                out.repartition("date").write.mode("overwrite").partitionBy("param", "date").parquet(str(OUT))
            out.unpersist()
            log.info(
                "EPA %s %s: %s national rows -> %s kept at %d sites", name, year, f"{rows_in:,}", f"{n_kept:,}", n_sites
            )
            record(
                "epa_hourly",
                param=name,
                year=year,
                bytes_downloaded=size,
                rows_in=rows_in,
                rows_kept=n_kept,
                sites=n_sites,
            )
    spark.stop()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--window", default="dev")
    a = p.parse_args()
    setup_logging()
    run(a.window)


if __name__ == "__main__":
    main()
