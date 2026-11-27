"""Batch side of the vessel features: historical AIS in the lake -> hourly features + vessel registry.

* lake/features/vessel_hourly  same columns as the streaming job's output (shared `vessel_hourly`)
* lake/raw/vessel_registry      latest known type/size per MMSI, used by the stream-static join
"""

from __future__ import annotations

import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from harbor_haze.common import LAKE, load_config, log, record, setup_logging
from harbor_haze.features import vessel_hourly
from harbor_haze.spark import get_spark

AIS = LAKE / "raw" / "ais"


BRONZE_STATIC = LAKE / "stream" / "bronze" / "topic=ais.static"
REGISTRY = LAKE / "raw" / "vessel_registry"
STATIC_JSON = "mmsi long, ts string, vessel_type int, vessel_name string, length double, draft double"


def build_registry(spark, ais) -> int:
    """Latest known type/size per MMSI from historical AIS plus live ShipStaticData (bronze)."""
    hist = ais.where(F.col("vessel_type") > 0).select(
        "mmsi", "vessel_type", "vessel_name", "length", "draft", F.col("ts").alias("seen_at")
    )
    if BRONZE_STATIC.exists():
        live = (
            spark.read.parquet(str(BRONZE_STATIC))
            .select(F.from_json("payload", STATIC_JSON).alias("s"))
            .select("s.*")
            .where(F.col("vessel_type") > 0)
            .select("mmsi", "vessel_type", "vessel_name", "length", "draft", F.to_timestamp("ts").alias("seen_at"))
        )
        hist = hist.unionByName(live)
    latest = Window.partitionBy("mmsi").orderBy(F.col("seen_at").desc())
    registry = hist.withColumn("rn", F.row_number().over(latest)).where("rn = 1").drop("rn")
    rows = registry.collect()  # small (thousands of vessels); collect so we can overwrite the path we read from
    spark.createDataFrame(rows, registry.schema).coalesce(1).write.mode("overwrite").parquet(str(REGISTRY))
    return len(rows)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--registry-only", action="store_true", help="only refresh the vessel registry")
    a = p.parse_args()
    setup_logging()
    cfg = load_config()
    spark = get_spark("vessel-batch")
    ais = spark.read.parquet(str(AIS))
    if a.registry_only:
        log.info("vessel registry: %s vessels", f"{build_registry(spark, ais):,}")
        spark.stop()
        return

    hourly = vessel_hourly(ais, cfg["study_area"]["grid_deg"]).withColumn("date", F.to_date("hour_utc"))
    hourly.repartition("date").write.mode("overwrite").partitionBy("date").parquet(
        str(LAKE / "features" / "vessel_hourly")
    )

    build_registry(spark, ais)

    n_hours = spark.read.parquet(str(LAKE / "features" / "vessel_hourly")).count()
    n_vessels = spark.read.parquet(str(LAKE / "raw" / "vessel_registry")).count()
    log.info("vessel features: %s cell-hours; registry: %s vessels", f"{n_hours:,}", f"{n_vessels:,}")
    record("vessel_batch", cell_hours=n_hours, registry_vessels=n_vessels)
    spark.stop()


if __name__ == "__main__":
    main()
