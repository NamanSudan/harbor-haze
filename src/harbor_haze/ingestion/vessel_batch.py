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


def main() -> None:
    argparse.ArgumentParser(description=__doc__).parse_args()
    setup_logging()
    cfg = load_config()
    spark = get_spark("vessel-batch")
    ais = spark.read.parquet(str(AIS))

    hourly = vessel_hourly(ais, cfg["study_area"]["grid_deg"]).withColumn("date", F.to_date("hour_utc"))
    hourly.repartition("date").write.mode("overwrite").partitionBy("date").parquet(
        str(LAKE / "features" / "vessel_hourly")
    )

    latest = Window.partitionBy("mmsi").orderBy(F.col("ts").desc())
    registry = (
        ais.where(F.col("vessel_type").isNotNull() & (F.col("vessel_type") > 0))
        .withColumn("rn", F.row_number().over(latest))
        .where("rn = 1")
        .select("mmsi", "vessel_type", "vessel_name", "length", "draft", F.col("ts").alias("seen_at"))
    )
    registry.coalesce(1).write.mode("overwrite").parquet(str(LAKE / "raw" / "vessel_registry"))

    n_hours = spark.read.parquet(str(LAKE / "features" / "vessel_hourly")).count()
    n_vessels = spark.read.parquet(str(LAKE / "raw" / "vessel_registry")).count()
    log.info("vessel features: %s cell-hours; registry: %s vessels", f"{n_hours:,}", f"{n_vessels:,}")
    record("vessel_batch", cell_hours=n_hours, registry_vessels=n_vessels)
    spark.stop()


if __name__ == "__main__":
    main()
