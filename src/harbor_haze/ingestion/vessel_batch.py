"""Batch side of the vessel features: historical AIS in the lake -> hourly features + vessel registry.

* lake/features/vessel_hourly  same columns as the streaming job's output (shared `vessel_hourly`)
* lake/features/bay_hourly     the same counts for the whole bay, one row for every hour of every AIS day
* lake/raw/vessel_registry      latest known type/size per MMSI, used by the stream-static join
"""

from __future__ import annotations

import argparse
from datetime import date

from pyspark.sql import Window
from pyspark.sql import functions as F

from harbor_haze.common import LAKE, load_config, log, record, setup_logging, window_days
from harbor_haze.features import bay_hourly, vessel_hourly
from harbor_haze.spark import get_spark

AIS = LAKE / "raw" / "ais"


def spine_days(cfg: dict, ais_days: set[date]) -> list[date]:
    """Every day of each configured window that has at least one day in raw/ais, plus any other AIS day.

    A day that failed to load inside a loaded window still gets its hours (as low coverage).
    """
    days = set(ais_days)
    for name in cfg["windows"]:
        window = set(window_days(cfg, name))
        if window & ais_days:
            days |= window
    return sorted(days)


def bay_hourly_full(ais, low_coverage_pings: int, days: list[date]):
    """Bay-wide counts for every hour of `days`: hours without pings get zero counts.

    `low_coverage` flags hours with fewer pings than the threshold (AIS gaps, not empty water).
    """
    day = F.to_timestamp("date")
    hours = ais.sparkSession.createDataFrame([(d,) for d in days], "date date").select(
        F.explode(F.sequence(day, day + F.expr("INTERVAL 23 HOURS"), F.expr("INTERVAL 1 HOUR"))).alias("hour_utc")
    )
    counts = bay_hourly(ais)
    zero = {c: 0 for c in counts.columns if c.startswith("n_")}
    return (
        hours.join(counts, "hour_utc", "left")
        .fillna(zero)
        .withColumn("low_coverage", F.col("n_pings") < low_coverage_pings)
        .select(*counts.columns, "low_coverage")
    )


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

    ais_days = {r.date for r in ais.select("date").distinct().collect()}
    bay = bay_hourly_full(ais, cfg["features"]["low_coverage_pings"], spine_days(cfg, ais_days))
    bay.repartition(1).sortWithinPartitions("hour_utc").write.mode("overwrite").parquet(
        str(LAKE / "features" / "bay_hourly")
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
    n_bay = spark.read.parquet(str(LAKE / "features" / "bay_hourly")).count()
    log.info(
        "vessel features: %s cell-hours, %s bay hours; registry: %s vessels",
        f"{n_hours:,}",
        f"{n_bay:,}",
        f"{n_vessels:,}",
    )
    record("vessel_batch", cell_hours=n_hours, bay_hours=n_bay, registry_vessels=n_vessels)
    spark.stop()


if __name__ == "__main__":
    main()
