"""Spark Structured Streaming: live AIS positions (Kafka) -> hourly vessel features per grid cell.

Reads `ais.positions`, joins the vessel type from the vessel registry (built from historical AIS
plus live static messages), aggregates with the same `vessel_hourly` function used in batch, and
writes:

* lake/stream/vessel_hourly (Parquet, append): each hour once, after the watermark closes it (final values)
* `features.vessel_hourly` topic (update): the current hour's running values every trigger, for the live UI;
  consumers keep the latest message per (cell, hour)
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import functions as F
from pyspark.sql import types as T

from harbor_haze.common import DATA_DIR, LAKE, load_config, setup_logging
from harbor_haze.features import vessel_hourly
from harbor_haze.spark import get_spark

POSITION = T.StructType(
    [
        T.StructField("mmsi", T.LongType()),
        T.StructField("ts", T.StringType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("sog", T.DoubleType()),
        T.StructField("cog", T.DoubleType()),
        T.StructField("heading", T.IntegerType()),
        T.StructField("status", T.IntegerType()),
    ]
)
REGISTRY = LAKE / "raw" / "vessel_registry"


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--watermark", default="15 minutes", help="how late a position may arrive")
    p.add_argument("--trigger", default="1 minute")
    a = p.parse_args()
    setup_logging()
    cfg = load_config()
    spark = get_spark("vessel-stream", kafka=True)

    positions = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"))
        .option("subscribe", "ais.positions")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(F.from_json(F.col("value").cast("string"), POSITION).alias("p"))
        .select("p.*")
        .withColumn("ts", F.to_timestamp("ts"))
        .where(F.col("lat").isNotNull() & F.col("lon").isNotNull())
        .withColumn("sog", F.when(F.col("sog").between(0, 60), F.col("sog")))
    )

    # Stream-static join: vessel type is not in position reports.
    if REGISTRY.exists():
        # Copy it into memory once: the batch job rewrites these files, and a lazy read would lose them mid-stream.
        rows = spark.read.parquet(str(REGISTRY)).select("mmsi", "vessel_type").collect()
        registry = spark.createDataFrame(rows, "mmsi long, vessel_type int")
        positions = positions.join(F.broadcast(registry), "mmsi", "left")
    else:
        positions = positions.withColumn("vessel_type", F.lit(None).cast("int"))

    hourly = vessel_hourly(positions.withWatermark("ts", a.watermark), cfg["study_area"]["grid_deg"])
    checkpoints = DATA_DIR / "checkpoints"

    to_lake = (
        hourly.writeStream.outputMode("append")
        .format("parquet")
        .option("path", str(LAKE / "stream" / "vessel_hourly"))
        .option("checkpointLocation", str(checkpoints / "vessel_hourly_lake"))
        .trigger(processingTime=a.trigger)
        .start()
    )
    to_kafka = (
        hourly.select(
            F.concat_ws(":", "cell_lat", "cell_lon").alias("key"),
            F.to_json(F.struct(*hourly.columns)).alias("value"),
        )
        .writeStream.outputMode("update")
        .format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"))
        .option("topic", "features.vessel_hourly")
        .option("checkpointLocation", str(checkpoints / "vessel_hourly_kafka_live"))
        .trigger(processingTime=a.trigger)
        .start()
    )
    for q in (to_lake, to_kafka):
        print(f"started query {q.name or q.id}")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
