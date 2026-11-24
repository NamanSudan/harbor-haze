"""Spark Structured Streaming: land every live topic in the lake as-is (bronze), partitioned by day.

Keeps the raw live record so the warehouse can rebuild from it; the payload stays JSON and is
parsed downstream. Exactly-once into Parquet through the checkpoint.
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import functions as F

from harbor_haze.common import DATA_DIR, LAKE, setup_logging
from harbor_haze.spark import get_spark

TOPICS = ["ais.positions", "ais.static", "airnow.observations", "weather.observations"]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--trigger", default="1 minute")
    p.add_argument("--starting-offsets", default="earliest")
    a = p.parse_args()
    setup_logging()
    spark = get_spark("kafka-to-lake", kafka=True)
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"))
        .option("subscribe", ",".join(TOPICS))
        .option("startingOffsets", a.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
        .select(
            "topic",
            F.col("key").cast("string").alias("key"),
            F.col("value").cast("string").alias("payload"),
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_ts"),
            F.to_date("timestamp").alias("date"),
        )
    )
    q = (
        raw.writeStream.format("parquet")
        .partitionBy("topic", "date")
        .option("path", str(LAKE / "stream" / "bronze"))
        .option("checkpointLocation", str(DATA_DIR / "checkpoints" / "bronze"))
        .trigger(processingTime=a.trigger)
        .start()
    )
    q.awaitTermination()


if __name__ == "__main__":
    main()
