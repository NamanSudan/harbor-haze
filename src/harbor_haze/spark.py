"""One place to build the SparkSession, so batch and streaming jobs share settings."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"


def get_spark(app: str, kafka: bool = False) -> SparkSession:
    # Python workers must run the same interpreter as the driver (pandas UDFs)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.appName(app)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "6g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "16"))
        .config("spark.ui.showConsoleProgress", "false")
    )
    if kafka:
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
