"""Runs the real streaming logic (watermark, dedupe, registry join, hourly windows) on a file source."""

from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from harbor_haze.streaming.vessel_stream import POSITION, live_hourly


@pytest.fixture(scope="module")
def spark():
    from harbor_haze.spark import get_spark

    s = get_spark("tests-stream")
    yield s
    s.stop()


def write_batch(folder, name, rows):
    cols = ["mmsi", "ts", "lat", "lon", "sog", "cog", "heading", "status"]
    data = {c: [r[i] for r in rows] for i, c in enumerate(cols)}
    schema = pa.schema(
        [("mmsi", pa.int64()), ("ts", pa.timestamp("us")), ("lat", pa.float64()), ("lon", pa.float64()),
         ("sog", pa.float64()), ("cog", pa.float64()), ("heading", pa.int32()), ("status", pa.int32())]
    )  # fmt: skip
    pq.write_table(pa.table(data, schema=schema), folder / name)


def test_hour_is_emitted_once_after_watermark_with_duplicates_dropped(spark, tmp_path):
    src, out, chk = tmp_path / "src", tmp_path / "out", tmp_path / "chk"
    src.mkdir()
    registry = spark.createDataFrame([(1, 71), (2, 81)], "mmsi long, vessel_type int")
    # same fields as the Kafka payload, with ts already parsed to a timestamp
    stream_schema = T.StructType(
        [T.StructField("ts", T.TimestampType()) if f.name == "ts" else f for f in POSITION.fields]
    )

    def run_once():
        positions = spark.readStream.schema(stream_schema).parquet(str(src))
        q = (
            live_hourly(positions, registry, "15 minutes", 0.02)
            .writeStream.outputMode("append")
            .format("parquet")
            .option("path", str(out))
            .option("checkpointLocation", str(chk))
            .trigger(availableNow=True)
            .start()
        )
        q.awaitTermination()

    # hour 10:00 with a duplicated report from vessel 1, plus vessel 2
    write_batch(src, "a.parquet", [
        (1, datetime(2024, 9, 1, 10, 5), 33.741, -118.241, 0.0, 0.0, 0, 5),
        (1, datetime(2024, 9, 1, 10, 5), 33.741, -118.241, 0.0, 0.0, 0, 5),
        (2, datetime(2024, 9, 1, 10, 20), 33.745, -118.245, 11.0, 0.0, 0, 0),
    ])  # fmt: skip
    run_once()
    assert spark.read.parquet(str(out)).count() == 0, "the hour must not be emitted before the watermark passes it"

    # a report at 11:30 moves the watermark past 11:00 + 15 min, closing the 10:00 hour
    write_batch(src, "b.parquet", [(1, datetime(2024, 9, 1, 11, 30), 33.741, -118.241, 0.0, 0.0, 0, 5)])
    run_once()
    run_once()  # watermark advances at the end of a batch; the next batch emits the closed window
    # format in the Spark session (UTC); collect() would convert timestamps to the laptop's local time
    rows = spark.read.parquet(str(out)).withColumn("hour", F.date_format("hour_utc", "yyyy-MM-dd HH:mm")).collect()
    assert len(rows) == 1
    r = rows[0]
    assert r.hour == "2024-09-01 10:00"
    assert (r.n_pings, r.n_vessels, r.n_stationary, r.n_underway, r.n_cargo, r.n_tanker) == (2, 2, 1, 1, 1, 1)
