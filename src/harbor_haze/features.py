"""Vessel-activity features per grid cell and hour.

The same function runs on the historical AIS (batch) and on the live AISStream topic
(Structured Streaming), so the model sees identical features in training and serving.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Canonical AIS position schema shared by every AIS source.
AIS_SCHEMA = T.StructType(
    [
        T.StructField("mmsi", T.LongType()),
        T.StructField("ts", T.TimestampType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("lon", T.DoubleType()),
        T.StructField("sog", T.DoubleType()),
        T.StructField("cog", T.DoubleType()),
        T.StructField("heading", T.IntegerType()),
        T.StructField("status", T.IntegerType()),
        T.StructField("vessel_type", T.IntegerType()),
        T.StructField("length", T.DoubleType()),
        T.StructField("draft", T.DoubleType()),
        T.StructField("vessel_name", T.StringType()),
        T.StructField("source", T.StringType()),
    ]
)

# AIS navigational status codes: 1 = at anchor, 5 = moored
STATIONARY_STATUS = (1, 5)
STATIONARY_SOG_KNOTS = 0.5


def vessel_class(vessel_type: Column) -> Column:
    """Coarse class from the AIS ship-type code (ITU-R M.1371)."""
    return (
        F.when(vessel_type.between(70, 79), "cargo")
        .when(vessel_type.between(80, 89), "tanker")
        .when(vessel_type.between(60, 69), "passenger")
        .when(vessel_type.isin(31, 32, 52), "tug")
        .when(vessel_type.isNull() | (vessel_type == 0), "unknown")
        .otherwise("other")
    )


def with_grid(df: DataFrame, grid_deg: float) -> DataFrame:
    """Snap each position to the south-west corner of its grid cell.

    The small epsilon keeps points that sit exactly on a cell edge (33.60 / 0.02) in that cell
    despite floating-point error.
    """
    eps = 1e-9
    return df.withColumn("cell_lat", F.round(F.floor(F.col("lat") / grid_deg + eps) * grid_deg, 4)).withColumn(
        "cell_lon", F.round(F.floor(F.col("lon") / grid_deg + eps) * grid_deg, 4)
    )


def _distinct_vessels(cond: Column) -> Column:
    # approx_count_distinct works in both batch and streaming aggregations (exact distinct does not stream)
    return F.approx_count_distinct(F.when(cond, F.col("mmsi")), rsd=0.01)


def vessel_hourly(positions: DataFrame, grid_deg: float) -> DataFrame:
    """Aggregate positions to one row per (hour, cell).

    Counting rules:

    - Within a cell-hour a vessel counts as stationary if any of its pings is stationary
      (status 1 or 5, or speed under 0.5 kn) and as underway if any of its pings is not.
      A ship that arrives and anchors inside the hour is counted in both, so
      ``n_stationary + n_underway >= n_vessels``. Exclusive counts can be derived:
      stationary only = ``n_vessels - n_underway``, underway only = ``n_vessels - n_stationary``,
      both = ``n_stationary + n_underway - n_vessels``.
    - Counts are distinct vessels per cell-hour. Do not sum them across cells: a moving vessel
      is counted in every cell it crosses.
    - Counts use ``approx_count_distinct`` (needed for streaming), so they can be off by one.

    The categories are not made mutually exclusive here because that needs a per-vessel
    aggregation before the per-cell one, and the streaming job runs this same function in
    update mode, where Spark does not support chained aggregations.
    """
    # coalesce: a ping with unknown speed and an "underway" status would otherwise be null and count as neither
    stationary = F.coalesce(
        F.col("status").isin(*STATIONARY_STATUS) | (F.col("sog") < STATIONARY_SOG_KNOTS), F.lit(False)
    )
    cls = vessel_class(F.col("vessel_type"))
    df = with_grid(positions, grid_deg)
    return (
        df.groupBy(F.window("ts", "1 hour").alias("w"), "cell_lat", "cell_lon")
        .agg(
            F.count(F.lit(1)).alias("n_pings"),
            _distinct_vessels(F.lit(True)).alias("n_vessels"),
            _distinct_vessels(stationary).alias("n_stationary"),
            _distinct_vessels(~stationary).alias("n_underway"),
            _distinct_vessels(cls == "cargo").alias("n_cargo"),
            _distinct_vessels(cls == "tanker").alias("n_tanker"),
            _distinct_vessels(cls == "passenger").alias("n_passenger"),
            _distinct_vessels(cls == "tug").alias("n_tug"),
            F.avg("sog").alias("mean_sog"),
            F.avg(F.when(~stationary, F.col("sog"))).alias("mean_sog_underway"),
        )
        .withColumn("hour_utc", F.col("w.start"))
        .drop("w")
    )
