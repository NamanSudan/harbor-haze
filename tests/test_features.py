from datetime import datetime

import pytest

from harbor_haze.features import AIS_SCHEMA, vessel_hourly


@pytest.fixture(scope="module")
def spark():
    from harbor_haze.spark import get_spark

    s = get_spark("tests")
    yield s
    s.stop()


def row(mmsi, minute, lat, lon, sog, status, vtype):
    return (mmsi, datetime(2024, 9, 1, 10, minute), lat, lon, sog, 0.0, 0, status, vtype, 200.0, 10.0, "X", "test")


def test_vessel_hourly_counts_by_cell_and_class(spark):
    rows = [
        row(1, 0, 33.741, -118.241, 0.0, 5, 71),  # cargo, moored
        row(1, 30, 33.742, -118.242, 0.0, 5, 71),  # same vessel, same hour and cell
        row(2, 10, 33.745, -118.245, 12.0, 0, 81),  # tanker underway, same cell
        row(3, 5, 33.60, -118.10, 0.2, 1, 52),  # tug at anchor, another cell
    ]
    df = spark.createDataFrame(rows, AIS_SCHEMA)
    out = {(r.cell_lat, r.cell_lon): r for r in vessel_hourly(df, 0.02).collect()}
    assert len(out) == 2
    port = out[(33.74, -118.26)]
    assert (port.n_pings, port.n_vessels, port.n_stationary, port.n_underway) == (3, 2, 1, 1)
    assert (port.n_cargo, port.n_tanker, port.n_tug) == (1, 1, 0)
    assert port.hour_utc == datetime(2024, 9, 1, 10, 0)
    assert port.mean_sog_underway == 12.0
    other = out[(33.6, -118.1)]
    assert (other.n_vessels, other.n_tug, other.n_stationary) == (1, 1, 1)


def test_unknown_speed_counts_as_underway_not_dropped(spark):
    rows = [row(7, 0, 33.741, -118.241, None, 0, 70), row(8, 0, 33.741, -118.241, None, 5, 70)]
    r = vessel_hourly(spark.createDataFrame(rows, AIS_SCHEMA), 0.02).collect()[0]
    assert (r.n_vessels, r.n_stationary, r.n_underway) == (2, 1, 1)
