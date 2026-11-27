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


def test_vessel_that_anchors_inside_the_hour_counts_as_both(spark):
    rows = [row(9, 0, 33.741, -118.241, 8.0, 0, 70), row(9, 40, 33.742, -118.242, 0.0, 1, 70)]
    r = vessel_hourly(spark.createDataFrame(rows, AIS_SCHEMA), 0.02).collect()[0]
    assert (r.n_vessels, r.n_stationary, r.n_underway) == (1, 1, 1)


def test_stationary_plus_underway_covers_every_vessel(spark):
    rows = [
        row(1, 0, 33.741, -118.241, 0.0, 5, 71),  # moored all hour
        row(2, 5, 33.741, -118.241, 11.0, 0, 81),  # underway all hour
        row(3, 10, 33.742, -118.242, 6.0, 0, 70),  # slows down and anchors
        row(3, 50, 33.742, -118.242, 0.1, 1, 70),
        row(4, 20, 33.743, -118.243, None, 0, 52),  # unknown speed, underway status
        row(5, 15, 33.60, -118.10, 0.3, 0, 60),  # drifting, other cell
        row(5, 45, 33.60, -118.10, 9.0, 0, 60),
    ]
    out = vessel_hourly(spark.createDataFrame(rows, AIS_SCHEMA), 0.02).collect()
    assert len(out) == 2
    for r in out:
        assert r.n_stationary + r.n_underway >= r.n_vessels
    port = next(r for r in out if r.cell_lat == 33.74)
    assert (port.n_vessels, port.n_stationary, port.n_underway) == (4, 2, 3)
