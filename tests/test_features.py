from datetime import date, datetime

import pytest

from harbor_haze.features import AIS_SCHEMA, bay_hourly, vessel_hourly


@pytest.fixture(scope="module")
def spark():
    from harbor_haze.spark import get_spark

    s = get_spark("tests")
    yield s
    s.stop()


def row(mmsi, minute, lat, lon, sog, status, vtype, hour=10):
    return (mmsi, datetime(2024, 9, 1, hour, minute), lat, lon, sog, 0.0, 0, status, vtype, 200.0, 10.0, "X", "test")


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


def test_cargo_tanker_anchored_means_status_1_only(spark):
    rows = [
        row(1, 0, 33.741, -118.241, 0.0, 1, 71),  # cargo at anchor
        row(2, 0, 33.742, -118.242, 0.0, 5, 72),  # cargo moored at a berth: stationary, not anchored
        row(3, 0, 33.743, -118.243, 0.0, 1, 52),  # tug at anchor: not cargo or tanker
        row(4, 0, 33.744, -118.244, 11.0, 0, 84),  # tanker underway
        row(5, 0, 33.745, -118.245, 0.0, 1, 60),  # passenger ship at anchor: not cargo or tanker
        row(6, 0, 33.746, -118.246, 0.0, 1, 90),  # "other" type at anchor: not cargo or tanker
    ]
    r = vessel_hourly(spark.createDataFrame(rows, AIS_SCHEMA), 0.02).collect()[0]
    assert (r.n_cargo_tanker, r.n_cargo_tanker_stationary, r.n_cargo_tanker_anchored) == (3, 2, 1)
    assert (r.n_vessels, r.n_tug, r.n_passenger) == (6, 1, 1)


def test_ship_crossing_two_cells_counts_once_bay_wide(spark):
    rows = [
        row(1, 0, 33.741, -118.241, 10.0, 0, 70),  # cargo underway in one cell ...
        row(1, 20, 33.761, -118.241, 10.0, 0, 70),  # ... and in the next cell north, same hour
        row(2, 30, 33.761, -118.241, 0.0, 1, 80),  # tanker at anchor in the second cell
    ]
    df = spark.createDataFrame(rows, AIS_SCHEMA)
    cells = vessel_hourly(df, 0.02).collect()
    assert len(cells) == 2
    assert sum(r.n_cargo_tanker for r in cells) == 3  # the cargo ship is in both cells
    (bay,) = bay_hourly(df).collect()
    assert (bay.n_pings, bay.n_vessels, bay.n_cargo, bay.n_cargo_tanker) == (3, 2, 1, 2)
    assert (bay.n_cargo_tanker_stationary, bay.n_cargo_tanker_anchored, bay.n_underway) == (1, 1, 1)
    assert "cell_lat" not in bay.asDict()


def test_bay_and_cell_tables_agree_on_one_cell(spark):
    rows = [
        row(1, 0, 33.741, -118.241, 0.0, 5, 71),
        row(1, 30, 33.742, -118.242, 0.0, 1, 71),
        row(2, 10, 33.745, -118.245, 12.0, 0, 81),
        row(3, 5, 33.746, -118.246, 0.2, 1, 52),
        row(4, 50, 33.747, -118.247, None, 0, 60),
    ]
    df = spark.createDataFrame(rows, AIS_SCHEMA)
    (cell,) = vessel_hourly(df, 0.02).collect()
    (bay,) = bay_hourly(df).collect()
    cell = cell.asDict()
    assert {k: cell[k] for k in bay.asDict()} == bay.asDict()


def test_batch_bay_table_fills_empty_hours(spark):
    from pyspark.sql import functions as F

    from harbor_haze.ingestion.vessel_batch import bay_hourly_full

    rows = [row(1, m, 33.741, -118.241, 0.0, 1, 71) for m in range(3)] + [row(2, 0, 33.741, -118.241, 9.0, 0, 70)]
    at_noon = [row(3, m, 33.741, -118.241, 5.0, 0, 70, hour=12) for m in (0, 1)]
    ais = spark.createDataFrame(rows + at_noon, AIS_SCHEMA).withColumn("date", F.to_date("ts"))
    days = [date(2024, 9, 1), date(2024, 9, 2)]  # 09-02 has no pings at all (a failed day)
    out = sorted(bay_hourly_full(ais, low_coverage_pings=2, days=days).collect(), key=lambda r: r.hour_utc)
    assert len(out) == 48
    assert [r.hour_utc.hour for r in out] == list(range(24)) * 2
    busy = out[10]
    assert (busy.n_pings, busy.n_vessels, busy.n_cargo_tanker_anchored, busy.low_coverage) == (4, 2, 1, False)
    assert (out[12].n_pings, out[12].low_coverage) == (2, False)  # exactly at the threshold: not low coverage
    empty = out[3]
    assert (empty.n_pings, empty.n_vessels, empty.n_cargo_tanker, empty.low_coverage) == (0, 0, 0, True)
    assert empty.mean_sog is None
    missing_day = out[24:]
    assert all(r.n_pings == 0 and r.low_coverage for r in missing_day)


def test_spine_does_not_pull_in_a_larger_window_from_a_nested_one():
    from harbor_haze.ingestion.vessel_batch import spine_days

    cfg = {
        "windows": {
            "full": [{"start": "2024-09-01", "end": "2024-09-30"}],
            "dev": [{"start": "2024-09-01", "end": "2024-09-02"}],  # inside full
        }
    }
    dev = [date(2024, 9, 1), date(2024, 9, 2)]
    assert spine_days(cfg, set(dev)) == dev  # dev load only: no fake outages for the rest of September
    assert spine_days(cfg, {date(2024, 9, 1)}) == dev  # a failed dev day still shows up
    full = spine_days(cfg, set(dev) | {date(2024, 9, 20)})  # full load with a failed 09-21
    assert len(full) == 30 and date(2024, 9, 21) in full


def test_spine_covers_loaded_windows_including_failed_days():
    from harbor_haze.ingestion.vessel_batch import spine_days

    cfg = {
        "windows": {
            "a": [{"start": "2024-09-01", "end": "2024-09-03"}],
            "b": [{"start": "2025-01-01", "end": "2025-01-02"}],  # nothing loaded: left out
        }
    }
    loaded = {date(2024, 9, 1), date(2024, 9, 3), date(2023, 5, 5)}  # 09-02 failed; 2023-05-05 is outside any window
    assert spine_days(cfg, loaded) == [date(2023, 5, 5), date(2024, 9, 1), date(2024, 9, 2), date(2024, 9, 3)]
