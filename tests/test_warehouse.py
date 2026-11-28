"""Warehouse transforms on tiny synthetic tables shaped like the lake handoff (no Parquet, no Postgres)."""

import numpy as np
import pandas as pd
import pytest

from harbor_haze.warehouse import build as wb

SITE_A = dict(site_id="06-037-4009", county="Los Angeles", lat=33.7937, lon=-118.1710, distance_km=5.0)
SITE_B = dict(site_id="06-037-1301", county="Los Angeles", lat=33.9, lon=-118.2, distance_km=20.0)


def epa():
    rows = []
    # two collocated instruments at site A, same hour -> averaged
    for poc, v in [(1, 10.0), (2, 20.0)]:
        hour1 = "2021-10-01T18:00:00Z"
        rows.append({**SITE_A, "poc": poc, "param": "pm25", "ts_utc": hour1, "value": v, "units": "UG/M3"})
    h1, h2 = "2021-10-01T18:00:00Z", "2021-10-02T18:00:00Z"
    rows.append({**SITE_A, "poc": 1, "param": "pm25", "ts_utc": h2, "value": 8.0, "units": "UG/M3"})
    rows.append({**SITE_B, "poc": 1, "param": "pm25", "ts_utc": h1, "value": 12.0, "units": "UG/M3"})
    rows.append({**SITE_A, "poc": 1, "param": "no2", "ts_utc": h1, "value": 15.0, "units": "PPB"})
    rows.append({**SITE_A, "poc": 1, "param": "wind", "ts_utc": h1, "value": 3.0, "units": "KNOTS"})
    return pd.DataFrame(rows)


def ghcnh():
    base = dict(
        temperature=20.0,
        dew_point_temperature=10.0,
        relative_humidity=50.0,
        wind_speed=4.0,
        wind_gust=6.0,
        station_level_pressure=1010.0,
        precipitation=0.0,
        visibility=10.0,
    )
    return pd.DataFrame(
        [
            {**base, "station": "USW00023129", "ts_utc": "2021-10-01T18:05:00Z", "wind_direction": 350.0},
            {**base, "station": "USW00023129", "ts_utc": "2021-10-01T18:40:00Z", "wind_direction": 10.0},
            {**base, "station": "USW00023174", "ts_utc": "2021-10-01T18:53:00Z", "wind_direction": 270.0},
        ]
    )


def hms():
    return pd.DataFrame(
        [
            {"date": "2021-10-01", "smoke_density": 2, "smoke_polygons": 3, "file_found": True},
            {"date": "2021-10-02", "smoke_density": 0, "smoke_polygons": 0, "file_found": False},
        ]
    )


def vessels():
    base = dict(
        n_vessels=1,
        n_stationary=0,
        n_underway=0,
        n_cargo=0,
        n_tanker=0,
        n_passenger=0,
        n_tug=0,
        mean_sog_underway=np.nan,
    )
    h = "2021-10-01T18:00:00Z"
    return pd.DataFrame(
        [
            # two cells near site A in the same hour: weighted mean sog = (10*5 + 30*1) / 40 = 2.0
            {
                **base,
                "cell_lat": 33.78,
                "cell_lon": -118.18,
                "hour_utc": h,
                "n_pings": 10,
                "mean_sog": 5.0,
                "n_cargo": 4,
            },
            {
                **base,
                "cell_lat": 33.80,
                "cell_lon": -118.16,
                "hour_utc": h,
                "n_pings": 30,
                "mean_sog": 1.0,
                "n_tanker": 2,
            },
            # far away cell (about 55 km): must not be linked to either monitor
            {
                **base,
                "cell_lat": 33.30,
                "cell_lon": -118.50,
                "hour_utc": h,
                "n_pings": 99,
                "mean_sog": 9.0,
                "n_cargo": 50,
            },
        ]
    )


@pytest.fixture(scope="module")
def frames():
    return wb.build_all({"epa": epa(), "ghcnh": ghcnh(), "hms": hms(), "vessels": vessels()})


def test_fact_grain_and_poc_average(frames):
    f, st, p = frames["fact_air_quality"], frames["dim_station"], frames["dim_parameter"]
    assert not f.duplicated(["time_key", "station_key", "parameter_key"]).any()
    assert set(f["parameter_key"]) <= set(p["parameter_key"])  # wind is not a pollutant
    a = st.loc[st.site_id == SITE_A["site_id"], "station_key"].item()
    row = f[(f.station_key == a) & (f.parameter_key == 1) & (f.time_key == 2021100118)].iloc[0]
    assert row["value"] == 15.0 and row["n_poc"] == 2
    # A pm25 hour 1 (two POCs -> one row), A pm25 hour 2, B pm25, A no2; nothing for wind
    assert len(f) == 4


def test_foreign_keys_resolve(frames):
    f = frames["fact_air_quality"]
    assert set(f["time_key"]) <= set(frames["dim_time"]["time_key"])
    assert set(f["station_key"]) <= set(frames["dim_station"]["station_key"])
    assert set(f["weather_key"].dropna()) <= set(frames["dim_weather"]["weather_key"])
    assert set(f["vessel_key"].dropna()) <= set(frames["dim_vessel_activity"]["vessel_key"])
    assert set(f["smoke_key"].dropna()) <= set(frames["dim_smoke"]["date_key"])
    for name in ("dim_weather", "dim_vessel_activity"):
        assert set(frames[name]["time_key"]) <= set(frames["dim_time"]["time_key"])


def test_nearest_weather_station(frames):
    st = frames["dim_station"].set_index("site_id")
    assert st.loc[SITE_A["site_id"], "wx_station"] == "LGB"  # Long Beach monitor -> Long Beach airport


def test_wind_direction_is_vector_averaged(frames):
    w = frames["dim_weather"]
    lgb = w[w.wx_station == "LGB"].iloc[0]
    assert lgb["n_obs"] == 2
    d = lgb["wind_direction"]
    assert min(d, 360 - d) < 1e-6  # 350 and 10 degrees average to north, not 180


def test_vessel_radius_and_weighted_sog(frames):
    v, st = frames["dim_vessel_activity"], frames["dim_station"]
    a = st.loc[st.site_id == SITE_A["site_id"], "station_key"].item()
    row = v[(v.station_key == a) & (v.time_key == 2021100118)].iloc[0]
    assert row["n_cells"] == 2 and row["n_pings"] == 40
    assert row["n_cargo"] == 4 and row["n_tanker"] == 2  # the far cell's 50 cargo pings are excluded
    assert row["mean_sog"] == pytest.approx(2.0)


def test_smoke_unknown_is_null_not_zero(frames):
    s = frames["dim_smoke"].set_index("date_key")
    assert s.loc[20211001, "smoke_density"] == 2
    assert pd.isna(s.loc[20211002, "smoke_density"]) and not s.loc[20211002, "file_found"]


def test_time_dimension_local_hour(frames):
    t = frames["dim_time"].set_index("time_key")
    assert t.loc[2021100118, "hour_local"] == 11  # 18:00 UTC = 11:00 PDT
    assert t.loc[2021100118, "dow_local"] == 4  # Friday


def test_columns_match_schema_lists(frames):
    for name, cols in wb.TABLE_COLUMNS.items():
        missing = [c for c in cols if c not in frames[name].columns]
        assert not missing, (name, missing)


def test_unknown_weather_stations_fail_loudly():
    with pytest.raises(ValueError, match="No GHCN station matched"):
        wb.classify_wx(["SOMEWHERE_ELSE"])


def bay():
    base = dict(
        n_pings=5000,
        n_vessels=300,
        n_stationary=120,
        n_underway=200,
        n_cargo=40,
        n_tanker=20,
        n_passenger=5,
        n_tug=9,
        n_cargo_tanker=60,
        n_cargo_tanker_stationary=35,
        n_cargo_tanker_anchored=12,
        mean_sog=3.0,
        mean_sog_underway=6.0,
    )
    return pd.DataFrame(
        [
            {**base, "hour_utc": "2021-10-01T18:00:00Z", "low_coverage": False},
            {**base, "hour_utc": "2021-10-01T19:00:00Z", "n_pings": 0, "n_vessels": 0, "low_coverage": True},
        ]
    )


def test_bay_table_flags_low_coverage_and_joins_time():
    out = wb.build_all({"epa": epa(), "ghcnh": ghcnh(), "hms": hms(), "vessels": vessels(), "bay": bay()})
    b = out["dim_bay_activity"].set_index("time_key")
    assert not b.loc[2021100118, "low_coverage"]
    assert b.loc[2021100119, "low_coverage"]
    assert b.loc[2021100118, "n_cargo_tanker_anchored"] == 12
    assert set(b.index) <= set(out["dim_time"]["time_key"])  # FK to dim_time resolves


def test_bay_table_is_optional():
    out = wb.build_all({"epa": epa(), "ghcnh": ghcnh(), "hms": hms(), "vessels": vessels()})
    assert out["dim_bay_activity"].empty


def test_calm_wind_gives_null_direction_not_zero():
    g = ghcnh().copy()
    g["wind_direction"] = np.nan  # the lake stores the source's 999 (calm/variable) as null
    out = wb.build_all({"epa": epa(), "ghcnh": g, "hms": hms(), "vessels": vessels()})
    assert out["dim_weather"]["wind_direction"].isna().all()
    assert out["dim_weather"]["wind_speed"].notna().any()


def test_local_timestamp_is_wall_clock_time(frames):
    t = frames["dim_time"].set_index("time_key")
    assert t.loc[2021100118, "ts_local"].hour == 11  # naive 11:00, not a UTC-shifted instant
