import struct
from datetime import date

import pandas as pd

from harbor_haze.common import load_config
from harbor_haze.ingestion.ais_history import _wkb_coord, day_url


def wkb_point(x: float, y: float) -> bytes:
    return struct.pack("<BIdd", 1, 1, x, y)


def test_day_url_switches_format_in_2024():
    src = load_config()["sources"]["ais_history"]
    url, kind = day_url(src, date(2021, 9, 1))
    assert kind == "csv" and url.endswith("AIS_2021_09_01.zip")
    url, kind = day_url(src, date(2024, 9, 1))
    assert kind == "parquet" and url.endswith("ais-2024-09-01.parquet")


def test_wkb_decoding_fast_and_slow_paths():
    geoms = pd.Series([wkb_point(-118.25, 33.74), wkb_point(-118.1, 33.6)])
    assert _wkb_coord(geoms, 5).tolist() == [-118.25, -118.1]
    assert _wkb_coord(geoms, 13).tolist() == [33.74, 33.6]
    with_null = pd.Series([wkb_point(-118.25, 33.74), None])
    out = _wkb_coord(with_null, 13)
    assert out.iloc[0] == 33.74 and pd.isna(out.iloc[1])


def test_merge_existing_keeps_earlier_rows(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from harbor_haze.ingestion.ghcnh import merge_existing

    path = tmp_path / "part-0.parquet"
    pq.write_table(pa.table({"date": [1, 2, 3], "v": [10, 20, 30]}), path)
    out = merge_existing(path, pa.table({"date": [3, 4], "v": [31, 40]}), "date")
    assert out.to_pydict() == {"date": [1, 2, 3, 4], "v": [10, 20, 31, 40]}


def test_wind_direction_999_becomes_null_and_calm_is_flagged():
    import pyarrow as pa

    from harbor_haze.ingestion.ghcnh import clean_wind

    t = pa.table({"wind_direction": [999.0, 999.0, 250.0, None], "wind_speed": [0.0, 1.5, 3.1, None]})
    out = clean_wind(t)
    assert out["wind_direction"].to_pylist() == [None, None, 250.0, None]
    assert out["wind_calm"].to_pylist() == [True, False, False, None]
    assert clean_wind(out).equals(out)


def test_merge_existing_with_a_new_column(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from harbor_haze.ingestion.ghcnh import clean_wind, merge_existing

    # file written by the loader before wind_calm existed, still holding a raw 999
    path = tmp_path / "part-0.parquet"
    pq.write_table(pa.table({"ts": [1, 2], "wind_direction": [999.0, 90.0], "wind_speed": [0.0, 2.0]}), path)
    new = clean_wind(pa.table({"ts": [2, 3], "wind_direction": [100.0, 999.0], "wind_speed": [2.5, 0.0]}))
    out = clean_wind(merge_existing(path, new, "ts"))
    assert out.column_names == ["ts", "wind_direction", "wind_speed", "wind_calm"]
    assert out.to_pydict() == {
        "ts": [1, 2, 3],
        "wind_direction": [None, 100.0, None],
        "wind_speed": [0.0, 2.5, 0.0],
        "wind_calm": [True, False, True],
    }


def test_done_checks_the_partition_data(tmp_path, monkeypatch):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from harbor_haze.ingestion import ais_history

    monkeypatch.setattr(ais_history, "OUT", tmp_path)

    def part(d: date):
        p = tmp_path / f"date={d.isoformat()}"
        p.mkdir()
        return p / "part-00000.snappy.parquet"

    missing, empty, zero_rows, garbage, valid = (date(2024, 9, i) for i in range(1, 6))
    part(empty)
    pq.write_table(pa.table({"mmsi": pa.array([], pa.int64())}), part(zero_rows))
    good = tmp_path / "good.parquet"
    pq.write_table(pa.table({"mmsi": [1, 2, 3]}), good)
    part(garbage).write_bytes(good.read_bytes()[:-20])  # truncated: footer cut off
    pq.write_table(pa.table({"mmsi": [1, 2, 3]}), part(valid))

    assert not ais_history.done(missing)
    assert not ais_history.done(empty)
    assert not ais_history.done(zero_rows)
    assert not ais_history.done(garbage)
    assert ais_history.done(valid)
