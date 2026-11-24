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
