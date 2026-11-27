"""NOAA GHCN-Hourly weather for the stations next to the port -> lake/raw/ghcnh.

Files are small (one per station and year), so pyarrow is enough here; no Spark needed.
"""

from __future__ import annotations

import argparse

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from harbor_haze.common import DATA_DIR, LAKE, download, load_config, log, record, setup_logging, window_days

OUT = LAKE / "raw" / "ghcnh"
CACHE = DATA_DIR / "cache" / "ghcnh"
KEEP = ["STATION", "Station_name", "DATE", "LATITUDE", "LONGITUDE"]


def merge_existing(path, new: pa.Table, key: str) -> pa.Table:
    """Union with what is already in the file; for the same key the new row wins. Keeps earlier windows."""
    if not path.exists():
        return new
    old = pq.read_table(path)
    old = old.filter(pc.invert(pc.is_in(old[key], value_set=new[key])))
    return pa.concat_tables([old, new.cast(old.schema)]).sort_by(key)


def run(window: str) -> None:
    cfg = load_config()
    src = cfg["sources"]["ghcnh"]
    days = window_days(cfg, window)
    day_set = pa.array([d.isoformat() for d in days])
    for station in src["stations"]:
        for year in sorted({d.year for d in days}):
            url = src["url"].format(station=station, year=year)
            path = CACHE / url.rsplit("/", 1)[-1]
            if download(url, path) is None:
                log.warning("GHCNh %s %s: missing", station, year)
                record("ghcnh", station=station, year=year, status="missing", url=url)
                continue
            table = pq.read_table(path)
            cols = KEEP + [c for c in src["columns"] if c in table.column_names]
            table = table.select(cols)
            ts = pc.strptime(table["DATE"], format="%Y-%m-%dT%H:%M:%S", unit="s", error_is_null=True)
            table = table.append_column("ts_utc", ts)
            table = table.filter(pc.is_in(pc.strftime(ts, format="%Y-%m-%d"), value_set=day_set))
            for c in src["columns"]:
                if c in table.column_names:
                    i = table.column_names.index(c)
                    table = table.set_column(i, c, pc.cast(table[c], pa.float64(), safe=False))
            dest = OUT / f"station={station}" / f"year={year}"
            dest.mkdir(parents=True, exist_ok=True)
            table = merge_existing(dest / "part-0.parquet", table.drop_columns(["STATION"]), "ts_utc")
            pq.write_table(table, dest / "part-0.parquet")
            log.info("GHCNh %s %s: %d hourly rows kept", station, year, table.num_rows)
            record("ghcnh", station=station, year=year, rows_kept=table.num_rows)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--window", default="dev")
    a = p.parse_args()
    setup_logging()
    run(a.window)


if __name__ == "__main__":
    main()
