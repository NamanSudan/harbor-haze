"""Print what is in the lake: rows per table and bytes downloaded per source."""

from __future__ import annotations

import json
from collections import defaultdict

import pyarrow.dataset as ds

from harbor_haze.common import LAKE, MANIFEST


def main() -> None:
    print(f"{'table':40} {'rows':>14} {'files':>6}")
    for layer in ("raw", "features", "stream"):
        base = LAKE / layer
        if not base.exists():
            continue
        for table in sorted(p for p in base.iterdir() if p.is_dir()):
            try:
                d = ds.dataset(table, format="parquet", partitioning="hive")
                rows, files = d.count_rows(), len(d.files)
            except Exception as e:  # a table mid-write or an unreadable file
                print(f"{layer}/{table.name:34} unreadable ({e.__class__.__name__})")
                continue
            print(f"{layer + '/' + table.name:40} {rows:>14,} {files:>6}")
    if MANIFEST.exists():
        downloaded, read = defaultdict(int), defaultdict(int)
        for line in MANIFEST.read_text().splitlines():
            e = json.loads(line)
            downloaded[e["source"]] += e.get("bytes_downloaded", 0) or 0
            read[e["source"]] += e.get("rows_in", 0) or 0
        print(f"\n{'source':20} {'downloaded':>12} {'rows read':>16}")
        for s in sorted(downloaded):
            print(f"{s:20} {downloaded[s] / 1e9:>10.2f} GB {read[s]:>16,}")


if __name__ == "__main__":
    main()
