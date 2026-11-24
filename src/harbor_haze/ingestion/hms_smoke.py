"""NOAA HMS smoke polygons -> one row per day: was the port under analysed smoke, and how dense?

Polygons are analysed daily from satellite imagery with a density class (Light / Medium / Heavy).
We record the densest polygon covering the port centre and the count of polygons covering it.
"""

from __future__ import annotations

import argparse
import io
import logging
import zipfile

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import shapefile
from shapely.geometry import Point, shape

from harbor_haze.common import DATA_DIR, LAKE, download, load_config, log, record, setup_logging, window_days
from harbor_haze.ingestion.ghcnh import merge_existing

OUT = LAKE / "raw" / "hms_smoke"
CACHE = DATA_DIR / "cache" / "hms"
DENSITY_RANK = {"Light": 1, "Medium": 2, "Heavy": 3}


logging.getLogger("shapefile").setLevel(logging.ERROR)


def smoke_at(zip_path, point: Point) -> tuple[int, int]:
    """(max density rank 0-3, number of polygons) covering point, from one daily shapefile zip."""
    with zipfile.ZipFile(zip_path) as z:
        names = {n.rsplit(".", 1)[-1].lower(): n for n in z.namelist()}
        reader = shapefile.Reader(
            shp=io.BytesIO(z.read(names["shp"])),
            shx=io.BytesIO(z.read(names["shx"])),
            dbf=io.BytesIO(z.read(names["dbf"])),
        )
        fields = [f[0] for f in reader.fields[1:]]
        best, n = 0, 0
        for sr in reader.iterShapeRecords():
            if not sr.shape.points:
                continue
            if shape(sr.shape.__geo_interface__).covers(point):
                n += 1
                density = dict(zip(fields, sr.record, strict=False)).get("Density", "")
                best = max(best, DENSITY_RANK.get(str(density).strip(), 1))
        return best, n


def run(window: str) -> None:
    cfg = load_config()
    url_t = cfg["sources"]["hms_smoke"]["url"]
    c = cfg["study_area"]["center"]
    point = Point(c["lon"], c["lat"])
    rows = {"date": [], "smoke_density": [], "smoke_polygons": [], "file_found": []}
    for d in window_days(cfg, window):
        url = url_t.format(year=d.year, month=d.month, day=d.day)
        path = CACHE / url.rsplit("/", 1)[-1]
        found = download(url, path) is not None
        density, n = smoke_at(path, point) if found else (None, None)
        for k, v in zip(rows, (d, density, n, found), strict=True):
            rows[k].append(v)
    table = pa.table(rows)
    for year in sorted({d.year for d in rows["date"]}):
        part = table.filter(pc.equal(pc.year(table["date"]), year))
        dest = OUT / f"year={year}"
        dest.mkdir(parents=True, exist_ok=True)
        pq.write_table(merge_existing(dest / "part-0.parquet", part, "date"), dest / "part-0.parquet")
    smoky = sum(1 for v in rows["smoke_density"] if v)
    log.info("HMS smoke: %d day(s), %d with smoke over the port", len(rows["date"]), smoky)
    record("hms_smoke", days=len(rows["date"]), smoky_days=smoky, missing=rows["file_found"].count(False))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--window", default="dev")
    a = p.parse_args()
    setup_logging()
    run(a.window)


if __name__ == "__main__":
    main()
