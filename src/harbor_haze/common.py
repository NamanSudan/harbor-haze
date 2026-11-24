"""Shared helpers: config, paths, dates, downloads and the ingestion manifest."""

from __future__ import annotations

import json
import logging
import math
import os
import shutil
import time
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import requests
import yaml

ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = Path(os.environ.get("HARBOR_CONFIG", ROOT / "config" / "sources.yaml"))
DATA_DIR = Path(os.environ.get("HARBOR_DATA_DIR", ROOT / "data"))
LAKE = DATA_DIR / "lake"
STAGING = DATA_DIR / "staging"
MANIFEST = DATA_DIR / "manifest.jsonl"

USER_AGENT = "harbor-haze/0.1 (university course project)"

log = logging.getLogger("harbor_haze")


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def load_config(path: Path = CONFIG_PATH) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)


@dataclass(frozen=True)
class BBox:
    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float

    def contains(self, lat: float, lon: float) -> bool:
        return self.min_lat <= lat <= self.max_lat and self.min_lon <= lon <= self.max_lon


def study_bbox(cfg: dict) -> BBox:
    return BBox(**cfg["study_area"]["bbox"])


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def window_days(cfg: dict, window: str) -> list[date]:
    """All days of a named window from the config, sorted and de-duplicated."""
    days: set[date] = set()
    for span in cfg["windows"][window]:
        start, end = _as_date(span["start"]), _as_date(span["end"])
        if end < start:
            raise ValueError(f"window {window}: end {end} is before start {start}")
        days.update(start + timedelta(n) for n in range((end - start).days + 1))
    return sorted(days)


def _as_date(v) -> date:
    return v if isinstance(v, date) else date.fromisoformat(str(v))


def batched(items: list, size: int) -> Iterator[list]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def download(url: str, dest: Path, retries: int = 4, chunk: int = 1 << 20) -> int | None:
    """Stream url to dest atomically. Returns bytes written, or None when the server says 404.

    Skips the download when dest already exists, so reruns are cheap.
    """
    if dest.exists():
        return dest.stat().st_size
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=(15, 120), headers={"User-Agent": USER_AGENT}) as r:
                if r.status_code == 404:
                    return None
                r.raise_for_status()
                with open(tmp, "wb") as f:
                    for block in r.iter_content(chunk):
                        f.write(block)
            tmp.replace(dest)
            return dest.stat().st_size
        except (requests.RequestException, OSError) as e:
            wait = 2**attempt
            log.warning("download %s failed (%s), attempt %d/%d, retrying in %ss", url, e, attempt, retries, wait)
            time.sleep(wait)
    raise RuntimeError(f"could not download {url}")


def record(source: str, **fields) -> None:
    """Append one line to the ingestion manifest (what was read, how much, what was kept)."""
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    entry = {"source": source, "at": datetime.now(UTC).isoformat(timespec="seconds"), **fields}
    with open(MANIFEST, "a") as f:
        f.write(json.dumps(entry, default=str) + "\n")


def clear_dir(path: Path) -> None:
    shutil.rmtree(path, ignore_errors=True)
    path.mkdir(parents=True, exist_ok=True)
