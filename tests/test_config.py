from datetime import date

import pytest
import yaml

from harbor_haze.common import ROOT, BBox, haversine_km, load_config, window_days


def test_config_windows_are_valid():
    cfg = load_config()
    for name in cfg["windows"]:
        days = window_days(cfg, name)
        assert days == sorted(set(days))
    full = window_days(cfg, "full")
    assert date(2021, 9, 1) in full and date(2024, 11, 30) in full


def test_window_days_rejects_reversed_span():
    cfg = {"windows": {"bad": [{"start": "2024-09-02", "end": "2024-09-01"}]}}
    with pytest.raises(ValueError):
        window_days(cfg, "bad")


def test_topics_file_lists_every_topic_the_code_uses():
    names = {t["name"] for t in yaml.safe_load(open(ROOT / "config/kafka/topics.yaml"))["topics"]}
    assert {
        "ais.positions",
        "ais.static",
        "airnow.observations",
        "weather.observations",
        "features.vessel_hourly",
    } <= names


def test_haversine_known_distance():
    # Long Beach Airport to LAX is about 26 km
    assert 24 < haversine_km(33.8117, -118.1472, 33.9382, -118.3866) < 28


def test_bbox_contains_port():
    b = BBox(33.35, 33.85, -118.55, -117.95)
    assert b.contains(33.74, -118.24)
    assert not b.contains(34.05, -118.24)
