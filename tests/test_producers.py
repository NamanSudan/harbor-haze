from harbor_haze.common import load_config
from harbor_haze.streaming.producers import (
    TOPIC_AIS,
    TOPIC_AIS_STATIC,
    Seen,
    airnow_bbox,
    aqs_site_id,
    normalise_airnow,
    normalise_ais,
    normalise_nws,
)

POSITION = {
    "MessageType": "PositionReport",
    "MetaData": {
        "MMSI": 367000001,
        "ShipName": "TEST",
        "latitude": 33.74,
        "longitude": -118.24,
        "time_utc": "2026-11-23 19:51:23.402591632 +0000 UTC",
    },
    "Message": {
        "PositionReport": {
            "UserID": 367000001,
            "Latitude": 33.74,
            "Longitude": -118.24,
            "Sog": 0.1,
            "Cog": 12.0,
            "TrueHeading": 511,
            "NavigationalStatus": 5,
        }
    },
}
STATIC = {
    "MessageType": "ShipStaticData",
    "MetaData": {"MMSI": 367000001, "time_utc": "2026-11-23 19:51:23.4 +0000 UTC"},
    "Message": {
        "ShipStaticData": {
            "Type": 71,
            "Name": "TEST SHIP   ",
            "Dimension": {"A": 200, "B": 50},
            "MaximumStaticDraught": 11.2,
            "ImoNumber": 9000001,
        }
    },
}


def test_position_report_is_flattened():
    topic, rec = normalise_ais(POSITION)
    assert topic == TOPIC_AIS
    assert rec["mmsi"] == 367000001 and rec["ts"] == "2026-11-23T19:51:23Z"
    assert (rec["lat"], rec["lon"], rec["status"]) == (33.74, -118.24, 5)


def test_static_data_goes_to_its_own_topic():
    topic, rec = normalise_ais(STATIC)
    assert topic == TOPIC_AIS_STATIC
    assert rec["vessel_type"] == 71 and rec["length"] == 250 and rec["vessel_name"] == "TEST SHIP"


def test_subscription_confirmation_is_skipped():
    assert normalise_ais({"MessageType": "SubscriptionConfirmation"}) is None


def test_airnow_site_ids_match_epa_format():
    assert aqs_site_id("840060374004") == "06-037-4004"
    assert aqs_site_id("060371302") == "06-037-1302"
    assert aqs_site_id(None) is None


def test_unknown_message_types_are_dropped():
    assert normalise_ais({"MessageType": "Interrogation", "MetaData": {"MMSI": 1}, "Message": {}}) is None


def test_airnow_prefers_raw_concentration():
    row = {
        "Latitude": 33.8,
        "Longitude": -118.2,
        "UTC": "2026-11-23T19:00",
        "Parameter": "PM2.5",
        "Unit": "UG/M3",
        "Value": 10.0,
        "RawConcentration": 12.5,
        "AQI": 52,
        "Category": 2,
        "SiteName": "Long Beach",
        "FullAQSCode": "840060374004",
    }
    rec = normalise_airnow(row)
    assert rec["value"] == 12.5 and rec["ts"] == "2026-11-23T19:00:00Z" and rec["site_id"] == "06-037-4004"
    row["RawConcentration"] = -999  # missing raw value: never fall back to the NowCast "Value"
    assert normalise_airnow(row)["value"] is None


def test_nws_reads_nested_values():
    props = {
        "timestamp": "2026-11-23T19:53:00+00:00",
        "temperature": {"value": 18.3},
        "windSpeed": {"value": 11.2},
        "windDirection": {"value": 270},
        "dewpoint": {"value": None},
    }
    rec = normalise_nws("KLGB", props)
    assert rec["temperature_c"] == 18.3 and rec["wind_direction_deg"] == 270 and rec["dew_point_c"] is None


def test_airnow_bbox_covers_the_monitor_radius():
    from harbor_haze.common import haversine_km

    cfg = load_config()
    c, km = cfg["study_area"]["center"], cfg["study_area"]["monitor_radius_km"]
    min_lon, min_lat, max_lon, max_lat = map(float, airnow_bbox(cfg).split(","))
    assert min_lon < c["lon"] < max_lon and min_lat < c["lat"] < max_lat
    # east-west half-width must reach the full radius (km -> degrees needs cos(lat))
    assert haversine_km(c["lat"], c["lon"], c["lat"], max_lon) >= km - 0.5


def test_seen_is_bounded():
    s = Seen(size=2)
    assert s.add("a") and s.add("b") and not s.add("a")
    s.add("c")  # evicts "a"
    assert s.add("a")
