"""Live producers: AISStream (WebSocket), AirNow and NWS (polling) -> Kafka.

    python -m harbor_haze.streaming.producers aisstream
    python -m harbor_haze.streaming.producers airnow
    python -m harbor_haze.streaming.producers nws
    ... --dry-run   prints messages instead of sending them (no Kafka needed)

Every message is normalised to flat JSON before it is produced, so consumers never
depend on a vendor's payload shape. Keys keep one vessel / site / station per partition.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import os
import signal
import time
from collections import deque
from datetime import UTC, datetime, timedelta

import requests

from harbor_haze.common import USER_AGENT, load_config, log, setup_logging, study_bbox

TOPIC_AIS = "ais.positions"
TOPIC_AIS_STATIC = "ais.static"
TOPIC_AIRNOW = "airnow.observations"
TOPIC_WEATHER = "weather.observations"


class Sink:
    """Kafka producer, or stdout when dry_run."""

    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.sent = 0
        if not dry_run:
            from confluent_kafka import Producer

            self.p = Producer(
                {
                    "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094"),
                    "client.id": "harbor-haze-producer",
                    "enable.idempotence": True,
                    "linger.ms": 50,
                    "compression.type": "zstd",
                }
            )

    def send(self, topic: str, key: str, value: dict) -> None:
        self.sent += 1
        if self.dry_run:
            print(topic, key, json.dumps(value))
            return
        self.p.produce(topic, key=key.encode(), value=json.dumps(value).encode(), on_delivery=_on_delivery)
        self.p.poll(0)

    def flush(self) -> None:
        if not self.dry_run:
            self.p.flush(10)


def _on_delivery(err, msg) -> None:
    if err is not None:
        log.error("delivery failed for %s: %s", msg.topic(), err)


class Seen:
    """Small bounded memory of keys already sent (polling APIs return overlapping data)."""

    def __init__(self, size: int = 50_000):
        self.keys: set = set()
        self.order: deque = deque()
        self.size = size

    def add(self, key) -> bool:
        if key in self.keys:
            return False
        self.keys.add(key)
        self.order.append(key)
        if len(self.order) > self.size:
            self.keys.discard(self.order.popleft())
        return True


# --- AISStream -------------------------------------------------------------------------


def ais_time(meta_time: str) -> str:
    """'2026-11-23 19:51:23.402591632 +0000 UTC' -> '2026-11-23T19:51:23Z'."""
    return meta_time[:19].replace(" ", "T") + "Z"


def normalise_ais(msg: dict) -> tuple[str, dict] | None:
    """Map an AISStream message to (topic, flat record), or None for types we ignore."""
    kind = msg.get("MessageType")
    meta = msg.get("MetaData", {})
    body = msg.get("Message", {}).get(kind, {})
    mmsi = meta.get("MMSI") or body.get("UserID")
    if not mmsi:
        return None
    if kind in ("PositionReport", "StandardClassBPositionReport"):
        return TOPIC_AIS, {
            "mmsi": int(mmsi),
            "ts": ais_time(meta["time_utc"]),
            "lat": body.get("Latitude", meta.get("latitude")),
            "lon": body.get("Longitude", meta.get("longitude")),
            "sog": body.get("Sog"),
            "cog": body.get("Cog"),
            "heading": body.get("TrueHeading"),
            "status": body.get("NavigationalStatus"),
            "source": "aisstream",
        }
    if kind == "ShipStaticData":
        dim = body.get("Dimension") or {}
        return TOPIC_AIS_STATIC, {
            "mmsi": int(mmsi),
            "ts": ais_time(meta["time_utc"]),
            "vessel_type": body.get("Type"),
            "vessel_name": (body.get("Name") or meta.get("ShipName") or "").strip(),
            "length": (dim.get("A") or 0) + (dim.get("B") or 0) or None,
            "draft": body.get("MaximumStaticDraught"),
            "imo": body.get("ImoNumber"),
        }
    return None


async def run_aisstream(cfg: dict, sink: Sink) -> None:
    import websockets

    key = os.environ.get("AISSTREAM_API_KEY")
    if not key:
        raise SystemExit("AISSTREAM_API_KEY is not set (see .env.example)")
    b = study_bbox(cfg)
    sub = {
        "APIKey": key,
        "BoundingBoxes": [[[b.min_lat, b.min_lon], [b.max_lat, b.max_lon]]],
        "FilterMessageTypes": cfg["live"]["aisstream"]["message_types"],
    }
    backoff = 1
    while True:
        try:
            # the AISStream handshake can take 20+ s; the library default (10 s) times out
            async with websockets.connect(cfg["live"]["aisstream"]["url"], ping_interval=20, open_timeout=90) as ws:
                await ws.send(json.dumps(sub))  # must arrive within 3 s of connecting
                log.info("AISStream connected; subscribed to the study bbox")
                backoff = 1
                last = time.monotonic()
                async for raw in ws:  # frames arrive as bytes; the first one is a SubscriptionConfirmation
                    out = normalise_ais(json.loads(raw))
                    if out:
                        topic, rec = out
                        sink.send(topic, str(rec["mmsi"]), rec)
                    if time.monotonic() - last > 60:
                        log.info("AISStream: %d messages sent so far", sink.sent)
                        last = time.monotonic()
        except (OSError, TimeoutError, websockets.WebSocketException) as e:
            log.warning("AISStream disconnected (%s); reconnecting in %ss", e, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# --- AirNow ----------------------------------------------------------------------------


def airnow_bbox(cfg: dict) -> str:
    c, km = cfg["study_area"]["center"], cfg["study_area"]["monitor_radius_km"]
    dlat = km / 111.0
    dlon = km / (111.0 * math.cos(math.radians(c["lat"])))  # a degree of longitude is shorter at 33.7 N
    return f"{c['lon'] - dlon:.3f},{c['lat'] - dlat:.3f},{c['lon'] + dlon:.3f},{c['lat'] + dlat:.3f}"


def aqs_site_id(code: str | None) -> str | None:
    """AirNow's '840060374009' / '060374009' -> EPA AQS style '06-037-4009' (joins with the EPA history)."""
    if not code:
        return None
    code = str(code)
    if len(code) == 12 and code.startswith("840"):
        code = code[3:]
    return f"{code[:2]}-{code[2:5]}-{code[5:]}" if len(code) == 9 and code.isdigit() else code


def normalise_airnow(row: dict) -> dict:
    return {
        "site_id": aqs_site_id(row.get("FullAQSCode") or row.get("IntlAQSCode")),
        "site_name": row.get("SiteName"),
        "lat": row.get("Latitude"),
        "lon": row.get("Longitude"),
        "ts": row["UTC"] + ":00Z",
        "parameter": row.get("Parameter"),
        # raw hourly concentration only; AirNow's "Value" for PM2.5 is a multi-hour NowCast (not like EPA hourly)
        "value": row.get("RawConcentration") if row.get("RawConcentration") not in (None, -999) else None,
        "unit": row.get("Unit"),
        "aqi": row.get("AQI"),
        "category": row.get("Category"),
        "source": "airnow",
    }


def run_airnow(cfg: dict, sink: Sink, once: bool) -> None:
    key = os.environ.get("AIRNOW_API_KEY")
    if not key:
        raise SystemExit("AIRNOW_API_KEY is not set (see .env.example)")
    a = cfg["live"]["airnow"]
    seen = Seen()
    while True:
        now = datetime.now(UTC)
        params = {
            "startDate": (now - timedelta(hours=3)).strftime("%Y-%m-%dT%H"),
            "endDate": now.strftime("%Y-%m-%dT%H"),
            "parameters": ",".join(a["parameters"]),
            "BBOX": airnow_bbox(cfg),
            "dataType": "B",
            "format": "application/json",
            "verbose": 1,
            "monitorType": 0,
            "includerawconcentrations": 1,
            "API_KEY": key,
        }
        try:
            r = requests.get(a["url"], params=params, timeout=30, headers={"User-Agent": USER_AGENT})
            r.raise_for_status()
            new = 0
            for row in r.json():
                try:
                    rec = normalise_airnow(row)
                except (KeyError, TypeError):
                    log.warning("AirNow: skipped a malformed row")
                    continue
                if seen.add((rec["site_id"], rec["parameter"], rec["ts"])):
                    sink.send(TOPIC_AIRNOW, str(rec["site_id"]), rec)
                    new += 1
            sink.flush()
            log.info("AirNow: %d new observations", new)
        except requests.RequestException as e:
            log.warning("AirNow poll failed: %s", e.__class__.__name__)
        if once:
            return
        time.sleep(a["poll_seconds"])


# --- NWS -------------------------------------------------------------------------------


def _v(props: dict, name: str):
    return (props.get(name) or {}).get("value")


def normalise_nws(station: str, props: dict) -> dict:
    return {
        "station": station,
        "ts": props["timestamp"],
        "temperature_c": _v(props, "temperature"),
        "dew_point_c": _v(props, "dewpoint"),
        "relative_humidity": _v(props, "relativeHumidity"),
        "wind_direction_deg": _v(props, "windDirection"),
        "wind_speed_kmh": _v(props, "windSpeed"),
        "pressure_pa": _v(props, "barometricPressure"),
        "visibility_m": _v(props, "visibility"),
        "source": "nws",
    }


def run_nws(cfg: dict, sink: Sink, once: bool) -> None:
    n = cfg["live"]["nws"]
    seen = Seen(1000)
    headers = {"User-Agent": USER_AGENT, "Accept": "application/geo+json"}
    while True:
        for station in n["stations"]:
            try:
                r = requests.get(n["url"].format(station=station), timeout=30, headers=headers)
                r.raise_for_status()
                rec = normalise_nws(station, r.json()["properties"])
                if seen.add((station, rec["ts"])):
                    sink.send(TOPIC_WEATHER, station, rec)
            except (requests.RequestException, KeyError) as e:
                log.warning("NWS %s poll failed: %s", station, e.__class__.__name__)
        sink.flush()
        log.info("NWS: %d observations sent so far", sink.sent)
        if once:
            return
        time.sleep(n["poll_seconds"])


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("source", choices=["aisstream", "airnow", "nws"])
    p.add_argument("--dry-run", action="store_true", help="print instead of producing to Kafka")
    p.add_argument("--once", action="store_true", help="poll once and exit (airnow, nws)")
    a = p.parse_args()
    setup_logging()
    cfg = load_config()
    sink = Sink(a.dry_run)
    signal.signal(signal.SIGTERM, lambda *_: (sink.flush(), exit(0)))
    try:
        if a.source == "aisstream":
            asyncio.run(run_aisstream(cfg, sink))
        elif a.source == "airnow":
            run_airnow(cfg, sink, a.once)
        else:
            run_nws(cfg, sink, a.once)
    except KeyboardInterrupt:
        pass
    finally:
        sink.flush()


if __name__ == "__main__":
    main()
