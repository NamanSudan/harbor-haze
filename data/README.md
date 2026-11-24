# data/

Nothing here is committed except this file. Everything is rebuilt by `make ingest`.

| Path | Contents |
|---|---|
| `lake/raw/ais/date=YYYY-MM-DD/` | Vessel positions in the study bbox (canonical schema in `src/harbor_haze/features.py`) |
| `lake/raw/epa_hourly/param=<pm25\|pm25_nonref\|no2\|wind>/date=YYYY-MM-DD/` | Hourly monitor readings within 30 km of the port |
| `lake/raw/ghcnh/station=<id>/year=YYYY/` | Hourly weather at the configured stations |
| `lake/raw/hms_smoke/year=YYYY/` | One row per day: smoke density over the port (0-3) |
| `lake/raw/vessel_registry/` | Latest type / size per MMSI |
| `lake/features/vessel_hourly/date=YYYY-MM-DD/` | Vessel activity per 0.02 deg cell and hour (batch) |
| `lake/stream/bronze/topic=<topic>/date=YYYY-MM-DD/` | Every live Kafka message, as received |
| `lake/stream/vessel_hourly/` | Vessel activity per cell and hour (streaming) |
| `cache/` | Re-usable downloads (EPA, GHCN-Hourly, HMS). AIS national files are deleted after each batch. |
| `manifest.jsonl` | One line per load: bytes downloaded, rows read, rows kept |
