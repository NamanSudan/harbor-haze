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

## `features/vessel_hourly` columns

One row per hour and 0.02 deg grid cell (cell named by its south-west corner).

| Column | Meaning |
|---|---|
| `hour_utc` | Start of the hour, UTC |
| `cell_lat`, `cell_lon` | South-west corner of the cell |
| `n_pings` | Position reports in the cell-hour |
| `n_vessels` | Distinct vessels (MMSI) |
| `n_stationary` | Vessels with at least one stationary ping (status 1 at anchor or 5 moored, or speed under 0.5 kn) |
| `n_underway` | Vessels with at least one ping that is not stationary |
| `n_cargo`, `n_tanker`, `n_passenger`, `n_tug` | Distinct vessels by AIS ship type |
| `mean_sog` | Mean speed over ground of all pings, knots |
| `mean_sog_underway` | Mean speed of the non-stationary pings, knots |

Counting rules:

- A vessel that changes state inside the hour (arrives and anchors) is in both `n_stationary` and
  `n_underway`, so `n_stationary + n_underway >= n_vessels`. Exclusive counts:
  stationary only = `n_vessels - n_underway`, underway only = `n_vessels - n_stationary`,
  both = `n_stationary + n_underway - n_vessels`.
- Counts are distinct vessels per cell-hour. Don't sum them across cells: a moving vessel is
  counted in every cell it crosses.
- Counts use `approx_count_distinct` (it works in streaming too), so they can be off by one.
