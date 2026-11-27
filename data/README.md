# data/

Nothing here is committed except this file. Everything is rebuilt by `make ingest`.

| Path | Contents |
|---|---|
| `lake/raw/ais/date=YYYY-MM-DD/` | Vessel positions in the study bbox (canonical schema in `src/harbor_haze/features.py`) |
| `lake/raw/epa_hourly/param=<pm25\|pm25_nonref\|no2\|wind>/date=YYYY-MM-DD/` | Hourly monitor readings within 30 km of the port |
| `lake/raw/ghcnh/station=<id>/year=YYYY/` | Hourly weather at the configured stations (can be more than one row per hour) |
| `lake/raw/hms_smoke/year=YYYY/` | One row per day: smoke density over the port (0-3) |
| `lake/raw/vessel_registry/` | Latest type / size per MMSI |
| `lake/features/vessel_hourly/date=YYYY-MM-DD/` | Vessel activity per 0.02 deg cell and hour (batch) |
| `lake/features/bay_hourly/` | Vessel activity for the whole study area, one row per hour, one Parquet file (batch) |
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
| `n_cargo_tanker` | Distinct cargo or tanker vessels (AIS ship type 70-89) |
| `n_cargo_tanker_stationary` | Cargo or tanker vessels with at least one stationary ping |
| `n_cargo_tanker_anchored` | Cargo or tanker vessels with at least one ping with status 1 (at anchor). Status 5 (moored at a berth) is not included: many ships at a berth run on shore power |

Counting rules:

- A vessel that changes state inside the hour (arrives and anchors) is in both `n_stationary` and
  `n_underway`, so `n_stationary + n_underway >= n_vessels`. Exclusive counts:
  stationary only = `n_vessels - n_underway`, underway only = `n_vessels - n_stationary`,
  both = `n_stationary + n_underway - n_vessels`.
- Counts are distinct vessels per cell-hour. **Don't sum them across cells**: a moving vessel is
  counted in every cell it crosses (summing cargo over cells overcounts by about 1.4x in 2021 and
  2x in 2024). For bay-wide counts use `features/bay_hourly`.
- Counts use `approx_count_distinct` (it works in streaming too) with a 2% relative error setting
  (`CELL_RSD` in `features.py`). Most cell-hours hold one or two vessels and are exact. Against an
  exact recount of the full window, `n_vessels` is off in 0.4% of cell-hours (by up to 3) and the
  cargo and tanker counts in under 0.02% (by 1).

## `features/bay_hourly` columns

One row per hour for the whole study area: every column of `vessel_hourly` except `cell_lat` and
`cell_lon`, with the same counting rules, plus `low_coverage`. Each vessel is counted once per hour,
so there is no cross-cell overcount. Batch only. The counts are approximate with a tighter setting
(`BAY_RSD`, 1% relative error), which at 300+ vessels an hour can be a few vessels either way.

| Column | Meaning |
|---|---|
| `hour_utc`, `n_*`, `mean_sog`, `mean_sog_underway` | As in `vessel_hourly`, for the whole bay |
| `low_coverage` | `n_pings` is below `features.low_coverage_pings` in `config/sources.yaml` (default 2,000, about the 1st percentile of hourly pings) |

- Every hour of every day of each window in `config/sources.yaml` that has at least one day in
  `raw/ais` has a row, so a day that failed to load shows up as 24 empty hours instead of vanishing.
  Hours with no pings have `n_pings = 0`, zero counts and null speeds. They are AIS outages, not
  empty water: filter on `low_coverage` before using the counts, never read them as "no ships".

## `raw/ghcnh` wind fields

- `wind_direction`: degrees, or null. The source writes 999 for calm (measurement code C) and
  variable (code V) wind; that is stored as null, not as a bearing.
- `wind_speed`: m/s. `wind_calm` is true when `wind_speed` is 0 (null when speed is missing).
- A station can report more than once in an hour (routine plus special observations), so
  aggregate to the hour before joining on `hour_utc`.
