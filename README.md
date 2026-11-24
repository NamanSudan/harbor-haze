# harbor-haze

**Does ship traffic near the Port of Los Angeles / Long Beach help predict hourly air quality?**

We join vessel positions (AIS) to nearby air-quality monitors and test whether vessel activity
(how many ships, how fast, anchored or underway, what type) improves hourly PM2.5 and NO2
prediction beyond weather, road traffic and wildfire smoke. The same pipeline runs on live data
to drive a prototype early-warning alert based on AirNow AQI categories.

## Data

| Source | Mode | What we use |
|---|---|---|
| [NOAA MarineCadastre AIS](https://hub.marinecadastre.gov/pages/vesseltraffic) | historical | Every vessel position in US waters, one file per day (national), cut to San Pedro Bay |
| [AISStream](https://aisstream.io) | live | Vessel positions and static data over WebSocket |
| [EPA AQS hourly data](https://aqs.epa.gov/aqsweb/airdata/download_files.html) | historical | Hourly PM2.5, NO2 and wind at monitors within 30 km of the port |
| [AirNow API](https://docs.airnowapi.org/) | live | Current PM2.5, NO2 and ozone near the port |
| [NOAA GHCN-Hourly](https://www.ncei.noaa.gov/products/global-historical-climatology-network-hourly) | historical | Wind, temperature, humidity at Long Beach and LAX airports |
| [NWS API](https://www.weather.gov/documentation/services-web-api) | live | Latest observations at KLGB and KLAX |
| [NOAA HMS smoke](https://www.ospo.noaa.gov/products/land/hms.html) | historical | Daily smoke polygons (is the port under smoke?) |

The study area, time windows and every URL live in [`config/sources.yaml`](config/sources.yaml).
Two windows are compared on purpose: **Sep-Nov 2021**, when dozens of ships queued at anchor in
San Pedro Bay, and **Sep-Nov 2024**, after ships were held offshore. The AIS alone is about
370 MB per day compressed, so the full window reads well over 100 GB of national data with Spark
and keeps only the study area.

## Architecture

```
 historical (batch, Spark)                                live (streaming)
 ─────────────────────────                                ────────────────
 MarineCadastre AIS ─┐                      AISStream ──► ais.positions / ais.static ─┐
 EPA AQS hourly ─────┤                      AirNow ─────► airnow.observations ────────┤ Kafka
 GHCN-Hourly ────────┼─► data/lake/raw      NWS ────────► weather.observations ───────┘
 HMS smoke ──────────┘        │                                   │
                              ▼                                   ▼
               vessel_hourly (batch) ◄── same feature code ──► vessel_hourly (Spark Structured Streaming)
                              │                                   │
                              ▼                                   ▼
                       warehouse / model / API / UI   ◄──   features.vessel_hourly + data/lake/stream
```

Vessel features are computed by one function (`src/harbor_haze/features.py`) for both the
historical backfill and the live stream, so training and serving see the same features.

## Quick start

Requirements: Docker, [uv](https://docs.astral.sh/uv/), Java 17 (for Spark on the host), GNU make, Terraform (repo owner only).

```bash
make setup                 # .venv from uv.lock + git hooks
make env                   # creates .env; add AISSTREAM_API_KEY and AIRNOW_API_KEY
make check                 # lint + tests

make ingest                # historical sources for the small dev window (2 days)
make ingest WINDOW=full    # the full study windows (long; run in the background)
make lake-summary          # what landed, rows per table, GB downloaded

make up                    # Kafka + topics + console at http://localhost:8085
make live                  # + live producers + streaming jobs
make logs s=producer-ais   # follow one service
make down
```

All ingestion jobs are idempotent: rerunning a window rebuilds the same partitions, and AIS days
already in the lake are skipped unless `--force` is passed.

## Repository layout

```
config/            study area, windows, source URLs, Kafka topics
src/harbor_haze/
  ingestion/       historical loaders (AIS, EPA, GHCN-Hourly, HMS) and batch vessel features
  streaming/       live producers, topic creation, Spark Structured Streaming jobs
  features.py      vessel features shared by batch and streaming
infra/
  docker/          compose stack and the pipeline image
  github/          this repository's settings, collaborators, branch protection and labels (Terraform)
tests/             unit tests (no network, Spark in local mode)
```

## How we work

- **`dev`** is where work is integrated and tested; **`main`** holds the deliverable.
  Branch off `dev` (`feat/...`, `fix/...`), open a PR into `dev`, and get one teammate's approval.
  PRs into `dev` are squash-merged. When `dev` is ready, a PR from `dev` into `main` promotes it with a
  merge commit, so the two branches keep one history.
- Nobody commits or pushes straight to `dev` or `main`. CI (`branch-policy`) fails PRs that break the
  flow and flags any commit on `dev`/`main` that did not come from a merged PR; the git hooks from
  `make setup` stop it locally. Every PR also needs green lint, tests, secret scan, image build and
  Terraform validate.
- One issue per task, labelled by lane (`lane: ingestion`, `lane: warehouse`, `lane: modeling`, ...).
- Keys live only in `.env` (gitignored). `make setup` installs pre-commit hooks that block
  private keys and secrets.
