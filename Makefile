# Single entry point. `make help` lists every target.
SHELL := /bin/bash
.DEFAULT_GOAL := help

PY      := .venv/bin/python
WINDOW  ?= dev
COMPOSE := docker compose -f infra/docker/compose.yaml --env-file .env
GH_TF   := terraform -chdir=infra/github

ifneq (,$(wildcard .env))
include .env
export
endif

.PHONY: help
help: ## list targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n",$$1,$$2}'

# --- setup -----------------------------------------------------------------------------
.PHONY: setup env
setup: ## create .venv from the lock file and install git hooks
	uv sync --frozen
	.venv/bin/pre-commit install

env: ## create .env from .env.example (never overwrites)
	@test -f .env && echo ".env exists, leaving it alone" || (cp .env.example .env && echo "created .env; add your API keys")

# --- historical ingestion (batch) ----------------------------------------------------------
.PHONY: ingest ingest-ais ingest-epa ingest-weather ingest-smoke vessel-features lake-summary
ingest: ingest-epa ingest-weather ingest-smoke ingest-ais vessel-features ## every historical source for WINDOW (dev|full)

ingest-ais: ## NOAA MarineCadastre AIS -> lake/raw/ais (Spark)
	$(PY) -m harbor_haze.ingestion.ais_history --window $(WINDOW)

ingest-epa: ## EPA AQS hourly PM2.5 / NO2 / wind -> lake/raw/epa_hourly (Spark)
	$(PY) -m harbor_haze.ingestion.epa_hourly --window $(WINDOW)

ingest-weather: ## NOAA GHCN-Hourly -> lake/raw/ghcnh
	$(PY) -m harbor_haze.ingestion.ghcnh --window $(WINDOW)

ingest-smoke: ## NOAA HMS smoke polygons -> lake/raw/hms_smoke
	$(PY) -m harbor_haze.ingestion.hms_smoke --window $(WINDOW)

vessel-features: ## lake/raw/ais -> lake/features/vessel_hourly + bay_hourly + vessel registry (Spark)
	$(PY) -m harbor_haze.ingestion.vessel_batch

lake-summary: ## row counts per lake table + bytes downloaded
	$(PY) -m harbor_haze.lake_summary

# --- warehouse (Postgres star schema) ------------------------------------------------------
.PHONY: warehouse-up warehouse-load warehouse-psql
warehouse-up: ## start Postgres and wait until healthy
	$(COMPOSE) up -d --wait warehouse-pg

warehouse-load: warehouse-up ## lake -> Postgres star schema (drops and rebuilds the tables)
	$(PY) -m harbor_haze.warehouse.build --lake data/lake

warehouse-psql: ## open psql in the warehouse database
	docker exec -it harbor-warehouse-pg psql -U $${WAREHOUSE_PG_USER:-harbor} -d $${WAREHOUSE_PG_DB:-harbor_haze}

# --- streaming stack (Docker) --------------------------------------------------------------
.PHONY: up live down ps logs
up: ## Kafka + topics + console (http://localhost:8085)
	$(COMPOSE) up -d --build kafka kafka-init console

live: ## up + live producers + Spark streaming jobs (needs API keys in .env)
	$(COMPOSE) --profile live up -d --build

down: ## stop everything (keeps the Kafka volume)
	$(COMPOSE) --profile live down

ps: ## container status
	$(COMPOSE) --profile live ps

logs: ## follow logs, e.g. make logs s=producer-ais
	$(COMPOSE) --profile live logs -f --tail=100 $(s)

# --- quality gates -------------------------------------------------------------------------
.PHONY: lint test check
lint: ## ruff + terraform fmt
	.venv/bin/ruff check src tests
	.venv/bin/ruff format --check src tests
	@if command -v terraform >/dev/null; then terraform fmt -check -recursive infra; \
	else echo "terraform not found, skipped terraform fmt (CI runs it)"; fi

test: ## unit tests (no network, no Kafka)
	$(PY) -m pytest -q

check: lint test ## every local gate; run before opening a PR

# --- GitHub as code (repo owner only) ------------------------------------------------------
.PHONY: gh-init gh-import gh-plan gh-apply
gh-init:
	$(GH_TF) init -input=false

gh-import: gh-init ## adopt the existing repo into Terraform state (once)
	GITHUB_TOKEN=$$(gh auth token) $(GH_TF) import github_repository.repo $$(awk -F'"' '/^repo_name/{print $$2}' infra/github/terraform.tfvars)

gh-plan: gh-init ## preview repo settings, collaborators, branch protection, labels
	GITHUB_TOKEN=$$(gh auth token) $(GH_TF) plan -input=false -out=tfplan

gh-apply: ## apply the reviewed plan
	GITHUB_TOKEN=$$(gh auth token) $(GH_TF) apply -input=false tfplan
