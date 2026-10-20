VENV   := .venv
PY     := $(VENV)/bin/python
DATE   ?= $(shell date -u +%F)
# PySpark needs a JVM (17 or 21). Use JAVA_HOME if already set; otherwise fall back
# to Homebrew's openjdk@21 when present. Scoped to make only -- no global changes.
JAVA_HOME ?= $(wildcard /opt/homebrew/opt/openjdk@21)
export JAVA_HOME
# spark-submit's wrapper finds Spark via $PYSPARK_PYTHON (else `python3` on PATH).
export PYSPARK_PYTHON := $(CURDIR)/$(PY)

COMPOSE := docker compose

.PHONY: help setup lint format test up down reset-db psql demo demo-local \
        ingest generate pipeline load report clean

help:  ## List targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

# --- Docker (recruiter path: only Docker required) -----------------------------
demo:  ## One command: start PostgreSQL, build image, run the full pipeline for DATE
	PROCESSING_DATE=$(DATE) $(COMPOSE) run --rm --build pipeline

up:  ## Start PostgreSQL and wait until healthy
	$(COMPOSE) up -d --wait postgres

down:  ## Stop containers (keeps the database volume)
	$(COMPOSE) down

reset-db:  ## Stop containers AND delete the database volume
	$(COMPOSE) down -v

psql:  ## Open psql inside the PostgreSQL container
	$(COMPOSE) exec postgres sh -c 'psql -U "$$POSTGRES_USER" -d "$$POSTGRES_DB"'

# --- Local development (.venv + local JVM) -----------------------------------------
setup:  ## Create .venv and install the project with dev + api extras
	python3.12 -m venv $(VENV)
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e ".[dev,api]"

lint:  ## Static checks (no changes)
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .

format:  ## Auto-fix lint issues and format code
	$(PY) -m ruff check --fix .
	$(PY) -m ruff format .

test:  ## Run the test suite (PostgreSQL tests run when `make up` is active)
	$(PY) -m pytest -q

demo-local:  ## Full pipeline on the host against the Compose PostgreSQL
	$(MAKE) up
	PATH="$(CURDIR)/$(VENV)/bin:$$PATH" PROCESSING_DATE=$(DATE) bash scripts/run_demo.sh

ingest:  ## Fetch all default subjects for DATE
	$(PY) -m src.ingestion.fetch_books --all-subjects --ingestion-date $(DATE)

generate:  ## Generate simulated observations for DATE
	$(PY) -m src.generators.generate_observations --processing-date $(DATE)

pipeline:  ## Run the Spark job for DATE
	$(VENV)/bin/spark-submit src/jobs/book_pricing_pipeline.py --processing-date $(DATE)

load:  ## Load DATE into PostgreSQL
	$(PY) -m src.loaders.postgres_loader --processing-date $(DATE)

report:  ## Print the console report for DATE
	$(PY) -m src.loaders.report --processing-date $(DATE)

clean:  ## Remove generated data and caches (keeps data/.gitkeep)
	find data -mindepth 1 ! -name .gitkeep -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache spark-warehouse metastore_db derby.log
