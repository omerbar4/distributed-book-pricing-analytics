VENV   := .venv
PY     := $(VENV)/bin/python
# PySpark needs a JVM (17 or 21). Use JAVA_HOME if already set; otherwise fall back
# to Homebrew's openjdk@21 when present. Scoped to make only -- no global changes.
JAVA_HOME ?= $(wildcard /opt/homebrew/opt/openjdk@21)
export JAVA_HOME

.PHONY: setup lint format test clean

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

test:  ## Run the test suite
	$(PY) -m pytest -q

clean:  ## Remove generated data and caches (keeps data/.gitkeep)
	find data -mindepth 1 ! -name .gitkeep -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache spark-warehouse metastore_db derby.log
