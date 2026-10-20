# Distributed Book Pricing Analytics Pipeline

> **Status: in progress.** Only the project scaffold exists so far. Sections below
> describe the target design; anything not yet implemented is marked as planned.
> No dataset sizes or results are reported until the pipeline has actually run.

A PySpark batch pipeline for used-book market analytics, grounded in my own
book-resale work (BooksAreReborn / TechBaR). It will:

1. Ingest book metadata from the public **Open Library** REST API (planned).
2. Land untouched API responses as immutable raw JSON (planned).
3. Generate **clearly labeled, simulated** daily price/inventory observations from
   a fixed random seed. Open Library does not publish used-book prices (planned).
4. Clean, validate, deduplicate, join and aggregate with **PySpark DataFrames**
   through bronze → silver → gold layers (planned).
5. Write partitioned **Parquet** outputs and load daily pricing recommendations and
   data-quality results into **PostgreSQL**, idempotently per processing date (planned).

## How Spark runs here (honest local mode)

Spark runs in **`local[*]` mode**: one JVM on your machine, using every CPU core as
an executor thread. There is no standalone master/worker cluster, because on one
laptop that adds moving parts and no real parallelism.

The job code is ordinary Spark DataFrame code. The same job runs on a real cluster
(YARN, Kubernetes, Databricks, EMR) by changing `--master`/`SPARK_MASTER`, with no
code changes. The local datasets are intentionally small. This project shows correct
distributed-processing patterns (explicit schemas, window-based dedup, partitioned
output, incremental processing by date), not large-scale volume.

## Requirements

- Python 3.11 or 3.12 (PySpark 4.0 does not support newer versions yet)
- Java 17 or 21. The Makefile uses `JAVA_HOME`, or falls back to Homebrew `openjdk@21`
- Docker (planned: PostgreSQL plus a one-command containerized pipeline run)

## Quick start (current scaffold)

```bash
make setup   # creates .venv and installs the project from pyproject.toml
make test    # pytest: local Spark session smoke test
make lint    # ruff check + format check
```

## Project layout

```
src/common/      config (env vars), JSON logging, SparkSession builder
src/ingestion/   Open Library client + raw JSON landing          (planned)
src/generators/  seeded demo price/inventory observations       (planned)
src/transforms/  bronze / silver / gold / quality (PySpark)      (planned)
src/jobs/        spark-submit entry point                        (planned)
src/loaders/     idempotent PostgreSQL loader                    (planned)
src/api/         optional read-only FastAPI service              (planned)
tests/           pytest suite with a shared local SparkSession
```

## License

MIT, see [LICENSE](LICENSE).
