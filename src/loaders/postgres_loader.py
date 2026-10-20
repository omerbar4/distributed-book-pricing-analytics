"""CLI: load one processing date of curated outputs into PostgreSQL, idempotently.

For each table, inside ONE transaction (all tables for the date commit together or not
at all):

1. ``COPY`` the date's rows from Parquet into a temp staging table (streamed partition by
   partition via ``toLocalIterator``, so the driver never holds the full dataset);
2. ``DELETE`` the date's existing rows from the target;
3. ``INSERT ... SELECT`` from staging.

Re-running a date therefore replaces it and never duplicates rows. A transaction-scoped
advisory lock serializes concurrent loads. Spark's JDBC writer was not used because it
cannot replace one date's rows atomically.

Usage::

    python -m src.loaders.postgres_loader --processing-date 2026-10-20
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import psycopg
from psycopg import sql
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from src.common.config import Settings
from src.common.logging import configure_logging, get_logger
from src.common.paths import DataLayout
from src.common.spark import build_spark_session
from src.transforms.storage import read_date_partition

DDL_PATH = Path(__file__).resolve().parents[2] / "sql" / "001_init.sql"
LOAD_LOCK_KEY = 726110  # arbitrary constant identifying this loader's advisory lock

log = get_logger(__name__)


class QualityGateError(RuntimeError):
    pass


@dataclass(frozen=True)
class TableSpec:
    table: str
    layer: str
    dataset: str
    columns: tuple[str, ...]
    # Map target column -> source column when names differ.
    renames: tuple[tuple[str, str], ...] = ()


TABLES = (
    TableSpec(
        "daily_pricing_recommendations",
        "gold",
        "daily_pricing_recommendations",
        (
            "processing_date",
            "isbn_13",
            "condition",
            "title",
            "author",
            "category",
            "current_listing_price",
            "estimated_market_price",
            "recommended_price",
            "price_change",
            "inventory_count",
            "recommendation_reason",
            "data_origin",
        ),
    ),
    TableSpec(
        "category_pricing_summary",
        "gold",
        "category_pricing_summary",
        (
            "processing_date",
            "category",
            "valid_book_count",
            "valid_listing_count",
            "avg_listing_price",
            "median_listing_price",
            "avg_recommended_price",
            "total_inventory",
            "invalid_record_count",
        ),
    ),
    TableSpec(
        "data_quality_report",
        "gold",
        "data_quality_report",
        (
            "processing_date",
            "input_record_count",
            "valid_record_count",
            "rejected_record_count",
            "duplicate_count",
            "exact_duplicate_count",
            "business_duplicate_count",
            "invalid_isbn_count",
            "negative_price_count",
            "blank_title_count",
            "invalid_inventory_count",
            "malformed_record_count",
            "other_rule_violation_count",
            "quality_pass_rate",
            "min_pass_rate",
            "quality_gate_passed",
            "generated_at",
        ),
    ),
    TableSpec(
        "rejected_records",
        "silver",
        "rejected_observations",
        (
            "processing_date",
            "observation_id",
            "isbn_raw",
            "isbn_invalid_reason",
            "title",
            "category",
            "condition",
            "listing_price",
            "estimated_market_price",
            "inventory_count",
            "observed_at",
            "rejection_reasons",
            "rejection_reason",
            "raw_record",
            "source_file",
        ),
        renames=(("source_file", "_source_file"),),
    ),
)


def _source_frame(spark: SparkSession, layout: DataLayout, spec: TableSpec, d: date) -> DataFrame:
    df = read_date_partition(spark, layout.dataset(spec.layer, spec.dataset), d)
    renames = dict(spec.renames)
    df = df.select(*[F.col(renames.get(c, c)).alias(c) for c in spec.columns])
    # Ship timestamps as explicit-UTC ISO strings: Python datetimes from Spark are naive
    # local times, which would shift by the host's UTC offset in a timestamptz column.
    for field in df.schema.fields:
        if isinstance(field.dataType, TimestampType):
            df = df.withColumn(
                field.name, F.date_format(field.name, "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
            )
    return df


def _replace_date(cur: psycopg.Cursor, spec: TableSpec, rows: DataFrame, d: date) -> dict:
    target = sql.Identifier("analytics", spec.table)
    staging = sql.Identifier(f"staging_{spec.table}")
    columns = sql.SQL(", ").join(map(sql.Identifier, spec.columns))

    cur.execute(
        sql.SQL(
            "CREATE TEMP TABLE {staging} (LIKE {target} INCLUDING DEFAULTS) ON COMMIT DROP"
        ).format(staging=staging, target=target)
    )
    copied = 0
    with cur.copy(
        sql.SQL("COPY {staging} ({columns}) FROM STDIN").format(staging=staging, columns=columns)
    ) as copy:
        for row in rows.toLocalIterator():
            copy.write_row(tuple(row))
            copied += 1

    cur.execute(
        sql.SQL("DELETE FROM {target} WHERE processing_date = %s").format(target=target), (d,)
    )
    deleted = cur.rowcount
    cur.execute(
        sql.SQL("INSERT INTO {target} ({columns}) SELECT {columns} FROM {staging}").format(
            target=target, columns=columns, staging=staging
        )
    )
    return {"copied": copied, "deleted": deleted, "inserted": cur.rowcount}


def check_quality_gate(spark: SparkSession, layout: DataLayout, d: date) -> None:
    reports = read_date_partition(spark, layout.dataset("gold", "data_quality_report"), d).collect()
    if not reports:
        raise QualityGateError(f"no data_quality_report for {d}; run the Spark job first")
    if not reports[0]["quality_gate_passed"]:
        raise QualityGateError(
            f"quality gate failed for {d} (pass rate {reports[0]['quality_pass_rate']}); "
            "refusing to publish. Re-run with --allow-failed-quality to load anyway."
        )


def load_processing_date(
    spark: SparkSession,
    layout: DataLayout,
    conninfo: str,
    processing_date: date,
    allow_failed_quality: bool = False,
) -> dict[str, dict]:
    if not allow_failed_quality:
        check_quality_gate(spark, layout, processing_date)

    results: dict[str, dict] = {}
    # The connection context manager commits on success and rolls back on any exception.
    with psycopg.connect(conninfo, connect_timeout=10) as conn, conn.cursor() as cur:
        cur.execute(DDL_PATH.read_text(encoding="utf-8"))
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (LOAD_LOCK_KEY,))
        for spec in TABLES:
            rows = _source_frame(spark, layout, spec, processing_date)
            results[spec.table] = _replace_date(cur, spec, rows, processing_date)
            log.info(
                "table loaded",
                extra={"table": f"analytics.{spec.table}", **results[spec.table]},
            )
    return results


def main(argv: Sequence[str] | None = None) -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    parser = argparse.ArgumentParser(description="Load curated outputs into PostgreSQL")
    parser.add_argument("--processing-date", type=date.fromisoformat, required=True)
    parser.add_argument("--allow-failed-quality", action="store_true")
    args = parser.parse_args(argv)

    spark = build_spark_session(
        "book-pricing-postgres-loader",
        master=settings.spark_master,
        shuffle_partitions=settings.spark_shuffle_partitions,
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        results = load_processing_date(
            spark,
            DataLayout(settings.data_dir),
            settings.postgres_conninfo,
            args.processing_date,
            args.allow_failed_quality,
        )
    except QualityGateError as exc:
        log.error("load refused", extra={"error": str(exc)})
        return 2
    except Exception:
        log.exception("load failed")
        return 1
    finally:
        spark.stop()

    print(json.dumps({"processing_date": args.processing_date.isoformat(), "tables": results}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
