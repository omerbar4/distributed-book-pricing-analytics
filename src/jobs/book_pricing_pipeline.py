"""Spark batch job: raw -> bronze -> silver -> gold for one processing date.

Usage::

    spark-submit src/jobs/book_pricing_pipeline.py --processing-date 2026-10-20

Incremental by design: a run reads only its own date's observations (plus Open Library
metadata landed up to that date) and overwrites only that date's partitions, so dates can
be backfilled or re-run in any order.

Exit codes: 0 success, 1 error, 2 quality gate failed (outputs are still written so the
quality report and rejected records can be inspected).
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import date

from pyspark.sql import SparkSession

from src.common.config import Settings
from src.common.logging import configure_logging, get_logger
from src.common.paths import DataLayout
from src.common.spark import build_spark_session
from src.transforms import bronze, gold, quality, silver
from src.transforms.storage import read_date_partition, write_date_partition

DEFAULT_MIN_PASS_RATE = 0.80

log = get_logger(__name__)


def run_pipeline(
    spark: SparkSession,
    layout: DataLayout,
    processing_date: date,
    min_pass_rate: float = DEFAULT_MIN_PASS_RATE,
) -> dict:
    raw_obs_dir = layout.raw_observations(processing_date)
    if not (raw_obs_dir / "observations.jsonl").exists():
        raise FileNotFoundError(f"no demo observations at {raw_obs_dir}; run the generator")
    if not layout.raw_open_library.exists():
        raise FileNotFoundError(f"no Open Library landing zone at {layout.raw_open_library}")

    def stage(layer: str, name: str, df, partition_by: Sequence[str] = ()):
        base = layout.dataset(layer, name)
        path = write_date_partition(df, base, processing_date, partition_by)
        log.info("dataset written", extra={"dataset": f"{layer}/{name}", "path": str(path)})
        # Re-read what was written: downstream stages consume the persisted layer, which
        # keeps lineage short and mirrors how separate jobs would chain on a cluster.
        return read_date_partition(spark, base, processing_date)

    # Bronze
    bronze_obs = stage(
        "bronze", "observations", bronze.build_observations(spark, raw_obs_dir, processing_date)
    )
    bronze_books = stage(
        "bronze",
        "book_isbns",
        bronze.build_book_isbns(spark, layout.raw_open_library, processing_date),
    )

    # Silver
    book_metadata = stage("silver", "book_metadata", silver.build_book_metadata(bronze_books))
    result = silver.build_observations(bronze_obs, book_metadata)
    valid = stage("silver", "observations", result.valid)
    rejected = stage("silver", "rejected_observations", result.rejected)

    # Gold
    recommendations = stage(
        "gold",
        "daily_pricing_recommendations",
        gold.build_recommendations(valid),
        partition_by=("category",),
    )
    summary = stage(
        "gold",
        "category_pricing_summary",
        gold.build_category_summary(recommendations, rejected, processing_date),
    )

    metrics = quality.compute_quality_metrics(
        processing_date=processing_date,
        input_count=bronze_obs.count(),
        evaluated_count=result.evaluated_count,
        rejected_count=rejected.count(),
        business_duplicate_count=result.superseded.count(),
        valid_count=valid.count(),
        reason_counts=quality.rejection_reason_counts(rejected),
        min_pass_rate=min_pass_rate,
    )
    stage("gold", "data_quality_report", quality.build_quality_report(spark, metrics))

    return {
        **{k: v for k, v in metrics.items() if k not in ("processing_date", "generated_at")},
        "processing_date": processing_date.isoformat(),
        "book_metadata_isbns": book_metadata.count(),
        "recommendation_rows": recommendations.count(),
        "category_rows": summary.count(),
    }


def main(argv: Sequence[str] | None = None) -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    parser = argparse.ArgumentParser(description="Book pricing PySpark batch pipeline")
    parser.add_argument("--processing-date", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--min-pass-rate",
        type=float,
        default=DEFAULT_MIN_PASS_RATE,
        help="fail (exit 2) if the share of rows passing validation is below this",
    )
    args = parser.parse_args(argv)

    spark = build_spark_session(
        "book-pricing-pipeline",
        master=settings.spark_master,
        shuffle_partitions=settings.spark_shuffle_partitions,
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        summary = run_pipeline(
            spark, DataLayout(settings.data_dir), args.processing_date, args.min_pass_rate
        )
    except Exception:
        log.exception("pipeline failed", extra={"processing_date": str(args.processing_date)})
        return 1
    finally:
        spark.stop()

    log.info("pipeline finished", extra={"summary": summary})
    print(json.dumps(summary))
    if not summary["quality_gate_passed"]:
        log.error("quality gate failed", extra={"pass_rate": summary["quality_pass_rate"]})
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
