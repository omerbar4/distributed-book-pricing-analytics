"""Data-quality metrics for one processing date.

Every input line ends up in exactly one bucket, which the report asserts::

    input = exact_duplicates + rejected + business_duplicates + valid

``quality_pass_rate`` = rows passing validation / rows evaluated (input minus exact
duplicates). Business duplicates count as passing: they were valid, just superseded.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StructField,
    StructType,
    TimestampType,
)

REASON_COLUMNS = {
    "invalid_isbn": "invalid_isbn_count",
    "negative_price": "negative_price_count",
    "blank_title": "blank_title_count",
    "invalid_inventory": "invalid_inventory_count",
    "malformed_record": "malformed_record_count",
}

_COUNT_FIELDS = [
    "input_record_count",
    "valid_record_count",
    "rejected_record_count",
    "duplicate_count",
    "exact_duplicate_count",
    "business_duplicate_count",
    *REASON_COLUMNS.values(),
    "other_rule_violation_count",
]

QUALITY_REPORT_SCHEMA = StructType(
    [
        StructField("processing_date", DateType(), False),
        *[StructField(name, LongType(), False) for name in _COUNT_FIELDS],
        StructField("quality_pass_rate", DoubleType(), False),
        StructField("min_pass_rate", DoubleType(), False),
        StructField("quality_gate_passed", BooleanType(), False),
        StructField("generated_at", TimestampType(), False),
    ]
)


def rejection_reason_counts(rejected: DataFrame) -> dict[str, int]:
    """Rows per rule. A row failing several rules counts once under each."""
    rows = (
        rejected.select(F.explode("rejection_reasons").alias("reason"))
        .groupBy("reason")
        .count()
        .collect()
    )
    return {row["reason"]: row["count"] for row in rows}


def compute_quality_metrics(
    *,
    processing_date: date,
    input_count: int,
    evaluated_count: int,
    rejected_count: int,
    business_duplicate_count: int,
    valid_count: int,
    reason_counts: dict[str, int],
    min_pass_rate: float,
) -> dict:
    exact_duplicates = input_count - evaluated_count
    accounted = exact_duplicates + rejected_count + business_duplicate_count + valid_count
    if accounted != input_count:
        raise ValueError(f"row accounting mismatch: {accounted} accounted vs {input_count} input")

    passed = evaluated_count - rejected_count
    pass_rate = round(passed / evaluated_count, 4) if evaluated_count else 0.0
    metrics = {
        "processing_date": processing_date,
        "input_record_count": input_count,
        "valid_record_count": valid_count,
        "rejected_record_count": rejected_count,
        "duplicate_count": exact_duplicates + business_duplicate_count,
        "exact_duplicate_count": exact_duplicates,
        "business_duplicate_count": business_duplicate_count,
        **{column: reason_counts.get(reason, 0) for reason, column in REASON_COLUMNS.items()},
        "other_rule_violation_count": sum(
            n for reason, n in reason_counts.items() if reason not in REASON_COLUMNS
        ),
        "quality_pass_rate": pass_rate,
        "min_pass_rate": min_pass_rate,
        "quality_gate_passed": pass_rate >= min_pass_rate,
        "generated_at": datetime.now(UTC),
    }
    return metrics


def build_quality_report(spark: SparkSession, metrics: dict) -> DataFrame:
    return spark.createDataFrame([metrics], schema=QUALITY_REPORT_SCHEMA)
