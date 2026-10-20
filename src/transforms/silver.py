"""Silver: standardize, validate, deduplicate and enrich.

Order of operations for observations (each step is deliberate):

1. Exact duplicates (identical content) are dropped first; they carry no information.
2. Every remaining row is validated; failures go to the rejected dataset with reasons.
3. Business duplicates are resolved *after* validation, so a newer but invalid record can
   never shadow an older valid one. A window over (isbn_13, condition, source) keeps the
   newest observation.
4. Survivors are joined to Open Library metadata (broadcast join: the dimension is small).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from src.transforms.bronze import CORRUPT_RECORD_COLUMN, OBSERVATION_DATA_COLUMNS
from src.transforms.isbn_columns import add_normalized_isbn

ALLOWED_CONDITIONS = ("VERY_GOOD", "GOOD", "ACCEPTABLE")
CONDITION_ALIASES = {"VG": "VERY_GOOD", "VERYGOOD": "VERY_GOOD", "ACC": "ACCEPTABLE"}
MAX_INVENTORY = 10_000
BUSINESS_KEY = ("isbn_13", "condition_std", "source")
UNKNOWN_CATEGORY = "unknown"


@dataclass(frozen=True)
class SilverObservations:
    valid: DataFrame
    rejected: DataFrame
    superseded: DataFrame
    evaluated_count: int


def _blank(col: Column) -> Column:
    return col.isNull() | (F.trim(col) == "")


def _squash_whitespace(col: Column) -> Column:
    return F.trim(F.regexp_replace(col, r"\s+", " "))


def standardize_observations(df: DataFrame) -> DataFrame:
    condition = F.regexp_replace(F.upper(F.trim(F.col("condition"))), r"[\s-]+", "_")
    for alias, canonical in CONDITION_ALIASES.items():
        condition = F.when(condition == alias, F.lit(canonical)).otherwise(condition)
    return (
        add_normalized_isbn(df, "isbn")
        .withColumn("title_clean", _squash_whitespace(F.col("title")))
        .withColumn("author_clean", _squash_whitespace(F.col("author")))
        .withColumn(
            "category_clean",
            F.regexp_replace(F.lower(F.trim(F.col("category"))), r"[\s-]+", "_"),
        )
        .withColumn("condition_std", condition)
        # try_* returns null instead of failing the job under ANSI mode.
        .withColumn("observed_at_ts", F.try_to_timestamp(F.col("observed_at")))
    )


def validation_rules() -> list[tuple[str, Column]]:
    """(rule name, condition that marks a row as failing). Built lazily: Columns need Spark."""
    return [
        ("missing_observation_id", _blank(F.col("observation_id"))),
        ("invalid_isbn", F.col("isbn_13").isNull()),
        ("blank_title", _blank(F.col("title_clean"))),
        ("missing_category", _blank(F.col("category_clean"))),
        (
            "invalid_condition",
            F.col("condition_std").isNull() | ~F.col("condition_std").isin(*ALLOWED_CONDITIONS),
        ),
        (
            "missing_price",
            F.col("listing_price").isNull() | F.col("estimated_market_price").isNull(),
        ),
        ("negative_price", (F.col("listing_price") < 0) | (F.col("estimated_market_price") < 0)),
        (
            "invalid_inventory",
            F.col("inventory_count").isNull()
            | (F.col("inventory_count") < 0)
            | (F.col("inventory_count") > MAX_INVENTORY),
        ),
        ("invalid_observed_at", F.col("observed_at_ts").isNull()),
    ]


def add_rejection_reasons(df: DataFrame) -> DataFrame:
    # A malformed line has no trustworthy fields, so it gets exactly one reason.
    rule_hits = F.array_compact(
        F.array(*[F.when(cond, F.lit(name)) for name, cond in validation_rules()])
    )
    reasons = F.when(
        F.col(CORRUPT_RECORD_COLUMN).isNotNull(), F.array(F.lit("malformed_record"))
    ).otherwise(rule_hits)
    return df.withColumn("rejection_reasons", reasons)


def build_book_metadata(bronze_isbns: DataFrame) -> DataFrame:
    """One row per valid ISBN-13, keeping the most recently ingested work for it."""
    newest_first = Window.partitionBy("isbn_13").orderBy(
        F.col("ingestion_date").desc(), F.col("run_id").desc(), F.col("work_key")
    )
    return (
        add_normalized_isbn(bronze_isbns, "isbn_raw")
        .where(F.col("isbn_13").isNotNull())
        .withColumn("_rank", F.row_number().over(newest_first))
        .where(F.col("_rank") == 1)
        .select(
            "isbn_13",
            "work_key",
            F.col("title").alias("metadata_title"),
            F.col("author_names").alias("metadata_authors"),
            "first_publish_year",
            F.col("subject").alias("metadata_subject"),
            F.col("ingestion_date").alias("metadata_ingestion_date"),
            "processing_date",
        )
    )


def build_observations(bronze_obs: DataFrame, book_metadata: DataFrame) -> SilverObservations:
    evaluated = add_rejection_reasons(
        standardize_observations(bronze_obs.dropDuplicates(OBSERVATION_DATA_COLUMNS))
    ).cache()
    evaluated_count = evaluated.count()

    rejected = evaluated.where(F.size("rejection_reasons") > 0).select(
        "observation_id",
        F.col("isbn").alias("isbn_raw"),
        "isbn_invalid_reason",
        "title",
        F.when(_blank(F.col("category_clean")), F.lit(UNKNOWN_CATEGORY))
        .otherwise(F.col("category_clean"))
        .alias("category"),
        "condition",
        "listing_price",
        "estimated_market_price",
        "inventory_count",
        "observed_at",
        "rejection_reasons",
        F.array_join("rejection_reasons", ";").alias("rejection_reason"),
        F.col(CORRUPT_RECORD_COLUMN).alias("raw_record"),
        "_source_file",
        "processing_date",
    )

    newest_first = Window.partitionBy(*BUSINESS_KEY).orderBy(
        F.col("observed_at_ts").desc(), F.col("observation_id").desc()
    )
    ranked = evaluated.where(F.size("rejection_reasons") == 0).withColumn(
        "_rank", F.row_number().over(newest_first)
    )
    superseded = ranked.where(F.col("_rank") > 1).drop("_rank")

    metadata = book_metadata.select("isbn_13", "work_key", "first_publish_year", "metadata_title")
    valid = (
        ranked.where(F.col("_rank") == 1)
        .join(F.broadcast(metadata), on="isbn_13", how="left")
        .withColumn(
            "quality_flags",
            F.array_compact(
                F.array(
                    F.when(F.col("work_key").isNull(), F.lit("no_metadata_match")),
                    F.when(
                        F.col("listing_price") > 2 * F.col("estimated_market_price"),
                        F.lit("listing_far_above_market"),
                    ),
                )
            ),
        )
        .select(
            "observation_id",
            "isbn_13",
            "isbn_status",
            F.col("title_clean").alias("title"),
            F.col("author_clean").alias("author"),
            F.col("category_clean").alias("category"),
            F.col("condition_std").alias("condition"),
            "listing_price",
            "estimated_market_price",
            "inventory_count",
            "source",
            F.col("observed_at_ts").alias("observed_at"),
            "work_key",
            "first_publish_year",
            F.col("work_key").isNotNull().alias("metadata_matched"),
            "quality_flags",
            "processing_date",
        )
    )
    return SilverObservations(
        valid=valid, rejected=rejected, superseded=superseded, evaluated_count=evaluated_count
    )
