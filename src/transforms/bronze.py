"""Bronze: raw files -> typed, explicitly-schema'd DataFrames with lineage metadata.

No business rules here: bronze keeps every input row (including malformed ones) so later
layers can account for all of them.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

CORRUPT_RECORD_COLUMN = "_corrupt_record"

OBSERVATION_SCHEMA = StructType(
    [
        StructField("observation_id", StringType()),
        StructField("isbn", StringType()),
        StructField("title", StringType()),
        StructField("author", StringType()),
        StructField("category", StringType()),
        StructField("condition", StringType()),
        StructField("listing_price", DoubleType()),
        StructField("estimated_market_price", DoubleType()),
        StructField("inventory_count", LongType()),
        StructField("source", StringType()),
        StructField("observed_at", StringType()),
        StructField("processing_date", StringType()),
        # Populated by Spark's PERMISSIVE mode for lines that don't fit the schema.
        StructField(CORRUPT_RECORD_COLUMN, StringType()),
    ]
)

OPEN_LIBRARY_DOC_SCHEMA = StructType(
    [
        StructField("key", StringType()),
        StructField("title", StringType()),
        StructField("author_name", ArrayType(StringType())),
        StructField("isbn", ArrayType(StringType())),
        StructField("first_publish_year", LongType()),
        StructField("edition_count", LongType()),
    ]
)

OPEN_LIBRARY_RESPONSE_SCHEMA = StructType(
    [
        StructField("numFound", LongType()),
        StructField("docs", ArrayType(OPEN_LIBRARY_DOC_SCHEMA)),
    ]
)

# Columns that define an observation's content (used for exact-duplicate detection);
# lineage columns such as _bronze_loaded_at are deliberately excluded.
OBSERVATION_DATA_COLUMNS = [
    "record_processing_date" if f.name == "processing_date" else f.name
    for f in OBSERVATION_SCHEMA.fields
]


def _with_lineage(df: DataFrame, processing_date: date) -> DataFrame:
    return df.withColumn("_bronze_loaded_at", F.current_timestamp()).withColumn(
        "processing_date", F.lit(processing_date.isoformat()).cast("date")
    )


def build_observations(spark: SparkSession, raw_dir: Path, processing_date: date) -> DataFrame:
    df = (
        spark.read.schema(OBSERVATION_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .json(str(raw_dir))
        .withColumn("_source_file", F.col("_metadata.file_path"))
        # The record's own date is kept for audit; the partition date comes from the run.
        .withColumnRenamed("processing_date", "record_processing_date")
    )
    return _with_lineage(df, processing_date)


def build_book_isbns(spark: SparkSession, raw_root: Path, processing_date: date) -> DataFrame:
    """One row per (landed work, ISBN) from every Open Library response up to the date."""
    responses = (
        spark.read.schema(OPEN_LIBRARY_RESPONSE_SCHEMA)
        .option("multiLine", True)
        .option("pathGlobFilter", "*.response.json")  # skip *.meta.json sidecars
        .json(str(raw_root))
        # ingestion_date/subject come from partition discovery; this filter prunes dirs.
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .where(F.col("ingestion_date") <= F.lit(processing_date.isoformat()).cast("date"))
        .withColumn("run_id", F.regexp_extract("_source_file", r"([^/]+)\.response\.json$", 1))
    )
    docs = responses.select(
        "ingestion_date", "subject", "run_id", "_source_file", F.explode("docs").alias("doc")
    )
    isbns = docs.select(
        F.col("doc.key").alias("work_key"),
        F.col("doc.title").alias("title"),
        F.array_join(F.col("doc.author_name"), ", ").alias("author_names"),
        F.col("doc.first_publish_year").alias("first_publish_year"),
        F.col("subject"),
        F.col("ingestion_date").cast("date").alias("ingestion_date"),
        F.col("run_id"),
        F.col("_source_file"),
        F.explode("doc.isbn").alias("isbn_raw"),
    )
    return _with_lineage(isbns, processing_date)
