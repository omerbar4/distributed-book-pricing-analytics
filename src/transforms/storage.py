"""Read/write one processing-date partition of a Parquet dataset.

Each run overwrites exactly ``<dataset>/processing_date=<date>/`` and nothing else, so
re-running a date is idempotent and other dates are never touched. Overwriting the whole
date directory (rather than Spark's dynamic partition overwrite) also clears sub-partitions
that disappear between runs, e.g. a category with no valid rows on a re-run.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

PARTITION_COLUMN = "processing_date"


def date_partition_path(base: Path, processing_date: date) -> Path:
    return base / f"{PARTITION_COLUMN}={processing_date.isoformat()}"


def write_date_partition(
    df: DataFrame,
    base: Path,
    processing_date: date,
    partition_by: Sequence[str] = (),
) -> Path:
    target = date_partition_path(base, processing_date)
    df = df.drop(PARTITION_COLUMN)
    # Local datasets are tiny: one file per (sub)partition instead of many small files.
    # On a cluster you would size this from data volume instead.
    if partition_by:
        writer = df.repartition(*partition_by).write.partitionBy(*partition_by)
    else:
        writer = df.coalesce(1).write
    writer.mode("overwrite").parquet(str(target))
    return target


def read_date_partition(spark: SparkSession, base: Path, processing_date: date) -> DataFrame:
    """Read via the dataset root so partition columns are restored; the filter prunes dirs."""
    return spark.read.parquet(str(base)).where(
        F.col(PARTITION_COLUMN) == F.lit(processing_date.isoformat()).cast("date")
    )
