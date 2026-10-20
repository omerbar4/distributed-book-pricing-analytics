"""ISBN normalization and checksum validation as native Spark column expressions.

No Python UDFs: every step compiles to Catalyst expressions that run inside the JVM, so
the logic scales across executors without Python serialization. Digits are read with
``ascii(char) - 48`` instead of casts, which keeps the expressions safe under Spark's
ANSI mode (a failed cast would raise instead of returning null).

Mirrors ``src.common.isbn``; the tests assert both implementations agree.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.common import isbn as isbn_lib


def _digit(col: Column, position: int) -> Column:
    """Numeric value of the character at 1-based ``position``."""
    return F.ascii(F.substring(col, position, 1)) - F.lit(48)


def _sum(terms: list[Column]) -> Column:
    return reduce(lambda a, b: a + b, terms)


def isbn13_checksum_ok(col: Column) -> Column:
    weighted = [_digit(col, i + 1) * (1 if i % 2 == 0 else 3) for i in range(13)]
    return _sum(weighted) % 10 == 0


def isbn10_checksum_ok(col: Column) -> Column:
    weighted = [_digit(col, i + 1) * (10 - i) for i in range(9)]
    check = F.when(F.substring(col, 10, 1) == "X", F.lit(10)).otherwise(_digit(col, 10))
    return (_sum(weighted) + check) % 11 == 0


def isbn10_to_isbn13(col: Column) -> Column:
    core = F.concat(F.lit("978"), F.substring(col, 1, 9))
    total = _sum([_digit(core, i + 1) * (1 if i % 2 == 0 else 3) for i in range(12)])
    check = (F.lit(10) - total % 10) % 10
    return F.concat(core, check.cast("string"))


def add_normalized_isbn(df: DataFrame, source_col: str, prefix: str = "isbn") -> DataFrame:
    """Add ``{prefix}_13``, ``{prefix}_status`` and ``{prefix}_invalid_reason`` columns."""
    tmp = f"_{prefix}_clean"
    df = df.withColumn(
        tmp,
        F.upper(F.regexp_replace(F.coalesce(F.col(source_col), F.lit("")), r"[\s-]", "")),
    )
    clean = F.col(tmp)
    looks_13 = clean.rlike(r"^\d{13}$")
    looks_10 = clean.rlike(r"^\d{9}[\dX]$")
    ok_13 = looks_13 & isbn13_checksum_ok(clean)
    ok_10 = looks_10 & isbn10_checksum_ok(clean)
    no_reason = F.lit(None).cast("string")

    return (
        df.withColumn(f"{prefix}_13", F.when(ok_13, clean).when(ok_10, isbn10_to_isbn13(clean)))
        .withColumn(
            f"{prefix}_status",
            F.when(ok_13, F.lit(isbn_lib.VALID_ISBN13))
            .when(ok_10, F.lit(isbn_lib.CONVERTED_ISBN10))
            .otherwise(F.lit(isbn_lib.INVALID)),
        )
        .withColumn(
            f"{prefix}_invalid_reason",
            F.when(ok_13 | ok_10, no_reason)
            .when(clean == "", F.lit(isbn_lib.REASON_MISSING))
            .when(looks_13 | looks_10, F.lit(isbn_lib.REASON_CHECKSUM))
            .otherwise(F.lit(isbn_lib.REASON_FORMAT)),
        )
        .drop(tmp)
    )
