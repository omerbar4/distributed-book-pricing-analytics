from __future__ import annotations

import random

import pytest

from src.common import isbn as isbn_lib
from src.transforms.isbn_columns import add_normalized_isbn

# Textbook examples with published check digits.
CASES = [
    ("978-0-306-40615-7", "9780306406157", isbn_lib.VALID_ISBN13, None),
    (" 9780306406157 ", "9780306406157", isbn_lib.VALID_ISBN13, None),
    ("0-306-40615-2", "9780306406157", isbn_lib.CONVERTED_ISBN10, None),
    ("080442957x", "9780804429573", isbn_lib.CONVERTED_ISBN10, None),
    ("9780306406158", None, isbn_lib.INVALID, isbn_lib.REASON_CHECKSUM),
    ("0306406153", None, isbn_lib.INVALID, isbn_lib.REASON_CHECKSUM),
    ("12345", None, isbn_lib.INVALID, isbn_lib.REASON_FORMAT),
    ("97803064061X7", None, isbn_lib.INVALID, isbn_lib.REASON_FORMAT),
    ("ISBN 9780306406157", None, isbn_lib.INVALID, isbn_lib.REASON_FORMAT),
    ("", None, isbn_lib.INVALID, isbn_lib.REASON_MISSING),
    (None, None, isbn_lib.INVALID, isbn_lib.REASON_MISSING),
]


@pytest.mark.parametrize(("raw", "isbn_13", "status", "reason"), CASES)
def test_python_normalization(raw, isbn_13, status, reason):
    assert isbn_lib.normalize_isbn(raw) == isbn_lib.IsbnResult(isbn_13, status, reason)


def test_isbn13_to_isbn10_round_trip():
    assert isbn_lib.isbn13_to_isbn10("9780306406157") == "0306406152"
    assert isbn_lib.isbn13_to_isbn10("9790000000001") is None  # 979 has no ISBN-10 form
    assert isbn_lib.isbn10_to_isbn13("080442957X") == "9780804429573"


def test_spark_matches_python_reference(spark):
    rng = random.Random(1234)
    raws = [case[0] for case in CASES]
    for _ in range(300):
        first12 = "978" + "".join(rng.choice("0123456789") for _ in range(9))
        valid13 = first12 + isbn_lib.isbn13_check_digit(first12)
        raws += [
            valid13,
            isbn_lib.isbn13_to_isbn10(valid13),
            valid13[:-1] + str((int(valid13[-1]) + 1) % 10),
            "".join(rng.choice("0123456789X- ") for _ in range(rng.randint(8, 15))),
        ]

    df = add_normalized_isbn(spark.createDataFrame([(r,) for r in raws], "raw string"), "raw")
    rows = df.collect()

    assert len(rows) == len(raws)
    for row in rows:
        expected = isbn_lib.normalize_isbn(row["raw"])
        actual = (row["isbn_13"], row["isbn_status"], row["isbn_invalid_reason"])
        assert actual == (expected.isbn_13, expected.status, expected.reason), row["raw"]
