"""Pure-Python ISBN helpers.

The Spark pipeline uses the column-expression equivalents in
``src/transforms/isbn_columns.py``; the test suite asserts both implementations agree.
The demo observation generator uses this module directly.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

VALID_ISBN13 = "VALID_ISBN13"
CONVERTED_ISBN10 = "CONVERTED_ISBN10"
INVALID = "INVALID"

REASON_MISSING = "missing"
REASON_FORMAT = "invalid_format"
REASON_CHECKSUM = "invalid_checksum"

_SEPARATORS = re.compile(r"[\s-]")
_ISBN13 = re.compile(r"\d{13}")
_ISBN10 = re.compile(r"\d{9}[\dX]")


@dataclass(frozen=True)
class IsbnResult:
    isbn_13: str | None
    status: str
    reason: str | None


def clean_isbn(raw: str | None) -> str:
    """Remove hyphens/whitespace and upper-case a trailing ``x`` check digit."""
    return _SEPARATORS.sub("", raw or "").upper()


def isbn13_check_digit(first12: str) -> str:
    total = sum(int(d) * (1 if i % 2 == 0 else 3) for i, d in enumerate(first12))
    return str((10 - total % 10) % 10)


def is_valid_isbn13(value: str) -> bool:
    return bool(_ISBN13.fullmatch(value)) and isbn13_check_digit(value[:12]) == value[12]


def is_valid_isbn10(value: str) -> bool:
    if not _ISBN10.fullmatch(value):
        return False
    total = sum((10 - i) * (10 if c == "X" else int(c)) for i, c in enumerate(value))
    return total % 11 == 0


def isbn10_to_isbn13(value: str) -> str:
    core = "978" + value[:9]
    return core + isbn13_check_digit(core)


def isbn13_to_isbn10(value: str) -> str | None:
    """Only ``978``-prefixed ISBN-13s have an ISBN-10 form."""
    if not value.startswith("978"):
        return None
    core = value[3:12]
    check = (11 - sum((10 - i) * int(d) for i, d in enumerate(core)) % 11) % 11
    return core + ("X" if check == 10 else str(check))


def normalize_isbn(raw: str | None) -> IsbnResult:
    value = clean_isbn(raw)
    if not value:
        return IsbnResult(None, INVALID, REASON_MISSING)
    if _ISBN13.fullmatch(value):
        if is_valid_isbn13(value):
            return IsbnResult(value, VALID_ISBN13, None)
        return IsbnResult(None, INVALID, REASON_CHECKSUM)
    if _ISBN10.fullmatch(value):
        if is_valid_isbn10(value):
            return IsbnResult(isbn10_to_isbn13(value), CONVERTED_ISBN10, None)
        return IsbnResult(None, INVALID, REASON_CHECKSUM)
    return IsbnResult(None, INVALID, REASON_FORMAT)
