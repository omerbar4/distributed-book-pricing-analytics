"""Synthetic test fixtures.

The Open Library-shaped responses below are SYNTHETIC test data (made-up titles, ISBNs
built with valid checksums). They are used only by the test suite, never by the demo.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from src.common import isbn as isbn_lib
from src.ingestion.open_library_client import ApiResponse
from src.ingestion.raw_store import write_raw_response


def make_isbn13(n: int, prefix: str = "978") -> str:
    first12 = f"{prefix}{n:09d}"
    return first12 + isbn_lib.isbn13_check_digit(first12)


def synthetic_search_body(subject: str, count: int, offset: int) -> bytes:
    docs = []
    for i in range(count):
        isbn13 = make_isbn13(offset + i)
        docs.append(
            {
                "key": f"/works/OL{offset + i}W",
                "title": f"Synthetic {subject.replace('_', ' ').title()} Book {i}",
                "author_name": [f"Test Author {i % 3}"],
                # Mixed ISBN-10/13 like real Open Library docs, plus one junk value.
                "isbn": [isbn13, isbn_lib.isbn13_to_isbn10(isbn13), "not-an-isbn"],
                "first_publish_year": 1950 + i,
            }
        )
    return json.dumps({"numFound": count, "docs": docs}).encode()


def land_synthetic_raw(
    data_dir: Path,
    ingestion_date: date,
    subjects: dict[str, int],
    fetched_at: datetime | None = None,
) -> None:
    fetched_at = fetched_at or datetime(
        ingestion_date.year, ingestion_date.month, ingestion_date.day, 8, tzinfo=UTC
    )
    for index, (subject, count) in enumerate(sorted(subjects.items())):
        body = synthetic_search_body(subject, count, offset=index * 1000)
        response = ApiResponse(
            url=f"http://fixture.test/search.json?subject={subject}",
            params={"subject": subject},
            status_code=200,
            body=body,
            fetched_at=fetched_at,
            elapsed_ms=1,
            record_count=count,
        )
        write_raw_response(data_dir, subject, ingestion_date, response)
