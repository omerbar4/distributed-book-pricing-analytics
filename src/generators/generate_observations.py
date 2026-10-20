"""CLI: generate SIMULATED daily price/inventory observations from landed Open Library metadata.

Open Library provides book metadata, not used-book prices. This module stands in for a
marketplace price feed so the pipeline has something realistic to process. Every value it
produces is synthetic, derived from fixed rules and a configurable seed:

* the same raw input + seed + processing date always yields byte-identical output;
* every record carries ``source = "demo_simulated_feed_v1"``;
* a small, fixed number of deliberately invalid records is injected so the data-quality
  stage has something to catch. Their expected counts go to ``_manifest.json`` (Spark
  ignores files starting with ``_``), which tests use as ground truth.

This is a simulated upstream system, not the transformation layer, so plain Python is fine
here. All cleaning and validation happens later in PySpark.

Example::

    python -m src.generators.generate_observations --processing-date 2026-10-20
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path

from src.common import isbn as isbn_lib
from src.common.config import Settings
from src.common.logging import configure_logging, get_logger
from src.ingestion.raw_store import raw_open_library_root

SOURCE = "demo_simulated_feed_v1"
CONDITIONS = ("VERY_GOOD", "GOOD", "ACCEPTABLE")
CONDITION_PRICE_FACTOR = {"VERY_GOOD": 1.15, "GOOD": 1.0, "ACCEPTABLE": 0.7}
# Messy spellings a real seller feed might send; silver standardizes them.
CONDITION_SPELLINGS = {
    "VERY_GOOD": ("VERY_GOOD", "very good", "Very-Good", " VG "),
    "GOOD": ("GOOD", "good", " Good "),
    "ACCEPTABLE": ("ACCEPTABLE", "acceptable", "Acceptable "),
}
CATEGORY_BASE_PRICE = {
    "science_fiction": 9.0,
    "fantasy": 10.0,
    "history": 12.0,
    "technology": 18.0,
    "business": 14.0,
}
DEFAULT_BASE_PRICE = 11.0
DEFECT_TYPES = (
    "negative_price",
    "invalid_isbn",
    "blank_title",
    "impossible_inventory",
    "exact_duplicate",
    "stale_business_duplicate",
    "malformed_json",
)

log = get_logger(__name__)


@dataclass(frozen=True)
class Book:
    isbn_13: str
    title: str
    author: str
    category: str


def observations_dir(data_dir: Path, processing_date: date) -> Path:
    return data_dir / "raw" / "demo_observations" / f"processing_date={processing_date}"


def _rng(*parts: object) -> random.Random:
    # str seeds are hashed deterministically by `random` (unlike hash(), unaffected by
    # PYTHONHASHSEED), so each record's values depend only on these parts.
    return random.Random("|".join(str(p) for p in parts))


def _latest_runs(raw_root: Path, processing_date: date) -> dict[str, Path]:
    """Newest landed response per subject with ingestion_date <= processing_date."""
    latest: dict[str, tuple[str, str, Path]] = {}
    for path in raw_root.glob("ingestion_date=*/subject=*/*.response.json"):
        ingestion_date = path.parent.parent.name.split("=", 1)[1]
        if date.fromisoformat(ingestion_date) > processing_date:
            continue
        subject = path.parent.name.split("=", 1)[1]
        key = (ingestion_date, path.name)
        if subject not in latest or key > latest[subject][:2]:
            latest[subject] = (*key, path)
    return {subject: entry[2] for subject, entry in sorted(latest.items())}


def _pick_isbn(raw_isbns: Sequence[str]) -> str | None:
    results = [isbn_lib.normalize_isbn(v) for v in sorted(set(raw_isbns))]
    for wanted in (isbn_lib.VALID_ISBN13, isbn_lib.CONVERTED_ISBN10):
        for result in results:
            if result.status == wanted:
                return result.isbn_13
    return None


def load_books(data_dir: Path, processing_date: date, max_per_category: int) -> list[Book]:
    books: list[Book] = []
    seen: set[str] = set()
    for subject, path in _latest_runs(raw_open_library_root(data_dir), processing_date).items():
        taken = 0
        for doc in json.loads(path.read_bytes()).get("docs", []):
            isbn_13 = _pick_isbn(doc.get("isbn") or [])
            # A work can appear under several subjects; list it once (first subject wins).
            if not isbn_13 or isbn_13 in seen or not doc.get("title"):
                continue
            seen.add(isbn_13)
            authors = doc.get("author_name") or ["Unknown"]
            books.append(Book(isbn_13, doc["title"], ", ".join(authors[:2]), subject))
            taken += 1
            if taken >= max_per_category:
                break
    return books


def _feed_isbn(isbn_13: str, rng: random.Random) -> str:
    """Format the ISBN the way a sloppy feed might (hyphens, spaces, ISBN-10)."""
    style = rng.choice(("plain", "hyphenated", "spaced", "isbn10"))
    if style == "hyphenated":
        return f"{isbn_13[:3]}-{isbn_13[3]}-{isbn_13[4:8]}-{isbn_13[8:12]}-{isbn_13[12]}"
    if style == "spaced":
        return f" {isbn_13[:3]} {isbn_13[3:]} "
    if style == "isbn10":
        return isbn_lib.isbn13_to_isbn10(isbn_13) or isbn_13
    return isbn_13


def _observations_for(book: Book, seed: int, processing_date: date) -> list[dict]:
    rng = _rng(seed, book.isbn_13, processing_date)
    base = CATEGORY_BASE_PRICE.get(book.category, DEFAULT_BASE_PRICE)
    conditions = rng.sample(CONDITIONS, k=rng.randint(1, len(CONDITIONS)))
    day_start = datetime.combine(processing_date, time(6, 0), tzinfo=UTC)
    records = []
    for condition in sorted(conditions):
        market = base * CONDITION_PRICE_FACTOR[condition] * rng.uniform(0.75, 1.35)
        listing = market * rng.uniform(0.85, 1.25)
        observed_at = day_start + timedelta(minutes=rng.randint(0, 720))
        digest = hashlib.sha1(f"{processing_date}|{book.isbn_13}|{condition}".encode())
        records.append(
            {
                "observation_id": f"obs-{digest.hexdigest()[:16]}",
                "isbn": _feed_isbn(book.isbn_13, rng),
                "title": book.title if rng.random() > 0.2 else f"  {book.title}  ",
                "author": book.author,
                "category": book.category if rng.random() > 0.2 else book.category.upper(),
                "condition": rng.choice(CONDITION_SPELLINGS[condition]),
                "listing_price": round(listing, 2),
                "estimated_market_price": round(market, 2),
                "inventory_count": rng.randint(0, 20),
                "source": SOURCE,
                "observed_at": observed_at.isoformat().replace("+00:00", "Z"),
                "processing_date": processing_date.isoformat(),
            }
        )
    return records


def _wrong_check_digit(raw_isbn: str) -> str:
    digits = isbn_lib.clean_isbn(raw_isbn)
    last = 0 if digits[-1] == "X" else int(digits[-1])
    return digits[:-1] + str((last + 1) % 10)


def _inject_defects(
    clean: list[dict], seed: int, processing_date: date, per_type: int
) -> tuple[list[dict], list[str], dict[str, int]]:
    """Return (extra JSON records, extra raw malformed lines, counts per defect type)."""
    rng = _rng(seed, "defects", processing_date)
    extra: list[dict] = []
    malformed: list[str] = []
    counts = dict.fromkeys(DEFECT_TYPES, 0)
    if not clean:
        return extra, malformed, counts

    def variant(record: dict, defect: str, n: int) -> dict:
        copy = dict(record)
        copy["observation_id"] = f"{record['observation_id']}-{defect}-{n}"
        return copy

    for n in range(per_type):
        bad = variant(rng.choice(clean), "negative_price", n)
        bad["listing_price"] = -abs(bad["listing_price"])
        extra.append(bad)

        bad = variant(rng.choice(clean), "invalid_isbn", n)
        # Alternate between a wrong check digit and a malformed value.
        bad["isbn"] = _wrong_check_digit(bad["isbn"]) if n % 2 == 0 else "12345"
        extra.append(bad)

        bad = variant(rng.choice(clean), "blank_title", n)
        bad["title"] = "   " if n % 2 == 0 else None
        extra.append(bad)

        bad = variant(rng.choice(clean), "impossible_inventory", n)
        bad["inventory_count"] = -4 if n % 2 == 0 else 1_000_000
        extra.append(bad)

        extra.append(dict(rng.choice(clean)))  # exact duplicate: identical, same id

        stale = variant(rng.choice(clean), "stale", n)
        observed = datetime.fromisoformat(stale["observed_at"].replace("Z", "+00:00"))
        stale["observed_at"] = (observed - timedelta(hours=3)).isoformat().replace("+00:00", "Z")
        stale["listing_price"] = round(stale["listing_price"] * 1.1, 2)
        extra.append(stale)

        malformed.append(f'{{"observation_id": "obs-malformed-{n}", "listing_price": "abc", ')

        for defect in DEFECT_TYPES:
            counts[defect] += 1
    return extra, malformed, counts


def generate(
    data_dir: Path,
    processing_date: date,
    seed: int,
    max_books_per_category: int = 40,
    defects_per_type: int = 2,
) -> dict:
    books = load_books(data_dir, processing_date, max_books_per_category)
    if not books:
        raise FileNotFoundError(
            f"no landed Open Library responses with ingestion_date <= {processing_date} "
            f"under {raw_open_library_root(data_dir)}; run src.ingestion.fetch_books first"
        )
    clean = [obs for book in books for obs in _observations_for(book, seed, processing_date)]
    extra, malformed, defect_counts = _inject_defects(
        clean, seed, processing_date, defects_per_type
    )

    lines = [json.dumps(record, sort_keys=True) for record in clean + extra] + malformed
    _rng(seed, "order", processing_date).shuffle(lines)

    out_dir = observations_dir(data_dir, processing_date)
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "processing_date": processing_date.isoformat(),
        "seed": seed,
        "source": SOURCE,
        "data_origin": "SIMULATED_DEMO",
        "book_count": len(books),
        "books_per_category": {
            c: sum(b.category == c for b in books) for c in sorted({b.category for b in books})
        },
        "clean_observation_count": len(clean),
        "injected_defects": defect_counts,
        "total_lines": len(lines),
    }
    # The output is a pure function of its inputs, so regenerating replaces it atomically.
    for name, content in (
        ("observations.jsonl", "\n".join(lines) + "\n"),
        ("_manifest.json", json.dumps(manifest, indent=2, sort_keys=True) + "\n"),
    ):
        tmp = out_dir / f".{name}.tmp"
        tmp.write_text(content, encoding="utf-8")
        os.replace(tmp, out_dir / name)
    return manifest


def main(argv: Sequence[str] | None = None) -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    parser = argparse.ArgumentParser(
        prog="python -m src.generators.generate_observations",
        description="Generate SIMULATED demo price/inventory observations (not market data).",
    )
    parser.add_argument("--processing-date", type=date.fromisoformat, required=True)
    parser.add_argument("--seed", type=int, default=settings.demo_random_seed)
    parser.add_argument("--max-books-per-category", type=int, default=40)
    parser.add_argument("--defects-per-type", type=int, default=2)
    args = parser.parse_args(argv)

    manifest = generate(
        settings.data_dir,
        args.processing_date,
        args.seed,
        args.max_books_per_category,
        args.defects_per_type,
    )
    log.info("demo observations generated", extra={"manifest": manifest})
    print(json.dumps(manifest))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
