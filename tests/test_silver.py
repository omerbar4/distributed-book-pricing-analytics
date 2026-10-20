from __future__ import annotations

from datetime import date

import pytest
from pyspark.sql.types import StructField, StructType

from src.transforms import silver
from src.transforms.bronze import OBSERVATION_DATA_COLUMNS, OBSERVATION_SCHEMA

DAY = date(2026, 10, 20)
VALID_ISBN = "9780306406157"


def obs(**overrides):
    record = {
        "observation_id": "obs-1",
        "isbn": VALID_ISBN,
        "title": "Example Book",
        "author": "A. Writer",
        "category": "fantasy",
        "condition": "GOOD",
        "listing_price": 10.0,
        "estimated_market_price": 12.0,
        "inventory_count": 5,
        "source": "demo_simulated_feed_v1",
        "observed_at": "2026-10-20T08:00:00Z",
        "record_processing_date": "2026-10-20",
        "_corrupt_record": None,
    }
    record.update(overrides)
    return record


@pytest.fixture
def make_bronze(spark):
    columns = OBSERVATION_DATA_COLUMNS
    # Bronze schema, with the record's own date renamed exactly as bronze does it.
    schema = StructType(
        [
            StructField("record_processing_date", f.dataType) if f.name == "processing_date" else f
            for f in OBSERVATION_SCHEMA.fields
        ]
    )

    def _make(records):
        rows = [tuple(r[c] for c in columns) for r in records]
        df = spark.createDataFrame(rows, schema)
        return df.selectExpr(
            "*", "'test.jsonl' AS _source_file", "DATE'2026-10-20' AS processing_date"
        )

    return _make


@pytest.fixture
def empty_metadata(spark):
    return spark.createDataFrame(
        [], "isbn_13 string, work_key string, first_publish_year long, metadata_title string"
    )


def test_standardizes_isbn_title_category_and_condition(make_bronze, empty_metadata):
    bronze = make_bronze(
        [
            obs(
                isbn=" 978-0-306-40615-7 ",
                title="  Example   Book ",
                category=" Science Fiction ",
                condition="very good",
            ),
            # Different book (an ISBN-10), so business dedup doesn't merge the two rows.
            obs(observation_id="obs-2", isbn="0-8044-2957-x", condition=" VG "),
        ]
    )
    rows = {
        r["observation_id"]: r
        for r in silver.build_observations(bronze, empty_metadata).valid.collect()
    }

    assert rows["obs-1"]["isbn_13"] == VALID_ISBN
    assert rows["obs-1"]["title"] == "Example Book"
    assert rows["obs-1"]["category"] == "science_fiction"
    assert rows["obs-1"]["condition"] == "VERY_GOOD"
    assert rows["obs-2"]["isbn_status"] == "CONVERTED_ISBN10"
    assert rows["obs-2"]["isbn_13"] == "9780804429573"
    assert rows["obs-2"]["condition"] == "VERY_GOOD"


def test_exact_duplicates_are_removed(make_bronze, empty_metadata):
    result = silver.build_observations(make_bronze([obs(), obs(), obs()]), empty_metadata)
    assert result.evaluated_count == 1
    assert result.valid.count() == 1


def test_business_duplicate_keeps_newest_observation(make_bronze, empty_metadata):
    bronze = make_bronze(
        [
            obs(observation_id="old", observed_at="2026-10-20T06:00:00Z", listing_price=9.0),
            obs(observation_id="new", observed_at="2026-10-20T09:00:00Z", listing_price=11.0),
            # Same book, different condition: a different business key, so it is kept.
            obs(observation_id="other", condition="ACCEPTABLE"),
        ]
    )
    result = silver.build_observations(bronze, empty_metadata)

    assert sorted(r["observation_id"] for r in result.valid.collect()) == ["new", "other"]
    assert [r["observation_id"] for r in result.superseded.collect()] == ["old"]


def test_invalid_newer_record_does_not_shadow_valid_older_one(make_bronze, empty_metadata):
    bronze = make_bronze(
        [
            obs(observation_id="old-valid", observed_at="2026-10-20T06:00:00Z"),
            obs(observation_id="new-bad", observed_at="2026-10-20T09:00:00Z", listing_price=-1.0),
        ]
    )
    result = silver.build_observations(bronze, empty_metadata)

    assert [r["observation_id"] for r in result.valid.collect()] == ["old-valid"]
    assert [r["observation_id"] for r in result.rejected.collect()] == ["new-bad"]


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"listing_price": -3.5}, "negative_price"),
        ({"estimated_market_price": -1.0}, "negative_price"),
        ({"isbn": "9780306406158"}, "invalid_isbn"),
        ({"isbn": "12345"}, "invalid_isbn"),
        ({"title": "   "}, "blank_title"),
        ({"title": None}, "blank_title"),
        ({"inventory_count": -4}, "invalid_inventory"),
        ({"inventory_count": 1_000_000}, "invalid_inventory"),
        ({"condition": "MINT"}, "invalid_condition"),
        ({"listing_price": None}, "missing_price"),
        ({"category": " "}, "missing_category"),
        ({"observed_at": "yesterday"}, "invalid_observed_at"),
        ({"observation_id": ""}, "missing_observation_id"),
    ],
)
def test_invalid_rows_are_routed_to_rejected(make_bronze, empty_metadata, overrides, reason):
    result = silver.build_observations(
        make_bronze([obs(), obs(**{"observation_id": "bad", **overrides})]), empty_metadata
    )
    [rejected] = result.rejected.collect()

    assert reason in rejected["rejection_reasons"]
    assert reason in rejected["rejection_reason"]
    assert result.valid.count() == 1


def test_row_failing_several_rules_keeps_every_reason(make_bronze, empty_metadata):
    bad = obs(observation_id="bad", isbn="x", title="", listing_price=-1.0)
    [row] = silver.build_observations(make_bronze([bad]), empty_metadata).rejected.collect()
    assert row["rejection_reasons"] == ["invalid_isbn", "blank_title", "negative_price"]
    assert row["rejection_reason"] == "invalid_isbn;blank_title;negative_price"


def test_malformed_record_gets_single_reason_and_unknown_category(make_bronze, empty_metadata):
    corrupt = {c: None for c in OBSERVATION_DATA_COLUMNS}
    corrupt["_corrupt_record"] = '{"observation_id": "broken", '
    [row] = silver.build_observations(make_bronze([corrupt]), empty_metadata).rejected.collect()
    assert row["rejection_reasons"] == ["malformed_record"]
    assert row["category"] == silver.UNKNOWN_CATEGORY
    assert row["raw_record"].startswith('{"observation_id"')


def test_metadata_join_and_quality_flags(spark, make_bronze):
    metadata = spark.createDataFrame(
        [(VALID_ISBN, "/works/OL1W", 1999, "Example Book")],
        "isbn_13 string, work_key string, first_publish_year long, metadata_title string",
    )
    other_isbn = "9780804429573"
    bronze = make_bronze([obs(), obs(observation_id="obs-2", isbn=other_isbn, listing_price=30.0)])
    rows = {r["isbn_13"]: r for r in silver.build_observations(bronze, metadata).valid.collect()}

    assert rows[VALID_ISBN]["work_key"] == "/works/OL1W"
    assert rows[VALID_ISBN]["metadata_matched"] is True
    assert rows[VALID_ISBN]["quality_flags"] == []
    assert rows[other_isbn]["metadata_matched"] is False
    assert rows[other_isbn]["quality_flags"] == ["no_metadata_match", "listing_far_above_market"]


def test_book_metadata_dedups_isbn10_and_13_to_newest_ingestion(spark):
    bronze_isbns = spark.createDataFrame(
        [
            ("/works/A", "Old", "X", 1990, "fantasy", date(2026, 10, 1), "r1", VALID_ISBN),
            ("/works/B", "New", "X", 1990, "fantasy", date(2026, 10, 19), "r2", "0306406152"),
            ("/works/C", "Junk", "X", 1990, "fantasy", date(2026, 10, 19), "r2", "not-isbn"),
        ],
        "work_key string, title string, author_names string, first_publish_year long, "
        "subject string, ingestion_date date, run_id string, isbn_raw string",
    ).selectExpr("*", "DATE'2026-10-20' AS processing_date")

    rows = silver.build_book_metadata(bronze_isbns).collect()

    assert [(r["isbn_13"], r["work_key"]) for r in rows] == [(VALID_ISBN, "/works/B")]
