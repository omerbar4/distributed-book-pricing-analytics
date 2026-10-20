from __future__ import annotations

from datetime import date

import pytest

from src.transforms import gold, quality

DAY = date(2026, 10, 20)
VALID_SCHEMA = (
    "isbn_13 string, title string, author string, category string, condition string, "
    "listing_price double, estimated_market_price double, inventory_count long, "
    "processing_date date"
)


def _valid(spark, rows):
    return spark.createDataFrame(
        [
            ("978000000000" + str(i), f"T{i}", "A", cat, "GOOD", lp, mp, inv, DAY)
            for i, (cat, lp, mp, inv) in enumerate(rows)
        ],
        VALID_SCHEMA,
    )


@pytest.mark.parametrize(
    ("inventory", "market", "expected_price", "expected_reason"),
    [
        (15, 20.00, 19.00, "HIGH_INVENTORY_5PCT_BELOW_MARKET"),
        (10, 20.00, 19.00, "HIGH_INVENTORY_5PCT_BELOW_MARKET"),  # threshold is inclusive
        (2, 20.00, 21.00, "LOW_INVENTORY_5PCT_ABOVE_MARKET"),
        (0, 20.00, 21.00, "LOW_INVENTORY_5PCT_ABOVE_MARKET"),
        (5, 20.00, 20.00, "AT_MARKET"),
        (5, 12.345, 12.35, "AT_MARKET"),  # rounded to cents
        (15, 0.50, 1.00, "HIGH_INVENTORY_5PCT_BELOW_MARKET+PRICE_FLOOR"),
        (5, 0.00, 1.00, "AT_MARKET+PRICE_FLOOR"),
    ],
)
def test_pricing_rules(spark, inventory, market, expected_price, expected_reason):
    [row] = gold.build_recommendations(
        _valid(spark, [("fantasy", 10.0, market, inventory)])
    ).collect()
    assert row["recommended_price"] == pytest.approx(expected_price)
    assert row["recommendation_reason"] == expected_reason
    assert row["price_change"] == pytest.approx(round(expected_price - 10.0, 2))
    assert row["data_origin"] == "SIMULATED_DEMO"


def test_category_summary_includes_median_and_rejected_only_categories(spark):
    recs = gold.build_recommendations(
        _valid(
            spark,
            [
                ("fantasy", 10.0, 10.0, 5),
                ("fantasy", 20.0, 20.0, 5),
                ("fantasy", 60.0, 60.0, 5),
                ("history", 8.0, 8.0, 1),
            ],
        )
    )
    rejected = spark.createDataFrame(
        [("fantasy",), ("business",), ("business",)], "category string"
    )
    rows = {r["category"]: r for r in gold.build_category_summary(recs, rejected, DAY).collect()}

    assert rows["fantasy"]["valid_book_count"] == 3
    assert rows["fantasy"]["avg_listing_price"] == pytest.approx(30.0)
    assert rows["fantasy"]["median_listing_price"] == pytest.approx(20.0)
    assert rows["fantasy"]["total_inventory"] == 15
    assert rows["fantasy"]["invalid_record_count"] == 1
    assert rows["history"]["invalid_record_count"] == 0
    assert rows["business"]["valid_book_count"] == 0
    assert rows["business"]["invalid_record_count"] == 2
    assert rows["business"]["avg_listing_price"] is None


def _metrics(**overrides):
    args = {
        "processing_date": DAY,
        "input_count": 100,
        "evaluated_count": 98,  # 2 exact duplicates
        "rejected_count": 8,
        "business_duplicate_count": 3,
        "valid_count": 87,
        "reason_counts": {"invalid_isbn": 3, "negative_price": 2, "missing_price": 4},
        "min_pass_rate": 0.8,
    }
    args.update(overrides)
    return quality.compute_quality_metrics(**args)


def test_quality_metrics():
    m = _metrics()
    assert m["exact_duplicate_count"] == 2
    assert m["duplicate_count"] == 5
    assert m["invalid_isbn_count"] == 3
    assert m["blank_title_count"] == 0
    assert m["other_rule_violation_count"] == 4
    assert m["quality_pass_rate"] == pytest.approx(0.9184)  # 90 / 98
    assert m["quality_gate_passed"] is True


def test_quality_gate_fails_below_threshold():
    m = _metrics(rejected_count=40, valid_count=55, min_pass_rate=0.8)
    assert m["quality_pass_rate"] == pytest.approx(0.5918)
    assert m["quality_gate_passed"] is False


def test_row_accounting_mismatch_is_an_error():
    with pytest.raises(ValueError, match="accounting"):
        _metrics(valid_count=50)


def test_quality_report_dataframe_matches_schema(spark):
    [row] = quality.build_quality_report(spark, _metrics()).collect()
    assert row["processing_date"] == DAY
    assert row["input_record_count"] == 100
