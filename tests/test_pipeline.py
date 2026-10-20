"""Whole-job test: synthetic raw landing -> generator -> Spark job -> Parquet outputs."""

from __future__ import annotations

from datetime import date

import pytest

from src.common.paths import DataLayout
from src.generators.generate_observations import generate
from src.jobs.book_pricing_pipeline import run_pipeline
from src.transforms.storage import read_date_partition
from tests.fixtures import land_synthetic_raw

DAY = date(2026, 10, 20)


@pytest.fixture
def pipeline_run(spark, tmp_path):
    land_synthetic_raw(tmp_path, DAY, {"fantasy": 20, "history": 15, "technology": 15})
    manifest = generate(tmp_path, DAY, seed=42, defects_per_type=2)
    layout = DataLayout(tmp_path)
    summary = run_pipeline(spark, layout, DAY)
    return layout, manifest, summary


def test_quality_report_matches_generator_ground_truth(spark, pipeline_run):
    layout, manifest, summary = pipeline_run
    injected = manifest["injected_defects"]

    [report] = read_date_partition(
        spark, layout.dataset("gold", "data_quality_report"), DAY
    ).collect()

    assert report["input_record_count"] == manifest["total_lines"]
    assert report["valid_record_count"] == manifest["clean_observation_count"]
    assert report["exact_duplicate_count"] == injected["exact_duplicate"]
    assert report["business_duplicate_count"] == injected["stale_business_duplicate"]
    assert report["negative_price_count"] == injected["negative_price"]
    assert report["invalid_isbn_count"] == injected["invalid_isbn"]
    assert report["blank_title_count"] == injected["blank_title"]
    assert report["invalid_inventory_count"] == injected["impossible_inventory"]
    assert report["malformed_record_count"] == injected["malformed_json"]
    assert report["other_rule_violation_count"] == 0
    assert report["quality_pass_rate"] >= 0.8
    assert report["quality_gate_passed"] is True
    assert summary["recommendation_rows"] == manifest["clean_observation_count"]


def test_outputs_are_partitioned_by_date_and_category(pipeline_run):
    layout, manifest, _ = pipeline_run
    recs_dir = layout.dataset("gold", "daily_pricing_recommendations") / f"processing_date={DAY}"

    categories = sorted(p.name for p in recs_dir.glob("category=*"))
    assert categories == ["category=fantasy", "category=history", "category=technology"]
    assert all(list(p.glob("*.parquet")) for p in recs_dir.glob("category=*"))
    for layer, name in [
        ("bronze", "observations"),
        ("bronze", "book_isbns"),
        ("silver", "observations"),
        ("silver", "rejected_observations"),
        ("silver", "book_metadata"),
        ("gold", "category_pricing_summary"),
        ("gold", "data_quality_report"),
    ]:
        assert list((layout.dataset(layer, name) / f"processing_date={DAY}").glob("*.parquet"))


def test_every_valid_observation_matches_open_library_metadata(spark, pipeline_run):
    layout, _, _ = pipeline_run
    valid = read_date_partition(spark, layout.dataset("silver", "observations"), DAY)
    assert valid.where("NOT metadata_matched").count() == 0


def test_rerun_same_date_is_idempotent(spark, pipeline_run):
    layout, _, first = pipeline_run
    base = layout.dataset("gold", "daily_pricing_recommendations")
    before = sorted(map(tuple, read_date_partition(spark, base, DAY).collect()))

    second = run_pipeline(spark, layout, DAY)
    after = sorted(map(tuple, read_date_partition(spark, base, DAY).collect()))

    assert before == after
    assert {k: v for k, v in second.items()} == first


def test_other_dates_are_untouched(spark, pipeline_run):
    layout, _, _ = pipeline_run
    next_day = date(2026, 10, 21)
    generate(layout.root, next_day, seed=42)
    run_pipeline(spark, layout, next_day)

    base = layout.dataset("gold", "daily_pricing_recommendations")
    assert read_date_partition(spark, base, DAY).count() > 0
    assert read_date_partition(spark, base, next_day).count() > 0
    assert sorted(p.name for p in base.glob("processing_date=*")) == [
        "processing_date=2026-10-20",
        "processing_date=2026-10-21",
    ]


def test_quality_gate_reports_failure_but_still_writes_outputs(spark, pipeline_run):
    layout, _, _ = pipeline_run
    summary = run_pipeline(spark, layout, DAY, min_pass_rate=0.999)

    assert summary["quality_gate_passed"] is False
    [report] = read_date_partition(
        spark, layout.dataset("gold", "data_quality_report"), DAY
    ).collect()
    assert report["quality_gate_passed"] is False
    assert report["min_pass_rate"] == 0.999


def test_missing_input_fails_clearly(spark, tmp_path):
    with pytest.raises(FileNotFoundError, match="generator"):
        run_pipeline(spark, DataLayout(tmp_path), DAY)
