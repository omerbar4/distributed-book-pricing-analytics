"""Integration tests for the PostgreSQL loader.

They need a running PostgreSQL (``make up`` locally; a service container in CI) and are
skipped otherwise. They use a processing date far from any demo date and clean it up.
"""

from __future__ import annotations

import os
from datetime import date

import psycopg
import pytest

from src.common.config import Settings
from src.common.paths import DataLayout
from src.generators.generate_observations import generate
from src.jobs.book_pricing_pipeline import run_pipeline
from src.loaders.postgres_loader import (
    TABLES,
    QualityGateError,
    load_processing_date,
)
from src.transforms.storage import read_date_partition
from tests.fixtures import land_synthetic_raw

pytestmark = pytest.mark.integration

TEST_DATE = date(1999, 12, 31)


@pytest.fixture(scope="module")
def conninfo():
    conninfo = Settings.from_env().postgres_conninfo
    try:
        with psycopg.connect(conninfo, connect_timeout=3):
            pass
    except psycopg.OperationalError as exc:
        message = f"PostgreSQL not reachable ({exc.__class__.__name__}); run `make up`"
        if os.environ.get("REQUIRE_POSTGRES") == "1":  # CI: never skip silently
            pytest.fail(message)
        pytest.skip(message)
    yield conninfo
    with psycopg.connect(conninfo) as conn:
        for spec in TABLES:
            conn.execute(
                f"DELETE FROM analytics.{spec.table} WHERE processing_date = %s", (TEST_DATE,)
            )


def _counts(conninfo):
    with psycopg.connect(conninfo) as conn:
        return {
            spec.table: conn.execute(
                f"SELECT count(*) FROM analytics.{spec.table} WHERE processing_date = %s",
                (TEST_DATE,),
            ).fetchone()[0]
            for spec in TABLES
        }


@pytest.fixture
def gold_outputs(spark, tmp_path):
    land_synthetic_raw(tmp_path, TEST_DATE, {"fantasy": 20, "history": 15})
    generate(tmp_path, TEST_DATE, seed=42)
    layout = DataLayout(tmp_path)
    run_pipeline(spark, layout, TEST_DATE)
    return layout


def test_load_matches_parquet_and_rerun_is_idempotent(spark, conninfo, gold_outputs):
    layout = gold_outputs
    first = load_processing_date(spark, layout, conninfo, TEST_DATE)
    counts_after_first = _counts(conninfo)

    expected_recs = read_date_partition(
        spark, layout.dataset("gold", "daily_pricing_recommendations"), TEST_DATE
    ).count()
    assert counts_after_first["daily_pricing_recommendations"] == expected_recs
    assert counts_after_first["data_quality_report"] == 1
    assert counts_after_first["rejected_records"] == first["rejected_records"]["copied"]

    second = load_processing_date(spark, layout, conninfo, TEST_DATE)

    assert _counts(conninfo) == counts_after_first  # replaced, not duplicated
    assert second["daily_pricing_recommendations"]["deleted"] == expected_recs


def test_reload_replaces_values_for_the_date(spark, conninfo, gold_outputs):
    layout = gold_outputs
    load_processing_date(spark, layout, conninfo, TEST_DATE)

    generate(layout.root, TEST_DATE, seed=7)  # different simulated prices
    run_pipeline(spark, layout, TEST_DATE)
    load_processing_date(spark, layout, conninfo, TEST_DATE)

    parquet_total = (
        read_date_partition(
            spark, layout.dataset("gold", "daily_pricing_recommendations"), TEST_DATE
        )
        .agg({"recommended_price": "sum"})
        .first()[0]
    )
    with psycopg.connect(conninfo) as conn:
        db_total = conn.execute(
            "SELECT sum(recommended_price) FROM analytics.daily_pricing_recommendations "
            "WHERE processing_date = %s",
            (TEST_DATE,),
        ).fetchone()[0]
    assert float(db_total) == pytest.approx(parquet_total, abs=0.01)


def test_types_survive_round_trip(spark, conninfo, gold_outputs):
    load_processing_date(spark, gold_outputs, conninfo, TEST_DATE)
    with psycopg.connect(conninfo) as conn:
        reasons, generated_at = conn.execute(
            """SELECT r.rejection_reasons, q.generated_at
               FROM analytics.rejected_records r
               JOIN analytics.data_quality_report q USING (processing_date)
               WHERE r.processing_date = %s AND r.rejection_reason LIKE 'negative_price%%'
               LIMIT 1""",
            (TEST_DATE,),
        ).fetchone()
    assert reasons == ["negative_price"]
    assert generated_at.tzinfo is not None
    assert generated_at.year >= 2026


def test_failed_quality_gate_blocks_publishing(spark, conninfo, gold_outputs):
    layout = gold_outputs
    load_processing_date(spark, layout, conninfo, TEST_DATE)
    before = _counts(conninfo)

    run_pipeline(spark, layout, TEST_DATE, min_pass_rate=0.999)
    with pytest.raises(QualityGateError, match="refusing to publish"):
        load_processing_date(spark, layout, conninfo, TEST_DATE)
    assert _counts(conninfo) == before

    load_processing_date(spark, layout, conninfo, TEST_DATE, allow_failed_quality=True)
    with psycopg.connect(conninfo) as conn:
        passed = conn.execute(
            "SELECT quality_gate_passed FROM analytics.data_quality_report "
            "WHERE processing_date = %s",
            (TEST_DATE,),
        ).fetchone()[0]
    assert passed is False
