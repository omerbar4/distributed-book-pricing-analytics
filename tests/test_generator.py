from __future__ import annotations

import json
from datetime import date

import pytest

from src.common import isbn as isbn_lib
from src.generators.generate_observations import SOURCE, generate, observations_dir
from tests.fixtures import land_synthetic_raw

DAY = date(2026, 10, 20)


@pytest.fixture
def landed(tmp_path):
    land_synthetic_raw(tmp_path, DAY, {"fantasy": 6, "history": 5})
    return tmp_path


def _lines(data_dir):
    return (observations_dir(data_dir, DAY) / "observations.jsonl").read_text().splitlines()


def test_same_seed_produces_identical_output(landed):
    generate(landed, DAY, seed=42)
    first = _lines(landed)
    generate(landed, DAY, seed=42)
    assert _lines(landed) == first


def test_different_seed_changes_prices(landed):
    generate(landed, DAY, seed=42)
    first = _lines(landed)
    generate(landed, DAY, seed=7)
    assert _lines(landed) != first


def test_records_are_labeled_simulated_and_manifest_counts_match(landed):
    manifest = generate(landed, DAY, seed=42, defects_per_type=2)

    lines = _lines(landed)
    parsed = [json.loads(line) for line in lines if line.endswith("}")]
    assert manifest["data_origin"] == "SIMULATED_DEMO"
    assert {r["source"] for r in parsed} == {SOURCE}
    assert manifest["books_per_category"] == {"fantasy": 6, "history": 5}
    assert manifest["total_lines"] == len(lines)
    assert manifest["injected_defects"] == {
        "negative_price": 2,
        "invalid_isbn": 2,
        "blank_title": 2,
        "impossible_inventory": 2,
        "exact_duplicate": 2,
        "stale_business_duplicate": 2,
        "malformed_json": 2,
    }
    assert len(lines) == manifest["clean_observation_count"] + 7 * 2


def test_injected_defects_are_present(landed):
    generate(landed, DAY, seed=42, defects_per_type=2)
    lines = _lines(landed)
    parsed = [json.loads(line) for line in lines if line.endswith("}")]

    assert sum(r["listing_price"] < 0 for r in parsed) == 2
    assert sum(not (r["title"] or "").strip() for r in parsed) == 2
    assert sum(not 0 <= r["inventory_count"] <= 10_000 for r in parsed) == 2
    assert sum(isbn_lib.normalize_isbn(r["isbn"]).isbn_13 is None for r in parsed) == 2
    assert len(parsed) - len({json.dumps(r, sort_keys=True) for r in parsed}) == 2
    malformed = [line for line in lines if not line.endswith("}")]
    assert len(malformed) == 2


def test_no_defects_mode_is_all_valid(landed):
    manifest = generate(landed, DAY, seed=42, defects_per_type=0)
    assert manifest["total_lines"] == manifest["clean_observation_count"]


def test_missing_raw_input_fails_clearly(tmp_path):
    with pytest.raises(FileNotFoundError, match="fetch_books"):
        generate(tmp_path, DAY, seed=42)


def test_future_ingestions_are_ignored(tmp_path):
    land_synthetic_raw(tmp_path, date(2026, 10, 21), {"fantasy": 3})
    with pytest.raises(FileNotFoundError):
        generate(tmp_path, DAY, seed=42)
