"""CLI: print a short console report of one processing date straight from PostgreSQL.

Used at the end of the demo so the results are visible without installing ``psql``.

    python -m src.loaders.report --processing-date 2026-10-20
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from datetime import date

import psycopg

from src.common.config import Settings

SECTIONS = (
    (
        "Data quality",
        """SELECT input_record_count AS input, valid_record_count AS valid,
                  rejected_record_count AS rejected, exact_duplicate_count AS exact_dups,
                  business_duplicate_count AS business_dups, invalid_isbn_count AS bad_isbn,
                  negative_price_count AS neg_price, blank_title_count AS blank_title,
                  invalid_inventory_count AS bad_inventory, malformed_record_count AS malformed,
                  round(quality_pass_rate::numeric, 4) AS pass_rate, quality_gate_passed AS gate
           FROM analytics.data_quality_report WHERE processing_date = %(d)s""",
    ),
    (
        "Category pricing summary",
        """SELECT category, valid_book_count AS books, valid_listing_count AS listings,
                  avg_listing_price AS avg_list, median_listing_price AS median_list,
                  avg_recommended_price AS avg_recommended, total_inventory AS inventory,
                  invalid_record_count AS invalid
           FROM analytics.category_pricing_summary WHERE processing_date = %(d)s
           ORDER BY category""",
    ),
    (
        "Top 5 recommended prices",
        """SELECT isbn_13, left(title, 32) AS title, category, condition,
                  current_listing_price AS listing, recommended_price AS recommended,
                  recommendation_reason AS reason
           FROM analytics.daily_pricing_recommendations WHERE processing_date = %(d)s
           ORDER BY recommended_price DESC, isbn_13 LIMIT 5""",
    ),
    (
        "Rejected rows by rule",
        """SELECT reason, count(*) AS rows
           FROM analytics.rejected_records, unnest(rejection_reasons) AS reason
           WHERE processing_date = %(d)s GROUP BY reason ORDER BY rows DESC, reason""",
    ),
)


def _table(headers: list[str], rows: list[tuple]) -> str:
    cells = [headers, *[["" if v is None else str(v) for v in row] for row in rows]]
    widths = [max(len(r[i]) for r in cells) for i in range(len(headers))]
    lines = ["  ".join(v.ljust(w) for v, w in zip(r, widths, strict=True)) for r in cells]
    lines.insert(1, "  ".join("-" * w for w in widths))
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Print a console report for one date")
    parser.add_argument("--processing-date", type=date.fromisoformat, required=True)
    args = parser.parse_args(argv)

    settings = Settings.from_env()
    print(f"Processing date {args.processing_date} (prices are SIMULATED demo values)")
    with psycopg.connect(settings.postgres_conninfo, connect_timeout=10) as conn:
        for title, query in SECTIONS:
            cur = conn.execute(query, {"d": args.processing_date})
            headers = [col.name for col in cur.description]
            print(f"\n== {title} ==\n{_table(headers, cur.fetchall())}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
