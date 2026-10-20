"""CLI: fetch book metadata by subject from Open Library and land it as raw JSON.

Example::

    python -m src.ingestion.fetch_books --subject science_fiction --limit 100
    python -m src.ingestion.fetch_books --all-subjects --limit 40 --ingestion-date 2026-10-20
"""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Sequence
from datetime import UTC, date, datetime

from src.common.config import Settings
from src.common.logging import configure_logging, get_logger
from src.ingestion.open_library_client import OpenLibraryClient, OpenLibraryError
from src.ingestion.raw_store import validate_subject, write_raw_response

DEFAULT_SUBJECTS = ("science_fiction", "fantasy", "history", "technology", "business")
MAX_LIMIT = 1000

log = get_logger(__name__)


def _subject(value: str) -> str:
    try:
        return validate_subject(value.strip().lower())
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _limit(value: str) -> int:
    limit = int(value)
    if not 1 <= limit <= MAX_LIMIT:
        raise argparse.ArgumentTypeError(f"--limit must be between 1 and {MAX_LIMIT}")
    return limit


def parse_args(argv: Sequence[str] | None, settings: Settings) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m src.ingestion.fetch_books",
        description="Fetch Open Library search results per subject into the raw landing zone.",
    )
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument(
        "--subject",
        dest="subjects",
        action="append",
        type=_subject,
        help="subject/category key, e.g. science_fiction (repeatable)",
    )
    which.add_argument(
        "--all-subjects",
        action="store_true",
        help=f"fetch the default set: {', '.join(DEFAULT_SUBJECTS)}",
    )
    parser.add_argument(
        "--limit",
        type=_limit,
        default=settings.ingest_limit,
        help="max works per subject (default: INGEST_LIMIT or 50)",
    )
    parser.add_argument(
        "--ingestion-date",
        type=date.fromisoformat,
        default=datetime.now(UTC).date(),
        help="partition date for the landing zone (default: today, UTC)",
    )
    parser.add_argument(
        "--pause-seconds",
        type=float,
        default=1.0,
        help="pause between subjects to stay polite to the public API",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    args = parse_args(argv, settings)
    subjects = list(DEFAULT_SUBJECTS) if args.all_subjects else list(dict.fromkeys(args.subjects))

    succeeded: list[str] = []
    failed: list[str] = []
    with OpenLibraryClient(
        base_url=settings.open_library_base_url,
        user_agent=settings.http_user_agent,
        timeout_seconds=settings.http_timeout_seconds,
        max_retries=settings.http_max_retries,
    ) as client:
        for index, subject in enumerate(subjects):
            if index and args.pause_seconds > 0:
                time.sleep(args.pause_seconds)
            try:
                response = client.search_by_subject(subject, args.limit)
                artifact = write_raw_response(
                    settings.data_dir, subject, args.ingestion_date, response
                )
            except OpenLibraryError as exc:
                log.error("ingestion failed", extra={"subject": subject, "error": str(exc)})
                failed.append(subject)
                continue
            log.info(
                "raw response landed",
                extra={
                    "subject": subject,
                    "run_id": artifact.run_id,
                    "records": response.record_count,
                    "elapsed_ms": response.elapsed_ms,
                    "path": str(artifact.response_path),
                },
            )
            succeeded.append(subject)

    print(
        json.dumps(
            {
                "ingestion_date": args.ingestion_date.isoformat(),
                "succeeded": succeeded,
                "failed": failed,
            }
        )
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
