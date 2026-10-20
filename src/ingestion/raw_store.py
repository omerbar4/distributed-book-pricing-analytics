"""Immutable raw landing zone for API responses.

Layout::

    <data_dir>/raw/open_library/ingestion_date=YYYY-MM-DD/subject=<subject>/
        <run_id>.response.json   # API body, byte-for-byte as received
        <run_id>.meta.json       # request/response metadata incl. SHA-256 of the body

Files are created with exclusive-create mode, so an existing file can never be overwritten;
re-running ingestion adds a new run alongside the old one.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from src.ingestion.open_library_client import ApiResponse

SOURCE_NAME = "open_library_search_api"
_SUBJECT_PATTERN = re.compile(r"^[a-z0-9_]{1,64}$")


@dataclass(frozen=True)
class RawArtifact:
    run_id: str
    response_path: Path
    metadata_path: Path


def validate_subject(subject: str) -> str:
    # The subject becomes a directory name, so only allow a safe character set.
    if not _SUBJECT_PATTERN.fullmatch(subject):
        raise ValueError(f"subject must match {_SUBJECT_PATTERN.pattern}, got {subject!r}")
    return subject


def raw_open_library_root(data_dir: Path) -> Path:
    return data_dir / "raw" / "open_library"


def write_raw_response(
    data_dir: Path, subject: str, ingestion_date: date, response: ApiResponse
) -> RawArtifact:
    validate_subject(subject)
    sha256 = hashlib.sha256(response.body).hexdigest()
    run_id = f"{response.fetched_at:%Y%m%dT%H%M%S%f}Z-{sha256[:8]}"
    target_dir = (
        raw_open_library_root(data_dir)
        / f"ingestion_date={ingestion_date.isoformat()}"
        / f"subject={subject}"
    )
    target_dir.mkdir(parents=True, exist_ok=True)

    response_path = target_dir / f"{run_id}.response.json"
    metadata_path = target_dir / f"{run_id}.meta.json"
    with response_path.open("xb") as fh:
        fh.write(response.body)

    metadata = {
        "run_id": run_id,
        "source": SOURCE_NAME,
        "subject": subject,
        "ingestion_date": ingestion_date.isoformat(),
        "request_url": response.url,
        "request_params": response.params,
        "http_status": response.status_code,
        "fetched_at": response.fetched_at.isoformat(),
        "elapsed_ms": response.elapsed_ms,
        "record_count": response.record_count,
        "body_bytes": len(response.body),
        "body_sha256": sha256,
    }
    with metadata_path.open("x", encoding="utf-8") as fh:
        json.dump(metadata, fh, indent=2, sort_keys=True)
        fh.write("\n")

    return RawArtifact(run_id=run_id, response_path=response_path, metadata_path=metadata_path)
