"""Environment-driven settings shared by every pipeline component."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value or default


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    open_library_base_url: str
    http_user_agent: str
    http_timeout_seconds: float
    http_max_retries: int
    ingest_limit: int
    demo_random_seed: int
    spark_master: str | None
    spark_shuffle_partitions: int
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str
    log_level: str

    @classmethod
    def from_env(cls) -> Settings:
        base_url = _env("OPEN_LIBRARY_BASE_URL", "https://openlibrary.org")
        return cls(
            data_dir=Path(_env("DATA_DIR", "data")),
            open_library_base_url=base_url.rstrip("/"),
            http_user_agent=_env(
                "HTTP_USER_AGENT", "distributed-book-pricing-analytics/0.1 (portfolio project)"
            ),
            http_timeout_seconds=float(_env("HTTP_TIMEOUT_SECONDS", "15")),
            http_max_retries=int(_env("HTTP_MAX_RETRIES", "3")),
            ingest_limit=int(_env("INGEST_LIMIT", "50")),
            demo_random_seed=int(_env("DEMO_RANDOM_SEED", "42")),
            spark_master=os.environ.get("SPARK_MASTER", "").strip() or None,
            spark_shuffle_partitions=int(_env("SPARK_SHUFFLE_PARTITIONS", "4")),
            postgres_host=_env("POSTGRES_HOST", "localhost"),
            postgres_port=int(_env("POSTGRES_PORT", "55432")),
            postgres_db=_env("POSTGRES_DB", "book_analytics"),
            postgres_user=_env("POSTGRES_USER", "analytics"),
            postgres_password=_env("POSTGRES_PASSWORD", "change-me-local-only"),
            log_level=_env("LOG_LEVEL", "INFO").upper(),
        )

    @property
    def postgres_conninfo(self) -> str:
        """libpq keyword/value connection string (password is not logged anywhere)."""
        return (
            f"host={self.postgres_host} port={self.postgres_port} dbname={self.postgres_db} "
            f"user={self.postgres_user} password={self.postgres_password}"
        )
