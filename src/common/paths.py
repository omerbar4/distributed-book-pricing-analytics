"""Directory layout of the local data lake (medallion layers)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class DataLayout:
    root: Path

    @property
    def raw_open_library(self) -> Path:
        return self.root / "raw" / "open_library"

    def raw_observations(self, processing_date: date) -> Path:
        return self.root / "raw" / "demo_observations" / f"processing_date={processing_date}"

    def dataset(self, layer: str, name: str) -> Path:
        """Base path of a Parquet dataset, e.g. ``gold/daily_pricing_recommendations``."""
        return self.root / layer / name
