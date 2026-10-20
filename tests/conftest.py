from __future__ import annotations

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from src.common.spark import build_spark_session


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """One local Spark session for the whole test run (startup costs a few seconds)."""
    session = build_spark_session(
        "pytest", master="local[2]", shuffle_partitions=2, ui_enabled=False
    )
    yield session
    session.stop()
