from __future__ import annotations

import json
import logging

from pyspark.sql import functions as F

from src.common.config import Settings
from src.common.logging import JsonFormatter


def test_spark_session_runs_dataframe_aggregation(spark):
    df = spark.createDataFrame(
        [("fiction", 10.0), ("fiction", 20.0), ("history", 5.0)],
        ["category", "price"],
    )
    result = {
        row["category"]: row["avg_price"]
        for row in df.groupBy("category").agg(F.avg("price").alias("avg_price")).collect()
    }
    assert result == {"fiction": 15.0, "history": 5.0}
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_settings_read_from_environment(monkeypatch):
    monkeypatch.setenv("DEMO_RANDOM_SEED", "7")
    monkeypatch.setenv("OPEN_LIBRARY_BASE_URL", "https://example.test/")
    monkeypatch.delenv("SPARK_MASTER", raising=False)

    settings = Settings.from_env()

    assert settings.demo_random_seed == 7
    assert settings.open_library_base_url == "https://example.test"
    assert settings.spark_master == "local[*]"


def test_json_formatter_emits_extra_fields():
    record = logging.makeLogRecord(
        {"name": "t", "levelname": "INFO", "msg": "fetched", "subject": "fantasy"}
    )
    payload = json.loads(JsonFormatter().format(record))
    assert payload["message"] == "fetched"
    assert payload["subject"] == "fantasy"
