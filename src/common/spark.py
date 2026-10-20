"""Single place that builds the SparkSession, so jobs and tests share identical config."""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession


def build_spark_session(
    app_name: str,
    master: str | None = None,
    shuffle_partitions: int = 4,
    ui_enabled: bool = True,
) -> SparkSession:
    """Build (or reuse) the session.

    ``master=None`` leaves the master to ``spark-submit --master`` (default ``local[*]``),
    so the same job runs unchanged on YARN/Kubernetes.
    """
    # Python workers otherwise launch whatever `python3` is first on PATH, which can be a
    # different version than the driver (Spark refuses to run on a mismatch).
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    # The default of 200 shuffle partitions is tuned for clusters; locally it only
    # produces hundreds of tiny tasks and files. Override via SPARK_SHUFFLE_PARTITIONS.
    return (
        builder.config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.ui.enabled", str(ui_enabled).lower())
        .getOrCreate()
    )
