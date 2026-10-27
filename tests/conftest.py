import os
import sys
import time
from collections.abc import Iterator
from dataclasses import replace

import pytest
from pyspark.sql import SparkSession

from src.config import Settings, get_settings
from src.spark_session import build_spark_session


def pytest_configure(config: pytest.Config) -> None:
    os.environ["TZ"] = "UTC"
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    time.tzset()


@pytest.fixture(scope="session")
def settings(tmp_path_factory: pytest.TempPathFactory) -> Settings:
    return replace(
        get_settings(),
        data_path=tmp_path_factory.mktemp("data"),
        lake_format="parquet",
        spark_master="local[2]",
        spark_driver_memory="1g",
        spark_shuffle_partitions=2,
        spark_event_log_dir=None,
    )


@pytest.fixture(scope="session")
def spark(settings: Settings) -> Iterator[SparkSession]:
    session = build_spark_session("tests", settings)
    yield session
    session.stop()
