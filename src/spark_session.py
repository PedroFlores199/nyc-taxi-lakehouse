from collections.abc import Iterator, Sequence
from contextlib import contextmanager

from pyspark.sql import SparkSession

from src.config import Settings, get_settings

MONGO_CONNECTOR_PACKAGE = "org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"
DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.1"


def build_spark_session(
    app_name: str,
    settings: Settings | None = None,
    packages: Sequence[str] = (),
) -> SparkSession:
    settings = settings or get_settings()
    jars = list(packages)
    builder = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if settings.lake_format == "delta":
        jars.append(DELTA_PACKAGE)
        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    if jars:
        builder = builder.config("spark.jars.packages", ",".join(jars))
    if settings.spark_event_log_dir:
        builder = builder.config("spark.eventLog.enabled", "true").config(
            "spark.eventLog.dir", settings.spark_event_log_dir
        )
    return builder.getOrCreate()


@contextmanager
def spark_session(
    app_name: str,
    settings: Settings | None = None,
    packages: Sequence[str] = (),
) -> Iterator[SparkSession]:
    session = build_spark_session(app_name, settings, packages)
    try:
        yield session
    finally:
        session.stop()
