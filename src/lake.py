from collections.abc import Sequence
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from src.config import Settings
from src.domain import YearMonth

SILVER_TRIPS = "trips"
GOLD_DEMAND = "demand_zone_hour"
GOLD_TOP_ROUTES = "top_routes_borough"
GOLD_TRAVEL_TIME = "travel_time_zones"
GOLD_TRIPS_AGGREGATED = "trips_aggregated"
GOLD_MONTHLY_RANKING = "monthly_zone_ranking"
GOLD_ANOMALIES = "anomalies"

ZONE_LOOKUP_FILE = "taxi_zone_lookup.csv"
ZONE_SHAPEFILE_FILE = "taxi_zones.zip"
ZONE_GEOJSON_FILE = "taxi_zones.geojson"


def bronze_trip_file(settings: Settings, period: YearMonth) -> Path:
    return (
        settings.bronze_path
        / "trips"
        / f"year={period.year}"
        / f"month={period.month:02d}"
        / f"yellow_tripdata_{period}.parquet"
    )


def bronze_zone_file(settings: Settings, file_name: str) -> Path:
    return settings.bronze_path / "zones" / file_name


def zone_geojson_file(settings: Settings) -> Path:
    return settings.silver_path / "zones" / ZONE_GEOJSON_FILE


def silver_table(settings: Settings, name: str) -> Path:
    return settings.silver_path / name


def gold_table(settings: Settings, name: str) -> Path:
    return settings.gold_path / name


def write_table(
    df: DataFrame,
    path: Path,
    settings: Settings,
    partition_by: Sequence[str] = (),
) -> None:
    writer = df.write.mode("overwrite").format(settings.lake_format)
    if settings.lake_format == "delta":
        writer = writer.option("partitionOverwriteMode", "dynamic")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(str(path))


def read_table(
    spark: SparkSession,
    path: Path,
    settings: Settings,
    version: int | None = None,
    timestamp: str | None = None,
) -> DataFrame:
    reader = spark.read.format(settings.lake_format)
    if version is not None or timestamp is not None:
        if settings.lake_format != "delta":
            raise ValueError("El time travel solo está disponible con LAKE_FORMAT=delta")
        if version is not None:
            reader = reader.option("versionAsOf", version)
        else:
            reader = reader.option("timestampAsOf", timestamp)
    return reader.load(str(path))


def table_history(spark: SparkSession, path: Path) -> DataFrame:
    return spark.sql(f"DESCRIBE HISTORY delta.`{path.resolve()}`")


def read_zone_lookup(spark: SparkSession, settings: Settings) -> DataFrame:
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(str(bronze_zone_file(settings, ZONE_LOOKUP_FILE)))
    )
