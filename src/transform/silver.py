import json
import logging
from collections.abc import Iterable
from datetime import date
from pathlib import Path

import holidays
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, Row, SparkSession, Window
from pyspark.sql import functions as F

from src.config import Settings
from src.domain import TIME_SLOTS, YearMonth
from src.lake import (
    SILVER_TRIPS,
    ZONE_SHAPEFILE_FILE,
    bronze_trip_file,
    bronze_zone_file,
    read_zone_lookup,
    silver_table,
    write_table,
    zone_geojson_file,
)
from src.quality.checks import validate_silver
from src.transform.zones import (
    shapefile_to_feature_collection,
    standardize_zones,
    write_feature_collection,
)

logger = logging.getLogger(__name__)

RAW_COLUMNS: dict[str, tuple[str, str]] = {
    "VendorID": ("vendor_id", "int"),
    "tpep_pickup_datetime": ("pickup_datetime", "timestamp"),
    "tpep_dropoff_datetime": ("dropoff_datetime", "timestamp"),
    "passenger_count": ("passenger_count", "int"),
    "trip_distance": ("trip_distance", "double"),
    "PULocationID": ("pickup_location_id", "int"),
    "DOLocationID": ("dropoff_location_id", "int"),
    "payment_type": ("payment_type", "int"),
    "fare_amount": ("fare_amount", "double"),
    "tip_amount": ("tip_amount", "double"),
    "tolls_amount": ("tolls_amount", "double"),
    "total_amount": ("total_amount", "double"),
}

TRIP_ID_COLUMNS = (
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "trip_distance",
    "total_amount",
)

MAX_DURATION_MINUTES = 360
MAX_DISTANCE_MILES = 100
MAX_PASSENGERS = 6

REASON_NULL_KEYS = "claves_nulas"
REASON_OUT_OF_MONTH = "fecha_fuera_de_mes"
REASON_DURATION = "duracion_invalida"
REASON_DISTANCE = "distancia_invalida"
REASON_AMOUNT = "importe_negativo"
REASON_PASSENGERS = "pasajeros_invalidos"
REASON_DUPLICATE = "duplicado"


def standardize_columns(df: DataFrame) -> DataFrame:
    return df.select(
        [F.col(source).cast(dtype).alias(target) for source, (target, dtype) in RAW_COLUMNS.items()]
    )


def with_trip_id(df: DataFrame) -> DataFrame:
    return df.withColumn("trip_id", F.xxhash64(*[F.col(column) for column in TRIP_ID_COLUMNS]))


def duration_minutes() -> Column:
    return (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60.0


def rejection_rules(period: YearMonth) -> list[tuple[str, Column]]:
    month_start = F.lit(period.first_day).cast("timestamp")
    next_month_start = F.lit(period.next.first_day).cast("timestamp")
    duration = duration_minutes()
    return [
        (
            REASON_NULL_KEYS,
            F.col("pickup_datetime").isNull()
            | F.col("dropoff_datetime").isNull()
            | F.col("pickup_location_id").isNull()
            | F.col("dropoff_location_id").isNull(),
        ),
        (
            REASON_OUT_OF_MONTH,
            (F.col("pickup_datetime") < month_start) | (F.col("pickup_datetime") >= next_month_start),
        ),
        (REASON_DURATION, (duration <= 0) | (duration > MAX_DURATION_MINUTES)),
        (
            REASON_DISTANCE,
            F.col("trip_distance").isNull()
            | (F.col("trip_distance") <= 0)
            | (F.col("trip_distance") > MAX_DISTANCE_MILES),
        ),
        (
            REASON_AMOUNT,
            F.col("fare_amount").isNull()
            | F.col("total_amount").isNull()
            | (F.col("fare_amount") < 0)
            | (F.col("total_amount") < 0),
        ),
        (
            REASON_PASSENGERS,
            F.col("passenger_count").isNull()
            | (F.col("passenger_count") == 0)
            | (F.col("passenger_count") > MAX_PASSENGERS),
        ),
    ]


def tag_rejections(df: DataFrame, period: YearMonth) -> DataFrame:
    reason = F.lit(None).cast("string")
    for name, condition in reversed(rejection_rules(period)):
        reason = F.when(condition, F.lit(name)).otherwise(reason)
    occurrence = Window.partitionBy("trip_id", "rejection_reason").orderBy("pickup_datetime")
    return df.withColumn("rejection_reason", reason).withColumn(
        "rejection_reason",
        F.when(
            F.col("rejection_reason").isNull() & (F.row_number().over(occurrence) > 1),
            F.lit(REASON_DUPLICATE),
        ).otherwise(F.col("rejection_reason")),
    )


def split_valid_rejected(tagged: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = tagged.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")
    rejected = tagged.filter(F.col("rejection_reason").isNotNull())
    return valid, rejected


def holidays_for(years: Iterable[int]) -> list[date]:
    return sorted(holidays.country_holidays("US", subdiv="NY", years=sorted(set(years))))


def time_slot(hour: Column) -> Column:
    slot = F.lit(None).cast("string")
    for name, start, end in reversed(TIME_SLOTS):
        slot = F.when(hour.between(start, end), F.lit(name)).otherwise(slot)
    return slot


def add_derived_columns(df: DataFrame, holiday_dates: Iterable[date]) -> DataFrame:
    duration = duration_minutes()
    hour = F.hour("pickup_datetime")
    return (
        df.withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("year_month", F.date_format("pickup_datetime", "yyyy-MM"))
        .withColumn("pickup_hour", hour)
        .withColumn("duration_minutes", F.round(duration, 2))
        .withColumn(
            "avg_speed_mph",
            F.when(duration > 0, F.round(F.col("trip_distance") / (duration / 60.0), 2)),
        )
        .withColumn(
            "tip_pct",
            F.when(
                F.col("fare_amount") > 0,
                F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2),
            ),
        )
        .withColumn("time_slot", time_slot(hour))
        .withColumn("day_of_week", ((F.dayofweek("pickup_datetime") + 5) % 7) + 1)
        .withColumn("is_holiday", F.col("pickup_date").isin(list(holiday_dates)))
    )


def join_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    pickup = zones.select(
        F.col("location_id").alias("pickup_location_id"),
        F.col("zone").alias("pickup_zone"),
        F.col("borough").alias("pickup_borough"),
    )
    dropoff = zones.select(
        F.col("location_id").alias("dropoff_location_id"),
        F.col("zone").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough"),
    )
    return trips.join(F.broadcast(pickup), "pickup_location_id", "left").join(
        F.broadcast(dropoff), "dropoff_location_id", "left"
    )


def build_silver(raw: DataFrame, zones: DataFrame, period: YearMonth) -> tuple[DataFrame, DataFrame]:
    tagged = tag_rejections(with_trip_id(standardize_columns(raw)), period)
    valid, _ = split_valid_rejected(tagged)
    enriched = join_zones(add_derived_columns(valid, holidays_for([period.year])), standardize_zones(zones))
    return enriched, tagged


def rejection_counts(tagged: DataFrame) -> DataFrame:
    return tagged.groupBy("rejection_reason").count()


def summarize_rejections(rows: Iterable[Row], period: YearMonth) -> dict:
    counts = {row["rejection_reason"]: int(row["count"]) for row in rows}
    valid = counts.pop(None, 0)
    discarded = sum(counts.values())
    return {
        "mes": str(period),
        "filas_leidas": valid + discarded,
        "filas_validas": valid,
        "filas_descartadas": discarded,
        "motivos": dict(sorted(counts.items(), key=lambda item: (-item[1], item[0]))),
    }


def write_report(report: dict, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return destination


def ensure_zone_geojson(settings: Settings) -> Path:
    destination = zone_geojson_file(settings)
    if destination.exists():
        return destination
    collection = shapefile_to_feature_collection(bronze_zone_file(settings, ZONE_SHAPEFILE_FILE))
    return write_feature_collection(collection, destination)


def run_silver(spark: SparkSession, settings: Settings, period: YearMonth) -> dict:
    raw = spark.read.parquet(str(bronze_trip_file(settings, period)))
    silver, tagged = build_silver(raw, read_zone_lookup(spark, settings), period)
    tagged.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        report = summarize_rejections(rejection_counts(tagged).collect(), period)
        write_report(report, settings.reports_path / "silver" / f"{period}.json")
        logger.info("Informe de descartes %s: %s", period, report)
        validate_silver(silver)
        write_table(
            silver.repartition("pickup_date").sortWithinPartitions("pickup_datetime"),
            silver_table(settings, SILVER_TRIPS),
            settings,
            partition_by=("pickup_date",),
        )
    finally:
        tagged.unpersist()
    ensure_zone_geojson(settings)
    return report
