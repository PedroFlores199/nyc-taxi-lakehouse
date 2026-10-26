from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


US_FEDERAL_HOLIDAYS: set[date] = {
    date(2023, 1, 1),
    date(2023, 1, 2),
    date(2023, 1, 16),
    date(2023, 2, 20),
    date(2023, 5, 29),
    date(2023, 6, 19),
    date(2023, 7, 4),
    date(2023, 9, 4),
    date(2023, 10, 9),
    date(2023, 11, 10),
    date(2023, 11, 23),
    date(2023, 12, 25),
    date(2024, 1, 1),
    date(2024, 1, 15),
    date(2024, 2, 19),
    date(2024, 5, 27),
    date(2024, 6, 19),
    date(2024, 7, 4),
    date(2024, 9, 2),
    date(2024, 10, 14),
    date(2024, 11, 11),
    date(2024, 11, 28),
    date(2024, 12, 25),
}

HOLIDAY_DATES_LIST: list[str] = [d.isoformat() for d in US_FEDERAL_HOLIDAYS]


def remove_invalid_trips(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    duration_minutes = (
        F.col("tpep_dropoff_datetime").cast("long")
        - F.col("tpep_pickup_datetime").cast("long")
    ) / 60.0

    df_with_duration = df.withColumn("_duration_minutes", duration_minutes)

    invalid_distance = (F.col("trip_distance") <= 0) | (F.col("trip_distance") > 100)
    invalid_fare = F.col("fare_amount") < 0
    invalid_passengers = (
        F.col("passenger_count").isNull()
        | (F.col("passenger_count") == 0)
        | (F.col("passenger_count") > 6)
    )
    null_pickup = F.col("tpep_pickup_datetime").isNull()
    null_dropoff = F.col("tpep_dropoff_datetime").isNull()
    invalid_duration = (F.col("_duration_minutes") <= 0) | (F.col("_duration_minutes") > 360)

    rejection_reason = (
        F.when(null_pickup, F.lit("null_pickup_datetime"))
        .when(null_dropoff, F.lit("null_dropoff_datetime"))
        .when(invalid_distance, F.lit("invalid_trip_distance"))
        .when(invalid_fare, F.lit("negative_fare_amount"))
        .when(invalid_passengers, F.lit("invalid_passenger_count"))
        .when(invalid_duration, F.lit("invalid_duration"))
        .otherwise(F.lit(None))
    )

    df_tagged = df_with_duration.withColumn("rejection_reason", rejection_reason)

    is_rejected = F.col("rejection_reason").isNotNull()

    valid = (
        df_tagged
        .filter(~is_rejected)
        .drop("rejection_reason", "_duration_minutes")
    )

    rejected = (
        df_tagged
        .filter(is_rejected)
        .drop("_duration_minutes")
    )

    return valid, rejected


def add_calculated_columns(df: DataFrame) -> DataFrame:
    duration_minutes = (
        F.col("tpep_dropoff_datetime").cast("long")
        - F.col("tpep_pickup_datetime").cast("long")
    ) / 60.0

    duration_hours = duration_minutes / 60.0

    return (
        df
        .withColumn("duration_minutes", F.round(duration_minutes, 2))
        .withColumn(
            "avg_speed_mph",
            F.round(
                F.when(duration_hours > 0, F.col("trip_distance") / duration_hours)
                .otherwise(F.lit(0.0)),
                2,
            ),
        )
        .withColumn(
            "tip_percentage",
            F.round(
                F.when(
                    F.col("fare_amount") > 0,
                    F.col("tip_amount") / F.col("fare_amount") * 100,
                ).otherwise(F.lit(0.0)),
                2,
            ),
        )
        .withColumn(
            "time_slot",
            F.when(F.hour("tpep_pickup_datetime").between(0, 5), F.lit("MADRUGADA"))
            .when(F.hour("tpep_pickup_datetime").between(6, 11), F.lit("MAÑANA"))
            .when(F.hour("tpep_pickup_datetime").between(12, 17), F.lit("TARDE"))
            .otherwise(F.lit("NOCHE")),
        )
        .withColumn("day_of_week", F.date_format("tpep_pickup_datetime", "EEEE"))
        .withColumn(
            "is_holiday",
            F.to_date("tpep_pickup_datetime").cast("string").isin(HOLIDAY_DATES_LIST),
        )
    )


def join_with_zones(df: DataFrame, zones_df: DataFrame) -> DataFrame:
    pickup_zones = zones_df.select(
        F.col("LocationID").alias("pu_location_id"),
        F.col("Zone").alias("pickup_zone"),
        F.col("Borough").alias("pickup_borough"),
    )

    dropoff_zones = zones_df.select(
        F.col("LocationID").alias("do_location_id"),
        F.col("Zone").alias("dropoff_zone"),
        F.col("Borough").alias("dropoff_borough"),
    )

    return (
        df
        .join(
            F.broadcast(pickup_zones),
            df["PULocationID"] == pickup_zones["pu_location_id"],
            "left",
        )
        .drop("pu_location_id")
        .join(
            F.broadcast(dropoff_zones),
            df["DOLocationID"] == dropoff_zones["do_location_id"],
            "left",
        )
        .drop("do_location_id")
    )


def filter_date_range(df: DataFrame, year: int, month: int) -> DataFrame:
    return df.filter(
        (F.year("tpep_pickup_datetime") == year)
        & (F.month("tpep_pickup_datetime") == month)
    )


def build_rejection_report(rejected: DataFrame) -> DataFrame:
    return (
        rejected
        .groupBy("rejection_reason")
        .agg(F.count("*").alias("count"))
        .orderBy(F.col("count").desc())
    )


def run_silver(
    spark: SparkSession,
    year: int,
    month: int,
    bronze_path: str = "data/bronze",
    silver_path: str = "data/silver",
) -> None:
    trip_data_path = f"{bronze_path}/year={year}/month={month}/tripdata.parquet"
    zones_path = f"{bronze_path}/zones/taxi_zone_lookup.csv"

    raw_df = spark.read.parquet(trip_data_path)
    zones_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(zones_path)
    )

    filtered_df = filter_date_range(raw_df, year, month)
    valid_df, rejected_df = remove_invalid_trips(filtered_df)
    enriched_df = add_calculated_columns(valid_df)
    final_df = join_with_zones(enriched_df, zones_df)

    (
        final_df
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .write
        .mode("overwrite")
        .partitionBy("pickup_date")
        .parquet(f"{silver_path}/trips/year={year}/month={month:02d}")
    )

    rejection_report = build_rejection_report(rejected_df)
    (
        rejection_report
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(f"{silver_path}/reports/year={year}/month={month:02d}")
    )
