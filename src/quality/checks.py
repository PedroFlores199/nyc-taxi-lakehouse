from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def check_no_nulls(df: DataFrame, columns: list[str]) -> bool:
    null_conditions = [F.col(c).isNull() for c in columns]
    combined_condition = null_conditions[0]
    for condition in null_conditions[1:]:
        combined_condition = combined_condition | condition
    null_count = df.filter(combined_condition).count()
    if null_count > 0:
        raise ValueError(
            f"Found {null_count} rows with nulls in columns: {columns}"
        )
    return True


def check_valid_ranges(df: DataFrame) -> bool:
    invalid_distance = df.filter(F.col("trip_distance") <= 0).count()
    if invalid_distance > 0:
        raise ValueError(
            f"Found {invalid_distance} rows with trip_distance <= 0"
        )

    invalid_fare = df.filter(F.col("fare_amount") < 0).count()
    if invalid_fare > 0:
        raise ValueError(
            f"Found {invalid_fare} rows with fare_amount < 0"
        )

    invalid_passengers = df.filter(
        (F.col("passenger_count") < 1) | (F.col("passenger_count") > 6)
    ).count()
    if invalid_passengers > 0:
        raise ValueError(
            f"Found {invalid_passengers} rows with passenger_count outside 1-6"
        )

    return True


def check_no_duplicates(df: DataFrame, columns: list[str]) -> bool:
    total_count = df.count()
    distinct_count = df.select(columns).distinct().count()
    if total_count != distinct_count:
        duplicate_count = total_count - distinct_count
        raise ValueError(
            f"Found {duplicate_count} duplicate rows on columns: {columns}"
        )
    return True


def run_quality_checks(df: DataFrame, stage: str) -> None:
    if stage == "bronze":
        check_no_nulls(
            df,
            ["tpep_pickup_datetime", "tpep_dropoff_datetime"],
        )

    elif stage == "silver":
        check_no_nulls(
            df,
            [
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "trip_distance",
                "fare_amount",
                "passenger_count",
            ],
        )
        check_valid_ranges(df)

    elif stage == "gold":
        check_no_nulls(df, ["pickup_zone", "pickup_borough"])

    else:
        raise ValueError(f"Unknown stage: {stage}")
