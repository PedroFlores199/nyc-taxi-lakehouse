from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from src.quality.checks import check_no_duplicates, check_no_nulls, check_valid_ranges


TRIP_SCHEMA = StructType([
    StructField("VendorID", IntegerType(), False),
    StructField("tpep_pickup_datetime", TimestampType(), False),
    StructField("tpep_dropoff_datetime", TimestampType(), False),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), False),
    StructField("PULocationID", IntegerType(), False),
    StructField("DOLocationID", IntegerType(), False),
    StructField("fare_amount", DoubleType(), False),
    StructField("tip_amount", DoubleType(), False),
    StructField("total_amount", DoubleType(), False),
])


def _clean_row(overrides: dict | None = None) -> dict:
    defaults = {
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(2024, 1, 15, 10, 0, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 15, 10, 30, 0),
        "passenger_count": 1,
        "trip_distance": 5.0,
        "PULocationID": 100,
        "DOLocationID": 200,
        "fare_amount": 25.0,
        "tip_amount": 5.0,
        "total_amount": 30.0,
    }
    if overrides:
        defaults.update(overrides)
    return defaults


def test_no_nulls_passes_clean_data(spark: SparkSession) -> None:
    df = spark.createDataFrame([_clean_row()], schema=TRIP_SCHEMA)
    assert check_no_nulls(df, ["VendorID", "trip_distance", "fare_amount"]) is True


def test_no_nulls_fails_with_nulls(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [_clean_row({"passenger_count": None})],
        schema=TRIP_SCHEMA,
    )
    with pytest.raises(ValueError):
        check_no_nulls(df, ["passenger_count"])


def test_valid_ranges_passes(spark: SparkSession) -> None:
    df = spark.createDataFrame([_clean_row()], schema=TRIP_SCHEMA)
    assert check_valid_ranges(df) is True


def test_valid_ranges_fails_negative_distance(spark: SparkSession) -> None:
    schema_nullable = StructType([
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
    ])
    df = spark.createDataFrame(
        [_clean_row({"trip_distance": -3.0})],
        schema=schema_nullable,
    )
    with pytest.raises(ValueError):
        check_valid_ranges(df)


def test_no_duplicates_passes(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            _clean_row({"VendorID": 1, "tpep_pickup_datetime": datetime(2024, 1, 15, 10, 0, 0)}),
            _clean_row({"VendorID": 2, "tpep_pickup_datetime": datetime(2024, 1, 15, 11, 0, 0)}),
        ],
        schema=TRIP_SCHEMA,
    )
    assert check_no_duplicates(
        df, ["VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID"]
    ) is True


def test_no_duplicates_fails(spark: SparkSession) -> None:
    row = _clean_row()
    df = spark.createDataFrame([row, row], schema=TRIP_SCHEMA)
    with pytest.raises(ValueError):
        check_no_duplicates(
            df, ["VendorID", "tpep_pickup_datetime", "PULocationID", "DOLocationID"]
        )
