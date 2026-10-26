from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.transform.silver import (
    add_calculated_columns,
    filter_date_range,
    join_with_zones,
    remove_invalid_trips,
)


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


def _make_trip(
    spark: SparkSession,
    overrides: dict | None = None,
) -> "DataFrame":
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
    return spark.createDataFrame([defaults], schema=TRIP_SCHEMA)


def _make_trips(spark: SparkSession, rows: list[dict]) -> "DataFrame":
    base = {
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
    data = []
    for row in rows:
        record = {**base, **row}
        data.append(record)
    return spark.createDataFrame(data, schema=TRIP_SCHEMA)


def test_removes_zero_distance_trips(spark: SparkSession) -> None:
    df = _make_trips(spark, [
        {"trip_distance": 0.0},
        {"trip_distance": 5.0},
    ])
    result = remove_invalid_trips(df)
    assert result.count() == 1
    assert result.collect()[0]["trip_distance"] == 5.0


def test_removes_negative_fare(spark: SparkSession) -> None:
    df = _make_trips(spark, [
        {"fare_amount": -10.0},
        {"fare_amount": 25.0},
    ])
    result = remove_invalid_trips(df)
    assert result.count() == 1
    assert result.collect()[0]["fare_amount"] == 25.0


def test_removes_excessive_passengers(spark: SparkSession) -> None:
    df = _make_trips(spark, [
        {"passenger_count": 0},
        {"passenger_count": 7},
        {"passenger_count": 3},
    ])
    result = remove_invalid_trips(df)
    assert result.count() == 1
    assert result.collect()[0]["passenger_count"] == 3


def test_removes_long_duration_trips(spark: SparkSession) -> None:
    df = _make_trips(spark, [
        {
            "tpep_pickup_datetime": datetime(2024, 1, 15, 10, 0, 0),
            "tpep_dropoff_datetime": datetime(2024, 1, 15, 17, 0, 0),
        },
        {
            "tpep_pickup_datetime": datetime(2024, 1, 15, 10, 0, 0),
            "tpep_dropoff_datetime": datetime(2024, 1, 15, 10, 30, 0),
        },
    ])
    result = remove_invalid_trips(df)
    assert result.count() == 1


def test_removes_zero_duration_trips(spark: SparkSession) -> None:
    df = _make_trip(spark, {
        "tpep_pickup_datetime": datetime(2024, 1, 15, 10, 0, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 15, 10, 0, 0),
    })
    result = remove_invalid_trips(df)
    assert result.count() == 0


def test_calculates_duration_minutes(spark: SparkSession) -> None:
    df = _make_trip(spark, {
        "tpep_pickup_datetime": datetime(2024, 1, 15, 10, 0, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 15, 10, 30, 0),
    })
    result = add_calculated_columns(df)
    row = result.collect()[0]
    assert row["duration_minutes"] == 30.0


def test_calculates_avg_speed(spark: SparkSession) -> None:
    df = _make_trip(spark, {
        "trip_distance": 10.0,
        "tpep_pickup_datetime": datetime(2024, 1, 15, 10, 0, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 15, 10, 30, 0),
    })
    result = add_calculated_columns(df)
    row = result.collect()[0]
    assert row["avg_speed_mph"] == 20.0


def test_calculates_tip_percentage(spark: SparkSession) -> None:
    df = _make_trip(spark, {
        "fare_amount": 100.0,
        "tip_amount": 15.0,
    })
    result = add_calculated_columns(df)
    row = result.collect()[0]
    assert row["tip_percentage"] == 15.0


def test_tip_percentage_handles_zero_fare(spark: SparkSession) -> None:
    df = _make_trip(spark, {
        "fare_amount": 0.0,
        "tip_amount": 5.0,
    })
    result = add_calculated_columns(df)
    row = result.collect()[0]
    assert row["tip_percentage"] == 0.0


def test_assigns_time_slot(spark: SparkSession) -> None:
    df = _make_trips(spark, [
        {
            "tpep_pickup_datetime": datetime(2024, 1, 15, 3, 0, 0),
            "tpep_dropoff_datetime": datetime(2024, 1, 15, 3, 30, 0),
        },
        {
            "tpep_pickup_datetime": datetime(2024, 1, 15, 9, 0, 0),
            "tpep_dropoff_datetime": datetime(2024, 1, 15, 9, 30, 0),
        },
        {
            "tpep_pickup_datetime": datetime(2024, 1, 15, 14, 0, 0),
            "tpep_dropoff_datetime": datetime(2024, 1, 15, 14, 30, 0),
        },
        {
            "tpep_pickup_datetime": datetime(2024, 1, 15, 21, 0, 0),
            "tpep_dropoff_datetime": datetime(2024, 1, 15, 21, 30, 0),
        },
    ])
    result = add_calculated_columns(df).orderBy("tpep_pickup_datetime")
    rows = result.collect()
    assert rows[0]["time_slot"] == "MADRUGADA"
    assert rows[1]["time_slot"] == "MAÑANA"
    assert rows[2]["time_slot"] == "TARDE"
    assert rows[3]["time_slot"] == "NOCHE"


def test_joins_with_zones(spark: SparkSession) -> None:
    trips_df = _make_trip(spark, {
        "PULocationID": 1,
        "DOLocationID": 2,
    })

    zones_schema = StructType([
        StructField("LocationID", IntegerType(), False),
        StructField("zone", StringType(), False),
        StructField("borough", StringType(), False),
    ])
    zones_df = spark.createDataFrame([
        {"LocationID": 1, "zone": "A", "borough": "Manhattan"},
        {"LocationID": 2, "zone": "B", "borough": "Brooklyn"},
    ], schema=zones_schema)

    result = join_with_zones(trips_df, zones_df)
    row = result.collect()[0]
    assert row["pickup_zone"] == "A"
    assert row["pickup_borough"] == "Manhattan"
    assert row["dropoff_zone"] == "B"
    assert row["dropoff_borough"] == "Brooklyn"


def test_filters_out_of_range_dates(spark: SparkSession) -> None:
    df = _make_trips(spark, [
        {
            "tpep_pickup_datetime": datetime(2024, 1, 15, 10, 0, 0),
            "tpep_dropoff_datetime": datetime(2024, 1, 15, 10, 30, 0),
        },
        {
            "tpep_pickup_datetime": datetime(2024, 2, 15, 10, 0, 0),
            "tpep_dropoff_datetime": datetime(2024, 2, 15, 10, 30, 0),
        },
    ])
    result = filter_date_range(df, "2024-01-01", "2024-01-31")
    assert result.count() == 1
    row = result.collect()[0]
    assert row["tpep_pickup_datetime"].month == 1
