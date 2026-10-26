from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.transform.gold import (
    demand_by_zone_and_hour,
    detect_anomalies,
    top_routes_by_borough,
)


SILVER_SCHEMA = StructType([
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
    StructField("duration_minutes", DoubleType(), False),
    StructField("avg_speed_mph", DoubleType(), False),
    StructField("tip_percentage", DoubleType(), False),
    StructField("time_slot", StringType(), False),
    StructField("day_of_week", StringType(), False),
    StructField("is_holiday", BooleanType(), False),
    StructField("pickup_zone", StringType(), False),
    StructField("pickup_borough", StringType(), False),
    StructField("dropoff_zone", StringType(), False),
    StructField("dropoff_borough", StringType(), False),
])


def _silver_row(overrides: dict | None = None) -> dict:
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
        "duration_minutes": 30.0,
        "avg_speed_mph": 10.0,
        "tip_percentage": 20.0,
        "time_slot": "MAÑANA",
        "day_of_week": "Monday",
        "is_holiday": False,
        "pickup_zone": "ZoneA",
        "pickup_borough": "Manhattan",
        "dropoff_zone": "ZoneB",
        "dropoff_borough": "Brooklyn",
    }
    if overrides:
        defaults.update(overrides)
    return defaults


def test_demand_counts_trips_per_zone_hour(spark: SparkSession) -> None:
    rows = []
    for i in range(3):
        rows.append(_silver_row({
            "pickup_zone": "ZoneA",
            "tpep_pickup_datetime": datetime(2024, 1, 15, 10, i, 0),
            "tpep_dropoff_datetime": datetime(2024, 1, 15, 10, 30 + i, 0),
            "PULocationID": 100 + i,
        }))
    for i in range(2):
        rows.append(_silver_row({
            "pickup_zone": "ZoneB",
            "tpep_pickup_datetime": datetime(2024, 1, 15, 14, i, 0),
            "tpep_dropoff_datetime": datetime(2024, 1, 15, 14, 30 + i, 0),
            "PULocationID": 200 + i,
        }))

    df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
    result = demand_by_zone_and_hour(df)

    result_map = {
        (row["pickup_zone"], row["hour"]): row["trip_count"]
        for row in result.collect()
    }
    assert result_map[("ZoneA", 10)] == 3
    assert result_map[("ZoneB", 14)] == 2


def test_top_routes_returns_correct_number(spark: SparkSession) -> None:
    rows = []
    for i in range(15):
        for j in range(i + 1):
            rows.append(_silver_row({
                "pickup_zone": f"PU_{i}",
                "dropoff_zone": f"DO_{i}",
                "pickup_borough": "Manhattan",
                "dropoff_borough": "Manhattan",
                "PULocationID": i,
                "DOLocationID": i + 100,
                "tpep_pickup_datetime": datetime(2024, 1, 15, 10, j % 60, 0),
                "tpep_dropoff_datetime": datetime(2024, 1, 15, 10, 30 + (j % 30), 0),
            }))

    df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
    result = top_routes_by_borough(df, top_n=5)

    manhattan_routes = result.filter(result["pickup_borough"] == "Manhattan")
    assert manhattan_routes.count() == 5


def test_anomaly_detection_flags_outliers(spark: SparkSession) -> None:
    rows = []
    for i in range(10):
        rows.append(_silver_row({
            "pickup_zone": "ZoneA",
            "dropoff_zone": "ZoneB",
            "trip_distance": 5.0,
            "fare_amount": 25.0,
            "PULocationID": 1,
            "DOLocationID": 2,
            "tpep_pickup_datetime": datetime(2024, 1, 15, 10, i, 0),
            "tpep_dropoff_datetime": datetime(2024, 1, 15, 10, 30 + i, 0),
        }))

    rows.append(_silver_row({
        "pickup_zone": "ZoneA",
        "dropoff_zone": "ZoneB",
        "trip_distance": 5.0,
        "fare_amount": 250.0,
        "total_amount": 260.0,
        "PULocationID": 1,
        "DOLocationID": 2,
        "tpep_pickup_datetime": datetime(2024, 1, 15, 11, 0, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 15, 11, 30, 0),
    }))

    df = spark.createDataFrame(rows, schema=SILVER_SCHEMA)
    result = detect_anomalies(df)

    anomalies = result.filter(result["is_anomaly"] == True)
    assert anomalies.count() >= 1

    anomaly_row = anomalies.collect()[0]
    assert anomaly_row["fare_amount"] == 250.0
