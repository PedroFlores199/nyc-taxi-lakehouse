import zlib
from datetime import date, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

RAW_TRIP_SCHEMA = T.StructType(
    [
        T.StructField("VendorID", T.IntegerType()),
        T.StructField("tpep_pickup_datetime", T.TimestampType()),
        T.StructField("tpep_dropoff_datetime", T.TimestampType()),
        T.StructField("passenger_count", T.LongType()),
        T.StructField("trip_distance", T.DoubleType()),
        T.StructField("PULocationID", T.IntegerType()),
        T.StructField("DOLocationID", T.IntegerType()),
        T.StructField("payment_type", T.LongType()),
        T.StructField("fare_amount", T.DoubleType()),
        T.StructField("tip_amount", T.DoubleType()),
        T.StructField("tolls_amount", T.DoubleType()),
        T.StructField("total_amount", T.DoubleType()),
    ]
)

ZONE_SCHEMA = T.StructType(
    [
        T.StructField("LocationID", T.IntegerType()),
        T.StructField("Borough", T.StringType()),
        T.StructField("Zone", T.StringType()),
        T.StructField("service_zone", T.StringType()),
    ]
)

ZONES = [
    (1, "EWR", "Newark Airport", "EWR"),
    (132, "Queens", "JFK Airport", "Airports"),
    (161, "Manhattan", "Midtown Center", "Yellow Zone"),
    (236, "Manhattan", "Upper East Side North", "Yellow Zone"),
    (61, "Brooklyn", "Crown Heights North", "Boro Zone"),
]

BASE_PICKUP = datetime(2024, 1, 16, 10, 0, 0)


def raw_trip(**overrides: object) -> dict:
    pickup = overrides.pop("pickup", BASE_PICKUP)
    minutes = overrides.pop("minutes", 30)
    trip = {
        "VendorID": 2,
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": pickup + timedelta(minutes=minutes) if pickup else None,
        "passenger_count": 1,
        "trip_distance": 5.0,
        "PULocationID": 161,
        "DOLocationID": 236,
        "payment_type": 1,
        "fare_amount": 20.0,
        "tip_amount": 4.0,
        "tolls_amount": 0.0,
        "total_amount": 28.5,
    }
    trip.update(overrides)
    return trip


def raw_trips(spark: SparkSession, trips: list[dict]) -> DataFrame:
    return spark.createDataFrame(trips, RAW_TRIP_SCHEMA)


def zones_frame(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(ZONES, ZONE_SCHEMA)


def trip_key(label: str) -> int:
    return zlib.crc32(label.encode("utf-8"))


def silver_trip(**overrides: object) -> dict:
    pickup = overrides.pop("pickup", BASE_PICKUP)
    minutes = overrides.pop("minutes", 30.0)
    trip = {
        "trip_id": trip_key(overrides.pop("trip_id", f"{pickup.isoformat()}-{overrides.get('fare_amount')}")),
        "vendor_id": 2,
        "pickup_datetime": pickup,
        "dropoff_datetime": pickup + timedelta(minutes=minutes),
        "passenger_count": 1,
        "trip_distance": 5.0,
        "pickup_location_id": 161,
        "dropoff_location_id": 236,
        "payment_type": 1,
        "fare_amount": 20.0,
        "tip_amount": 4.0,
        "tolls_amount": 0.0,
        "total_amount": 28.5,
        "pickup_date": pickup.date(),
        "year_month": pickup.strftime("%Y-%m"),
        "pickup_hour": pickup.hour,
        "duration_minutes": float(minutes),
        "avg_speed_mph": 10.0,
        "tip_pct": 20.0,
        "time_slot": "mediodia",
        "day_of_week": pickup.isoweekday(),
        "is_holiday": False,
        "pickup_zone": "Midtown Center",
        "pickup_borough": "Manhattan",
        "dropoff_zone": "Upper East Side North",
        "dropoff_borough": "Manhattan",
    }
    trip.update(overrides)
    return trip


SILVER_SCHEMA = T.StructType(
    [
        T.StructField("trip_id", T.LongType()),
        T.StructField("vendor_id", T.IntegerType()),
        T.StructField("pickup_datetime", T.TimestampType()),
        T.StructField("dropoff_datetime", T.TimestampType()),
        T.StructField("passenger_count", T.IntegerType()),
        T.StructField("trip_distance", T.DoubleType()),
        T.StructField("pickup_location_id", T.IntegerType()),
        T.StructField("dropoff_location_id", T.IntegerType()),
        T.StructField("payment_type", T.IntegerType()),
        T.StructField("fare_amount", T.DoubleType()),
        T.StructField("tip_amount", T.DoubleType()),
        T.StructField("tolls_amount", T.DoubleType()),
        T.StructField("total_amount", T.DoubleType()),
        T.StructField("pickup_date", T.DateType()),
        T.StructField("year_month", T.StringType()),
        T.StructField("pickup_hour", T.IntegerType()),
        T.StructField("duration_minutes", T.DoubleType()),
        T.StructField("avg_speed_mph", T.DoubleType()),
        T.StructField("tip_pct", T.DoubleType()),
        T.StructField("time_slot", T.StringType()),
        T.StructField("day_of_week", T.IntegerType()),
        T.StructField("is_holiday", T.BooleanType()),
        T.StructField("pickup_zone", T.StringType()),
        T.StructField("pickup_borough", T.StringType()),
        T.StructField("dropoff_zone", T.StringType()),
        T.StructField("dropoff_borough", T.StringType()),
    ]
)


def silver_trips(spark: SparkSession, trips: list[dict]) -> DataFrame:
    return spark.createDataFrame(trips, SILVER_SCHEMA)


def month_days(year: int, month: int) -> list[date]:
    day = date(year, month, 1)
    days = []
    while day.month == month:
        days.append(day)
        day += timedelta(days=1)
    return days
