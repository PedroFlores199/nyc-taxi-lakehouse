from datetime import date, datetime

import pytest
from pyspark.sql import Row, SparkSession

from src.domain import YearMonth
from src.transform.silver import (
    REASON_AMOUNT,
    REASON_DISTANCE,
    REASON_DUPLICATE,
    REASON_DURATION,
    REASON_NULL_KEYS,
    REASON_OUT_OF_MONTH,
    REASON_PASSENGERS,
    add_derived_columns,
    build_silver,
    holidays_for,
    join_zones,
    split_valid_rejected,
    standardize_columns,
    summarize_rejections,
    tag_rejections,
    with_trip_id,
)
from src.transform.zones import standardize_zones
from tests.factories import raw_trip, raw_trips, zones_frame

JANUARY = YearMonth(2024, 1)


def _reasons(spark: SparkSession, trips: list[dict]) -> list[str | None]:
    tagged = tag_rejections(with_trip_id(standardize_columns(raw_trips(spark, trips))), JANUARY)
    return [row["rejection_reason"] for row in tagged.orderBy("fare_amount").collect()]


def test_standardize_columns_renames_and_casts(spark: SparkSession) -> None:
    standardized = standardize_columns(raw_trips(spark, [raw_trip()]))
    assert "pickup_datetime" in standardized.columns
    assert "PULocationID" not in standardized.columns
    assert dict(standardized.dtypes)["passenger_count"] == "int"


def test_valid_trip_is_kept(spark: SparkSession) -> None:
    assert _reasons(spark, [raw_trip()]) == [None]


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        pytest.param({"minutes": 0}, REASON_DURATION, id="duracion_cero"),
        pytest.param({"minutes": -5}, REASON_DURATION, id="duracion_negativa"),
        pytest.param({"minutes": 361}, REASON_DURATION, id="duracion_mayor_6h"),
        pytest.param({"trip_distance": 0.0}, REASON_DISTANCE, id="distancia_cero"),
        pytest.param({"trip_distance": 100.5}, REASON_DISTANCE, id="distancia_mayor_100"),
        pytest.param({"fare_amount": -3.0}, REASON_AMOUNT, id="tarifa_negativa"),
        pytest.param({"total_amount": -1.0}, REASON_AMOUNT, id="total_negativo"),
        pytest.param({"passenger_count": 0}, REASON_PASSENGERS, id="sin_pasajeros"),
        pytest.param({"passenger_count": 7}, REASON_PASSENGERS, id="mas_de_6_pasajeros"),
        pytest.param({"passenger_count": None}, REASON_PASSENGERS, id="pasajeros_desconocidos"),
        pytest.param({"pickup": datetime(2024, 2, 1, 0, 5)}, REASON_OUT_OF_MONTH, id="mes_siguiente"),
        pytest.param({"pickup": datetime(2023, 12, 31, 23, 50)}, REASON_OUT_OF_MONTH, id="mes_anterior"),
        pytest.param({"pickup": None}, REASON_NULL_KEYS, id="recogida_nula"),
        pytest.param({"PULocationID": None}, REASON_NULL_KEYS, id="zona_origen_nula"),
    ],
)
def test_cleaning_rule(spark: SparkSession, overrides: dict, expected: str) -> None:
    assert _reasons(spark, [raw_trip(**overrides)]) == [expected]


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"minutes": 360}, id="duracion_6h_exacta"),
        pytest.param({"trip_distance": 100.0}, id="distancia_100_exacta"),
        pytest.param({"passenger_count": 6}, id="seis_pasajeros"),
        pytest.param({"fare_amount": 0.0}, id="tarifa_cero"),
        pytest.param({"pickup": datetime(2024, 1, 31, 23, 59)}, id="ultimo_minuto_del_mes"),
    ],
)
def test_boundary_values_are_valid(spark: SparkSession, overrides: dict) -> None:
    assert _reasons(spark, [raw_trip(**overrides)]) == [None]


def test_exact_duplicates_are_rejected_once(spark: SparkSession) -> None:
    reasons = _reasons(spark, [raw_trip(), raw_trip()])
    assert sorted(reasons, key=str) == sorted([None, REASON_DUPLICATE], key=str)


def test_first_matching_rule_wins(spark: SparkSession) -> None:
    assert _reasons(spark, [raw_trip(minutes=0, trip_distance=0.0)]) == [REASON_DURATION]


def test_split_valid_rejected(spark: SparkSession) -> None:
    trips = raw_trips(spark, [raw_trip(), raw_trip(trip_distance=0.0, fare_amount=1.0)])
    tagged = tag_rejections(with_trip_id(standardize_columns(trips)), JANUARY)
    valid, rejected = split_valid_rejected(tagged)
    assert valid.count() == 1
    assert "rejection_reason" not in valid.columns
    assert [row["rejection_reason"] for row in rejected.collect()] == [REASON_DISTANCE]


def test_summarize_rejections() -> None:
    rows = [
        Row(rejection_reason=None, count=90),
        Row(rejection_reason=REASON_DISTANCE, count=7),
        Row(rejection_reason=REASON_DURATION, count=3),
    ]
    report = summarize_rejections(rows, JANUARY)
    assert report == {
        "mes": "2024-01",
        "filas_leidas": 100,
        "filas_validas": 90,
        "filas_descartadas": 10,
        "motivos": {REASON_DISTANCE: 7, REASON_DURATION: 3},
    }


def _derived(spark: SparkSession, **overrides: object) -> Row:
    trips = standardize_columns(raw_trips(spark, [raw_trip(**overrides)]))
    return add_derived_columns(trips, holidays_for([2024])).first()


def test_duration_speed_and_tip(spark: SparkSession) -> None:
    row = _derived(spark, minutes=30, trip_distance=10.0, fare_amount=40.0, tip_amount=6.0)
    assert row["duration_minutes"] == 30.0
    assert row["avg_speed_mph"] == 20.0
    assert row["tip_pct"] == 15.0


def test_tip_pct_is_null_without_fare(spark: SparkSession) -> None:
    assert _derived(spark, fare_amount=0.0, tip_amount=2.0)["tip_pct"] is None


@pytest.mark.parametrize(
    ("hour", "slot"),
    [
        (0, "madrugada"),
        (5, "madrugada"),
        (6, "punta_manana"),
        (9, "punta_manana"),
        (10, "mediodia"),
        (15, "mediodia"),
        (16, "punta_tarde"),
        (19, "punta_tarde"),
        (20, "noche"),
        (23, "noche"),
    ],
)
def test_time_slot(spark: SparkSession, hour: int, slot: str) -> None:
    assert _derived(spark, pickup=datetime(2024, 1, 16, hour, 15))["time_slot"] == slot


def test_day_of_week_is_iso(spark: SparkSession) -> None:
    assert _derived(spark, pickup=datetime(2024, 1, 15, 10))["day_of_week"] == 1
    assert _derived(spark, pickup=datetime(2024, 1, 21, 10))["day_of_week"] == 7


def test_holidays(spark: SparkSession) -> None:
    assert _derived(spark, pickup=datetime(2024, 1, 15, 10))["is_holiday"] is True
    assert _derived(spark, pickup=datetime(2024, 1, 16, 10))["is_holiday"] is False
    assert date(2024, 7, 4) in holidays_for([2024])


def test_join_zones_is_broadcast(spark: SparkSession) -> None:
    trips = standardize_columns(raw_trips(spark, [raw_trip(PULocationID=132, DOLocationID=61)]))
    joined = join_zones(trips, standardize_zones(zones_frame(spark)))
    row = joined.first()
    assert (row["pickup_zone"], row["pickup_borough"]) == ("JFK Airport", "Queens")
    assert (row["dropoff_zone"], row["dropoff_borough"]) == ("Crown Heights North", "Brooklyn")
    plan = joined._jdf.queryExecution().executedPlan().toString()
    assert "BroadcastHashJoin" in plan
    assert "SortMergeJoin" not in plan


def test_build_silver_end_to_end(spark: SparkSession) -> None:
    raw = raw_trips(
        spark,
        [
            raw_trip(),
            raw_trip(pickup=datetime(2024, 1, 20, 3, 0), PULocationID=132, DOLocationID=1),
            raw_trip(passenger_count=0),
            raw_trip(pickup=datetime(2024, 2, 2, 8, 0)),
        ],
    )
    silver, tagged = build_silver(raw, zones_frame(spark), JANUARY)
    assert silver.count() == 2
    assert {row["pickup_borough"] for row in silver.collect()} == {"Manhattan", "Queens"}
    reasons = summarize_rejections(tagged.groupBy("rejection_reason").count().collect(), JANUARY)
    assert reasons["motivos"] == {REASON_OUT_OF_MONTH: 1, REASON_PASSENGERS: 1}
