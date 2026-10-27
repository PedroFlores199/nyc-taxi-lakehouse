from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from src.quality.checks import (
    DataQualityError,
    between,
    check_unique,
    enforce,
    evaluate,
    greater_than,
    not_null,
    validate_gold,
    validate_silver,
)
from tests.factories import silver_trip, silver_trips


def test_not_null_counts_failing_rows(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1, None), (2, "b"), (None, None)], "id int, name string")
    results = {result.name: result for result in evaluate(df, not_null("id", "name"))}
    assert results["id sin nulos"].failing_rows == 1
    assert results["name sin nulos"].failing_rows == 2
    assert results["tabla no vacía"].passed


def test_between_and_greater_than(spark: SparkSession) -> None:
    df = spark.createDataFrame([(0.0,), (5.0,), (101.0,)], "distance double")
    results = evaluate(df, [between("distance", 0, 100), greater_than("distance", 0)])
    assert [result.failing_rows for result in results[:2]] == [1, 1]


def test_empty_table_fails(spark: SparkSession) -> None:
    df = spark.createDataFrame([], "id int")
    assert not evaluate(df, not_null("id"))[-1].passed


def test_check_unique(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (1,), (2,)], "id int")
    assert check_unique(df, ["id"]).failing_rows == 1


def test_enforce_raises_with_all_failures(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None,), (None,)], "id int")
    with pytest.raises(DataQualityError) as error:
        enforce("tabla", [*evaluate(df, not_null("id")), check_unique(df, ["id"])])
    assert {failure.name for failure in error.value.failures} == {"id sin nulos", "sin duplicados en id"}


def test_validate_silver_accepts_clean_data(spark: SparkSession) -> None:
    validate_silver(silver_trips(spark, [silver_trip(trip_id="a"), silver_trip(trip_id="b")]))


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"trip_distance": 0.0}, id="distancia_cero"),
        pytest.param({"passenger_count": 9}, id="pasajeros"),
        pytest.param({"fare_amount": -1.0}, id="importe_negativo"),
        pytest.param({"pickup_borough": None}, id="distrito_nulo"),
        pytest.param({"duration_minutes": 400.0}, id="duracion"),
    ],
)
def test_validate_silver_stops_on_bad_data(spark: SparkSession, overrides: dict) -> None:
    with pytest.raises(DataQualityError):
        validate_silver(silver_trips(spark, [silver_trip(trip_id="a", **overrides)]))


def test_validate_silver_rejects_duplicated_ids(spark: SparkSession) -> None:
    trips = [silver_trip(trip_id="a"), silver_trip(trip_id="a", pickup=datetime(2024, 1, 16, 11))]
    with pytest.raises(DataQualityError):
        validate_silver(silver_trips(spark, trips))


def test_validate_gold_allows_empty_anomalies(spark: SparkSession) -> None:
    validate_gold("gold.anomalies", spark.createDataFrame([], "trip_id string"), ["trip_id"], (), True)
