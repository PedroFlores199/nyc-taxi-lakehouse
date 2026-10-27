from datetime import datetime, timedelta

from pyspark.sql import SparkSession

from src.transform.gold import (
    demand_by_zone_hour,
    detect_anomalies,
    monthly_zone_ranking,
    top_routes_by_borough,
    travel_time_between_zones,
    trips_aggregated,
)
from tests.factories import BASE_PICKUP, silver_trip, silver_trips, trip_key

MORNING = datetime(2024, 1, 16, 8, 0)


def _trips_on_route(count: int, start: datetime = MORNING, **overrides: object) -> list[dict]:
    return [
        silver_trip(
            pickup=start + timedelta(minutes=index), trip_id=f"{start}-{overrides}-{index}", **overrides
        )
        for index in range(count)
    ]


def test_demand_by_zone_hour(spark: SparkSession) -> None:
    trips = silver_trips(
        spark,
        [
            silver_trip(trip_id="a", pickup=datetime(2024, 1, 16, 8, 5), total_amount=10.0, tip_pct=10.0),
            silver_trip(trip_id="b", pickup=datetime(2024, 1, 16, 8, 40), total_amount=30.0, tip_pct=None),
            silver_trip(trip_id="c", pickup=datetime(2024, 1, 16, 9, 10), total_amount=20.0),
            silver_trip(trip_id="d", pickup=datetime(2024, 1, 16, 8, 15), pickup_location_id=132),
        ],
    )
    rows = {
        (row["pickup_location_id"], row["pickup_hour"]): row for row in demand_by_zone_hour(trips).collect()
    }
    eight = rows[(161, 8)]
    assert eight["trips"] == 2
    assert eight["avg_total_amount"] == 20.0
    assert eight["avg_tip_pct"] == 10.0
    assert eight["tip_pct_count"] == 1
    assert rows[(161, 9)]["trips"] == 1
    assert rows[(132, 8)]["trips"] == 1


def test_top_routes_keeps_n_per_borough_ordered(spark: SparkSession) -> None:
    trips = []
    for destination in range(1, 13):
        trips += _trips_on_route(destination, dropoff_location_id=destination)
    trips += _trips_on_route(3, pickup_location_id=132, pickup_borough="Queens", dropoff_location_id=1)
    result = top_routes_by_borough(silver_trips(spark, trips), n=10)

    manhattan = result.filter("pickup_borough = 'Manhattan'").orderBy("rank").collect()
    assert len(manhattan) == 10
    assert [row["dropoff_location_id"] for row in manhattan[:3]] == [12, 11, 10]
    assert [row["rank"] for row in manhattan] == list(range(1, 11))
    assert result.filter("pickup_borough = 'Queens'").count() == 1


def test_travel_time_by_slot(spark: SparkSession) -> None:
    trips = silver_trips(
        spark,
        [
            silver_trip(trip_id="a", minutes=20.0, time_slot="punta_manana"),
            silver_trip(trip_id="b", minutes=40.0, time_slot="punta_manana"),
            silver_trip(trip_id="c", minutes=12.0, time_slot="noche", pickup=datetime(2024, 1, 16, 22)),
        ],
    )
    rows = {row["time_slot"]: row for row in travel_time_between_zones(trips).collect()}
    assert rows["punta_manana"]["avg_duration_minutes"] == 30.0
    assert rows["punta_manana"]["trips"] == 2
    assert rows["noche"]["avg_duration_minutes"] == 12.0


def test_trips_aggregated_sums_measures(spark: SparkSession) -> None:
    trips = silver_trips(
        spark,
        [
            silver_trip(trip_id="a", passenger_count=2, tip_amount=3.0),
            silver_trip(
                trip_id="b", passenger_count=1, tip_amount=1.0, pickup=BASE_PICKUP.replace(minute=30)
            ),
        ],
    )
    row = trips_aggregated(trips).first()
    assert row["trips"] == 2
    assert row["passengers"] == 3
    assert row["tip_amount_sum"] == 4.0


def _monthly(spark: SparkSession, rows: list[tuple]) -> object:
    return spark.createDataFrame(
        rows,
        "year_month string, pickup_borough string, pickup_location_id int, pickup_zone string, trips long",
    )


def test_monthly_ranking_top3_and_variation(spark: SparkSession) -> None:
    demand = _monthly(
        spark,
        [
            ("2024-01", "Manhattan", 1, "A", 100),
            ("2024-01", "Manhattan", 2, "B", 90),
            ("2024-01", "Manhattan", 3, "C", 80),
            ("2024-01", "Manhattan", 4, "D", 70),
            ("2024-02", "Manhattan", 1, "A", 50),
            ("2024-02", "Manhattan", 2, "B", 95),
            ("2024-02", "Manhattan", 3, "C", 10),
            ("2024-02", "Manhattan", 4, "D", 140),
            ("2024-02", "Manhattan", 5, "E", 60),
        ],
    )
    february = {
        row["pickup_location_id"]: row
        for row in monthly_zone_ranking(demand).filter("year_month = '2024-02'").collect()
    }
    assert sorted(february) == [2, 4, 5]
    assert february[4]["rank"] == 1
    assert february[4]["variation_pct"] == 100.0
    assert february[2]["variation_pct"] == round((95 - 90) / 90 * 100, 2)
    assert february[5]["previous_month_trips"] is None


def test_monthly_ranking_ignores_non_consecutive_months(spark: SparkSession) -> None:
    demand = _monthly(
        spark,
        [
            ("2024-01", "Queens", 7, "X", 10),
            ("2024-03", "Queens", 7, "X", 20),
        ],
    )
    march = monthly_zone_ranking(demand).filter("year_month = '2024-03'").first()
    assert march["previous_month_trips"] is None
    assert march["variation_pct"] is None


def test_anomaly_detection_flags_expensive_trip(spark: SparkSession) -> None:
    trips = [
        silver_trip(
            trip_id=f"normal-{index}",
            pickup=MORNING + timedelta(minutes=index),
            fare_amount=20.0 + (index % 5),
            trip_distance=5.0,
        )
        for index in range(40)
    ]
    trips.append(silver_trip(trip_id="outlier", pickup=MORNING, fare_amount=250.0, trip_distance=5.0))
    anomalies = detect_anomalies(silver_trips(spark, trips)).collect()
    assert [row["trip_id"] for row in anomalies] == [trip_key("outlier")]
    assert anomalies[0]["z_score"] > 3
    assert anomalies[0]["route_trips"] == 41


def test_anomaly_detection_needs_enough_route_trips(spark: SparkSession) -> None:
    trips = [silver_trip(trip_id=f"t{index}", fare_amount=20.0 + index) for index in range(10)]
    trips.append(silver_trip(trip_id="outlier", fare_amount=500.0))
    assert detect_anomalies(silver_trips(spark, trips)).count() == 0
