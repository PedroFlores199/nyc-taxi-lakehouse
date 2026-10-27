import logging

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.config import Settings
from src.domain import YearMonth
from src.lake import (
    GOLD_ANOMALIES,
    GOLD_DEMAND,
    GOLD_MONTHLY_RANKING,
    GOLD_TOP_ROUTES,
    GOLD_TRAVEL_TIME,
    GOLD_TRIPS_AGGREGATED,
    SILVER_TRIPS,
    gold_table,
    read_table,
    silver_table,
    write_table,
)
from src.quality.checks import validate_gold

logger = logging.getLogger(__name__)

TOP_ROUTES_PER_BOROUGH = 10
TOP_ZONES_PER_BOROUGH = 3
ANOMALY_STD_THRESHOLD = 3.0
ANOMALY_MIN_ROUTE_TRIPS = 30


def read_silver_month(spark: SparkSession, settings: Settings, period: YearMonth) -> DataFrame:
    return read_table(spark, silver_table(settings, SILVER_TRIPS), settings).filter(
        (F.col("pickup_date") >= F.lit(period.first_day))
        & (F.col("pickup_date") < F.lit(period.next.first_day))
    )


def demand_by_zone_hour(silver: DataFrame) -> DataFrame:
    return silver.groupBy(
        "year_month",
        "pickup_date",
        "pickup_hour",
        "day_of_week",
        "is_holiday",
        "time_slot",
        "pickup_location_id",
        "pickup_zone",
        "pickup_borough",
    ).agg(
        F.count(F.lit(1)).alias("trips"),
        F.round(F.sum("total_amount"), 2).alias("total_amount_sum"),
        F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
        F.round(F.sum("tip_pct"), 2).alias("tip_pct_sum"),
        F.count("tip_pct").alias("tip_pct_count"),
        F.round(F.avg("tip_pct"), 2).alias("avg_tip_pct"),
    )


def top_routes_by_borough(silver: DataFrame, n: int = TOP_ROUTES_PER_BOROUGH) -> DataFrame:
    routes = silver.groupBy(
        "year_month",
        "pickup_borough",
        "pickup_location_id",
        "pickup_zone",
        "dropoff_location_id",
        "dropoff_zone",
        "dropoff_borough",
    ).agg(F.count(F.lit(1)).alias("trips"))
    ranking = Window.partitionBy("year_month", "pickup_borough").orderBy(
        F.desc("trips"), "pickup_location_id", "dropoff_location_id"
    )
    return routes.withColumn("rank", F.row_number().over(ranking)).filter(F.col("rank") <= n)


def travel_time_between_zones(silver: DataFrame) -> DataFrame:
    return silver.groupBy("year_month", "pickup_location_id", "dropoff_location_id", "time_slot").agg(
        F.count(F.lit(1)).alias("trips"),
        F.round(F.sum("duration_minutes"), 2).alias("duration_minutes_sum"),
        F.round(F.avg("duration_minutes"), 2).alias("avg_duration_minutes"),
        F.round(F.sum("total_amount"), 2).alias("total_amount_sum"),
        F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
        F.round(F.sum("trip_distance"), 2).alias("trip_distance_sum"),
        F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
    )


def trips_aggregated(silver: DataFrame) -> DataFrame:
    return silver.groupBy(
        "year_month", "pickup_date", "pickup_hour", "pickup_location_id", "dropoff_location_id"
    ).agg(
        F.count(F.lit(1)).alias("trips"),
        F.sum("passenger_count").alias("passengers"),
        F.round(F.sum("total_amount"), 2).alias("total_amount_sum"),
        F.round(F.sum("fare_amount"), 2).alias("fare_amount_sum"),
        F.round(F.sum("tip_amount"), 2).alias("tip_amount_sum"),
        F.round(F.sum("duration_minutes"), 2).alias("duration_minutes_sum"),
        F.round(F.sum("trip_distance"), 2).alias("trip_distance_sum"),
    )


def monthly_zone_ranking(demand: DataFrame, n: int = TOP_ZONES_PER_BOROUGH) -> DataFrame:
    monthly = demand.groupBy("year_month", "pickup_borough", "pickup_location_id", "pickup_zone").agg(
        F.sum("trips").alias("trips")
    )
    history = Window.partitionBy("pickup_location_id").orderBy("year_month")
    ranking = Window.partitionBy("year_month", "pickup_borough").orderBy(
        F.desc("trips"), "pickup_location_id"
    )
    expected_previous = F.date_format(
        F.add_months(F.to_date(F.concat(F.col("year_month"), F.lit("-01"))), -1), "yyyy-MM"
    )
    return (
        monthly.withColumn(
            "previous_month_trips",
            F.when(
                F.lag("year_month").over(history) == expected_previous,
                F.lag("trips").over(history),
            ),
        )
        .withColumn(
            "variation_pct",
            F.round(
                (F.col("trips") - F.col("previous_month_trips")) / F.col("previous_month_trips") * 100,
                2,
            ),
        )
        .withColumn("rank", F.row_number().over(ranking))
        .filter(F.col("rank") <= n)
    )


def detect_anomalies(
    silver: DataFrame,
    std_threshold: float = ANOMALY_STD_THRESHOLD,
    min_route_trips: int = ANOMALY_MIN_ROUTE_TRIPS,
) -> DataFrame:
    route = Window.partitionBy("pickup_location_id", "dropoff_location_id")
    fare_per_mile = F.col("fare_amount") / F.col("trip_distance")
    with_stats = (
        silver.filter(F.col("trip_distance") > 0)
        .withColumn("fare_per_mile", F.round(fare_per_mile, 4))
        .withColumn("route_trips", F.count(F.lit(1)).over(route))
        .withColumn("route_mean_fare_per_mile", F.avg(fare_per_mile).over(route))
        .withColumn("route_std_fare_per_mile", F.stddev_samp(fare_per_mile).over(route))
    )
    threshold = F.col("route_mean_fare_per_mile") + std_threshold * F.col("route_std_fare_per_mile")
    return (
        with_stats.filter(
            (F.col("route_trips") >= min_route_trips)
            & (F.col("route_std_fare_per_mile") > 0)
            & (fare_per_mile > threshold)
        )
        .withColumn(
            "z_score",
            F.round(
                (fare_per_mile - F.col("route_mean_fare_per_mile")) / F.col("route_std_fare_per_mile"),
                2,
            ),
        )
        .withColumn("route_mean_fare_per_mile", F.round("route_mean_fare_per_mile", 4))
        .withColumn("route_std_fare_per_mile", F.round("route_std_fare_per_mile", 4))
    )


def build_monthly_gold(silver: DataFrame) -> dict[str, DataFrame]:
    return {
        GOLD_DEMAND: demand_by_zone_hour(silver),
        GOLD_TOP_ROUTES: top_routes_by_borough(silver),
        GOLD_TRAVEL_TIME: travel_time_between_zones(silver),
        GOLD_TRIPS_AGGREGATED: trips_aggregated(silver),
        GOLD_ANOMALIES: detect_anomalies(silver),
    }


GOLD_KEYS: dict[str, tuple[str, ...]] = {
    GOLD_DEMAND: ("year_month", "pickup_date", "pickup_hour", "pickup_location_id"),
    GOLD_TOP_ROUTES: ("year_month", "pickup_borough", "rank"),
    GOLD_TRAVEL_TIME: ("year_month", "pickup_location_id", "dropoff_location_id", "time_slot"),
    GOLD_TRIPS_AGGREGATED: (
        "year_month",
        "pickup_date",
        "pickup_hour",
        "pickup_location_id",
        "dropoff_location_id",
    ),
    GOLD_ANOMALIES: ("trip_id",),
    GOLD_MONTHLY_RANKING: ("year_month", "pickup_borough", "rank"),
}

SMALL_GOLD_TABLES = frozenset({GOLD_TOP_ROUTES, GOLD_ANOMALIES, GOLD_MONTHLY_RANKING})


def _write_gold(df: DataFrame, name: str, settings: Settings, partitioned: bool) -> None:
    output = df.coalesce(1) if name in SMALL_GOLD_TABLES else df
    write_table(
        output,
        gold_table(settings, name),
        settings,
        partition_by=("year_month",) if partitioned else (),
    )


def run_gold(spark: SparkSession, settings: Settings, period: YearMonth) -> dict[str, int]:
    silver = read_silver_month(spark, settings, period).persist(StorageLevel.MEMORY_AND_DISK)
    counts: dict[str, int] = {}
    try:
        for name, table in build_monthly_gold(silver).items():
            table = table.persist(StorageLevel.MEMORY_AND_DISK)
            validate_gold(
                f"gold.{name}",
                table,
                GOLD_KEYS[name],
                positive=() if name == GOLD_ANOMALIES else ("trips",),
                allow_empty=name == GOLD_ANOMALIES,
            )
            _write_gold(table, name, settings, partitioned=True)
            counts[name] = table.count()
            table.unpersist()
    finally:
        silver.unpersist()

    demand_history = read_table(spark, gold_table(settings, GOLD_DEMAND), settings)
    ranking = monthly_zone_ranking(demand_history).persist(StorageLevel.MEMORY_AND_DISK)
    validate_gold(f"gold.{GOLD_MONTHLY_RANKING}", ranking, GOLD_KEYS[GOLD_MONTHLY_RANKING])
    _write_gold(ranking, GOLD_MONTHLY_RANKING, settings, partitioned=False)
    counts[GOLD_MONTHLY_RANKING] = ranking.count()
    ranking.unpersist()
    logger.info("Gold %s escrito: %s", period, counts)
    return counts
