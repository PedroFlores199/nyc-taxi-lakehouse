from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def demand_by_zone_and_hour(df: DataFrame) -> DataFrame:
    return (
        df
        .groupBy(
            "pickup_zone",
            "pickup_borough",
            F.hour("tpep_pickup_datetime").alias("pickup_hour"),
        )
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("tip_percentage"), 2).alias("avg_tip_percentage"),
        )
        .orderBy("pickup_borough", "pickup_zone", "pickup_hour")
    )


def top_routes_by_borough(df: DataFrame, n: int = 10) -> DataFrame:
    route_counts = (
        df
        .groupBy("pickup_borough", "pickup_zone", "dropoff_zone")
        .agg(F.count("*").alias("trip_count"))
    )

    borough_window = Window.partitionBy("pickup_borough").orderBy(
        F.col("trip_count").desc()
    )

    return (
        route_counts
        .withColumn("rank", F.row_number().over(borough_window))
        .filter(F.col("rank") <= n)
        .orderBy("pickup_borough", "rank")
    )


def avg_time_between_zones(df: DataFrame) -> DataFrame:
    return (
        df
        .groupBy("pickup_zone", "dropoff_zone", "time_slot")
        .agg(F.round(F.avg("duration_minutes"), 2).alias("avg_duration_minutes"))
        .orderBy("pickup_zone", "dropoff_zone", "time_slot")
    )


def monthly_zone_ranking(df: DataFrame) -> DataFrame:
    monthly_counts = (
        df
        .withColumn(
            "year_month",
            F.date_format("tpep_pickup_datetime", "yyyy-MM"),
        )
        .groupBy("pickup_borough", "pickup_zone", "year_month")
        .agg(F.count("*").alias("trip_count"))
    )

    ranking_window = Window.partitionBy("pickup_borough", "year_month").orderBy(
        F.col("trip_count").desc()
    )

    mom_window = Window.partitionBy("pickup_borough", "pickup_zone").orderBy(
        "year_month"
    )

    return (
        monthly_counts
        .withColumn("rank", F.row_number().over(ranking_window))
        .filter(F.col("rank") <= 3)
        .withColumn("prev_trip_count", F.lag("trip_count").over(mom_window))
        .withColumn(
            "mom_variation",
            F.round(
                F.when(
                    F.col("prev_trip_count").isNotNull()
                    & (F.col("prev_trip_count") > 0),
                    (F.col("trip_count") - F.col("prev_trip_count"))
                    / F.col("prev_trip_count")
                    * 100,
                ).otherwise(F.lit(None)),
                2,
            ),
        )
        .orderBy("pickup_borough", "year_month", "rank")
    )


def detect_anomalies(df: DataFrame) -> DataFrame:
    df_with_fare_per_mile = df.filter(F.col("trip_distance") > 0).withColumn(
        "fare_per_mile", F.col("fare_amount") / F.col("trip_distance")
    )

    route_stats = (
        df_with_fare_per_mile
        .groupBy("pickup_zone", "dropoff_zone")
        .agg(
            F.avg("fare_per_mile").alias("mean_fare_per_mile"),
            F.stddev("fare_per_mile").alias("stddev_fare_per_mile"),
        )
        .filter(F.col("stddev_fare_per_mile").isNotNull())
    )

    joined = df_with_fare_per_mile.join(
        route_stats,
        on=["pickup_zone", "dropoff_zone"],
        how="inner",
    )

    anomaly_threshold = (
        F.col("mean_fare_per_mile") + 3 * F.col("stddev_fare_per_mile")
    )

    return (
        joined
        .filter(F.col("fare_per_mile") > anomaly_threshold)
        .orderBy(F.col("fare_per_mile").desc())
    )


def run_gold(
    spark: SparkSession,
    silver_path: str = "data/silver",
    gold_path: str = "data/gold",
) -> None:
    silver_df = spark.read.parquet(f"{silver_path}/trips/")

    demand_df = demand_by_zone_and_hour(silver_df)
    demand_df.write.mode("overwrite").parquet(f"{gold_path}/demand_by_zone_and_hour")

    top_routes_df = top_routes_by_borough(silver_df)
    top_routes_df.write.mode("overwrite").parquet(f"{gold_path}/top_routes_by_borough")

    avg_time_df = avg_time_between_zones(silver_df)
    avg_time_df.write.mode("overwrite").parquet(f"{gold_path}/avg_time_between_zones")

    monthly_ranking_df = monthly_zone_ranking(silver_df)
    monthly_ranking_df.write.mode("overwrite").parquet(
        f"{gold_path}/monthly_zone_ranking"
    )

    anomalies_df = detect_anomalies(silver_df)
    anomalies_df.write.mode("overwrite").parquet(f"{gold_path}/anomalies")
