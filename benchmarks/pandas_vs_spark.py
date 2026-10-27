import argparse
import gc
import logging
import time

import pandas as pd
from matplotlib.figure import Figure
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from benchmarks.common import RESULTS_DIR, write_result
from src.config import Settings, get_settings
from src.domain import YearMonth, month_range
from src.lake import ZONE_LOOKUP_FILE, bronze_trip_file, bronze_zone_file, read_zone_lookup
from src.spark_session import spark_session
from src.transform.silver import (
    MAX_DISTANCE_MILES,
    MAX_DURATION_MINUTES,
    MAX_PASSENGERS,
    RAW_COLUMNS,
    TRIP_ID_COLUMNS,
    build_silver,
)

logger = logging.getLogger(__name__)

FIRST_MONTH = YearMonth(2024, 1)
SIZES = (1, 3, 6)


def pandas_month(settings: Settings, period: YearMonth, zones: pd.DataFrame) -> pd.DataFrame:
    raw = pd.read_parquet(bronze_trip_file(settings, period), columns=list(RAW_COLUMNS))
    trips = raw.rename(columns={source: target for source, (target, _) in RAW_COLUMNS.items()})
    trips["pickup_datetime"] = pd.to_datetime(trips["pickup_datetime"])
    trips["dropoff_datetime"] = pd.to_datetime(trips["dropoff_datetime"])
    duration = (trips["dropoff_datetime"] - trips["pickup_datetime"]).dt.total_seconds() / 60
    month_start = pd.Timestamp(period.first_day)
    next_month = pd.Timestamp(period.next.first_day)
    valid = (
        trips[["pickup_datetime", "dropoff_datetime", "pickup_location_id", "dropoff_location_id"]]
        .notna()
        .all(axis=1)
        & (trips["pickup_datetime"] >= month_start)
        & (trips["pickup_datetime"] < next_month)
        & (duration > 0)
        & (duration <= MAX_DURATION_MINUTES)
        & (trips["trip_distance"] > 0)
        & (trips["trip_distance"] <= MAX_DISTANCE_MILES)
        & (trips["fare_amount"] >= 0)
        & (trips["total_amount"] >= 0)
        & trips["passenger_count"].between(1, MAX_PASSENGERS)
    )
    clean = trips[valid].drop_duplicates(subset=list(TRIP_ID_COLUMNS)).copy()
    clean["pickup_hour"] = clean["pickup_datetime"].dt.hour
    return clean.merge(zones, left_on="pickup_location_id", right_on="LocationID", how="left")


def pandas_demand(settings: Settings, months: list[YearMonth]) -> int:
    zones = pd.read_csv(bronze_zone_file(settings, ZONE_LOOKUP_FILE))
    enriched = pd.concat([pandas_month(settings, period, zones) for period in months], ignore_index=True)
    demand = enriched.groupby(["Borough", "Zone", "pickup_hour"], dropna=False).agg(
        trips=("total_amount", "size"), avg_amount=("total_amount", "mean")
    )
    return len(demand)


def spark_demand(spark: SparkSession, settings: Settings, months: list[YearMonth]) -> int:
    zones = read_zone_lookup(spark, settings)
    frames: list[DataFrame] = []
    for period in months:
        silver, _ = build_silver(spark.read.parquet(str(bronze_trip_file(settings, period))), zones, period)
        frames.append(silver)
    enriched = frames[0]
    for frame in frames[1:]:
        enriched = enriched.unionByName(frame)
    demand = enriched.groupBy("pickup_borough", "pickup_zone", "pickup_hour").agg(
        F.count(F.lit(1)).alias("trips"), F.avg("total_amount").alias("avg_amount")
    )
    return len(demand.collect())


def timed(action) -> tuple[float, int]:
    gc.collect()
    start = time.perf_counter()
    result = action()
    return round(time.perf_counter() - start, 1), result


def run(sizes: tuple[int, ...]) -> list[dict]:
    settings = get_settings()
    results = []
    with spark_session("benchmark-pandas-spark", settings) as spark:
        spark_demand(spark, settings, [FIRST_MONTH])
        for size in sizes:
            months = month_range(FIRST_MONTH, YearMonth(2024, size))
            rows = sum(
                pd.read_parquet(bronze_trip_file(settings, m), columns=["VendorID"]).shape[0] for m in months
            )
            spark_seconds, spark_groups = timed(lambda months=months: spark_demand(spark, settings, months))
            pandas_seconds, pandas_groups = timed(lambda months=months: pandas_demand(settings, months))
            results.append(
                {
                    "meses": size,
                    "filas": rows,
                    "pandas_s": pandas_seconds,
                    "spark_s": spark_seconds,
                    "grupos_pandas": pandas_groups,
                    "grupos_spark": spark_groups,
                }
            )
            logger.info("Resultado %s", results[-1])
    return results


def chart(results: list[dict]) -> None:
    figure = Figure(figsize=(7, 4))
    axis = figure.subplots()
    months = [result["meses"] for result in results]
    axis.plot(months, [result["pandas_s"] for result in results], marker="o", label="pandas")
    axis.plot(months, [result["spark_s"] for result in results], marker="o", label="PySpark (local[*])")
    axis.set_xticks(months, [f"{m} mes" if m == 1 else f"{m} meses" for m in months])
    axis.set_ylabel("Segundos")
    axis.set_title("Limpieza + enriquecimiento + demanda por zona y hora")
    axis.grid(alpha=0.3)
    axis.legend()
    figure.tight_layout()
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    figure.savefig(RESULTS_DIR / "pandas_vs_spark.png", dpi=150)


def report(results: list[dict]) -> str:
    lines = [
        "# pandas frente a Spark",
        "",
        "Mismo trabajo en las dos herramientas: leer los Parquet de bronze, aplicar las reglas de limpieza,",
        "quitar duplicados, unir con las zonas y agregar la demanda por zona y hora.",
        "",
        "| Meses | Filas leídas | pandas (s) | Spark (s) |",
        "|---|---|---|---|",
    ]
    lines += [
        f"| {result['meses']} | {result['filas']:,} | {result['pandas_s']} | {result['spark_s']} |".replace(
            ",", "."
        )
        for result in results
    ]
    return "\n".join([*lines, "", "![pandas frente a Spark](pandas_vs_spark.png)", ""])


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Compara pandas y Spark procesando 1, 3 y 6 meses")
    parser.add_argument("--meses", type=int, nargs="+", default=list(SIZES))
    args = parser.parse_args()
    results = run(tuple(args.meses))
    chart(results)
    destination = write_result("pandas_vs_spark.md", report(results))
    logger.info("Resultado guardado en %s", destination)


if __name__ == "__main__":
    main()
