import argparse
import contextlib
import io
import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from benchmarks.common import measure, median_ms, write_result
from src.config import Settings, get_settings
from src.lake import SILVER_TRIPS, read_table, read_zone_lookup, silver_table
from src.spark_session import spark_session

logger = logging.getLogger(__name__)

DEFAULT_START = date(2024, 1, 15)
DEFAULT_END = date(2024, 1, 21)
BROADCAST_THRESHOLD_DEFAULT = "10485760"


def unoptimized(spark: SparkSession, settings: Settings, start: date, end: date) -> DataFrame:
    trips = read_table(spark, silver_table(settings, SILVER_TRIPS), settings)
    zones = read_zone_lookup(spark, settings)
    return (
        trips.join(zones, trips["pickup_location_id"] == zones["LocationID"])
        .filter(F.to_date("pickup_datetime").between(F.lit(start), F.lit(end)))
        .groupBy("Borough", "service_zone", "pickup_hour")
        .agg(F.count(F.lit(1)).alias("trips"), F.round(F.avg("total_amount"), 2).alias("avg_amount"))
    )


def optimized(spark: SparkSession, settings: Settings, start: date, end: date) -> DataFrame:
    trips = (
        read_table(spark, silver_table(settings, SILVER_TRIPS), settings)
        .filter(F.col("pickup_date").between(F.lit(start), F.lit(end)))
        .select("pickup_location_id", "pickup_hour", "total_amount")
    )
    zones = read_zone_lookup(spark, settings).select(
        F.col("LocationID").alias("pickup_location_id"), "Borough", "service_zone"
    )
    return (
        trips.join(F.broadcast(zones), "pickup_location_id")
        .groupBy("Borough", "service_zone", "pickup_hour")
        .agg(F.count(F.lit(1)).alias("trips"), F.round(F.avg("total_amount"), 2).alias("avg_amount"))
    )


def physical_plan(df: DataFrame) -> str:
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        df.explain(mode="formatted")
    return buffer.getvalue().strip()


def run(start: date, end: date, repetitions: int) -> str:
    settings = get_settings()
    with spark_session("benchmark-explain", settings) as spark:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        before = unoptimized(spark, settings, start, end)
        before_timings = measure(before.collect, repetitions)
        before_plan = physical_plan(before)
        before_rows = sorted(tuple(row) for row in before.collect())

        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", BROADCAST_THRESHOLD_DEFAULT)
        after = optimized(spark, settings, start, end)
        after_timings = measure(after.collect, repetitions)
        after_plan = physical_plan(after)
        after_rows = sorted(tuple(row) for row in after.collect())

    if before_rows != after_rows:
        raise AssertionError("Las dos versiones de la consulta no devuelven el mismo resultado")
    return "\n".join(
        [
            "# Plan de ejecución antes y después de optimizar",
            "",
            f"Consulta: viajes e importe medio por distrito, zona de servicio y hora entre {start} y {end}.",
            f"Ambas versiones devuelven las mismas {len(after_rows)} filas.",
            f"Tiempos: mediana de {repetitions} ejecuciones.",
            "",
            "| Versión | Tiempo (ms) |",
            "|---|---|",
            f"| Antes: sin broadcast, filtro sobre una columna calculada | {median_ms(before_timings)} |",
            f"| Después: broadcast + poda de particiones + proyección | {median_ms(after_timings)} |",
            "",
            "## Antes",
            "",
            "```",
            before_plan,
            "```",
            "",
            "## Después",
            "",
            "```",
            after_plan,
            "```",
            "",
        ]
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Compara el plan de una consulta antes y después de optimizar"
    )
    parser.add_argument("--desde", type=date.fromisoformat, default=DEFAULT_START)
    parser.add_argument("--hasta", type=date.fromisoformat, default=DEFAULT_END)
    parser.add_argument("--repeticiones", type=int, default=5)
    args = parser.parse_args()
    destination = write_result("spark_explain.md", run(args.desde, args.hasta, args.repeticiones))
    logger.info("Resultado guardado en %s", destination)


if __name__ == "__main__":
    main()
