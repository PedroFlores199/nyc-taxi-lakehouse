import argparse
import logging
import shutil
from dataclasses import replace

from pyspark.sql import functions as F

from benchmarks.common import write_result
from src.config import get_settings
from src.domain import YearMonth
from src.lake import SILVER_TRIPS, read_table, silver_table, table_history
from src.spark_session import spark_session
from src.transform.silver import run_silver

logger = logging.getLogger(__name__)

DEMO_DIRECTORY = "delta-demo"


def _counts(frame) -> dict[str, int]:
    return {row["year_month"]: row["count"] for row in frame.groupBy("year_month").count().collect()}


def run(first: YearMonth, second: YearMonth) -> str:
    base = get_settings()
    settings = replace(base, data_path=base.data_path / DEMO_DIRECTORY, lake_format="delta")
    shutil.rmtree(settings.data_path, ignore_errors=True)
    settings.data_path.mkdir(parents=True)
    settings.bronze_path.symlink_to(base.bronze_path.resolve(), target_is_directory=True)
    table = silver_table(settings, SILVER_TRIPS)

    with spark_session("delta-time-travel", settings) as spark:
        run_silver(spark, settings, first)
        run_silver(spark, settings, second)
        history = (
            table_history(spark, table)
            .select(
                "version", "timestamp", "operation", F.col("operationMetrics.numOutputRows").alias("rows")
            )
            .orderBy("version")
            .collect()
        )
        first_commit = history[0]["timestamp"].isoformat(sep=" ")
        latest = _counts(read_table(spark, table, settings))
        by_version = _counts(read_table(spark, table, settings, version=0))
        by_timestamp = _counts(read_table(spark, table, settings, timestamp=first_commit))

    lines = [
        "# Delta Lake: time travel sobre silver",
        "",
        f"Se carga {first} (versión 0) y después {second} (versión 1) en la misma tabla Delta.",
        "",
        "| Versión | Fecha | Operación | Filas escritas |",
        "|---|---|---|---|",
        *[
            f"| {row['version']} | {row['timestamp']} | {row['operation']} | {row['rows']} |"
            for row in history
        ],
        "",
        "| Lectura | Filas por mes |",
        "|---|---|",
        f"| Última versión | {latest} |",
        f'| `option("versionAsOf", 0)` | {by_version} |',
        f'| `option("timestampAsOf", "{first_commit}")` | {by_timestamp} |',
        "",
    ]
    return "\n".join(lines)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Demuestra el time travel de Delta Lake en silver")
    parser.add_argument("--primero", type=YearMonth.parse, default=YearMonth(2024, 1))
    parser.add_argument("--segundo", type=YearMonth.parse, default=YearMonth(2024, 2))
    args = parser.parse_args()
    destination = write_result("delta_time_travel.md", run(args.primero, args.segundo))
    logger.info("Resultado guardado en %s", destination)


if __name__ == "__main__":
    main()
