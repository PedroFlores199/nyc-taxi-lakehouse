import argparse
import json
import logging
from collections.abc import Sequence
from pathlib import Path

from src.config import Settings, get_settings
from src.domain import YearMonth, month_range
from src.ingest.bronze import ingest_range
from src.load.mongo import load_mongo
from src.load.warehouse import load_warehouse
from src.ml.duration_model import train_and_publish
from src.spark_session import MONGO_CONNECTOR_PACKAGE, POSTGRES_JDBC_PACKAGE, spark_session
from src.transform.gold import run_gold
from src.transform.silver import run_silver

logger = logging.getLogger(__name__)


def run_bronze(start: YearMonth, end: YearMonth, settings: Settings | None = None) -> list[Path]:
    return ingest_range(settings or get_settings(), start, end)


def run_silver_stage(period: YearMonth, settings: Settings | None = None) -> dict:
    settings = settings or get_settings()
    with spark_session(f"silver-{period}", settings) as spark:
        return run_silver(spark, settings, period)


def run_gold_stage(period: YearMonth, settings: Settings | None = None) -> dict[str, int]:
    settings = settings or get_settings()
    with spark_session(f"gold-{period}", settings) as spark:
        return run_gold(spark, settings, period)


def run_postgres_load(period: YearMonth, settings: Settings | None = None) -> int:
    settings = settings or get_settings()
    with spark_session(f"postgres-{period}", settings, (POSTGRES_JDBC_PACKAGE,)) as spark:
        return load_warehouse(spark, settings, period)


def run_mongo_load(period: YearMonth, settings: Settings | None = None) -> dict[str, int]:
    settings = settings or get_settings()
    with spark_session(f"mongo-{period}", settings, (MONGO_CONNECTOR_PACKAGE,)) as spark:
        return load_mongo(spark, settings, period)


def run_duration_model(period: YearMonth, settings: Settings | None = None) -> dict:
    settings = settings or get_settings()
    with spark_session(f"modelo-{period}", settings) as spark:
        return train_and_publish(spark, settings, period)


def run_all(start: YearMonth, end: YearMonth, settings: Settings | None = None) -> None:
    settings = settings or get_settings()
    run_bronze(start, end, settings)
    for period in month_range(start, end):
        run_silver_stage(period, settings)
        run_gold_stage(period, settings)
        run_postgres_load(period, settings)
        run_mongo_load(period, settings)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.pipeline",
        description="Pipeline medallion de los taxis de Nueva York",
    )
    stages = parser.add_subparsers(dest="etapa", required=True)

    for name, help_text in (
        ("bronze", "Descarga los meses indicados (idempotente)"),
        ("todo", "Ejecuta bronze, silver, gold y las cargas para un rango de meses"),
    ):
        stage = stages.add_parser(name, help=help_text)
        stage.add_argument("--desde", type=YearMonth.parse, required=True, help="Mes inicial AAAA-MM")
        stage.add_argument(
            "--hasta", type=YearMonth.parse, help="Mes final AAAA-MM (por defecto, el inicial)"
        )

    for name, help_text in (
        ("silver", "Limpia y enriquece un mes"),
        ("gold", "Calcula los agregados de negocio de un mes"),
        ("postgres", "Carga el modelo en estrella de un mes en PostgreSQL"),
        ("mongo", "Carga la capa de servicio en MongoDB"),
        ("modelo", "Entrena el modelo de duración con Spark MLlib"),
    ):
        stage = stages.add_parser(name, help=help_text)
        stage.add_argument("--mes", type=YearMonth.parse, required=True, help="Mes AAAA-MM")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parser().parse_args(argv)
    if args.etapa == "bronze":
        result = [str(path) for path in run_bronze(args.desde, args.hasta or args.desde)]
    elif args.etapa == "todo":
        run_all(args.desde, args.hasta or args.desde)
        result = "ok"
    elif args.etapa == "silver":
        result = run_silver_stage(args.mes)
    elif args.etapa == "gold":
        result = run_gold_stage(args.mes)
    elif args.etapa == "postgres":
        result = run_postgres_load(args.mes)
    elif args.etapa == "mongo":
        result = run_mongo_load(args.mes)
    else:
        result = run_duration_model(args.mes)
    logger.info("Resultado: %s", json.dumps(result, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
