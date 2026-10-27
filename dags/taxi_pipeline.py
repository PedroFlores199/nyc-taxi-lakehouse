from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param

PUBLICATION_LAG_MONTHS = 2

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}


@dag(
    dag_id="nyc_taxi_lakehouse",
    description="Ingesta, silver, gold y carga en PostgreSQL y MongoDB de un mes de viajes",
    schedule="0 6 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "mes": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="Mes a procesar (AAAA-MM). Vacío: el último mes publicado por la TLC.",
        )
    },
    tags=["nyc-taxi", "medallion"],
)
def nyc_taxi_lakehouse() -> None:
    @task
    def resolver_mes(**context) -> str:
        from src.domain import YearMonth

        requested = context["params"].get("mes")
        if requested:
            return str(YearMonth.parse(requested))
        interval_start = context["data_interval_start"]
        period = YearMonth(interval_start.year, interval_start.month)
        for _ in range(PUBLICATION_LAG_MONTHS):
            period = period.previous
        return str(period)

    @task
    def ingesta_bronze(mes: str) -> list[str]:
        from src.domain import YearMonth
        from src.pipeline import run_bronze

        period = YearMonth.parse(mes)
        return [str(path) for path in run_bronze(period, period)]

    @task
    def transformacion_silver(mes: str) -> dict:
        from src.domain import YearMonth
        from src.pipeline import run_silver_stage

        return run_silver_stage(YearMonth.parse(mes))

    @task
    def agregados_gold(mes: str) -> dict:
        from src.domain import YearMonth
        from src.pipeline import run_gold_stage

        return run_gold_stage(YearMonth.parse(mes))

    @task
    def carga_postgres(mes: str) -> int:
        from src.domain import YearMonth
        from src.pipeline import run_postgres_load

        return run_postgres_load(YearMonth.parse(mes))

    @task
    def carga_mongo(mes: str) -> dict:
        from src.domain import YearMonth
        from src.pipeline import run_mongo_load

        return run_mongo_load(YearMonth.parse(mes))

    mes = resolver_mes()
    gold = agregados_gold(mes)
    ingesta_bronze(mes) >> transformacion_silver(mes) >> gold
    gold >> [carga_postgres(mes), carga_mongo(mes)]


nyc_taxi_lakehouse()
