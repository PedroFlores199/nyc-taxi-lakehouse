from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.bronze.ingest import run as run_ingest_bronze
from src.silver.transform import run as run_transform_silver
from src.gold.transform import run as run_transform_gold

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["data-lake", "nyc-taxi"],
    params={"year": 2024, "month": 1},
) as dag:

    ingest_bronze = PythonOperator(
        task_id="ingest_bronze",
        python_callable=run_ingest_bronze,
        op_kwargs={
            "year": "{{ params.year }}",
            "month": "{{ params.month }}",
        },
    )

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=run_transform_silver,
        op_kwargs={
            "year": "{{ params.year }}",
            "month": "{{ params.month }}",
        },
    )

    transform_gold = PythonOperator(
        task_id="transform_gold",
        python_callable=run_transform_gold,
        op_kwargs={
            "year": "{{ params.year }}",
            "month": "{{ params.month }}",
        },
    )

    ingest_bronze >> transform_silver >> transform_gold
