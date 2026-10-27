import os
from collections.abc import Iterator
from datetime import date
from decimal import Decimal

import psycopg2
import pytest
from psycopg2.extensions import connection as PgConnection
from pyspark.sql import Row, SparkSession

from src.domain import YearMonth
from src.load.warehouse import (
    STAGING_TABLE,
    apply_ddl,
    build_date_rows,
    build_hour_rows,
    build_zone_rows,
    fact_frame,
    replace_month_from_staging,
    upsert_dimensions,
)
from src.transform.silver import holidays_for

JANUARY = YearMonth(2024, 1)


def test_date_dimension_for_month() -> None:
    rows = build_date_rows(JANUARY, set(holidays_for([2024])))
    assert len(rows) == 31
    first = rows[0]
    assert first[:2] == (20240101, date(2024, 1, 1))
    assert first[6] == "lunes"
    assert first[8] is True
    weekend = {row[1]: row[7] for row in rows}
    assert weekend[date(2024, 1, 6)] is True
    assert weekend[date(2024, 1, 8)] is False


def test_hour_dimension() -> None:
    rows = dict(build_hour_rows())
    assert len(rows) == 24
    assert rows[7] == "punta_manana"
    assert rows[23] == "noche"


def test_zone_rows_fill_missing_names() -> None:
    rows = build_zone_rows([Row(location_id=264, zone=None, borough=None, service_zone="N/A")])
    assert rows == [(264, "Desconocida", "Desconocido", "N/A")]


def test_fact_frame_maps_to_warehouse_columns(spark: SparkSession) -> None:
    trips = spark.createDataFrame(
        [("2024-01", date(2024, 1, 16), 8, 161, 236, 3, 4, 60.5, 45.0, 9.0, 75.0, 12.3)],
        "year_month string, pickup_date date, pickup_hour int, pickup_location_id int, "
        "dropoff_location_id int, trips long, passengers long, total_amount_sum double, "
        "fare_amount_sum double, tip_amount_sum double, duration_minutes_sum double, "
        "trip_distance_sum double",
    )
    row = fact_frame(trips).first()
    assert row["fecha_id"] == 20240116
    assert row["zona_origen_id"] == 161
    assert row["importe_total"] == Decimal("60.50")
    assert dict(fact_frame(trips).dtypes)["hora"] == "smallint"


@pytest.fixture
def postgres() -> Iterator[PgConnection]:
    dsn = os.environ.get("POSTGRES_TEST_DSN")
    if not dsn:
        pytest.skip("POSTGRES_TEST_DSN no definido: se omite la prueba de integración con PostgreSQL")
    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
            apply_ddl(conn)
        yield conn
    finally:
        conn.close()


def _stage(conn: PgConnection, rows: list[tuple]) -> None:
    with conn.cursor() as cursor:
        cursor.execute(f"TRUNCATE {STAGING_TABLE}")
        for row in rows:
            cursor.execute(
                f"INSERT INTO {STAGING_TABLE} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row
            )


def test_month_reload_does_not_duplicate(postgres: PgConnection) -> None:
    with postgres:
        upsert_dimensions(
            postgres,
            [
                (161, "Midtown Center", "Manhattan", "Yellow Zone"),
                (236, "UES North", "Manhattan", "Yellow Zone"),
            ],
            build_date_rows(JANUARY, set()),
            build_hour_rows(),
        )
    fact = (20240116, 8, 161, 236, 3, 4, 60.5, 45.0, 9.0, 75.0, 12.3)
    for _ in range(2):
        with postgres:
            _stage(postgres, [fact])
            assert replace_month_from_staging(postgres, JANUARY) == 1
    with postgres, postgres.cursor() as cursor:
        cursor.execute("SELECT COUNT(*), SUM(viajes) FROM warehouse.fact_viajes")
        assert cursor.fetchone() == (1, 3)
        cursor.execute("SELECT viajes, importe_medio FROM warehouse.v_demanda_zona_hora")
        assert cursor.fetchone() == (3, Decimal("20.17"))
