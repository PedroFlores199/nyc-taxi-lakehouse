import logging
from collections.abc import Callable, Collection, Iterator
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

import psycopg2
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

from src.config import Settings
from src.domain import DAY_NAMES, YearMonth, time_slot_for_hour
from src.lake import GOLD_TRIPS_AGGREGATED, gold_table, read_table, read_zone_lookup
from src.transform.silver import holidays_for
from src.transform.zones import standardize_zones

logger = logging.getLogger(__name__)

DDL_FILE = Path(__file__).resolve().parents[2] / "sql" / "warehouse.sql"
STAGING_TABLE = "warehouse.stg_fact_viajes"

Connector = Callable[[str], PgConnection]


def date_id(day: date) -> int:
    return day.year * 10000 + day.month * 100 + day.day


def build_date_rows(period: YearMonth, holiday_dates: Collection[date]) -> list[tuple]:
    rows = []
    day = period.first_day
    while day < period.next.first_day:
        rows.append(
            (
                date_id(day),
                day,
                day.year,
                day.month,
                day.day,
                day.isoweekday(),
                DAY_NAMES[day.weekday()],
                day.isoweekday() >= 6,
                day in holiday_dates,
            )
        )
        day += timedelta(days=1)
    return rows


def build_hour_rows() -> list[tuple[int, str]]:
    return [(hour, time_slot_for_hour(hour)) for hour in range(24)]


def build_zone_rows(zones: list[Row]) -> list[tuple]:
    return [
        (
            row["location_id"],
            row["zone"] or "Desconocida",
            row["borough"] or "Desconocido",
            row["service_zone"],
        )
        for row in zones
    ]


def fact_frame(trips: DataFrame) -> DataFrame:
    money = "decimal(14,2)"
    return trips.select(
        F.date_format("pickup_date", "yyyyMMdd").cast("int").alias("fecha_id"),
        F.col("pickup_hour").cast("smallint").alias("hora"),
        F.col("pickup_location_id").cast("smallint").alias("zona_origen_id"),
        F.col("dropoff_location_id").cast("smallint").alias("zona_destino_id"),
        F.col("trips").cast("int").alias("viajes"),
        F.col("passengers").cast("int").alias("pasajeros"),
        F.col("total_amount_sum").cast(money).alias("importe_total"),
        F.col("fare_amount_sum").cast(money).alias("tarifa_total"),
        F.col("tip_amount_sum").cast(money).alias("propina_total"),
        F.col("duration_minutes_sum").cast(money).alias("duracion_total_min"),
        F.col("trip_distance_sum").cast(money).alias("distancia_total_millas"),
    )


@contextmanager
def transaction(dsn: str, connect: Connector = psycopg2.connect) -> Iterator[PgConnection]:
    conn = connect(dsn)
    try:
        with conn:
            yield conn
    finally:
        conn.close()


def apply_ddl(conn: PgConnection) -> None:
    with conn.cursor() as cursor:
        cursor.execute(DDL_FILE.read_text(encoding="utf-8"))


def upsert_dimensions(
    conn: PgConnection,
    zone_rows: list[tuple],
    date_rows: list[tuple],
    hour_rows: list[tuple],
) -> None:
    with conn.cursor() as cursor:
        execute_values(
            cursor,
            """
            INSERT INTO warehouse.dim_zona (zona_id, nombre, distrito, zona_servicio)
            VALUES %s
            ON CONFLICT (zona_id) DO UPDATE SET
                nombre = EXCLUDED.nombre,
                distrito = EXCLUDED.distrito,
                zona_servicio = EXCLUDED.zona_servicio
            """,
            zone_rows,
        )
        execute_values(
            cursor,
            """
            INSERT INTO warehouse.dim_fecha (
                fecha_id, fecha, anio, mes, dia, dia_semana, nombre_dia, es_fin_de_semana, es_festivo
            )
            VALUES %s
            ON CONFLICT (fecha_id) DO UPDATE SET es_festivo = EXCLUDED.es_festivo
            """,
            date_rows,
        )
        execute_values(
            cursor,
            """
            INSERT INTO warehouse.dim_franja_horaria (hora, franja)
            VALUES %s
            ON CONFLICT (hora) DO UPDATE SET franja = EXCLUDED.franja
            """,
            hour_rows,
        )


def replace_month_from_staging(conn: PgConnection, period: YearMonth) -> int:
    last_day = period.next.first_day - timedelta(days=1)
    with conn.cursor() as cursor:
        cursor.execute(
            "DELETE FROM warehouse.fact_viajes WHERE fecha_id BETWEEN %s AND %s",
            (date_id(period.first_day), date_id(last_day)),
        )
        cursor.execute(
            f"""
            INSERT INTO warehouse.fact_viajes
            SELECT * FROM {STAGING_TABLE}
            WHERE fecha_id BETWEEN %s AND %s
            """,
            (date_id(period.first_day), date_id(last_day)),
        )
        return cursor.rowcount


def write_staging(fact: DataFrame, settings: Settings) -> None:
    (
        fact.write.format("jdbc")
        .mode("overwrite")
        .option("url", settings.postgres_jdbc_url)
        .option("dbtable", STAGING_TABLE)
        .option("user", settings.postgres_user)
        .option("password", settings.postgres_password)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .option("batchsize", 10000)
        .save()
    )


def load_warehouse(
    spark: SparkSession,
    settings: Settings,
    period: YearMonth,
    connect: Connector = psycopg2.connect,
) -> int:
    trips = read_table(spark, gold_table(settings, GOLD_TRIPS_AGGREGATED), settings).filter(
        F.col("year_month") == str(period)
    )
    zones = standardize_zones(read_zone_lookup(spark, settings)).collect()
    with transaction(settings.postgres_dsn, connect) as conn:
        apply_ddl(conn)
        upsert_dimensions(
            conn,
            build_zone_rows(zones),
            build_date_rows(period, set(holidays_for([period.year]))),
            build_hour_rows(),
        )
    write_staging(fact_frame(trips), settings)
    with transaction(settings.postgres_dsn, connect) as conn:
        loaded = replace_month_from_staging(conn, period)
    logger.info("Almacén: %s filas de hechos cargadas para %s", loaded, period)
    return loaded
