import argparse
import logging

import psycopg2
from pymongo import MongoClient

from benchmarks.common import measure, median_ms, p95_ms, write_result
from src.config import get_settings

logger = logging.getLogger(__name__)

POSTGRES_QUERY = """
SELECT hora, SUM(viajes) AS viajes
FROM warehouse.fact_viajes
WHERE zona_origen_id = %s
GROUP BY hora
ORDER BY hora
"""


def mongo_pipeline(zone_id: int) -> list[dict]:
    return [
        {"$match": {"_id": zone_id}},
        {"$unwind": "$demanda"},
        {"$group": {"_id": "$demanda.hora", "viajes": {"$sum": "$demanda.viajes"}}},
        {"$sort": {"_id": 1}},
    ]


def run(zones: list[int], repetitions: int) -> str:
    settings = get_settings()
    connection = psycopg2.connect(settings.postgres_dsn)
    client: MongoClient = MongoClient(settings.mongo_uri)
    db = client[settings.mongo_db]
    rows = [
        "# Demanda de una zona por hora: PostgreSQL frente a MongoDB",
        "",
        f"Mediana y percentil 95 de {repetitions} ejecuciones por zona, medidos desde el cliente.",
        "",
        "| Zona | PostgreSQL mediana (ms) | PostgreSQL p95 (ms) | MongoDB mediana (ms) | MongoDB p95 (ms) |",
        "|---|---|---|---|---|",
    ]
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM warehouse.fact_viajes")
            fact_rows = cursor.fetchone()[0]
            for zone_id in zones:

                def postgres_query(zone: int = zone_id) -> list[tuple]:
                    cursor.execute(POSTGRES_QUERY, (zone,))
                    return cursor.fetchall()

                def mongo_query(zone: int = zone_id) -> list[dict]:
                    return list(db.zonas.aggregate(mongo_pipeline(zone)))

                expected = {hour: int(trips) for hour, trips in postgres_query()}
                observed = {row["_id"]: int(row["viajes"]) for row in mongo_query()}
                if expected != observed:
                    raise AssertionError(f"PostgreSQL y MongoDB no coinciden para la zona {zone_id}")
                postgres_timings = measure(postgres_query, repetitions)
                mongo_timings = measure(mongo_query, repetitions)
                rows.append(
                    f"| {zone_id} | {median_ms(postgres_timings)} | {p95_ms(postgres_timings)} | "
                    f"{median_ms(mongo_timings)} | {p95_ms(mongo_timings)} |"
                )
    finally:
        connection.close()
        client.close()
    rows += ["", f"Tabla de hechos: {fact_rows} filas. Colección zonas: un documento por zona.", ""]
    return "\n".join(rows)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Misma consulta en PostgreSQL y en MongoDB")
    parser.add_argument("--zonas", type=int, nargs="+", default=[132, 161, 230, 236, 61])
    parser.add_argument("--repeticiones", type=int, default=200)
    args = parser.parse_args()
    destination = write_result("postgres_vs_mongo.md", run(args.zonas, args.repeticiones))
    logger.info("Resultado guardado en %s", destination)


if __name__ == "__main__":
    main()
