import argparse
import logging
from datetime import datetime, timedelta

from pymongo import MongoClient
from pymongo.collection import Collection

from benchmarks.common import measure, median_ms, write_result
from src.config import get_settings

logger = logging.getLogger(__name__)

NATURAL_ORDER = [("$natural", 1)]


def _winning_stage(plan: dict) -> str:
    stage = plan["queryPlanner"]["winningPlan"]
    stages = []
    while stage:
        stages.append(stage["stage"])
        stage = stage.get("inputStage")
    return " → ".join(reversed(stages))


def explain(collection: Collection, query: dict, hint: object, sort: list | None = None) -> dict:
    cursor = collection.find(query).hint(hint)
    if sort:
        cursor = cursor.sort(sort)
    plan = cursor.explain()
    stats = plan["executionStats"]

    def fetch() -> None:
        repeated = collection.find(query).hint(hint)
        if sort:
            repeated = repeated.sort(sort)
        list(repeated)

    return {
        "plan": _winning_stage(plan),
        "documentos": stats["totalDocsExamined"],
        "claves": stats["totalKeysExamined"],
        "devueltos": stats["nReturned"],
        "servidor_ms": stats["executionTimeMillis"],
        "cliente_ms": median_ms(measure(fetch, repetitions=50, warmup=3)),
    }


def run() -> str:
    settings = get_settings()
    client: MongoClient = MongoClient(settings.mongo_uri)
    db = client[settings.mongo_db]
    try:
        route = db.trayectos.find_one({}, sort=[("viajes", -1)], projection={"origen": 1, "destino": 1})
        route_query = {"origen": route["origen"], "destino": route["destino"]}
        latest = db.anomalias.find_one({}, sort=[("fecha_recogida", -1)])["fecha_recogida"]
        start = datetime(latest.year, latest.month, latest.day)
        anomaly_query = {"fecha_recogida": {"$gte": start, "$lt": start + timedelta(days=1)}}
        sort = [("fecha_recogida", -1), ("_id", 1)]
        cases = [
            ("trayectos {origen, destino}", db.trayectos, route_query, None, "origen_destino"),
            ("anomalias de un día ordenadas", db.anomalias, anomaly_query, sort, "fecha_recogida"),
        ]
        lines = [
            "# MongoDB: consulta con y sin índice",
            "",
            f"Colecciones: {db.trayectos.estimated_document_count()} trayectos y "
            f"{db.anomalias.estimated_document_count()} anomalías.",
            "",
            "| Consulta | Índice | Plan | Docs examinados | Claves examinadas | Devueltos | "
            "Servidor (ms) | Cliente, mediana (ms) |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for name, collection, query, order, index in cases:
            for label, hint in (("sin índice", NATURAL_ORDER), (index, index)):
                result = explain(collection, query, hint, order)
                lines.append(
                    f"| {name} | {label} | {result['plan']} | {result['documentos']} | {result['claves']} | "
                    f"{result['devueltos']} | {result['servidor_ms']} | {result['cliente_ms']} |"
                )
        return "\n".join([*lines, ""])
    finally:
        client.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    argparse.ArgumentParser(description="explain() de consultas MongoDB con y sin índice").parse_args()
    destination = write_result("mongo_explain.md", run())
    logger.info("Resultado guardado en %s", destination)


if __name__ == "__main__":
    main()
