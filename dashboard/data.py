from datetime import date, datetime, time, timedelta

import pandas as pd
from pymongo.database import Database
from sqlalchemy import Engine, bindparam, text

FACT_JOINS = """
FROM warehouse.fact_viajes v
JOIN warehouse.dim_fecha f USING (fecha_id)
JOIN warehouse.dim_zona zo ON zo.zona_id = v.zona_origen_id
JOIN warehouse.dim_zona zd ON zd.zona_id = v.zona_destino_id
WHERE f.fecha BETWEEN :desde AND :hasta
  AND zo.distrito IN :distritos
"""


def _query(engine: Engine, sql: str, desde: date, hasta: date, distritos: tuple[str, ...]) -> pd.DataFrame:
    statement = text(sql).bindparams(bindparam("distritos", expanding=True))
    with engine.connect() as connection:
        return pd.read_sql(
            statement,
            connection,
            params={"desde": desde, "hasta": hasta, "distritos": list(distritos)},
        )


def available_range(engine: Engine) -> tuple[date, date] | None:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT MIN(f.fecha), MAX(f.fecha) FROM warehouse.fact_viajes v "
                "JOIN warehouse.dim_fecha f USING (fecha_id)"
            )
        ).one()
    return (row[0], row[1]) if row[0] else None


def boroughs(engine: Engine) -> list[str]:
    with engine.connect() as connection:
        rows = connection.execute(text("SELECT DISTINCT distrito FROM warehouse.dim_zona ORDER BY distrito"))
        return [row[0] for row in rows]


def summary(engine: Engine, desde: date, hasta: date, distritos: tuple[str, ...]) -> pd.DataFrame:
    return _query(
        engine,
        f"""
        SELECT
            SUM(v.viajes) AS viajes,
            ROUND(SUM(v.importe_total) / NULLIF(SUM(v.viajes), 0), 2) AS importe_medio,
            ROUND(SUM(v.propina_total) / NULLIF(SUM(v.tarifa_total), 0) * 100, 2) AS propina_pct,
            ROUND(SUM(v.duracion_total_min) / NULLIF(SUM(v.viajes), 0), 1) AS duracion_media_min
        {FACT_JOINS}
        """,
        desde,
        hasta,
        distritos,
    )


def demand_heatmap(engine: Engine, desde: date, hasta: date, distritos: tuple[str, ...]) -> pd.DataFrame:
    return _query(
        engine,
        f"""
        WITH diario AS (
            SELECT f.fecha, f.dia_semana, v.hora, SUM(v.viajes) AS viajes
            {FACT_JOINS}
            GROUP BY f.fecha, f.dia_semana, v.hora
        )
        SELECT dia_semana, hora, ROUND(AVG(viajes), 1) AS viajes_medios
        FROM diario
        GROUP BY dia_semana, hora
        ORDER BY dia_semana, hora
        """,
        desde,
        hasta,
        distritos,
    )


def top_routes(engine: Engine, desde: date, hasta: date, distritos: tuple[str, ...]) -> pd.DataFrame:
    return _query(
        engine,
        f"""
        SELECT
            zo.distrito,
            zo.nombre AS origen,
            zd.nombre AS destino,
            SUM(v.viajes) AS viajes,
            ROUND(SUM(v.duracion_total_min) / SUM(v.viajes), 1) AS duracion_media_min,
            ROUND(SUM(v.importe_total) / SUM(v.viajes), 2) AS importe_medio
        {FACT_JOINS}
        GROUP BY zo.distrito, zo.nombre, zd.nombre
        ORDER BY viajes DESC
        LIMIT 10
        """,
        desde,
        hasta,
        distritos,
    )


def _date_filter(desde: date, hasta: date) -> dict:
    return {
        "fecha_recogida": {
            "$gte": datetime.combine(desde, time.min),
            "$lt": datetime.combine(hasta + timedelta(days=1), time.min),
        }
    }


def zone_geojson(db: Database) -> dict:
    features = [
        {
            "type": "Feature",
            "id": document["_id"],
            "properties": {"nombre": document["nombre"], "distrito": document["distrito"]},
            "geometry": document["geometria"],
        }
        for document in db.zonas.find(
            {"geometria": {"$exists": True}}, {"nombre": 1, "distrito": 1, "geometria": 1}
        )
    ]
    return {"type": "FeatureCollection", "features": features}


def anomalies_by_zone(db: Database, desde: date, hasta: date, distritos: tuple[str, ...]) -> pd.DataFrame:
    pipeline = [
        {"$match": {**_date_filter(desde, hasta), "distrito_origen": {"$in": list(distritos)}}},
        {
            "$group": {
                "_id": "$zona_origen_id",
                "zona": {"$first": "$zona_origen"},
                "anomalias": {"$sum": 1},
                "importe_por_milla_medio": {"$avg": "$importe_por_milla"},
            }
        },
    ]
    rows = list(db.anomalias.aggregate(pipeline))
    frame = pd.DataFrame(rows, columns=["_id", "zona", "anomalias", "importe_por_milla_medio"])
    return frame.rename(columns={"_id": "zona_id"})


def worst_anomalies(
    db: Database, desde: date, hasta: date, distritos: tuple[str, ...], limit: int = 100
) -> pd.DataFrame:
    projection = {
        "_id": 0,
        "fecha_recogida": 1,
        "zona_origen": 1,
        "zona_destino": 1,
        "distancia_millas": 1,
        "tarifa": 1,
        "importe_total": 1,
        "importe_por_milla": 1,
        "media_trayecto": 1,
        "puntuacion_z": 1,
    }
    cursor = (
        db.anomalias.find(
            {**_date_filter(desde, hasta), "distrito_origen": {"$in": list(distritos)}}, projection
        )
        .sort("puntuacion_z", -1)
        .limit(limit)
    )
    return pd.DataFrame(list(cursor))
