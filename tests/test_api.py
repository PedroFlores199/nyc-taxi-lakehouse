import os
from collections.abc import Iterator
from datetime import datetime, timedelta
from uuid import uuid4

import mongomock
import pytest
from bson import Int64
from fastapi.testclient import TestClient
from pymongo import MongoClient
from pymongo.database import Database

from api.main import create_app
from src.load.mongo_schema import ensure_collections
from src.ml.serving import MODEL_ID

UPDATED_AT = datetime(2024, 3, 1)


def _square(lon: float, lat: float, side: float = 0.01) -> list:
    return [[[[lon, lat], [lon + side, lat], [lon + side, lat + side], [lon, lat + side], [lon, lat]]]]


def _zone(zone_id: int, name: str, borough: str, lon: float, lat: float, **extra: object) -> dict:
    return {
        "_id": zone_id,
        "nombre": name,
        "distrito": borough,
        "zona_servicio": "Yellow Zone",
        "geometria": {"type": "MultiPolygon", "coordinates": _square(lon, lat)},
        "centroide": {"type": "Point", "coordinates": [lon + 0.005, lat + 0.005]},
        "actualizado_en": UPDATED_AT,
        **extra,
    }


ZONES = [
    _zone(
        161,
        "Midtown Center",
        "Manhattan",
        -73.99,
        40.75,
        demanda=[
            {
                "dia_semana": 1,
                "hora": 8,
                "viajes": Int64(100),
                "importe_medio": 20.0,
                "propina_media_pct": 18.0,
            },
            {
                "dia_semana": 2,
                "hora": 8,
                "viajes": Int64(300),
                "importe_medio": 24.0,
                "propina_media_pct": 22.0,
            },
            {
                "dia_semana": 2,
                "hora": 9,
                "viajes": Int64(50),
                "importe_medio": 30.0,
                "propina_media_pct": 20.0,
            },
        ],
        resumen_mensual=[
            {"mes": "2024-01", "viajes": Int64(1000), "importe_medio": 22.0, "propina_media_pct": 20.0},
            {"mes": "2024-02", "viajes": Int64(500), "importe_medio": 25.0, "propina_media_pct": 10.0},
        ],
        top_destinos=[
            {"viajes": Int64(40), "zona_id": 236, "nombre": "Upper East Side North", "distrito": "Manhattan"}
        ],
    ),
    _zone(
        236,
        "Upper East Side North",
        "Manhattan",
        -73.975,
        40.75,
        resumen_mensual=[
            {"mes": "2024-01", "viajes": Int64(1000), "importe_medio": 18.0, "propina_media_pct": 30.0}
        ],
    ),
    _zone(132, "JFK Airport", "Queens", -73.79, 40.64),
]

ROUTE = {
    "_id": "161-236",
    "origen": 161,
    "destino": 236,
    "origen_nombre": "Midtown Center",
    "destino_nombre": "Upper East Side North",
    "viajes": Int64(120),
    "duracion_media_min": 14.0,
    "importe_medio": 19.5,
    "distancia_media_millas": 2.1,
    "franjas": [
        {
            "franja": "punta_manana",
            "viajes": Int64(80),
            "duracion_media_min": 18.0,
            "importe_medio": 22.0,
            "distancia_media_millas": 2.0,
        },
        {
            "franja": "noche",
            "viajes": Int64(40),
            "duracion_media_min": 9.0,
            "importe_medio": 15.0,
            "distancia_media_millas": 2.3,
        },
    ],
    "actualizado_en": UPDATED_AT,
}


def _anomaly(index: int, pickup: datetime) -> dict:
    return {
        "_id": Int64(1_000 + index),
        "mes": pickup.strftime("%Y-%m"),
        "fecha_recogida": pickup,
        "fecha_llegada": pickup + timedelta(minutes=12),
        "zona_origen_id": 161,
        "zona_destino_id": 236,
        "distancia_millas": 1.5,
        "tarifa": 120.0,
        "importe_total": 130.0,
        "importe_por_milla": 80.0,
        "media_trayecto": 9.0,
        "desviacion_trayecto": 4.0,
        "puntuacion_z": 17.75,
        "cargado_en": UPDATED_AT,
    }


ANOMALIES = [_anomaly(index, datetime(2024, 1, 10 + index, 9)) for index in range(5)]

MODEL = {
    "_id": MODEL_ID,
    "mes_entrenamiento": "2024-01",
    "metricas": {"mae": 3.2},
    "intercepto": 2.0,
    "numericos": {"trip_distance": 3.0, "is_weekend": -1.0},
    "categoricos": {
        "time_slot": {"punta_manana": 4.0, "noche": 0.0},
        "pickup_borough": {"Manhattan": 1.0},
        "dropoff_borough": {"Manhattan": 0.5},
    },
}


def _seed(db: Database, with_model: bool = True) -> None:
    db.zonas.insert_many([dict(zone) for zone in ZONES])
    db.trayectos.insert_one(dict(ROUTE))
    db.anomalias.insert_many([dict(anomaly) for anomaly in ANOMALIES])
    if with_model:
        db.modelos.insert_one(dict(MODEL))


@pytest.fixture
def client() -> Iterator[TestClient]:
    db = mongomock.MongoClient()["test"]
    _seed(db)
    with TestClient(create_app(db)) as test_client:
        yield test_client


def test_health(client: TestClient) -> None:
    assert client.get("/salud").json() == {"estado": "ok"}


def test_route_estimate_uses_time_slot(client: TestClient) -> None:
    body = client.get("/trayecto", params={"origen": 161, "destino": 236, "hora": 8}).json()
    assert body["franja"] == "punta_manana"
    assert body["duracion_estimada_min"] == 18.0
    assert body["importe_estimado"] == 22.0
    assert body["fuente"] == "franja"
    assert body["origen"]["nombre"] == "Midtown Center"


def test_route_estimate_falls_back_to_route_average(client: TestClient) -> None:
    body = client.get("/trayecto", params={"origen": 161, "destino": 236, "hora": 3}).json()
    assert body["franja"] == "madrugada"
    assert body["fuente"] == "media_trayecto"
    assert body["duracion_estimada_min"] == 14.0


def test_route_not_found(client: TestClient) -> None:
    response = client.get("/trayecto", params={"origen": 132, "destino": 161, "hora": 8})
    assert response.status_code == 404


@pytest.mark.parametrize("hour", [-1, 24])
def test_route_validates_hour(client: TestClient, hour: int) -> None:
    assert client.get("/trayecto", params={"origen": 161, "destino": 236, "hora": hour}).status_code == 422


def test_zone_demand_by_hour(client: TestClient) -> None:
    body = client.get("/zonas/161/demanda").json()
    by_hour = {entry["hora"]: entry for entry in body["por_hora"]}
    assert by_hour[8]["viajes"] == 400
    assert by_hour[8]["importe_medio"] == 23.0
    assert by_hour[9]["viajes"] == 50
    assert len(body["por_dia_y_hora"]) == 3


def test_zone_demand_filtered_by_day(client: TestClient) -> None:
    body = client.get("/zonas/161/demanda", params={"dia_semana": 1}).json()
    assert body["por_hora"] == [{"hora": 8, "viajes": 100, "importe_medio": 20.0, "propina_media_pct": 18.0}]


def test_zone_without_demand_returns_empty_lists(client: TestClient) -> None:
    body = client.get("/zonas/132/demanda").json()
    assert body["por_hora"] == []


def test_zone_demand_not_found(client: TestClient) -> None:
    assert client.get("/zonas/999/demanda").status_code == 404


def test_anomalies_are_paginated(client: TestClient) -> None:
    params = {"desde": "2024-01-01", "hasta": "2024-01-31", "tamano": 2}
    first = client.get("/anomalias", params={**params, "pagina": 1}).json()
    last = client.get("/anomalias", params={**params, "pagina": 3}).json()
    assert (first["total"], first["paginas"]) == (5, 3)
    assert [item["fecha_recogida"][:10] for item in first["resultados"]] == ["2024-01-14", "2024-01-13"]
    assert len(last["resultados"]) == 1


def test_anomalies_date_range_is_inclusive(client: TestClient) -> None:
    body = client.get("/anomalias", params={"desde": "2024-01-11", "hasta": "2024-01-12"}).json()
    assert body["total"] == 2


def test_anomalies_validate_range(client: TestClient) -> None:
    response = client.get("/anomalias", params={"desde": "2024-02-01", "hasta": "2024-01-01"})
    assert response.status_code == 422


def test_duration_prediction(client: TestClient) -> None:
    params = {"origen": 161, "destino": 236, "hora": 8, "dia_semana": 2}
    body = client.get("/prediccion/duracion", params=params).json()
    assert body["distancia_millas"] == 2.1
    assert body["duracion_estimada_min"] == round(2.0 + 3.0 * 2.1 + 4.0 + 1.0 + 0.5, 1)
    assert body["error_medio_absoluto_min"] == 3.2


def test_duration_prediction_without_model() -> None:
    db = mongomock.MongoClient()["test"]
    _seed(db, with_model=False)
    with TestClient(create_app(db)) as test_client:
        response = test_client.get("/prediccion/duracion", params={"origen": 161, "destino": 236, "hora": 8})
    assert response.status_code == 503


@pytest.fixture
def real_client() -> Iterator[TestClient]:
    uri = os.environ.get("MONGO_TEST_URI")
    if not uri:
        pytest.skip("MONGO_TEST_URI no definido: las consultas geoespaciales necesitan un MongoDB real")
    mongo: MongoClient = MongoClient(uri, tz_aware=True)
    db = mongo[f"test_{uuid4().hex[:8]}"]
    try:
        ensure_collections(db)
        _seed(db)
        with TestClient(create_app(db)) as test_client:
            yield test_client
    finally:
        mongo.drop_database(db.name)
        mongo.close()


def test_nearby_zones_with_geo_queries(real_client: TestClient) -> None:
    body = real_client.get("/zonas/cerca", params={"lat": 40.755, "lon": -73.985, "radio_m": 3000}).json()
    assert body["zona_actual"]["zona_id"] == 161
    assert [zone["zona_id"] for zone in body["cercanas"]] == [236]
    assert 0 < body["cercanas"][0]["distancia_m"] < 3000


def test_nearby_zones_outside_the_city(real_client: TestClient) -> None:
    body = real_client.get("/zonas/cerca", params={"lat": 40.0, "lon": -75.0}).json()
    assert body == {"zona_actual": None, "cercanas": []}


def test_tip_by_borough_aggregation(real_client: TestClient) -> None:
    rows = real_client.get(
        "/informes/propina-distrito", params={"desde": "2024-01", "hasta": "2024-02"}
    ).json()
    by_key = {(row["distrito"], row["mes"]): row for row in rows}
    january = by_key[("Manhattan", "2024-01")]
    assert january["viajes"] == 2000
    assert january["propina_media_pct"] == 25.0
    assert january["anomalias"] == 5
    assert by_key[("Manhattan", "2024-02")]["anomalias"] == 0
