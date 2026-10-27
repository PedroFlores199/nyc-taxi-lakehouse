import os
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from bson import Int64
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import WriteError
from pyspark.sql import SparkSession

from src.load.mongo import anomaly_documents, route_documents, zone_documents
from src.load.mongo_schema import ensure_collections, index_definitions, validator_for
from src.transform.gold import (
    demand_by_zone_hour,
    detect_anomalies,
    travel_time_between_zones,
    trips_aggregated,
)
from src.transform.zones import standardize_zones
from tests.factories import BASE_PICKUP, silver_trip, silver_trips, trip_key, zones_frame

RUN_AT = datetime(2024, 3, 1, tzinfo=UTC)


@pytest.fixture
def shapes(spark: SparkSession):
    square = [[[[-73.99, 40.75], [-73.98, 40.75], [-73.98, 40.76], [-73.99, 40.76], [-73.99, 40.75]]]]
    return spark.createDataFrame(
        [
            (
                161,
                {"type": "MultiPolygon", "coordinates": square},
                {"type": "Point", "coordinates": [-73.985, 40.755]},
            )
        ],
        "location_id int, geometry struct<type:string,coordinates:array<array<array<array<double>>>>>, "
        "centroid struct<type:string,coordinates:array<double>>",
    )


def _silver(spark: SparkSession):
    trips = []
    for index, destination in enumerate([236] * 4 + [132] * 3 + [61] * 2 + [1, 1]):
        trips.append(
            silver_trip(
                trip_id=f"t{index}", dropoff_location_id=destination, pickup=BASE_PICKUP.replace(minute=index)
            )
        )
    trips.append(
        silver_trip(trip_id="night", pickup=datetime(2024, 1, 21, 22, 0), day_of_week=7, time_slot="noche")
    )
    return silver_trips(spark, trips)


def test_zone_documents_nest_demand_and_top_destinations(spark: SparkSession, shapes) -> None:
    silver = _silver(spark)
    documents = zone_documents(
        standardize_zones(zones_frame(spark)),
        shapes,
        demand_by_zone_hour(silver),
        trips_aggregated(silver),
        RUN_AT,
    )
    midtown = documents.filter("_id = 161").first().asDict(recursive=True)
    assert midtown["nombre"] == "Midtown Center"
    assert midtown["geometria"]["type"] == "MultiPolygon"
    assert [(entry["dia_semana"], entry["hora"]) for entry in midtown["demanda"]] == [(2, 10), (7, 22)]
    assert midtown["demanda"][0]["viajes"] == 11
    destinations = midtown["top_destinos"]
    assert [entry["zona_id"] for entry in destinations] == [236, 132, 61, 1]
    assert destinations[0]["viajes"] == 5
    assert midtown["resumen_mensual"][0]["mes"] == "2024-01"
    assert documents.filter("_id = 132").first()["demanda"] is None


def test_route_documents_weight_by_trips(spark: SparkSession) -> None:
    silver = silver_trips(
        spark,
        [
            silver_trip(trip_id="a", minutes=10.0, total_amount=20.0, time_slot="punta_manana"),
            silver_trip(trip_id="b", minutes=30.0, total_amount=40.0, time_slot="punta_manana"),
            silver_trip(trip_id="c", minutes=20.0, total_amount=30.0, time_slot="noche"),
        ],
    )
    route = route_documents(
        travel_time_between_zones(silver), standardize_zones(zones_frame(spark)), RUN_AT
    ).first()
    assert route["_id"] == "161-236"
    assert route["viajes"] == 3
    assert route["duracion_media_min"] == 20.0
    slots = {slot["franja"]: slot for slot in route["franjas"]}
    assert slots["punta_manana"]["duracion_media_min"] == 20.0
    assert slots["punta_manana"]["importe_medio"] == 30.0
    assert slots["noche"]["viajes"] == 1


def test_anomaly_documents_keep_trip_data(spark: SparkSession) -> None:
    trips = [silver_trip(trip_id=f"n{index}", fare_amount=20.0 + index % 3) for index in range(35)]
    trips.append(silver_trip(trip_id="sospechoso", fare_amount=300.0))
    documents = anomaly_documents(detect_anomalies(silver_trips(spark, trips)), RUN_AT).collect()
    assert len(documents) == 1
    document = documents[0].asDict()
    assert document["_id"] == trip_key("sospechoso")
    assert document["mes"] == "2024-01"
    assert document["tarifa"] == 300.0
    assert document["cargado_en"] == RUN_AT.replace(tzinfo=None)


def test_index_definitions() -> None:
    definitions = index_definitions()
    assert set(definitions) >= {"zonas", "trayectos", "anomalias"}
    ttl = next(index for index in definitions["anomalias"] if "expireAfterSeconds" in index)
    assert ttl["expireAfterSeconds"] == 90 * 24 * 3600
    assert ["geometria", "2dsphere"] in next(
        i["keys"] for i in definitions["zonas"] if i["name"] == "geometria_2dsphere"
    )
    compound = definitions["trayectos"][0]
    assert compound["keys"] == [["origen", 1], ["destino", 1]]
    for collection in ("zonas", "trayectos", "anomalias"):
        assert "$jsonSchema" in validator_for(collection)


@pytest.fixture
def mongo_db() -> Iterator[Database]:
    uri = os.environ.get("MONGO_TEST_URI")
    if not uri:
        pytest.skip("MONGO_TEST_URI no definido: se omite la prueba de integración con MongoDB")
    client: MongoClient = MongoClient(uri)
    db = client[f"test_{uuid4().hex[:8]}"]
    try:
        yield db
    finally:
        client.drop_database(db.name)
        client.close()


def test_ensure_collections_applies_validation_and_indexes(mongo_db: Database) -> None:
    ensure_collections(mongo_db)
    ensure_collections(mongo_db)
    indexes = mongo_db.anomalias.index_information()
    assert indexes["ttl_90_dias"]["expireAfterSeconds"] == 7776000
    assert mongo_db.trayectos.index_information()["origen_destino"]["unique"] is True
    with pytest.raises(WriteError):
        mongo_db.trayectos.insert_one({"_id": "1-2", "origen": "uno"})
    mongo_db.trayectos.insert_one(
        {
            "_id": "1-2",
            "origen": 1,
            "destino": 2,
            "viajes": Int64(1),
            "franjas": [
                {"franja": "noche", "viajes": Int64(1), "duracion_media_min": 5.0, "importe_medio": 9.5}
            ],
            "actualizado_en": RUN_AT,
        }
    )
