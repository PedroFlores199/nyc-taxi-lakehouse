from collections import defaultdict
from math import asin, cos, radians, sin, sqrt

from fastapi import APIRouter, HTTPException, Query

from api.dependencies import DatabaseDep
from api.schemas import DemandaDiaHora, DemandaHora, DemandaZona, ZonaCercana, ZonaResumen, ZonasCerca

router = APIRouter(prefix="/zonas", tags=["zonas"])

SUMMARY_PROJECTION = {"nombre": 1, "distrito": 1, "zona_servicio": 1, "centroide": 1}
EARTH_RADIUS_M = 6_371_000


def haversine_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    d_lat = radians(lat2 - lat1)
    d_lon = radians(lon2 - lon1)
    a = sin(d_lat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(d_lon / 2) ** 2
    return 2 * EARTH_RADIUS_M * asin(sqrt(a))


def _summary(document: dict) -> dict:
    centroid = document.get("centroide", {}).get("coordinates")
    return {
        "zona_id": document["_id"],
        "nombre": document["nombre"],
        "distrito": document["distrito"],
        "zona_servicio": document.get("zona_servicio"),
        "centroide": tuple(centroid) if centroid else None,
    }


@router.get("/cerca", response_model=ZonasCerca, summary="Zona en la que estás y zonas de alrededor")
def zonas_cerca(
    db: DatabaseDep,
    lat: float = Query(..., ge=-90, le=90),
    lon: float = Query(..., ge=-180, le=180),
    radio_m: int = Query(2000, ge=100, le=20000),
    limite: int = Query(5, ge=1, le=20),
) -> ZonasCerca:
    point = {"type": "Point", "coordinates": [lon, lat]}
    current = db.zonas.find_one({"geometria": {"$geoIntersects": {"$geometry": point}}}, SUMMARY_PROJECTION)
    query: dict = {"centroide": {"$near": {"$geometry": point, "$maxDistance": radio_m}}}
    if current is not None:
        query["_id"] = {"$ne": current["_id"]}
    nearby = []
    for document in db.zonas.find(query, SUMMARY_PROJECTION).limit(limite):
        summary = _summary(document)
        centroid = summary["centroide"]
        distance = round(haversine_m(lon, lat, centroid[0], centroid[1]), 1) if centroid else None
        nearby.append(ZonaCercana(**summary, distancia_m=distance))
    return ZonasCerca(
        zona_actual=ZonaResumen(**_summary(current)) if current else None,
        cercanas=nearby,
    )


def _hourly_totals(entries: list[dict]) -> list[DemandaHora]:
    totals: dict[int, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for entry in entries:
        bucket = totals[entry["hora"]]
        trips = entry["viajes"]
        bucket["viajes"] += trips
        if entry.get("importe_medio") is not None:
            bucket["importe"] += entry["importe_medio"] * trips
            bucket["viajes_importe"] += trips
        if entry.get("propina_media_pct") is not None:
            bucket["propina"] += entry["propina_media_pct"] * trips
            bucket["viajes_propina"] += trips
    return [
        DemandaHora(
            hora=hour,
            viajes=int(bucket["viajes"]),
            importe_medio=round(bucket["importe"] / bucket["viajes_importe"], 2)
            if bucket["viajes_importe"]
            else None,
            propina_media_pct=round(bucket["propina"] / bucket["viajes_propina"], 2)
            if bucket["viajes_propina"]
            else None,
        )
        for hour, bucket in sorted(totals.items())
    ]


@router.get("/{zona_id}/demanda", response_model=DemandaZona, summary="Demanda de una zona por hora")
def demanda_zona(
    zona_id: int,
    db: DatabaseDep,
    dia_semana: int | None = Query(None, ge=1, le=7, description="1 = lunes, 7 = domingo"),
) -> DemandaZona:
    document = db.zonas.find_one({"_id": zona_id}, {"nombre": 1, "distrito": 1, "demanda": 1})
    if document is None:
        raise HTTPException(status_code=404, detail=f"La zona {zona_id} no existe")
    entries = [
        entry
        for entry in document.get("demanda", [])
        if dia_semana is None or entry["dia_semana"] == dia_semana
    ]
    return DemandaZona(
        zona_id=zona_id,
        nombre=document["nombre"],
        distrito=document["distrito"],
        dia_semana=dia_semana,
        por_hora=_hourly_totals(entries),
        por_dia_y_hora=[DemandaDiaHora(**entry) for entry in entries],
    )
