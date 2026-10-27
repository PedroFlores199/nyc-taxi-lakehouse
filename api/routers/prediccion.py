from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query

from api.dependencies import DatabaseDep
from api.routers.trayectos import find_route
from api.schemas import Lugar, PrediccionDuracion
from src.domain import time_slot_for_hour
from src.ml.serving import MODEL_ID, predict_duration

router = APIRouter(prefix="/prediccion", tags=["prediccion"])

NEW_YORK = ZoneInfo("America/New_York")


@router.get("/duracion", response_model=PrediccionDuracion, summary="Duración estimada con Spark MLlib")
def predecir_duracion(
    db: DatabaseDep,
    origen: int = Query(..., ge=1),
    destino: int = Query(..., ge=1),
    hora: int = Query(..., ge=0, le=23),
    dia_semana: int | None = Query(None, ge=1, le=7, description="1 = lunes; por defecto, hoy"),
    distancia_millas: float | None = Query(None, gt=0, le=100),
) -> PrediccionDuracion:
    model = db.modelos.find_one({"_id": MODEL_ID})
    if model is None:
        raise HTTPException(status_code=503, detail="El modelo de duración todavía no se ha entrenado")
    zones = {
        document["_id"]: document
        for document in db.zonas.find({"_id": {"$in": [origen, destino]}}, {"nombre": 1, "distrito": 1})
    }
    missing = [zone for zone in (origen, destino) if zone not in zones]
    if missing:
        raise HTTPException(status_code=404, detail=f"Zonas inexistentes: {missing}")
    if distancia_millas is None:
        distancia_millas = find_route(db, origen, destino).get("distancia_media_millas")
        if not distancia_millas:
            raise HTTPException(status_code=422, detail="Indica distancia_millas para este trayecto")
    day = dia_semana or datetime.now(NEW_YORK).isoweekday()
    franja = time_slot_for_hour(hora)
    duration = predict_duration(
        model,
        {
            "trip_distance": distancia_millas,
            "is_weekend": 1.0 if day >= 6 else 0.0,
            "time_slot": franja,
            "pickup_borough": zones[origen]["distrito"],
            "dropoff_borough": zones[destino]["distrito"],
        },
    )
    return PrediccionDuracion(
        origen=Lugar(zona_id=origen, nombre=zones[origen]["nombre"]),
        destino=Lugar(zona_id=destino, nombre=zones[destino]["nombre"]),
        hora=hora,
        franja=franja,
        dia_semana=day,
        distancia_millas=distancia_millas,
        duracion_estimada_min=round(duration, 1),
        error_medio_absoluto_min=model.get("metricas", {}).get("mae"),
        mes_entrenamiento=model.get("mes_entrenamiento"),
    )
