from fastapi import APIRouter, HTTPException, Query
from pymongo.database import Database

from api.dependencies import DatabaseDep
from api.schemas import EstimacionTrayecto, Lugar
from src.domain import time_slot_for_hour

router = APIRouter(tags=["trayectos"])


def find_route(db: Database, origen: int, destino: int) -> dict:
    document = db.trayectos.find_one({"origen": origen, "destino": destino})
    if document is None:
        raise HTTPException(
            status_code=404,
            detail=f"No hay viajes registrados entre las zonas {origen} y {destino}",
        )
    return document


@router.get("/trayecto", response_model=EstimacionTrayecto, summary="Tiempo e importe estimados")
def estimar_trayecto(
    db: DatabaseDep,
    origen: int = Query(..., ge=1),
    destino: int = Query(..., ge=1),
    hora: int = Query(..., ge=0, le=23),
) -> EstimacionTrayecto:
    document = find_route(db, origen, destino)
    franja = time_slot_for_hour(hora)
    slot = next((entry for entry in document.get("franjas", []) if entry["franja"] == franja), None)
    source = slot or document
    return EstimacionTrayecto(
        origen=Lugar(zona_id=origen, nombre=document.get("origen_nombre")),
        destino=Lugar(zona_id=destino, nombre=document.get("destino_nombre")),
        hora=hora,
        franja=franja,
        duracion_estimada_min=source["duracion_media_min"],
        importe_estimado=source["importe_medio"],
        distancia_media_millas=source.get("distancia_media_millas"),
        viajes_observados=source["viajes"],
        fuente="franja" if slot else "media_trayecto",
    )
