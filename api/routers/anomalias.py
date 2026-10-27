from datetime import date, datetime, time, timedelta
from math import ceil

from fastapi import APIRouter, HTTPException, Query

from api.dependencies import DatabaseDep
from api.schemas import Anomalia, PaginaAnomalias

router = APIRouter(tags=["anomalias"])

MAX_PAGE_SIZE = 200


def _start_of(day: date) -> datetime:
    return datetime.combine(day, time.min)


@router.get("/anomalias", response_model=PaginaAnomalias, summary="Viajes sospechosos paginados")
def listar_anomalias(
    db: DatabaseDep,
    desde: date = Query(..., description="Fecha inicial (incluida), AAAA-MM-DD"),
    hasta: date = Query(..., description="Fecha final (incluida), AAAA-MM-DD"),
    pagina: int = Query(1, ge=1),
    tamano: int = Query(50, ge=1, le=MAX_PAGE_SIZE),
) -> PaginaAnomalias:
    if hasta < desde:
        raise HTTPException(status_code=422, detail="'hasta' no puede ser anterior a 'desde'")
    query = {
        "fecha_recogida": {
            "$gte": _start_of(desde),
            "$lt": _start_of(hasta + timedelta(days=1)),
        }
    }
    total = db.anomalias.count_documents(query)
    cursor = (
        db.anomalias.find(query)
        .sort([("fecha_recogida", -1), ("_id", 1)])
        .skip((pagina - 1) * tamano)
        .limit(tamano)
    )
    results = [Anomalia(id=str(document.pop("_id")), **document) for document in cursor]
    return PaginaAnomalias(
        total=total,
        pagina=pagina,
        tamano=tamano,
        paginas=ceil(total / tamano) if total else 0,
        resultados=results,
    )
