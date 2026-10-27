from fastapi import APIRouter, HTTPException, Query

from api.dependencies import DatabaseDep
from api.schemas import PropinaDistrito

router = APIRouter(prefix="/informes", tags=["informes"])

MONTH_PATTERN = r"^\d{4}-(0[1-9]|1[0-2])$"
EXCLUDED_BOROUGHS = ["Unknown", "N/A"]


def tip_by_borough_pipeline(desde: str, hasta: str) -> list[dict]:
    return [
        {"$match": {"distrito": {"$nin": EXCLUDED_BOROUGHS}}},
        {"$unwind": "$resumen_mensual"},
        {"$match": {"resumen_mensual.mes": {"$gte": desde, "$lte": hasta}}},
        {
            "$lookup": {
                "from": "anomalias",
                "let": {"zona": "$_id", "mes": "$resumen_mensual.mes"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$zona_origen_id", "$$zona"]},
                                    {"$eq": ["$mes", "$$mes"]},
                                ]
                            }
                        }
                    },
                    {"$count": "total"},
                ],
                "as": "anomalias",
            }
        },
        {"$unwind": {"path": "$anomalias", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": {"distrito": "$distrito", "mes": "$resumen_mensual.mes"},
                "viajes": {"$sum": "$resumen_mensual.viajes"},
                "propina_ponderada": {
                    "$sum": {
                        "$multiply": [
                            "$resumen_mensual.viajes",
                            {"$ifNull": ["$resumen_mensual.propina_media_pct", 0]},
                        ]
                    }
                },
                "viajes_con_propina": {
                    "$sum": {
                        "$cond": [
                            {"$ne": [{"$ifNull": ["$resumen_mensual.propina_media_pct", None]}, None]},
                            "$resumen_mensual.viajes",
                            0,
                        ]
                    }
                },
                "anomalias": {"$sum": {"$ifNull": ["$anomalias.total", 0]}},
            }
        },
        {
            "$project": {
                "_id": 0,
                "distrito": "$_id.distrito",
                "mes": "$_id.mes",
                "viajes": 1,
                "anomalias": 1,
                "propina_media_pct": {
                    "$cond": [
                        {"$gt": ["$viajes_con_propina", 0]},
                        {"$round": [{"$divide": ["$propina_ponderada", "$viajes_con_propina"]}, 2]},
                        None,
                    ]
                },
            }
        },
        {"$sort": {"mes": 1, "distrito": 1}},
    ]


@router.get(
    "/propina-distrito",
    response_model=list[PropinaDistrito],
    summary="Propina media y viajes anómalos por distrito y mes",
)
def propina_por_distrito(
    db: DatabaseDep,
    desde: str = Query(..., pattern=MONTH_PATTERN, description="Mes inicial AAAA-MM"),
    hasta: str = Query(..., pattern=MONTH_PATTERN, description="Mes final AAAA-MM"),
) -> list[PropinaDistrito]:
    if hasta < desde:
        raise HTTPException(status_code=422, detail="'hasta' no puede ser anterior a 'desde'")
    return [PropinaDistrito(**row) for row in db.zonas.aggregate(tip_by_borough_pipeline(desde, hasta))]
