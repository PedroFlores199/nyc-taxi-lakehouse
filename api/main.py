from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pymongo import MongoClient
from pymongo.database import Database

from api.dependencies import DatabaseDep
from api.routers import anomalias, informes, prediccion, trayectos, zonas
from src.config import get_settings

DESCRIPTION = (
    "Consulta en tiempo real de la capa de servicio del data lake de taxis de Nueva York: "
    "zonas, trayectos, demanda, anomalías y predicción de duración."
)


def create_app(database: Database | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if database is not None:
            app.state.db = database
            yield
            return
        settings = get_settings()
        client: MongoClient = MongoClient(settings.mongo_uri, tz_aware=True, serverSelectionTimeoutMS=5000)
        app.state.db = client[settings.mongo_db]
        try:
            yield
        finally:
            client.close()

    app = FastAPI(title="NYC Taxi API", version="1.0.0", description=DESCRIPTION, lifespan=lifespan)
    for router in (zonas.router, trayectos.router, anomalias.router, informes.router, prediccion.router):
        app.include_router(router)

    @app.get("/salud", tags=["sistema"])
    def salud(db: DatabaseDep) -> dict[str, str]:
        db.command("ping")
        return {"estado": "ok"}

    return app


app = create_app()
