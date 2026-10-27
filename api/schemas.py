from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict


class ZonaResumen(BaseModel):
    zona_id: int
    nombre: str
    distrito: str
    zona_servicio: str | None = None
    centroide: tuple[float, float] | None = None


class ZonaCercana(ZonaResumen):
    distancia_m: float | None = None


class ZonasCerca(BaseModel):
    zona_actual: ZonaResumen | None
    cercanas: list[ZonaCercana]


class Lugar(BaseModel):
    zona_id: int
    nombre: str | None = None


class EstimacionTrayecto(BaseModel):
    origen: Lugar
    destino: Lugar
    hora: int
    franja: str
    duracion_estimada_min: float
    importe_estimado: float
    distancia_media_millas: float | None
    viajes_observados: int
    fuente: Literal["franja", "media_trayecto"]


class DemandaHora(BaseModel):
    hora: int
    viajes: int
    importe_medio: float | None = None
    propina_media_pct: float | None = None


class DemandaDiaHora(DemandaHora):
    dia_semana: int


class DemandaZona(BaseModel):
    zona_id: int
    nombre: str
    distrito: str
    dia_semana: int | None
    por_hora: list[DemandaHora]
    por_dia_y_hora: list[DemandaDiaHora]


class Anomalia(BaseModel):
    model_config = ConfigDict(extra="allow")

    id: str
    mes: str
    fecha_recogida: datetime
    fecha_llegada: datetime
    zona_origen_id: int
    zona_destino_id: int
    distancia_millas: float
    importe_total: float
    importe_por_milla: float
    media_trayecto: float
    puntuacion_z: float


class PaginaAnomalias(BaseModel):
    total: int
    pagina: int
    tamano: int
    paginas: int
    resultados: list[Anomalia]


class PropinaDistrito(BaseModel):
    distrito: str
    mes: str
    viajes: int
    propina_media_pct: float | None
    anomalias: int


class PrediccionDuracion(BaseModel):
    origen: Lugar
    destino: Lugar
    hora: int
    franja: str
    dia_semana: int
    distancia_millas: float
    duracion_estimada_min: float
    error_medio_absoluto_min: float | None
    mes_entrenamiento: str | None
