from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo import MongoClient
from pymongo.database import Database
from sqlalchemy import Engine, create_engine

from dashboard import data
from src.config import get_settings
from src.domain import DAY_NAMES

NYC_CENTER = {"lat": 40.7128, "lon": -73.95}
CACHE_SECONDS = 600


@st.cache_resource
def engine() -> Engine:
    settings = get_settings()
    return create_engine(
        f"postgresql+psycopg2://{settings.postgres_user}:{settings.postgres_password}"
        f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}",
        pool_pre_ping=True,
    )


@st.cache_resource
def mongo() -> MongoClient:
    return MongoClient(get_settings().mongo_uri, serverSelectionTimeoutMS=5000)


def database() -> Database:
    return mongo()[get_settings().mongo_db]


@st.cache_data(ttl=CACHE_SECONDS)
def cached_range() -> tuple[date, date] | None:
    return data.available_range(engine())


@st.cache_data(ttl=CACHE_SECONDS)
def cached_boroughs() -> list[str]:
    return data.boroughs(engine())


@st.cache_data(ttl=CACHE_SECONDS)
def cached_summary(desde: date, hasta: date, distritos: tuple[str, ...]) -> pd.DataFrame:
    return data.summary(engine(), desde, hasta, distritos)


@st.cache_data(ttl=CACHE_SECONDS)
def cached_heatmap(desde: date, hasta: date, distritos: tuple[str, ...]) -> pd.DataFrame:
    return data.demand_heatmap(engine(), desde, hasta, distritos)


@st.cache_data(ttl=CACHE_SECONDS)
def cached_routes(desde: date, hasta: date, distritos: tuple[str, ...]) -> pd.DataFrame:
    return data.top_routes(engine(), desde, hasta, distritos)


@st.cache_data(ttl=CACHE_SECONDS)
def cached_geojson() -> dict:
    return data.zone_geojson(database())


@st.cache_data(ttl=CACHE_SECONDS)
def cached_anomalies(
    desde: date, hasta: date, distritos: tuple[str, ...]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    db = database()
    return data.anomalies_by_zone(db, desde, hasta, distritos), data.worst_anomalies(
        db, desde, hasta, distritos
    )


def _decimal(value: float, digits: int) -> str:
    return f"{float(value):.{digits}f}".replace(".", ",")


def render_summary(frame: pd.DataFrame) -> None:
    row = frame.iloc[0]
    columns = st.columns(4)
    columns[0].metric("Viajes", _decimal((row["viajes"] or 0) / 1_000_000, 2) + " M")
    columns[1].metric("Importe medio", _decimal(row["importe_medio"] or 0, 2) + " $")
    columns[2].metric("Propina media", _decimal(row["propina_pct"] or 0, 1) + " %")
    columns[3].metric("Duración media", _decimal(row["duracion_media_min"] or 0, 1) + " min")


def render_heatmap(frame: pd.DataFrame) -> None:
    grid = (
        frame.pivot_table(index="dia_semana", columns="hora", values="viajes_medios", fill_value=0)
        .reindex(index=range(1, 8), columns=range(24), fill_value=0)
        .rename(index=lambda day: DAY_NAMES[day - 1])
    )
    figure = px.imshow(
        grid,
        labels={"x": "Hora", "y": "Día", "color": "Viajes por día"},
        aspect="auto",
        color_continuous_scale="YlOrRd",
    )
    figure.update_xaxes(dtick=1)
    st.plotly_chart(figure, use_container_width=True)


def render_routes(frame: pd.DataFrame) -> None:
    frame = frame.assign(trayecto=frame["origen"] + " → " + frame["destino"])
    figure = px.bar(
        frame,
        x="viajes",
        y="trayecto",
        orientation="h",
        color="distrito",
        hover_data=["duracion_media_min", "importe_medio"],
        labels={"viajes": "Viajes", "trayecto": "", "distrito": "Distrito"},
    )
    figure.update_layout(yaxis={"autorange": "reversed"}, height=480)
    st.plotly_chart(figure, use_container_width=True)


def render_anomalies(by_zone: pd.DataFrame, worst: pd.DataFrame, geojson: dict) -> None:
    figure = px.choropleth_mapbox(
        by_zone,
        geojson=geojson,
        locations="zona_id",
        featureidkey="id",
        color="anomalias",
        hover_name="zona",
        hover_data={"zona_id": False, "importe_por_milla_medio": ":.2f"},
        color_continuous_scale="OrRd",
        mapbox_style="open-street-map",
        center=NYC_CENTER,
        zoom=9.4,
        opacity=0.75,
        labels={"anomalias": "Viajes sospechosos", "importe_por_milla_medio": "$/milla medio"},
    )
    figure.update_layout(height=560, margin={"l": 0, "r": 0, "t": 0, "b": 0})
    st.plotly_chart(figure, use_container_width=True)
    st.subheader("Viajes más sospechosos")
    st.dataframe(worst, use_container_width=True, hide_index=True)


def main() -> None:
    st.set_page_config(page_title="Taxis de Nueva York", layout="wide")
    st.title("Taxis de Nueva York")

    period = cached_range()
    if period is None:
        st.info("El almacén está vacío. Ejecuta el DAG nyc_taxi_lakehouse en Airflow para cargar un mes.")
        return

    with st.sidebar:
        st.header("Filtros")
        selected = st.date_input("Fechas", value=period, min_value=period[0], max_value=period[1])
        all_boroughs = cached_boroughs()
        chosen = st.multiselect("Distritos de origen", all_boroughs, default=all_boroughs)

    if not isinstance(selected, tuple) or len(selected) != 2:
        st.warning("Selecciona una fecha inicial y una final.")
        return
    if not chosen:
        st.warning("Selecciona al menos un distrito.")
        return
    desde, hasta = selected
    distritos = tuple(sorted(chosen))

    render_summary(cached_summary(desde, hasta, distritos))
    demand_tab, routes_tab, anomalies_tab = st.tabs(["Demanda", "Trayectos", "Anomalías"])

    with demand_tab:
        st.subheader("Viajes medios por día de la semana y hora")
        heatmap = cached_heatmap(desde, hasta, distritos)
        if heatmap.empty:
            st.info("No hay viajes para los filtros seleccionados.")
        else:
            render_heatmap(heatmap)

    with routes_tab:
        st.subheader("Los 10 trayectos con más viajes")
        routes = cached_routes(desde, hasta, distritos)
        if routes.empty:
            st.info("No hay trayectos para los filtros seleccionados.")
        else:
            render_routes(routes)

    with anomalies_tab:
        by_zone, worst = cached_anomalies(desde, hasta, distritos)
        if by_zone.empty:
            st.info("No hay viajes sospechosos para los filtros seleccionados.")
        else:
            render_anomalies(by_zone, worst, cached_geojson())


if __name__ == "__main__":
    main()
