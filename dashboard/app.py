from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.express as px
import streamlit as st

GOLD_PATH = Path(__file__).resolve().parent.parent / "data" / "gold"


@st.cache_data
def load_gold_table(table_name: str) -> Optional[pd.DataFrame]:
    table_path = GOLD_PATH / table_name
    if not table_path.exists():
        return None
    return pd.read_parquet(table_path)


def render_kpi_cards(df: pd.DataFrame) -> None:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total de Viajes", f"{len(df):,.0f}")
    col2.metric("Tarifa Promedio", f"${df['avg_fare'].mean():,.2f}")
    col3.metric("Propina Promedio", f"{df['avg_tip_pct'].mean():,.1f}%")
    col4.metric("Duración Promedio (min)", f"{df['avg_duration'].mean():,.1f}")


def render_demand_heatmap(df: pd.DataFrame) -> None:
    day_order = [
        "Monday", "Tuesday", "Wednesday",
        "Thursday", "Friday", "Saturday", "Sunday",
    ]
    pivot = df.pivot_table(
        index="day_of_week",
        columns="pickup_hour",
        values="trip_count",
        aggfunc="sum",
        fill_value=0,
    )
    pivot = pivot.reindex([d for d in day_order if d in pivot.index])
    fig = px.imshow(
        pivot,
        labels=dict(x="Hora", y="Día de la Semana", color="Viajes"),
        aspect="auto",
        color_continuous_scale="YlOrRd",
    )
    fig.update_layout(title="Demanda por Hora y Día de la Semana")
    st.plotly_chart(fig, use_container_width=True)


def render_top_routes(df: pd.DataFrame) -> None:
    top = df.nlargest(10, "trip_count")
    top["route"] = top["pickup_borough"] + " → " + top["dropoff_borough"]
    fig = px.bar(
        top,
        x="trip_count",
        y="route",
        orientation="h",
        labels={"trip_count": "Número de Viajes", "route": "Trayecto"},
        color="trip_count",
        color_continuous_scale="Blues",
    )
    fig.update_layout(
        title="Top 10 Trayectos por Volumen",
        yaxis=dict(autorange="reversed"),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)


def render_anomalies(df: pd.DataFrame) -> None:
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
    )


def apply_filters(
    df: pd.DataFrame,
    date_col: str,
    borough_col: Optional[str],
    date_range: tuple,
    selected_boroughs: list[str],
) -> pd.DataFrame:
    filtered = df.copy()
    if date_col in filtered.columns:
        filtered[date_col] = pd.to_datetime(filtered[date_col], errors="coerce")
        start, end = pd.Timestamp(date_range[0]), pd.Timestamp(date_range[1])
        filtered = filtered[
            (filtered[date_col] >= start) & (filtered[date_col] <= end)
        ]
    if borough_col and borough_col in filtered.columns and selected_boroughs:
        filtered = filtered[filtered[borough_col].isin(selected_boroughs)]
    return filtered


def get_boroughs(dataframes: dict[str, Optional[pd.DataFrame]]) -> list[str]:
    boroughs: set[str] = set()
    for df in dataframes.values():
        if df is None:
            continue
        for col in ["pickup_borough", "borough"]:
            if col in df.columns:
                boroughs.update(df[col].dropna().unique())
    return sorted(boroughs)


def main() -> None:
    st.set_page_config(
        page_title="NYC Taxi Data Lake",
        page_icon="🚕",
        layout="wide",
    )

    st.title("🚕 NYC Taxi Data Lake - Dashboard")

    demand_df = load_gold_table("demand_by_zone")
    routes_df = load_gold_table("top_routes")
    anomalies_df = load_gold_table("anomalies")
    metrics_df = load_gold_table("monthly_ranking")

    all_tables = {
        "demand": demand_df,
        "routes": routes_df,
        "anomalies": anomalies_df,
        "metrics": metrics_df,
    }

    if all(df is None for df in all_tables.values()):
        st.warning("⚠️ Run the pipeline first to generate data.")
        st.info(
            "Ejecuta el pipeline completo para generar los datos de las capas "
            "bronze → silver → gold antes de usar el dashboard."
        )
        return

    st.sidebar.header("🔍 Filtros")

    all_dates: list[pd.Timestamp] = []
    for df in all_tables.values():
        if df is None:
            continue
        for col in ["pickup_date", "date", "month"]:
            if col in df.columns:
                parsed = pd.to_datetime(df[col], errors="coerce").dropna()
                all_dates.extend(parsed.tolist())

    if all_dates:
        min_date = min(all_dates).date()
        max_date = max(all_dates).date()
    else:
        from datetime import date
        min_date = date(2024, 1, 1)
        max_date = date(2024, 12, 31)

    date_range = st.sidebar.date_input(
        "Rango de Fechas",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if not isinstance(date_range, tuple) or len(date_range) != 2:
        st.sidebar.warning("Selecciona un rango de fechas válido.")
        return

    boroughs = get_boroughs(all_tables)
    selected_boroughs = st.sidebar.multiselect(
        "Distrito (Borough)",
        options=boroughs,
        default=boroughs,
    )

    tab_demand, tab_routes, tab_anomalies, tab_metrics = st.tabs(
        ["📊 Demanda por Zona", "🛣️ Top Trayectos", "⚠️ Anomalías", "📈 Métricas"]
    )

    with tab_demand:
        if demand_df is not None:
            filtered = apply_filters(
                demand_df, "pickup_date", "borough", date_range, selected_boroughs
            )
            if filtered.empty:
                st.info("No hay datos para los filtros seleccionados.")
            else:
                render_demand_heatmap(filtered)
        else:
            st.info("Tabla de demanda por zona no disponible. Ejecuta el pipeline primero.")

    with tab_routes:
        if routes_df is not None:
            filtered = apply_filters(
                routes_df, "pickup_date", "pickup_borough", date_range, selected_boroughs
            )
            if filtered.empty:
                st.info("No hay datos para los filtros seleccionados.")
            else:
                render_top_routes(filtered)
        else:
            st.info("Tabla de trayectos no disponible. Ejecuta el pipeline primero.")

    with tab_anomalies:
        if anomalies_df is not None:
            filtered = apply_filters(
                anomalies_df, "pickup_date", "pickup_borough", date_range, selected_boroughs
            )
            if filtered.empty:
                st.info("No hay anomalías para los filtros seleccionados.")
            else:
                st.subheader(f"🔍 {len(filtered)} viajes anómalos detectados")
                render_anomalies(filtered)
        else:
            st.info("Tabla de anomalías no disponible. Ejecuta el pipeline primero.")

    with tab_metrics:
        if metrics_df is not None:
            filtered = apply_filters(
                metrics_df, "month", "borough", date_range, selected_boroughs
            )
            if filtered.empty:
                st.info("No hay métricas para los filtros seleccionados.")
            else:
                required_cols = {"avg_fare", "avg_tip_pct", "avg_duration"}
                if required_cols.issubset(set(filtered.columns)):
                    render_kpi_cards(filtered)
                    st.divider()
                st.dataframe(filtered, use_container_width=True, hide_index=True)
        else:
            st.info("Tabla de métricas no disponible. Ejecuta el pipeline primero.")


if __name__ == "__main__":
    main()
