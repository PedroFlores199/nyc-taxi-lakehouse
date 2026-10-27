CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_zona (
    zona_id SMALLINT PRIMARY KEY,
    nombre TEXT NOT NULL,
    distrito TEXT NOT NULL,
    zona_servicio TEXT
);

CREATE TABLE IF NOT EXISTS warehouse.dim_fecha (
    fecha_id INTEGER PRIMARY KEY,
    fecha DATE NOT NULL UNIQUE,
    anio SMALLINT NOT NULL,
    mes SMALLINT NOT NULL CHECK (mes BETWEEN 1 AND 12),
    dia SMALLINT NOT NULL CHECK (dia BETWEEN 1 AND 31),
    dia_semana SMALLINT NOT NULL CHECK (dia_semana BETWEEN 1 AND 7),
    nombre_dia TEXT NOT NULL,
    es_fin_de_semana BOOLEAN NOT NULL,
    es_festivo BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.dim_franja_horaria (
    hora SMALLINT PRIMARY KEY CHECK (hora BETWEEN 0 AND 23),
    franja TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS warehouse.fact_viajes (
    fecha_id INTEGER NOT NULL REFERENCES warehouse.dim_fecha (fecha_id),
    hora SMALLINT NOT NULL REFERENCES warehouse.dim_franja_horaria (hora),
    zona_origen_id SMALLINT NOT NULL REFERENCES warehouse.dim_zona (zona_id),
    zona_destino_id SMALLINT NOT NULL REFERENCES warehouse.dim_zona (zona_id),
    viajes INTEGER NOT NULL CHECK (viajes > 0),
    pasajeros INTEGER NOT NULL,
    importe_total NUMERIC(14, 2) NOT NULL,
    tarifa_total NUMERIC(14, 2) NOT NULL,
    propina_total NUMERIC(14, 2) NOT NULL,
    duracion_total_min NUMERIC(14, 2) NOT NULL,
    distancia_total_millas NUMERIC(14, 2) NOT NULL,
    PRIMARY KEY (fecha_id, hora, zona_origen_id, zona_destino_id)
);

CREATE INDEX IF NOT EXISTS fact_viajes_origen_hora_idx
    ON warehouse.fact_viajes (zona_origen_id, hora);

CREATE TABLE IF NOT EXISTS warehouse.stg_fact_viajes
    (LIKE warehouse.fact_viajes INCLUDING DEFAULTS);

CREATE OR REPLACE VIEW warehouse.v_demanda_zona_hora AS
SELECT
    f.fecha,
    f.anio,
    f.mes,
    f.dia_semana,
    f.nombre_dia,
    f.es_festivo,
    h.hora,
    h.franja,
    z.zona_id,
    z.nombre AS zona,
    z.distrito,
    SUM(v.viajes) AS viajes,
    ROUND(SUM(v.importe_total) / SUM(v.viajes), 2) AS importe_medio,
    ROUND(SUM(v.propina_total) / NULLIF(SUM(v.tarifa_total), 0) * 100, 2) AS propina_pct
FROM warehouse.fact_viajes v
JOIN warehouse.dim_fecha f USING (fecha_id)
JOIN warehouse.dim_franja_horaria h USING (hora)
JOIN warehouse.dim_zona z ON z.zona_id = v.zona_origen_id
GROUP BY f.fecha, f.anio, f.mes, f.dia_semana, f.nombre_dia, f.es_festivo,
         h.hora, h.franja, z.zona_id, z.nombre, z.distrito;

CREATE OR REPLACE VIEW warehouse.v_trayectos AS
SELECT
    f.anio,
    f.mes,
    h.franja,
    zo.zona_id AS origen_id,
    zo.nombre AS origen,
    zo.distrito AS distrito_origen,
    zd.zona_id AS destino_id,
    zd.nombre AS destino,
    zd.distrito AS distrito_destino,
    SUM(v.viajes) AS viajes,
    ROUND(SUM(v.duracion_total_min) / SUM(v.viajes), 2) AS duracion_media_min,
    ROUND(SUM(v.importe_total) / SUM(v.viajes), 2) AS importe_medio
FROM warehouse.fact_viajes v
JOIN warehouse.dim_fecha f USING (fecha_id)
JOIN warehouse.dim_franja_horaria h USING (hora)
JOIN warehouse.dim_zona zo ON zo.zona_id = v.zona_origen_id
JOIN warehouse.dim_zona zd ON zd.zona_id = v.zona_destino_id
GROUP BY f.anio, f.mes, h.franja, zo.zona_id, zo.nombre, zo.distrito,
         zd.zona_id, zd.nombre, zd.distrito;
