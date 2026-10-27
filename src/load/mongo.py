import logging
from datetime import UTC, datetime

from pymongo import MongoClient
from pymongo.collection import Collection
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.config import Settings
from src.domain import YearMonth
from src.lake import (
    GOLD_ANOMALIES,
    GOLD_DEMAND,
    GOLD_TRAVEL_TIME,
    GOLD_TRIPS_AGGREGATED,
    gold_table,
    read_table,
    read_zone_lookup,
    zone_geojson_file,
)
from src.load.mongo_schema import ensure_collections
from src.transform.zones import read_feature_collection, standardize_zones, zone_shapes_frame

logger = logging.getLogger(__name__)

ZONES = "zonas"
ROUTES = "trayectos"
ANOMALIES = "anomalias"
MODELS = "modelos"

TOP_DESTINATIONS = 5


def _ratio(numerator: str, denominator: str) -> Column:
    return F.round(F.col(numerator) / F.col(denominator), 2)


def zone_documents(
    zones: DataFrame,
    shapes: DataFrame,
    demand: DataFrame,
    trips: DataFrame,
    updated_at: datetime,
) -> DataFrame:
    hourly = (
        demand.groupBy("pickup_location_id", "day_of_week", "pickup_hour")
        .agg(
            F.sum("trips").alias("trips"),
            F.sum("total_amount_sum").alias("amount"),
            F.sum("tip_pct_sum").alias("tip_pct_sum"),
            F.sum("tip_pct_count").alias("tip_pct_count"),
        )
        .groupBy("pickup_location_id")
        .agg(
            F.sort_array(
                F.collect_list(
                    F.struct(
                        F.col("day_of_week").cast("int").alias("dia_semana"),
                        F.col("pickup_hour").cast("int").alias("hora"),
                        F.col("trips").cast("long").alias("viajes"),
                        _ratio("amount", "trips").alias("importe_medio"),
                        _ratio("tip_pct_sum", "tip_pct_count").alias("propina_media_pct"),
                    )
                )
            ).alias("demanda")
        )
    )
    monthly = (
        demand.groupBy("pickup_location_id", "year_month")
        .agg(
            F.sum("trips").alias("trips"),
            F.sum("total_amount_sum").alias("amount"),
            F.sum("tip_pct_sum").alias("tip_pct_sum"),
            F.sum("tip_pct_count").alias("tip_pct_count"),
        )
        .groupBy("pickup_location_id")
        .agg(
            F.sort_array(
                F.collect_list(
                    F.struct(
                        F.col("year_month").alias("mes"),
                        F.col("trips").cast("long").alias("viajes"),
                        _ratio("amount", "trips").alias("importe_medio"),
                        _ratio("tip_pct_sum", "tip_pct_count").alias("propina_media_pct"),
                    )
                )
            ).alias("resumen_mensual")
        )
    )
    destinations = zones.select(
        F.col("location_id").alias("dropoff_location_id"),
        F.col("zone").alias("dropoff_zone"),
        F.col("borough").alias("dropoff_borough"),
    )
    ranking = Window.partitionBy("pickup_location_id").orderBy(F.desc("trips"), "dropoff_location_id")
    top = (
        trips.groupBy("pickup_location_id", "dropoff_location_id")
        .agg(F.sum("trips").alias("trips"))
        .withColumn("rank", F.row_number().over(ranking))
        .filter(F.col("rank") <= TOP_DESTINATIONS)
        .join(F.broadcast(destinations), "dropoff_location_id", "left")
        .groupBy("pickup_location_id")
        .agg(
            F.sort_array(
                F.collect_list(
                    F.struct(
                        F.col("trips").cast("long").alias("viajes"),
                        F.col("dropoff_location_id").cast("int").alias("zona_id"),
                        F.col("dropoff_zone").alias("nombre"),
                        F.col("dropoff_borough").alias("distrito"),
                    )
                ),
                asc=False,
            ).alias("top_destinos")
        )
    )
    return (
        zones.join(shapes, "location_id", "left")
        .join(hourly.withColumnRenamed("pickup_location_id", "location_id"), "location_id", "left")
        .join(monthly.withColumnRenamed("pickup_location_id", "location_id"), "location_id", "left")
        .join(top.withColumnRenamed("pickup_location_id", "location_id"), "location_id", "left")
        .select(
            F.col("location_id").cast("int").alias("_id"),
            F.col("zone").alias("nombre"),
            F.col("borough").alias("distrito"),
            F.col("service_zone").alias("zona_servicio"),
            F.col("geometry").alias("geometria"),
            F.col("centroid").alias("centroide"),
            "demanda",
            "resumen_mensual",
            "top_destinos",
            F.lit(updated_at).alias("actualizado_en"),
        )
    )


def route_documents(travel_time: DataFrame, zones: DataFrame, updated_at: datetime) -> DataFrame:
    per_slot = travel_time.groupBy("pickup_location_id", "dropoff_location_id", "time_slot").agg(
        F.sum("trips").alias("trips"),
        F.sum("duration_minutes_sum").alias("duration"),
        F.sum("total_amount_sum").alias("amount"),
        F.sum("trip_distance_sum").alias("distance"),
    )
    routes = per_slot.groupBy("pickup_location_id", "dropoff_location_id").agg(
        F.sum("trips").alias("trips"),
        F.sum("duration").alias("duration"),
        F.sum("amount").alias("amount"),
        F.sum("distance").alias("distance"),
        F.sort_array(
            F.collect_list(
                F.struct(
                    F.col("time_slot").alias("franja"),
                    F.col("trips").cast("long").alias("viajes"),
                    _ratio("duration", "trips").alias("duracion_media_min"),
                    _ratio("amount", "trips").alias("importe_medio"),
                    _ratio("distance", "trips").alias("distancia_media_millas"),
                )
            )
        ).alias("franjas"),
    )
    origin = zones.select(
        F.col("location_id").alias("pickup_location_id"), F.col("zone").alias("origen_nombre")
    )
    destination = zones.select(
        F.col("location_id").alias("dropoff_location_id"), F.col("zone").alias("destino_nombre")
    )
    return (
        routes.join(F.broadcast(origin), "pickup_location_id", "left")
        .join(F.broadcast(destination), "dropoff_location_id", "left")
        .select(
            F.concat_ws("-", "pickup_location_id", "dropoff_location_id").alias("_id"),
            F.col("pickup_location_id").cast("int").alias("origen"),
            F.col("dropoff_location_id").cast("int").alias("destino"),
            "origen_nombre",
            "destino_nombre",
            F.col("trips").cast("long").alias("viajes"),
            _ratio("duration", "trips").alias("duracion_media_min"),
            _ratio("amount", "trips").alias("importe_medio"),
            _ratio("distance", "trips").alias("distancia_media_millas"),
            "franjas",
            F.lit(updated_at).alias("actualizado_en"),
        )
    )


def anomaly_documents(anomalies: DataFrame, loaded_at: datetime) -> DataFrame:
    return anomalies.select(
        F.col("trip_id").alias("_id"),
        F.col("year_month").alias("mes"),
        F.col("pickup_datetime").alias("fecha_recogida"),
        F.col("dropoff_datetime").alias("fecha_llegada"),
        F.col("pickup_location_id").cast("int").alias("zona_origen_id"),
        F.col("pickup_zone").alias("zona_origen"),
        F.col("pickup_borough").alias("distrito_origen"),
        F.col("dropoff_location_id").cast("int").alias("zona_destino_id"),
        F.col("dropoff_zone").alias("zona_destino"),
        F.col("dropoff_borough").alias("distrito_destino"),
        F.col("vendor_id").alias("proveedor_id"),
        F.col("payment_type").alias("tipo_pago"),
        F.col("passenger_count").alias("pasajeros"),
        F.col("trip_distance").alias("distancia_millas"),
        F.col("duration_minutes").alias("duracion_min"),
        F.col("avg_speed_mph").alias("velocidad_media_mph"),
        F.col("fare_amount").alias("tarifa"),
        F.col("tip_amount").alias("propina"),
        F.col("tip_pct").alias("propina_pct"),
        F.col("tolls_amount").alias("peajes"),
        F.col("total_amount").alias("importe_total"),
        F.col("fare_per_mile").alias("importe_por_milla"),
        F.col("route_mean_fare_per_mile").alias("media_trayecto"),
        F.col("route_std_fare_per_mile").alias("desviacion_trayecto"),
        F.col("route_trips").cast("long").alias("viajes_trayecto"),
        F.col("z_score").alias("puntuacion_z"),
        F.col("time_slot").alias("franja"),
        F.col("is_holiday").alias("es_festivo"),
        F.lit(loaded_at).alias("cargado_en"),
    )


def write_documents(df: DataFrame, settings: Settings, collection: str) -> None:
    (
        df.write.format("mongodb")
        .mode("append")
        .option("connection.uri", settings.mongo_uri)
        .option("database", settings.mongo_db)
        .option("collection", collection)
        .option("operationType", "replace")
        .option("idFieldList", "_id")
        .option("upsertDocument", "true")
        .option("ignoreNullValues", "true")
        .save()
    )


def remove_stale(collection: Collection, run_at: datetime) -> int:
    return collection.delete_many({"actualizado_en": {"$lt": run_at}}).deleted_count


def load_mongo(spark: SparkSession, settings: Settings, period: YearMonth) -> dict[str, int]:
    run_at = datetime.now(UTC).replace(microsecond=0)
    zones = standardize_zones(read_zone_lookup(spark, settings)).cache()
    shapes = zone_shapes_frame(spark, read_feature_collection(zone_geojson_file(settings)))
    demand = read_table(spark, gold_table(settings, GOLD_DEMAND), settings)
    trips = read_table(spark, gold_table(settings, GOLD_TRIPS_AGGREGATED), settings)
    travel_time = read_table(spark, gold_table(settings, GOLD_TRAVEL_TIME), settings)
    anomalies = read_table(spark, gold_table(settings, GOLD_ANOMALIES), settings).filter(
        F.col("year_month") == str(period)
    )

    client: MongoClient = MongoClient(settings.mongo_uri)
    try:
        db = client[settings.mongo_db]
        ensure_collections(db)

        write_documents(zone_documents(zones, shapes, demand, trips, run_at), settings, ZONES)
        remove_stale(db[ZONES], run_at)

        write_documents(route_documents(travel_time, zones, run_at), settings, ROUTES)
        remove_stale(db[ROUTES], run_at)

        removed = db[ANOMALIES].delete_many({"mes": str(period)}).deleted_count
        write_documents(anomaly_documents(anomalies, run_at), settings, ANOMALIES)

        counts = {name: db[name].count_documents({}) for name in (ZONES, ROUTES, ANOMALIES)}
        counts["anomalias_mes"] = db[ANOMALIES].count_documents({"mes": str(period)})
        counts["anomalias_sustituidas"] = removed
    finally:
        client.close()
    logger.info("MongoDB cargado para %s: %s", period, counts)
    return counts
