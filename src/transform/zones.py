import json
import zipfile
from collections import defaultdict
from pathlib import Path

import shapefile
from pyproj import CRS, Transformer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from shapely import make_valid, set_precision
from shapely.geometry import MultiPolygon, Polygon, mapping, shape
from shapely.geometry.base import BaseGeometry
from shapely.geometry.polygon import orient
from shapely.ops import transform, unary_union

TARGET_CRS = "EPSG:4326"
COORDINATE_GRID = 1e-6

POINT_TYPE = T.StructType(
    [
        T.StructField("type", T.StringType(), False),
        T.StructField("coordinates", T.ArrayType(T.DoubleType(), False), False),
    ]
)

MULTIPOLYGON_TYPE = T.StructType(
    [
        T.StructField("type", T.StringType(), False),
        T.StructField(
            "coordinates",
            T.ArrayType(T.ArrayType(T.ArrayType(T.ArrayType(T.DoubleType(), False), False), False), False),
            False,
        ),
    ]
)

ZONE_SHAPE_SCHEMA = T.StructType(
    [
        T.StructField("location_id", T.IntegerType(), False),
        T.StructField("geometry", MULTIPOLYGON_TYPE, False),
        T.StructField("centroid", POINT_TYPE, False),
    ]
)


def standardize_zones(df: DataFrame) -> DataFrame:
    return df.select(
        F.col("LocationID").cast("int").alias("location_id"),
        F.col("Borough").cast("string").alias("borough"),
        F.col("Zone").cast("string").alias("zone"),
        F.col("service_zone").cast("string").alias("service_zone"),
    )


def _polygons(geometry: BaseGeometry) -> list[Polygon]:
    if isinstance(geometry, Polygon):
        return [] if geometry.is_empty else [geometry]
    if hasattr(geometry, "geoms"):
        return [polygon for part in geometry.geoms for polygon in _polygons(part)]
    return []


def to_multipolygon(geometry: BaseGeometry) -> MultiPolygon:
    polygons = _polygons(make_valid(geometry))
    if not polygons:
        raise ValueError("La geometría no contiene ningún polígono válido")
    return MultiPolygon([orient(polygon) for polygon in polygons])


def _source_crs(archive: Path) -> CRS:
    with zipfile.ZipFile(archive) as bundle:
        projection = next(name for name in bundle.namelist() if name.lower().endswith(".prj"))
        return CRS.from_wkt(bundle.read(projection).decode("utf-8"))


def shapefile_to_feature_collection(archive: Path) -> dict:
    to_wgs84 = Transformer.from_crs(_source_crs(archive), TARGET_CRS, always_xy=True)
    parts: dict[int, list[BaseGeometry]] = defaultdict(list)
    with shapefile.Reader(str(archive)) as reader:
        for shape_record in reader.iterShapeRecords():
            location_id = int(shape_record.record.as_dict()["LocationID"])
            parts[location_id].append(shape(shape_record.shape.__geo_interface__))

    features = []
    for location_id in sorted(parts):
        projected = transform(to_wgs84.transform, unary_union(parts[location_id]))
        geometry = to_multipolygon(set_precision(to_multipolygon(projected), COORDINATE_GRID))
        centroid = geometry.representative_point()
        features.append(
            {
                "type": "Feature",
                "id": location_id,
                "properties": {
                    "location_id": location_id,
                    "centroid": [round(centroid.x, 6), round(centroid.y, 6)],
                },
                "geometry": mapping(geometry),
            }
        )
    return {"type": "FeatureCollection", "features": features}


def write_feature_collection(collection: dict, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".part")
    partial.write_text(json.dumps(collection), encoding="utf-8")
    partial.replace(destination)
    return destination


def read_feature_collection(source: Path) -> dict:
    return json.loads(source.read_text(encoding="utf-8"))


def zone_shapes_frame(spark: SparkSession, collection: dict) -> DataFrame:
    rows = [
        (
            int(feature["properties"]["location_id"]),
            {"type": "MultiPolygon", "coordinates": feature["geometry"]["coordinates"]},
            {"type": "Point", "coordinates": feature["properties"]["centroid"]},
        )
        for feature in collection["features"]
    ]
    return spark.createDataFrame(rows, ZONE_SHAPE_SCHEMA)
