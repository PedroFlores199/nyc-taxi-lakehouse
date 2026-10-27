import zipfile
from pathlib import Path

import pytest
import shapefile
from pyproj import CRS
from pyproj.enums import WktVersion
from pyspark.sql import SparkSession
from shapely.geometry import Point, shape

from src.transform.zones import (
    shapefile_to_feature_collection,
    to_multipolygon,
    zone_shapes_frame,
)

TIMES_SQUARE_FT = (988_500.0, 215_500.0)


def _square(x: float, y: float, side: float) -> list[list[float]]:
    return [[x, y], [x, y + side], [x + side, y + side], [x + side, y], [x, y]]


@pytest.fixture
def taxi_zones_zip(tmp_path: Path) -> Path:
    base = tmp_path / "taxi_zones"
    with shapefile.Writer(str(base), shapeType=shapefile.POLYGON) as writer:
        writer.field("OBJECTID", "N")
        writer.field("LocationID", "N")
        x, y = TIMES_SQUARE_FT
        writer.poly([_square(x, y, 1000)])
        writer.record(1, 161)
        writer.poly([_square(x + 5000, y, 800)])
        writer.record(2, 103)
        writer.poly([_square(x + 5800, y, 800)])
        writer.record(3, 103)
    base.with_suffix(".prj").write_text(CRS.from_epsg(2263).to_wkt(WktVersion.WKT1_ESRI))
    archive = tmp_path / "taxi_zones.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        for suffix in (".shp", ".shx", ".dbf", ".prj"):
            bundle.write(base.with_suffix(suffix), f"taxi_zones/taxi_zones{suffix}")
    return archive


def test_shapefile_is_reprojected_to_wgs84(taxi_zones_zip: Path) -> None:
    collection = shapefile_to_feature_collection(taxi_zones_zip)
    features = {feature["id"]: feature for feature in collection["features"]}
    assert sorted(features) == [103, 161]
    geometry = shape(features[161]["geometry"])
    assert geometry.geom_type == "MultiPolygon"
    lon, lat = geometry.centroid.x, geometry.centroid.y
    assert -74.1 < lon < -73.8
    assert 40.6 < lat < 40.9


def test_parts_of_the_same_zone_are_merged(taxi_zones_zip: Path) -> None:
    collection = shapefile_to_feature_collection(taxi_zones_zip)
    merged = next(feature for feature in collection["features"] if feature["id"] == 103)
    geometry = shape(merged["geometry"])
    assert geometry.is_valid
    assert Point(merged["properties"]["centroid"]).within(geometry)


def test_invalid_polygons_are_repaired() -> None:
    bow_tie = shape({"type": "Polygon", "coordinates": [[[0, 0], [1, 1], [1, 0], [0, 1], [0, 0]]]})
    repaired = to_multipolygon(bow_tie)
    assert repaired.is_valid
    assert repaired.geom_type == "MultiPolygon"


def test_zone_shapes_frame(spark: SparkSession, taxi_zones_zip: Path) -> None:
    frame = zone_shapes_frame(spark, shapefile_to_feature_collection(taxi_zones_zip))
    row = frame.filter("location_id = 161").first()
    assert row["geometry"]["type"] == "MultiPolygon"
    assert row["centroid"]["type"] == "Point"
    assert len(row["centroid"]["coordinates"]) == 2
