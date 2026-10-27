import random

import pytest
from pyspark.sql import SparkSession

from src.ml.duration_model import build_pipeline, export_coefficients
from src.ml.serving import predict_duration

SLOTS = {"madrugada": -4.0, "punta_manana": 6.0, "mediodia": 2.0, "punta_tarde": 8.0, "noche": 0.0}
BOROUGHS = {"Manhattan": 3.0, "Queens": 5.0, "Brooklyn": 1.0}


def _synthetic(spark: SparkSession):
    generator = random.Random(7)
    rows = []
    for _ in range(600):
        distance = generator.uniform(0.5, 15)
        weekend = float(generator.random() < 0.3)
        slot = generator.choice(list(SLOTS))
        pickup = generator.choice(list(BOROUGHS))
        dropoff = generator.choice(list(BOROUGHS))
        duration = (
            2.5 + 3.0 * distance - 2.0 * weekend + SLOTS[slot] + BOROUGHS[pickup] + 0.5 * BOROUGHS[dropoff]
        )
        rows.append((distance, weekend, slot, pickup, dropoff, duration + generator.gauss(0, 0.1)))
    return spark.createDataFrame(
        rows,
        "trip_distance double, is_weekend double, time_slot string, pickup_borough string, "
        "dropoff_borough string, label double",
    )


def test_exported_coefficients_reproduce_spark_predictions(spark: SparkSession) -> None:
    data = _synthetic(spark)
    model = build_pipeline().fit(data)
    exported = export_coefficients(model)
    assert exported["numericos"]["trip_distance"] == pytest.approx(3.0, abs=0.05)
    for row in model.transform(data).limit(25).collect():
        features = {
            "trip_distance": row["trip_distance"],
            "is_weekend": row["is_weekend"],
            "time_slot": row["time_slot"],
            "pickup_borough": row["pickup_borough"],
            "dropoff_borough": row["dropoff_borough"],
        }
        assert predict_duration(exported, features) == pytest.approx(max(row["prediction"], 0.0), abs=1e-6)


def test_unknown_category_falls_back_to_baseline() -> None:
    model = {
        "intercepto": 10.0,
        "numericos": {"trip_distance": 2.0, "is_weekend": -1.0},
        "categoricos": {"time_slot": {"noche": 1.0}, "pickup_borough": {}, "dropoff_borough": {}},
    }
    features = {
        "trip_distance": 3.0,
        "is_weekend": 0.0,
        "time_slot": "noche",
        "pickup_borough": "Desconocido",
        "dropoff_borough": "Manhattan",
    }
    assert predict_duration(model, features) == 17.0
