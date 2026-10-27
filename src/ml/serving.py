from collections.abc import Mapping

MODEL_ID = "duracion_viaje"
NUMERIC_FEATURES = ("trip_distance", "is_weekend")
CATEGORICAL_FEATURES = ("time_slot", "pickup_borough", "dropoff_borough")


def predict_duration(model: Mapping, features: Mapping[str, float | str]) -> float:
    prediction = float(model["intercepto"])
    for name, weight in model["numericos"].items():
        prediction += float(weight) * float(features[name])
    for name, weights in model["categoricos"].items():
        prediction += float(weights.get(str(features[name]), 0.0))
    return max(prediction, 0.0)
