import logging
from datetime import UTC, datetime

from pymongo import MongoClient
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import Settings
from src.domain import YearMonth
from src.lake import SILVER_TRIPS, read_table, silver_table
from src.load.mongo import MODELS
from src.ml.serving import CATEGORICAL_FEATURES, MODEL_ID, NUMERIC_FEATURES

logger = logging.getLogger(__name__)

TRAINING_FRACTION = 0.2
TEST_FRACTION = 0.2
SEED = 42


def training_frame(silver: DataFrame) -> DataFrame:
    return silver.select(
        F.col("trip_distance").cast("double").alias("trip_distance"),
        (F.col("day_of_week") >= 6).cast("double").alias("is_weekend"),
        "time_slot",
        "pickup_borough",
        "dropoff_borough",
        F.col("duration_minutes").cast("double").alias("label"),
    ).dropna()


def build_pipeline() -> Pipeline:
    indexers = [
        StringIndexer(inputCol=column, outputCol=f"{column}_index") for column in CATEGORICAL_FEATURES
    ]
    encoder = OneHotEncoder(
        inputCols=[f"{column}_index" for column in CATEGORICAL_FEATURES],
        outputCols=[f"{column}_vector" for column in CATEGORICAL_FEATURES],
        dropLast=True,
    )
    assembler = VectorAssembler(
        inputCols=[*NUMERIC_FEATURES, *[f"{column}_vector" for column in CATEGORICAL_FEATURES]],
        outputCol="features",
    )
    regression = LinearRegression(featuresCol="features", labelCol="label", maxIter=50)
    return Pipeline(stages=[*indexers, encoder, assembler, regression])


def export_coefficients(model: PipelineModel) -> dict:
    indexers = model.stages[: len(CATEGORICAL_FEATURES)]
    regression = model.stages[-1]
    coefficients = regression.coefficients.toArray()
    position = 0
    numeric = {}
    for name in NUMERIC_FEATURES:
        numeric[name] = float(coefficients[position])
        position += 1
    categorical = {}
    for name, indexer in zip(CATEGORICAL_FEATURES, indexers, strict=True):
        labels = indexer.labels
        weights = {label: float(coefficients[position + index]) for index, label in enumerate(labels[:-1])}
        weights[labels[-1]] = 0.0
        categorical[name] = weights
        position += len(labels) - 1
    return {
        "intercepto": float(regression.intercept),
        "numericos": numeric,
        "categoricos": categorical,
    }


def evaluate(model: PipelineModel, test: DataFrame) -> dict[str, float]:
    predictions = model.transform(test)
    metrics = {}
    for metric in ("mae", "rmse", "r2"):
        evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName=metric)
        metrics[metric] = round(float(evaluator.evaluate(predictions)), 4)
    return metrics


def train(silver: DataFrame) -> tuple[PipelineModel, dict]:
    sample = training_frame(silver).sample(fraction=TRAINING_FRACTION, seed=SEED)
    train_set, test_set = sample.randomSplit([1 - TEST_FRACTION, TEST_FRACTION], seed=SEED)
    train_set = train_set.cache()
    model = build_pipeline().fit(train_set)
    metrics = evaluate(model, test_set)
    metrics["filas_entrenamiento"] = train_set.count()
    train_set.unpersist()
    return model, metrics


def train_and_publish(spark: SparkSession, settings: Settings, period: YearMonth) -> dict:
    silver = read_table(spark, silver_table(settings, SILVER_TRIPS), settings).filter(
        (F.col("pickup_date") >= F.lit(period.first_day))
        & (F.col("pickup_date") < F.lit(period.next.first_day))
    )
    model, metrics = train(silver)
    model.write().overwrite().save(str(settings.models_path / MODEL_ID))
    document = {
        "_id": MODEL_ID,
        "tipo": "regresion_lineal_spark_mllib",
        "mes_entrenamiento": str(period),
        "metricas": metrics,
        "entrenado_en": datetime.now(UTC),
        **export_coefficients(model),
    }
    client: MongoClient = MongoClient(settings.mongo_uri)
    try:
        client[settings.mongo_db][MODELS].replace_one({"_id": MODEL_ID}, document, upsert=True)
    finally:
        client.close()
    logger.info("Modelo de duración entrenado con %s: %s", period, metrics)
    return metrics
