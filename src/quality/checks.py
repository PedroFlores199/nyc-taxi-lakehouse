import logging
from collections.abc import Sequence
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class DataQualityError(Exception):
    def __init__(self, dataset: str, failures: Sequence["CheckResult"]) -> None:
        self.dataset = dataset
        self.failures = list(failures)
        detail = "; ".join(f"{failure.name}: {failure.failing_rows} filas" for failure in failures)
        super().__init__(f"Fallan las comprobaciones de calidad de '{dataset}': {detail}")


@dataclass(frozen=True)
class Expectation:
    name: str
    violation: Column


@dataclass(frozen=True)
class CheckResult:
    name: str
    failing_rows: int

    @property
    def passed(self) -> bool:
        return self.failing_rows == 0


def not_null(*columns: str) -> list[Expectation]:
    return [Expectation(f"{column} sin nulos", F.col(column).isNull()) for column in columns]


def between(column: str, low: float | None = None, high: float | None = None) -> Expectation:
    violation = F.col(column).isNull()
    if low is not None:
        violation = violation | (F.col(column) < low)
    if high is not None:
        violation = violation | (F.col(column) > high)
    return Expectation(f"{column} en [{low}, {high}]", violation)


def greater_than(column: str, low: float) -> Expectation:
    return Expectation(f"{column} > {low}", F.col(column).isNull() | (F.col(column) <= low))


def evaluate(df: DataFrame, expectations: Sequence[Expectation]) -> list[CheckResult]:
    aggregations = [
        F.sum(F.when(expectation.violation, 1).otherwise(0)).alias(f"c{index}")
        for index, expectation in enumerate(expectations)
    ]
    counts = df.agg(F.count(F.lit(1)).alias("rows"), *aggregations).first()
    results = [
        CheckResult(expectation.name, int(counts[f"c{index}"] or 0))
        for index, expectation in enumerate(expectations)
    ]
    results.append(CheckResult("tabla no vacía", 0 if counts["rows"] else 1))
    return results


def check_unique(df: DataFrame, columns: Sequence[str]) -> CheckResult:
    duplicated = df.groupBy(*columns).count().filter(F.col("count") > 1).count()
    return CheckResult(f"sin duplicados en {', '.join(columns)}", int(duplicated))


def enforce(dataset: str, results: Sequence[CheckResult]) -> None:
    for result in results:
        logger.info(
            "[%s] %s: %s",
            dataset,
            result.name,
            "OK" if result.passed else f"FALLO ({result.failing_rows} filas)",
        )
    failures = [result for result in results if not result.passed]
    if failures:
        raise DataQualityError(dataset, failures)


def validate_silver(df: DataFrame) -> None:
    expectations = [
        *not_null(
            "trip_id",
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_date",
            "pickup_location_id",
            "dropoff_location_id",
            "pickup_borough",
            "dropoff_borough",
            "time_slot",
        ),
        greater_than("duration_minutes", 0),
        between("duration_minutes", high=360),
        greater_than("trip_distance", 0),
        between("trip_distance", high=100),
        between("fare_amount", low=0),
        between("total_amount", low=0),
        between("passenger_count", low=1, high=6),
        between("pickup_hour", low=0, high=23),
        between("day_of_week", low=1, high=7),
    ]
    enforce("silver.trips", [*evaluate(df, expectations), check_unique(df, ["trip_id"])])


def validate_gold(
    dataset: str,
    df: DataFrame,
    keys: Sequence[str],
    positive: Sequence[str] = ("trips",),
    allow_empty: bool = False,
) -> None:
    expectations = [*not_null(*keys), *[greater_than(column, 0) for column in positive]]
    results = evaluate(df, expectations)
    if allow_empty:
        results = [result for result in results if result.name != "tabla no vacía"]
    enforce(dataset, [*results, check_unique(df, keys)])
