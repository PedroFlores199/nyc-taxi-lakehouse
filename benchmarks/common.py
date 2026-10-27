import statistics
import time
from collections.abc import Callable
from pathlib import Path

RESULTS_DIR = Path(__file__).resolve().parents[1] / "docs" / "rendimiento"


def measure(action: Callable[[], object], repetitions: int, warmup: int = 1) -> list[float]:
    for _ in range(warmup):
        action()
    timings = []
    for _ in range(repetitions):
        start = time.perf_counter()
        action()
        timings.append(time.perf_counter() - start)
    return timings


def median_ms(timings: list[float]) -> float:
    return round(statistics.median(timings) * 1000, 2)


def p95_ms(timings: list[float]) -> float:
    ordered = sorted(timings)
    return round(ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))] * 1000, 2)


def write_result(name: str, content: str) -> Path:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    destination = RESULTS_DIR / name
    destination.write_text(content, encoding="utf-8")
    return destination
