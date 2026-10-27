from dataclasses import replace
from pathlib import Path

import pytest

from src.config import Settings
from src.domain import YearMonth, month_range
from src.ingest.bronze import IngestionError, download_once, ingest_range
from src.lake import bronze_trip_file


class FakeFetcher:
    def __init__(self, fail_on: str | None = None) -> None:
        self.calls: list[str] = []
        self.fail_on = fail_on

    def __call__(self, url: str, target: Path) -> None:
        self.calls.append(url)
        target.write_bytes(b"partial")
        if self.fail_on and self.fail_on in url:
            raise IngestionError(f"fallo simulado en {url}")
        target.write_bytes(b"contenido")


def test_ingestion_is_idempotent(settings: Settings, tmp_path: Path) -> None:
    local = replace(settings, data_path=tmp_path)
    fetch = FakeFetcher()
    ingest_range(local, YearMonth(2024, 1), YearMonth(2024, 2), fetch)
    assert len(fetch.calls) == 4
    ingest_range(local, YearMonth(2024, 1), YearMonth(2024, 3), fetch)
    assert len(fetch.calls) == 5
    assert fetch.calls[-1].endswith("yellow_tripdata_2024-03.parquet")


def test_partitioned_by_year_and_month(settings: Settings, tmp_path: Path) -> None:
    local = replace(settings, data_path=tmp_path)
    path = bronze_trip_file(local, YearMonth(2024, 3))
    assert path.relative_to(tmp_path).parts[:4] == ("bronze", "trips", "year=2024", "month=03")


def test_failed_download_leaves_no_file(tmp_path: Path) -> None:
    destination = tmp_path / "file.parquet"
    with pytest.raises(IngestionError):
        download_once("https://example.test/file.parquet", destination, FakeFetcher(fail_on="file"))
    assert not destination.exists()
    assert list(tmp_path.iterdir()) == []


def test_month_range_crosses_year() -> None:
    months = month_range(YearMonth(2023, 11), YearMonth(2024, 2))
    assert [str(month) for month in months] == ["2023-11", "2023-12", "2024-01", "2024-02"]


@pytest.mark.parametrize("value", ["2024-13", "2024-1", "24-01", "abc"])
def test_year_month_rejects_invalid_values(value: str) -> None:
    with pytest.raises(ValueError):
        YearMonth.parse(value)
