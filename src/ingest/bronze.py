import urllib.request
import urllib.error
from pathlib import Path


TRIP_DATA_URL_TEMPLATE = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_{year}-{month:02d}.parquet"
)

ZONE_LOOKUP_URL = (
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
)


def _download_file(url: str, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        urllib.request.urlretrieve(url, destination)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(
            f"Failed to download {url}: HTTP {exc.code}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"Failed to download {url}: {exc.reason}"
        ) from exc
    return destination


def ingest_month(
    year: int,
    month: int,
    base_path: str = "data/bronze",
) -> Path:
    destination = (
        Path(base_path) / f"year={year}" / f"month={month}" / "tripdata.parquet"
    )
    if destination.exists():
        return destination
    url = TRIP_DATA_URL_TEMPLATE.format(year=year, month=month)
    return _download_file(url, destination)


def ingest_zones(base_path: str = "data/bronze") -> Path:
    destination = Path(base_path) / "zones" / "taxi_zone_lookup.csv"
    if destination.exists():
        return destination
    return _download_file(ZONE_LOOKUP_URL, destination)


def ingest_range(
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
    base_path: str = "data/bronze",
) -> list[Path]:
    downloaded: list[Path] = []
    current_year = start_year
    current_month = start_month
    while (current_year, current_month) <= (end_year, end_month):
        path = ingest_month(current_year, current_month, base_path)
        downloaded.append(path)
        current_month += 1
        if current_month > 12:
            current_month = 1
            current_year += 1
    return downloaded
