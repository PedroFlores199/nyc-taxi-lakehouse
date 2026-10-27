import logging
import shutil
import urllib.error
import urllib.request
from collections.abc import Callable
from pathlib import Path

from src.config import Settings
from src.domain import YearMonth, month_range
from src.lake import (
    ZONE_LOOKUP_FILE,
    ZONE_SHAPEFILE_FILE,
    bronze_trip_file,
    bronze_zone_file,
)

logger = logging.getLogger(__name__)

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
TRIP_DATA_URL = TLC_BASE_URL + "/trip-data/yellow_tripdata_{period}.parquet"
ZONE_URLS = {
    ZONE_LOOKUP_FILE: TLC_BASE_URL + "/misc/taxi_zone_lookup.csv",
    ZONE_SHAPEFILE_FILE: TLC_BASE_URL + "/misc/taxi_zones.zip",
}
DOWNLOAD_TIMEOUT_SECONDS = 120

Fetcher = Callable[[str, Path], None]


class IngestionError(RuntimeError):
    pass


def http_fetch(url: str, target: Path) -> None:
    request = urllib.request.Request(url, headers={"User-Agent": "nyc-taxi-lakehouse"})
    try:
        with (
            urllib.request.urlopen(request, timeout=DOWNLOAD_TIMEOUT_SECONDS) as response,
            target.open("wb") as output,
        ):
            shutil.copyfileobj(response, output)
    except urllib.error.HTTPError as exc:
        raise IngestionError(f"No se pudo descargar {url}: HTTP {exc.code}") from exc
    except (urllib.error.URLError, TimeoutError) as exc:
        raise IngestionError(f"No se pudo descargar {url}: {exc}") from exc


def download_once(url: str, destination: Path, fetch: Fetcher = http_fetch) -> bool:
    if destination.exists():
        logger.info("Ya existe, no se descarga de nuevo: %s", destination)
        return False
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".part")
    try:
        fetch(url, partial)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    partial.replace(destination)
    logger.info("Descargado %s en %s", url, destination)
    return True


def ingest_month(settings: Settings, period: YearMonth, fetch: Fetcher = http_fetch) -> Path:
    destination = bronze_trip_file(settings, period)
    download_once(TRIP_DATA_URL.format(period=period), destination, fetch)
    return destination


def ingest_zones(settings: Settings, fetch: Fetcher = http_fetch) -> list[Path]:
    paths = []
    for file_name, url in ZONE_URLS.items():
        destination = bronze_zone_file(settings, file_name)
        download_once(url, destination, fetch)
        paths.append(destination)
    return paths


def ingest_range(
    settings: Settings,
    start: YearMonth,
    end: YearMonth,
    fetch: Fetcher = http_fetch,
) -> list[Path]:
    ingest_zones(settings, fetch)
    return [ingest_month(settings, period, fetch) for period in month_range(start, end)]
