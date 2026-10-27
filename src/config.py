import os
from dataclasses import dataclass
from pathlib import Path

LAKE_FORMATS = frozenset({"parquet", "delta"})


@dataclass(frozen=True)
class Settings:
    data_path: Path
    lake_format: str
    spark_master: str
    spark_driver_memory: str
    spark_shuffle_partitions: int
    spark_event_log_dir: str | None
    mongo_uri: str
    mongo_db: str
    postgres_host: str
    postgres_port: int
    postgres_db: str
    postgres_user: str
    postgres_password: str

    @property
    def bronze_path(self) -> Path:
        return self.data_path / "bronze"

    @property
    def silver_path(self) -> Path:
        return self.data_path / "silver"

    @property
    def gold_path(self) -> Path:
        return self.data_path / "gold"

    @property
    def reports_path(self) -> Path:
        return self.data_path / "reports"

    @property
    def models_path(self) -> Path:
        return self.data_path / "models"

    @property
    def postgres_jdbc_url(self) -> str:
        return (
            f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/"
            f"{self.postgres_db}?rewriteBatchedInserts=true"
        )

    @property
    def postgres_dsn(self) -> str:
        return (
            f"host={self.postgres_host} port={self.postgres_port} "
            f"dbname={self.postgres_db} user={self.postgres_user} "
            f"password={self.postgres_password}"
        )


def get_settings() -> Settings:
    lake_format = os.environ.get("LAKE_FORMAT", "parquet")
    if lake_format not in LAKE_FORMATS:
        raise ValueError(f"LAKE_FORMAT debe ser uno de {sorted(LAKE_FORMATS)}, no '{lake_format}'")
    return Settings(
        data_path=Path(os.environ.get("DATA_PATH", "data")),
        lake_format=lake_format,
        spark_master=os.environ.get("SPARK_MASTER", "local[*]"),
        spark_driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", "4g"),
        spark_shuffle_partitions=int(os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8")),
        spark_event_log_dir=os.environ.get("SPARK_EVENT_LOG_DIR") or None,
        mongo_uri=os.environ.get("MONGO_URI", "mongodb://localhost:27017"),
        mongo_db=os.environ.get("MONGO_DB", "nyc_taxi"),
        postgres_host=os.environ.get("POSTGRES_HOST", "localhost"),
        postgres_port=int(os.environ.get("POSTGRES_PORT", "5432")),
        postgres_db=os.environ.get("POSTGRES_DB", "taxi"),
        postgres_user=os.environ.get("POSTGRES_USER", "taxi"),
        postgres_password=os.environ.get("POSTGRES_PASSWORD", "taxi"),
    )
