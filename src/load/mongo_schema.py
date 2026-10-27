import json
from pathlib import Path

from pymongo import IndexModel
from pymongo.database import Database

MONGO_DIR = Path(__file__).resolve().parents[2] / "mongo"
INDEXES_FILE = MONGO_DIR / "indexes.json"
VALIDATORS_DIR = MONGO_DIR / "validators"


def index_definitions() -> dict[str, list[dict]]:
    return json.loads(INDEXES_FILE.read_text(encoding="utf-8"))


def validator_for(collection: str) -> dict | None:
    path = VALIDATORS_DIR / f"{collection}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def index_models(definitions: list[dict]) -> list[IndexModel]:
    models = []
    for definition in definitions:
        options = {key: value for key, value in definition.items() if key != "keys"}
        models.append(IndexModel([tuple(key) for key in definition["keys"]], **options))
    return models


def ensure_collections(db: Database) -> None:
    existing = set(db.list_collection_names())
    for collection, definitions in index_definitions().items():
        validator = validator_for(collection)
        options = (
            {"validator": validator, "validationLevel": "strict", "validationAction": "error"}
            if validator
            else {}
        )
        if collection not in existing:
            db.create_collection(collection, **options)
        elif options:
            db.command("collMod", collection, **options)
        models = index_models(definitions)
        if models:
            db[collection].create_indexes(models)
