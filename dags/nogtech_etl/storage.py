from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from nogtech_etl.config import CACHE_DIR, OUTPUT_DIR


def ensure_runtime_dirs() -> None:
    """Garante que as pastas geradas pela DAG existam antes de gravar arquivos."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def read_json_cache(path: Path) -> dict[str, Any]:
    """Le um cache JSON local e tolera arquivo ausente ou corrompido."""
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as cache_file:
        try:
            return json.load(cache_file)
        except json.JSONDecodeError:
            logging.warning("Cache %s estava invalido. Recriando arquivo.", path)
            return {}


def write_json_cache(path: Path, data: dict[str, Any]) -> None:
    """Grava o cache de forma atomica para evitar arquivo parcial em caso de erro."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(".tmp")

    with temporary_path.open("w", encoding="utf-8") as cache_file:
        json.dump(data, cache_file, ensure_ascii=False, indent=2, sort_keys=True)

    temporary_path.replace(path)
