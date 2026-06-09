from __future__ import annotations

from typing import Any

import pandas as pd

from nogtech_etl.config import (
    ENGAJAMENTO_PATH,
    STAGE_ENGAJAMENTO_PATH,
    STAGE_TRANSACOES_PATH,
    TRANSACOES_PATH,
)
from nogtech_etl.normalization import (
    normalize_engagement_columns,
    normalize_transaction_columns,
)
from nogtech_etl.storage import ensure_runtime_dirs


def extract_local_files() -> dict[str, Any]:
    """Le os arquivos locais, normaliza o conteudo e grava a area de stage."""
    ensure_runtime_dirs()

    # O CSV de transacoes vem separado por ponto e virgula e com encoding legado.
    transacoes = pd.read_csv(
        TRANSACOES_PATH,
        sep=";",
        encoding="latin-1",
        dtype=str,
    )
    engajamento = pd.read_json(ENGAJAMENTO_PATH, encoding="utf-8")

    transacoes = normalize_transaction_columns(transacoes)
    engajamento = normalize_engagement_columns(engajamento)

    transacoes.to_json(STAGE_TRANSACOES_PATH, orient="records", force_ascii=False, indent=2)
    engajamento.to_json(STAGE_ENGAJAMENTO_PATH, orient="records", force_ascii=False, indent=2)

    return {
        "transacoes_path": str(STAGE_TRANSACOES_PATH),
        "engajamento_path": str(STAGE_ENGAJAMENTO_PATH),
        "transacoes_extraidas": len(transacoes),
        "engajamentos_extraidos": len(engajamento),
    }
