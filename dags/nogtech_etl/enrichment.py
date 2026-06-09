from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

from nogtech_etl.config import (
    CEP_CACHE_PATH,
    FERIADOS_CACHE_PATH,
    FINAL_COLUMNS,
    RELATORIO_FINAL_PATH,
)
from nogtech_etl.http_client import build_http_session
from nogtech_etl.normalization import mask_cpf
from nogtech_etl.storage import ensure_runtime_dirs, read_json_cache, write_json_cache


def fetch_cep_data(cep: str, cache: dict[str, Any], session: requests.Session) -> dict[str, Any]:
    """Busca localidade do CEP na BrasilAPI usando cache local."""
    if cep in cache:
        return cache[cep]

    response = session.get(f"https://brasilapi.com.br/api/cep/v2/{cep}", timeout=10)
    if response.status_code == 404:
        cache[cep] = {"cidade": None, "uf": None, "bairro": None}
        return cache[cep]

    response.raise_for_status()
    payload = response.json()
    cache[cep] = {
        "cidade": payload.get("city") or payload.get("cidade"),
        "uf": payload.get("state") or payload.get("uf"),
        "bairro": payload.get("neighborhood") or payload.get("bairro"),
    }
    return cache[cep]


def fetch_holidays(year: int, cache: dict[str, Any], session: requests.Session) -> set[str]:
    """Busca feriados nacionais por ano, reaproveitando cache entre execucoes."""
    year_key = str(year)
    if year_key not in cache:
        response = session.get(f"https://brasilapi.com.br/api/feriados/v1/{year}", timeout=10)
        response.raise_for_status()
        cache[year_key] = sorted(item["date"] for item in response.json())

    return set(cache[year_key])


def transform_and_enrich(extraction_result: dict[str, Any]) -> dict[str, Any]:
    """Cruza transacoes com engajamento e enriquece a base final."""
    ensure_runtime_dirs()

    transacoes = pd.read_json(extraction_result["transacoes_path"])
    engajamento = pd.read_json(extraction_result["engajamento_path"])

    merged = transacoes.merge(
        engajamento[
            [
                "cpf_padronizado",
                "mes_referencia",
                "horas_assistidas",
                "percentual_conclusao",
            ]
        ],
        on=["cpf_padronizado", "mes_referencia"],
        how="left",
    )

    session = build_http_session()
    cep_cache = read_json_cache(CEP_CACHE_PATH)
    holiday_cache = read_json_cache(FERIADOS_CACHE_PATH)

    # CEP e feriado sao enriquecimentos externos, por isso ficam cacheados.
    unique_ceps = sorted(cep for cep in merged["cep_cobranca"].dropna().unique())
    cep_data = {cep: fetch_cep_data(cep, cep_cache, session) for cep in unique_ceps}
    write_json_cache(CEP_CACHE_PATH, cep_cache)

    merged["cidade"] = merged["cep_cobranca"].map(lambda cep: cep_data.get(cep, {}).get("cidade"))
    merged["uf"] = merged["cep_cobranca"].map(lambda cep: cep_data.get(cep, {}).get("uf"))
    merged["bairro"] = merged["cep_cobranca"].map(lambda cep: cep_data.get(cep, {}).get("bairro"))

    years = sorted(pd.to_datetime(merged["data_transacao"]).dt.year.unique())
    holidays_by_year = {
        int(year): fetch_holidays(int(year), holiday_cache, session) for year in years
    }
    write_json_cache(FERIADOS_CACHE_PATH, holiday_cache)

    def is_holiday(date_as_text: str) -> bool:
        transaction_date = pd.Timestamp(date_as_text)
        return date_as_text in holidays_by_year.get(transaction_date.year, set())

    merged["venda_em_feriado"] = merged["data_transacao"].apply(is_holiday)
    merged["cpf_aluno"] = merged["cpf_padronizado"].apply(mask_cpf)
    merged["data_processamento_utc"] = datetime.now(timezone.utc).isoformat()

    final_df = merged[FINAL_COLUMNS].sort_values("id_transacao")
    final_df.to_csv(RELATORIO_FINAL_PATH, index=False, encoding="utf-8")

    return {
        "relatorio_final_path": str(RELATORIO_FINAL_PATH),
        "registros_transformados": len(final_df),
        "ceps_em_cache": len(cep_cache),
        "anos_feriados_em_cache": len(holiday_cache),
    }
