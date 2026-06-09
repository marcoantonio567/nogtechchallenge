from __future__ import annotations

import re
from datetime import datetime
from typing import Any

import pandas as pd


def only_digits(value: Any) -> str:
    """Remove qualquer caractere que nao seja numero."""
    if pd.isna(value):
        return ""
    return re.sub(r"\D", "", str(value))


def format_cpf(value: Any) -> str | None:
    """Padroniza CPF no formato 000.000.000-00."""
    digits = only_digits(value)
    if len(digits) == 10:
        digits = "0" + digits
    if len(digits) != 11:
        return None
    return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"


def mask_cpf(value: Any) -> str | None:
    """Anonimiza CPF antes de enviar os dados para a camada final."""
    digits = only_digits(value)
    if len(digits) != 11:
        return None
    return f"***.{digits[3:6]}.{digits[6:9]}-**"


def normalize_cep(value: Any) -> str | None:
    """Normaliza CEP para oito digitos."""
    digits = only_digits(value)
    if not digits:
        return None
    return digits.zfill(8)[-8:]


def parse_date(value: Any) -> pd.Timestamp:
    """Converte datas vindas em diferentes formatos para Timestamp."""
    if pd.isna(value):
        return pd.NaT

    value_as_text = str(value).strip()
    for date_format in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return pd.Timestamp(datetime.strptime(value_as_text, date_format))
        except ValueError:
            continue

    return pd.to_datetime(value_as_text, dayfirst=True, errors="coerce")


def parse_brl(value: Any) -> float | None:
    """Converte valores em reais para numero decimal."""
    if pd.isna(value):
        return None

    text = str(value).strip().replace("R$", "").replace(" ", "")
    if "," in text:
        text = text.replace(".", "").replace(",", ".")

    try:
        return round(float(text), 2)
    except ValueError:
        return None


def normalize_transaction_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza colunas e valores do arquivo de transacoes."""
    df = df.rename(
        columns={
            "valor_brl": "valor",
            "plano_adquirido": "curso",
        }
    ).copy()

    required_columns = {
        "id_transacao",
        "cpf_aluno",
        "data_transacao",
        "valor",
        "curso",
        "cep_cobranca",
    }
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Colunas obrigatorias ausentes em transacoes: {sorted(missing_columns)}")

    if "nome_aluno" not in df.columns:
        df["nome_aluno"] = None

    df["cpf_padronizado"] = df["cpf_aluno"].apply(format_cpf)
    df["cep_cobranca"] = df["cep_cobranca"].apply(normalize_cep)
    df["data_transacao"] = df["data_transacao"].apply(parse_date)
    df["valor"] = df["valor"].apply(parse_brl)
    df["mes_referencia"] = df["data_transacao"].dt.strftime("%Y-%m")

    invalid_rows = df[
        df["id_transacao"].isna()
        | df["cpf_padronizado"].isna()
        | df["data_transacao"].isna()
        | df["cep_cobranca"].isna()
    ]
    if not invalid_rows.empty:
        raise ValueError(f"Existem {len(invalid_rows)} transacoes invalidas no arquivo de entrada.")

    df["data_transacao"] = df["data_transacao"].dt.strftime("%Y-%m-%d")
    return df


def normalize_engagement_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza metricas mensais de engajamento dos alunos."""
    df = df.copy()
    required_columns = {"cpf_aluno", "mes_referencia"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Colunas obrigatorias ausentes em engajamento: {sorted(missing_columns)}")

    for optional_column in ("horas_assistidas", "percentual_conclusao"):
        if optional_column not in df.columns:
            df[optional_column] = None

    df["cpf_padronizado"] = df["cpf_aluno"].apply(format_cpf)
    df["mes_referencia"] = df["mes_referencia"].astype(str).str.slice(0, 7)
    df["horas_assistidas"] = pd.to_numeric(df["horas_assistidas"], errors="coerce")
    df["percentual_conclusao"] = pd.to_numeric(df["percentual_conclusao"], errors="coerce")
    df = df.dropna(subset=["cpf_padronizado", "mes_referencia"])
    return df.drop_duplicates(subset=["cpf_padronizado", "mes_referencia"], keep="last")
