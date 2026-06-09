from __future__ import annotations

import pandas as pd
import pytest

from nogtech_etl.normalization import (
    mask_cpf,
    normalize_engagement_columns,
    normalize_transaction_columns,
    parse_brl,
)


def test_normalize_transaction_columns_success() -> None:
    df = pd.DataFrame(
        [
            {
                "id_transacao": "T001",
                "cpf_aluno": "12345678909",
                "nome_aluno": "Ana Silva",
                "data_transacao": "15/01/2024",
                "valor_brl": "R$ 1.234,56",
                "plano_adquirido": "Python Pro",
                "cep_cobranca": "01001-000",
            }
        ]
    )

    normalized = normalize_transaction_columns(df)

    assert normalized.loc[0, "cpf_padronizado"] == "123.456.789-09"
    assert normalized.loc[0, "data_transacao"] == "2024-01-15"
    assert normalized.loc[0, "mes_referencia"] == "2024-01"
    assert normalized.loc[0, "valor"] == 1234.56
    assert normalized.loc[0, "curso"] == "Python Pro"
    assert normalized.loc[0, "cep_cobranca"] == "01001000"


def test_normalize_transaction_columns_fails_when_required_column_is_missing() -> None:
    df = pd.DataFrame(
        [
            {
                "id_transacao": "T001",
                "cpf_aluno": "12345678909",
                "data_transacao": "2024-01-15",
                "valor_brl": "100,00",
                "cep_cobranca": "01001000",
            }
        ]
    )

    with pytest.raises(ValueError, match="Colunas obrigatorias ausentes"):
        normalize_transaction_columns(df)


def test_normalize_transaction_columns_fails_when_rows_are_invalid() -> None:
    df = pd.DataFrame(
        [
            {
                "id_transacao": "T001",
                "cpf_aluno": "CPF invalido",
                "data_transacao": "sem data",
                "valor": "100,00",
                "curso": "Python Pro",
                "cep_cobranca": "",
            }
        ]
    )

    with pytest.raises(ValueError, match="transacoes invalidas"):
        normalize_transaction_columns(df)


def test_normalize_engagement_columns_deduplicates_and_coerces_numbers() -> None:
    df = pd.DataFrame(
        [
            {
                "cpf_aluno": "12345678909",
                "mes_referencia": "2024-01-20",
                "horas_assistidas": "10.5",
                "percentual_conclusao": "80",
            },
            {
                "cpf_aluno": "123.456.789-09",
                "mes_referencia": "2024-01",
                "horas_assistidas": "12",
                "percentual_conclusao": "90",
            },
        ]
    )

    normalized = normalize_engagement_columns(df)

    assert len(normalized) == 1
    assert normalized.iloc[0]["cpf_padronizado"] == "123.456.789-09"
    assert normalized.iloc[0]["horas_assistidas"] == 12
    assert normalized.iloc[0]["percentual_conclusao"] == 90


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        ("R$ 2.500,10", 2500.10),
        ("99.90", 99.90),
        ("valor invalido", None),
    ],
)
def test_parse_brl(raw_value: str, expected: float | None) -> None:
    assert parse_brl(raw_value) == expected


def test_mask_cpf_anonymizes_valid_cpf_and_rejects_invalid_one() -> None:
    assert mask_cpf("123.456.789-09") == "***.456.789-**"
    assert mask_cpf("123") is None
