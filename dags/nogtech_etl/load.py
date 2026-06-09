from __future__ import annotations

from typing import Any

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from nogtech_etl.config import DESTINATION_TABLE, FINAL_COLUMNS, POSTGRES_CONN_ID


CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {DESTINATION_TABLE} (
    id_transacao VARCHAR(50) PRIMARY KEY,
    cpf_aluno VARCHAR(20),
    data_transacao DATE,
    mes_referencia VARCHAR(7),
    valor NUMERIC(12, 2),
    curso TEXT,
    cep_cobranca VARCHAR(8),
    cidade TEXT,
    uf CHAR(2),
    bairro TEXT,
    venda_em_feriado BOOLEAN,
    horas_assistidas NUMERIC(10, 2),
    percentual_conclusao NUMERIC(5, 2),
    data_processamento_utc TIMESTAMPTZ
);
"""


UPSERT_SQL = f"""
INSERT INTO {DESTINATION_TABLE} ({", ".join(FINAL_COLUMNS)})
VALUES %s
ON CONFLICT (id_transacao) DO UPDATE SET
    cpf_aluno = EXCLUDED.cpf_aluno,
    data_transacao = EXCLUDED.data_transacao,
    mes_referencia = EXCLUDED.mes_referencia,
    valor = EXCLUDED.valor,
    curso = EXCLUDED.curso,
    cep_cobranca = EXCLUDED.cep_cobranca,
    cidade = EXCLUDED.cidade,
    uf = EXCLUDED.uf,
    bairro = EXCLUDED.bairro,
    venda_em_feriado = EXCLUDED.venda_em_feriado,
    horas_assistidas = EXCLUDED.horas_assistidas,
    percentual_conclusao = EXCLUDED.percentual_conclusao,
    data_processamento_utc = EXCLUDED.data_processamento_utc;
"""


def none_if_nan(value: Any) -> Any:
    """Converte NaN do pandas em None para gravacao correta no Postgres."""
    if pd.isna(value):
        return None
    return value


def load_to_postgres(transform_result: dict[str, Any]) -> dict[str, Any]:
    """Cria/atualiza a tabela final no Postgres com upsert por id_transacao."""
    df = pd.read_csv(transform_result["relatorio_final_path"])
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    records = [
        tuple(none_if_nan(row[column]) for column in FINAL_COLUMNS)
        for _, row in df.iterrows()
    ]

    with hook.get_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(CREATE_TABLE_SQL)
            execute_values(cursor, UPSERT_SQL, records)
        connection.commit()

    return {
        "registros_carregados": len(records),
        "tabela_destino": DESTINATION_TABLE,
    }
