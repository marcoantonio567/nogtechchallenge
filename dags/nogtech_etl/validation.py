from __future__ import annotations

import logging
from typing import Any

from airflow.providers.postgres.hooks.postgres import PostgresHook

from nogtech_etl.config import DESTINATION_TABLE, POSTGRES_CONN_ID


def validate_result(load_result: dict[str, Any]) -> dict[str, Any]:
    """Valida regras minimas de qualidade depois da carga."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    with hook.get_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"SELECT COUNT(*), COUNT(DISTINCT id_transacao) FROM {DESTINATION_TABLE};"
            )
            total_rows, distinct_transactions = cursor.fetchone()

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM information_schema.columns
                WHERE table_name = %s
                  AND column_name = 'nome_aluno';
                """,
                (DESTINATION_TABLE,),
            )
            nome_aluno_columns = cursor.fetchone()[0]

            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {DESTINATION_TABLE}
                WHERE cpf_aluno !~ '^\\*{{3}}\\.\\d{{3}}\\.\\d{{3}}-\\*{{2}}$';
                """
            )
            invalid_cpfs = cursor.fetchone()[0]

    if total_rows != distinct_transactions:
        raise ValueError("Validacao falhou: existem ids de transacao duplicados.")
    if nome_aluno_columns != 0:
        raise ValueError("Validacao falhou: campo nome_aluno apareceu no destino final.")
    if invalid_cpfs != 0:
        raise ValueError("Validacao falhou: existem CPFs sem anonimizacao.")

    logging.info("Validacao concluida com %s registros em %s.", total_rows, DESTINATION_TABLE)
    return {
        **load_result,
        "total_registros_destino": total_rows,
        "ids_distintos": distinct_transactions,
    }
