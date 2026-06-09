from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task

from nogtech_etl.extract import extract_local_files
from nogtech_etl.load import load_to_postgres
from nogtech_etl.transform import transform_and_enrich
from nogtech_etl.validation import validate_result


@dag(
    dag_id="dag_nogtech_etl",
    description="ETL diario da NogTech com Airflow, BrasilAPI, cache local e PostgreSQL.",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "nogtech",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["nogtech", "etl", "brasilapi", "postgres"],
)
def nogtech_etl_dag() -> None:
    """Orquestra a pipeline; a logica de negocio fica nos modulos nogtech_etl."""

    @task(task_id="extract_local_files")
    def extract_task() -> dict[str, Any]:
        """Extrai arquivos locais e grava os dados normalizados em stage."""
        return extract_local_files()

    @task(task_id="transform_and_enrich")
    def transform_task(extraction_result: dict[str, Any]) -> dict[str, Any]:
        """Cruza bases, consulta BrasilAPI e gera o relatorio final."""
        return transform_and_enrich(extraction_result)

    @task(task_id="load_to_postgres")
    def load_task(transform_result: dict[str, Any]) -> dict[str, Any]:
        """Carrega o relatorio final no Postgres usando upsert."""
        return load_to_postgres(transform_result)

    @task(task_id="validate_result")
    def validate_task(load_result: dict[str, Any]) -> dict[str, Any]:
        """Confere duplicidade, anonimizacao de CPF e schema final."""
        return validate_result(load_result)

    extracted = extract_task()
    transformed = transform_task(extracted)
    loaded = load_task(transformed)
    validate_task(loaded)


nogtech_etl_dag()
