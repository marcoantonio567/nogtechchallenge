from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_DIR = Path("/opt/airflow/data")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
CACHE_DIR = BASE_DIR / "cache"

TRANSACOES_PATH = INPUT_DIR / "transacoes_nogtech.csv"
ENGAJAMENTO_PATH = INPUT_DIR / "engajamento_alunos.json"
STAGE_TRANSACOES_PATH = OUTPUT_DIR / "_stage_transacoes.json"
STAGE_ENGAJAMENTO_PATH = OUTPUT_DIR / "_stage_engajamento.json"
RELATORIO_FINAL_PATH = OUTPUT_DIR / "relatorio_final.csv"
CEP_CACHE_PATH = CACHE_DIR / "ceps_cache.json"
FERIADOS_CACHE_PATH = CACHE_DIR / "feriados_cache.json"

POSTGRES_CONN_ID = "postgres_nogtech"


def _ensure_dirs() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _read_json_cache(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    with path.open("r", encoding="utf-8") as cache_file:
        try:
            return json.load(cache_file)
        except json.JSONDecodeError:
            logging.warning("Cache %s estava invalido. Recriando arquivo.", path)
            return {}


def _write_json_cache(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_suffix(".tmp")
    with temporary_path.open("w", encoding="utf-8") as cache_file:
        json.dump(data, cache_file, ensure_ascii=False, indent=2, sort_keys=True)
    temporary_path.replace(path)


def _http_session() -> requests.Session:
    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    return session


def _only_digits(value: Any) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"\D", "", str(value))


def _format_cpf(value: Any) -> str | None:
    digits = _only_digits(value)
    if len(digits) == 10:
        digits = "0" + digits
    if len(digits) != 11:
        return None
    return f"{digits[:3]}.{digits[3:6]}.{digits[6:9]}-{digits[9:]}"


def _mask_cpf(value: Any) -> str | None:
    digits = _only_digits(value)
    if len(digits) != 11:
        return None
    return f"***.{digits[3:6]}.{digits[6:9]}-**"


def _normalize_cep(value: Any) -> str | None:
    digits = _only_digits(value)
    if not digits:
        return None
    return digits.zfill(8)[-8:]


def _parse_date(value: Any) -> pd.Timestamp:
    if pd.isna(value):
        return pd.NaT

    value_as_text = str(value).strip()
    for date_format in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d", "%d-%m-%Y"):
        try:
            return pd.Timestamp(datetime.strptime(value_as_text, date_format))
        except ValueError:
            continue

    return pd.to_datetime(value_as_text, dayfirst=True, errors="coerce")


def _parse_brl(value: Any) -> float | None:
    if pd.isna(value):
        return None

    text = str(value).strip().replace("R$", "").replace(" ", "")
    if "," in text:
        text = text.replace(".", "").replace(",", ".")

    try:
        return round(float(text), 2)
    except ValueError:
        return None


def _normalize_transaction_columns(df: pd.DataFrame) -> pd.DataFrame:
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

    df["cpf_padronizado"] = df["cpf_aluno"].apply(_format_cpf)
    df["cep_cobranca"] = df["cep_cobranca"].apply(_normalize_cep)
    df["data_transacao"] = df["data_transacao"].apply(_parse_date)
    df["valor"] = df["valor"].apply(_parse_brl)
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


def _normalize_engagement_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    required_columns = {"cpf_aluno", "mes_referencia"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Colunas obrigatorias ausentes em engajamento: {sorted(missing_columns)}")

    for optional_column in ("horas_assistidas", "percentual_conclusao"):
        if optional_column not in df.columns:
            df[optional_column] = None

    df["cpf_padronizado"] = df["cpf_aluno"].apply(_format_cpf)
    df["mes_referencia"] = df["mes_referencia"].astype(str).str.slice(0, 7)
    df["horas_assistidas"] = pd.to_numeric(df["horas_assistidas"], errors="coerce")
    df["percentual_conclusao"] = pd.to_numeric(df["percentual_conclusao"], errors="coerce")
    df = df.dropna(subset=["cpf_padronizado", "mes_referencia"])
    return df.drop_duplicates(subset=["cpf_padronizado", "mes_referencia"], keep="last")


def _fetch_cep_data(cep: str, cache: dict[str, Any], session: requests.Session) -> dict[str, Any]:
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


def _fetch_holidays(year: int, cache: dict[str, Any], session: requests.Session) -> set[str]:
    year_key = str(year)
    if year_key not in cache:
        response = session.get(f"https://brasilapi.com.br/api/feriados/v1/{year}", timeout=10)
        response.raise_for_status()
        cache[year_key] = sorted(item["date"] for item in response.json())

    return set(cache[year_key])


def _none_if_nan(value: Any) -> Any:
    if pd.isna(value):
        return None
    return value


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
def nogtech_etl_dag():
    @task(task_id="extract_local_files")
    def extract_local_files() -> dict[str, Any]:
        _ensure_dirs()

        transacoes = pd.read_csv(
            TRANSACOES_PATH,
            sep=";",
            encoding="latin-1",
            dtype=str,
        )
        engajamento = pd.read_json(ENGAJAMENTO_PATH, encoding="utf-8")

        transacoes = _normalize_transaction_columns(transacoes)
        engajamento = _normalize_engagement_columns(engajamento)

        transacoes.to_json(STAGE_TRANSACOES_PATH, orient="records", force_ascii=False, indent=2)
        engajamento.to_json(STAGE_ENGAJAMENTO_PATH, orient="records", force_ascii=False, indent=2)

        return {
            "transacoes_path": str(STAGE_TRANSACOES_PATH),
            "engajamento_path": str(STAGE_ENGAJAMENTO_PATH),
            "transacoes_extraidas": len(transacoes),
            "engajamentos_extraidos": len(engajamento),
        }

    @task(task_id="transform_and_enrich")
    def transform_and_enrich(extraction_result: dict[str, Any]) -> dict[str, Any]:
        _ensure_dirs()

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

        session = _http_session()
        cep_cache = _read_json_cache(CEP_CACHE_PATH)
        holiday_cache = _read_json_cache(FERIADOS_CACHE_PATH)

        unique_ceps = sorted(cep for cep in merged["cep_cobranca"].dropna().unique())
        cep_data = {cep: _fetch_cep_data(cep, cep_cache, session) for cep in unique_ceps}
        _write_json_cache(CEP_CACHE_PATH, cep_cache)

        merged["cidade"] = merged["cep_cobranca"].map(lambda cep: cep_data.get(cep, {}).get("cidade"))
        merged["uf"] = merged["cep_cobranca"].map(lambda cep: cep_data.get(cep, {}).get("uf"))
        merged["bairro"] = merged["cep_cobranca"].map(lambda cep: cep_data.get(cep, {}).get("bairro"))

        years = sorted(pd.to_datetime(merged["data_transacao"]).dt.year.unique())
        holidays_by_year = {
            int(year): _fetch_holidays(int(year), holiday_cache, session) for year in years
        }
        _write_json_cache(FERIADOS_CACHE_PATH, holiday_cache)

        def is_holiday(date_as_text: str) -> bool:
            transaction_date = pd.Timestamp(date_as_text)
            return date_as_text in holidays_by_year.get(transaction_date.year, set())

        merged["venda_em_feriado"] = merged["data_transacao"].apply(is_holiday)
        merged["cpf_aluno"] = merged["cpf_padronizado"].apply(_mask_cpf)
        merged["data_processamento_utc"] = datetime.now(timezone.utc).isoformat()

        final_columns = [
            "id_transacao",
            "cpf_aluno",
            "data_transacao",
            "mes_referencia",
            "valor",
            "curso",
            "cep_cobranca",
            "cidade",
            "uf",
            "bairro",
            "venda_em_feriado",
            "horas_assistidas",
            "percentual_conclusao",
            "data_processamento_utc",
        ]
        final_df = merged[final_columns].sort_values("id_transacao")
        final_df.to_csv(RELATORIO_FINAL_PATH, index=False, encoding="utf-8")

        return {
            "relatorio_final_path": str(RELATORIO_FINAL_PATH),
            "registros_transformados": len(final_df),
            "ceps_em_cache": len(cep_cache),
            "anos_feriados_em_cache": len(holiday_cache),
        }

    @task(task_id="load_to_postgres")
    def load_to_postgres(transform_result: dict[str, Any]) -> dict[str, Any]:
        df = pd.read_csv(transform_result["relatorio_final_path"])
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fato_vendas (
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

        columns = [
            "id_transacao",
            "cpf_aluno",
            "data_transacao",
            "mes_referencia",
            "valor",
            "curso",
            "cep_cobranca",
            "cidade",
            "uf",
            "bairro",
            "venda_em_feriado",
            "horas_assistidas",
            "percentual_conclusao",
            "data_processamento_utc",
        ]
        records = [
            tuple(_none_if_nan(row[column]) for column in columns)
            for _, row in df.iterrows()
        ]

        upsert_sql = f"""
        INSERT INTO fato_vendas ({", ".join(columns)})
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

        with hook.get_conn() as connection:
            with connection.cursor() as cursor:
                cursor.execute(create_table_sql)
                execute_values(cursor, upsert_sql, records)
            connection.commit()

        return {
            "registros_carregados": len(records),
            "tabela_destino": "fato_vendas",
        }

    @task(task_id="validate_result")
    def validate_result(load_result: dict[str, Any]) -> dict[str, Any]:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        with hook.get_conn() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT COUNT(*), COUNT(DISTINCT id_transacao) FROM fato_vendas;")
                total_rows, distinct_transactions = cursor.fetchone()

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.columns
                    WHERE table_name = 'fato_vendas'
                      AND column_name = 'nome_aluno';
                    """
                )
                nome_aluno_columns = cursor.fetchone()[0]

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM fato_vendas
                    WHERE cpf_aluno !~ '^\\*{3}\\.\\d{3}\\.\\d{3}-\\*{2}$';
                    """
                )
                invalid_cpfs = cursor.fetchone()[0]

        if total_rows != distinct_transactions:
            raise ValueError("Validacao falhou: existem ids de transacao duplicados.")
        if nome_aluno_columns != 0:
            raise ValueError("Validacao falhou: campo nome_aluno apareceu no destino final.")
        if invalid_cpfs != 0:
            raise ValueError("Validacao falhou: existem CPFs sem anonimizacao.")

        logging.info("Validacao concluida com %s registros em fato_vendas.", total_rows)
        return {
            **load_result,
            "total_registros_destino": total_rows,
            "ids_distintos": distinct_transactions,
        }

    extracted = extract_local_files()
    transformed = transform_and_enrich(extracted)
    loaded = load_to_postgres(transformed)
    validate_result(loaded)


nogtech_etl_dag()
