from __future__ import annotations

from pathlib import Path


# Caminhos usados dentro do container do Airflow. O docker-compose monta
# ./data em /opt/airflow/data, por isso a DAG trabalha sempre a partir daqui.
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
DESTINATION_TABLE = "fato_vendas"

FINAL_COLUMNS = [
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
