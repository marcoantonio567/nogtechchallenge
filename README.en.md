# NogTech Airflow ETL

[![Português](https://img.shields.io/badge/Portugu%C3%AAs-ler-009C3B?style=for-the-badge)](README.md)
[![English](https://img.shields.io/badge/English-active-1F6FEB?style=for-the-badge)](README.en.md)

Educational Big Data pipeline that orchestrates an ETL workflow with Apache
Airflow, Docker Compose, BrasilAPI, and PostgreSQL.

NogTech is a fictional technology course platform. This project consolidates
financial transactions with student engagement data, enriches sales with
public ZIP code and Brazilian holiday information, anonymizes personal data,
and persists the results idempotently.

## Technologies

- Apache Airflow 2.9.3
- PostgreSQL 13
- Docker Compose
- Python, pandas, requests, and psycopg2
- BrasilAPI

## Project structure

```text
.
|-- dags/
|   |-- dag_nogtech_etl.py
|   `-- nogtech_etl/
|       |-- config.py
|       |-- http_client.py
|       |-- normalization.py
|       |-- storage.py
|       |-- extract/
|       |   `-- local_files.py
|       |-- transform/
|       |   `-- enrichment.py
|       |-- load/
|       |   `-- postgres.py
|       `-- validation/
|           `-- result.py
|-- data/
|   |-- input/
|   |   |-- transacoes_nogtech.csv
|   |   `-- engajamento_alunos.json
|   |-- output/
|   |   `-- relatorio_final.csv
|   `-- cache/
|       |-- ceps_cache.json
|       `-- feriados_cache.json
|-- logs/
|-- plugins/
|-- docker-compose.yml
|-- requirements.txt
|-- etl_mermaid.md
|-- README.en.md
`-- README.md
```

> The `data/output`, `data/cache`, and `logs` directories are generated or used
> at runtime. `docker-compose.yml` mounts `./data` at `/opt/airflow/data` inside
> the Airflow containers.

## Prerequisites

- Docker
- Docker Compose
- Available local ports: `8080` and `5432`

## Running the project

From the project root, start the environment:

```bash
docker compose up -d
```

Follow the Airflow logs:

```bash
docker compose logs -f airflow-scheduler airflow-webserver
```

On Linux, if you encounter volume permission issues, set the Airflow user
before starting the containers:

```bash
export AIRFLOW_UID=50000
docker compose up -d
```

## Running the tests

Install the dependencies and run the automated test suite:

```bash
pip install -r requirements.txt
python -m pytest -q
```

The tests cover successful scenarios, error handling, and simulated failures
without requiring Airflow or PostgreSQL to be running.

## Ports and access

| Service | URL/port | Credentials |
| --- | --- | --- |
| Airflow Webserver | http://localhost:8080 | `airflow` / `airflow` |
| PostgreSQL | `localhost:5432` | `airflow` / `airflow` |

The PostgreSQL database used by the application is `airflow`.

## Running the DAG

1. Open http://localhost:8080.
2. Sign in with username `airflow` and password `airflow`.
3. Find the `dag_nogtech_etl` DAG.
4. Enable the DAG using the toggle.
5. Click `Trigger DAG` to run it manually.

The DAG is also scheduled with `@daily`, `start_date=datetime(2024, 1, 1)`,
and `catchup=False`, preventing automatic historical runs.

## DAG workflow

```text
extract_local_files >> transform_and_enrich >> load_to_postgres >> validate_result
```

### 1. Extract

The `extract_local_files` task reads the local sources:

- `data/input/transacoes_nogtech.csv`
  - encoding: `latin-1`
  - delimiter: `;`
- `data/input/engajamento_alunos.json`
  - encoding: `utf-8`

This stage also standardizes Brazilian taxpayer IDs (CPF), ZIP codes, dates,
and monetary values. The `mes_referencia` column is calculated from
`data_transacao`.

The normalized data is written to intermediate files:

```text
data/output/_stage_transacoes.json
data/output/_stage_engajamento.json
```

### 2. Transform and enrichment

The `transform_and_enrich` task performs a `LEFT JOIN` between transactions
and engagement data using:

```text
cpf_padronizado + mes_referencia
```

This keeps every transaction even when there is no engagement data for the
corresponding month. In those cases, engagement metrics remain null.

The task then queries BrasilAPI:

- ZIP code: `https://brasilapi.com.br/api/cep/v2/{CEP}`
- Holidays: `https://brasilapi.com.br/api/feriados/v1/{YEAR}`

The following fields are added to the result:

- `cidade`
- `uf`
- `bairro`
- `venda_em_feriado`

Before the final output, the CPF is anonymized using this format:

```text
***.456.789-**
```

The `nome_aluno` field is not loaded into the final dataset.

Final output file:

```text
data/output/relatorio_final.csv
```

### 3. Load

The `load_to_postgres` task creates the `fato_vendas` table if it does not
exist and writes the records to PostgreSQL.

Target table:

```text
fato_vendas
```

Main fields:

- `id_transacao`
- `cpf_aluno`
- `data_transacao`
- `mes_referencia`
- `valor`
- `curso`
- `cep_cobranca`
- `cidade`
- `uf`
- `bairro`
- `venda_em_feriado`
- `horas_assistidas`
- `percentual_conclusao`
- `data_processamento_utc`

### 4. Validation

The `validate_result` task runs validation rules after loading:

- no duplicate `id_transacao` values may exist
- the `nome_aluno` column must not exist in the final table
- every loaded CPF must be anonymized

If any rule fails, the task fails and the error appears in the Airflow logs.

## Idempotency strategy

Idempotency is implemented using a natural key and UPSERT.

The `id_transacao` field is the primary key of the `fato_vendas` table. During
the load, the pipeline uses:

```sql
ON CONFLICT (id_transacao) DO UPDATE
```

Running the DAG multiple times for the same batch therefore does not create
duplicate rows. If a transaction already exists, its fields are updated with
the reprocessed values.

This strategy was selected because `id_transacao` naturally identifies a sale
and enables simple, predictable, and safe reprocessing.

## BrasilAPI cache

The local cache is stored in `data/cache`:

- `ceps_cache.json`: avoids querying the same ZIP code more than once.
- `feriados_cache.json`: avoids querying holidays more than once per year.

The cache is persisted in JSON files and reused across DAG runs as long as the
local directory is retained.

## Failure handling

The DAG has retries configured in Airflow:

- `retries=3`
- `retry_delay=2 minutes`

HTTP requests use a 10-second timeout and retry transient errors:

- `429`
- `500`
- `502`
- `503`
- `504`

A nonexistent ZIP code (`404`) does not interrupt the pipeline; location
fields are filled with null values. Network errors, timeouts, or API outages
cause the task to fail, allowing Airflow to apply its retry policy.

JSON cache files are written atomically to reduce the risk of partial files if
a failure occurs while writing.

## Observability

The Airflow interface provides access to:

- DAG graph
- task status
- run history
- task duration
- detailed logs for each task
- retry attempts

## Checking the data in PostgreSQL

Access the PostgreSQL container:

```bash
docker compose exec postgres psql -U airflow -d airflow
```

Query the final table:

```sql
SELECT * FROM fato_vendas ORDER BY id_transacao;
```

Check the record count:

```sql
SELECT COUNT(*) FROM fato_vendas;
```

Check for duplicates:

```sql
SELECT id_transacao, COUNT(*)
FROM fato_vendas
GROUP BY id_transacao
HAVING COUNT(*) > 1;
```

Inspect the stored columns:

```sql
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'fato_vendas'
ORDER BY ordinal_position;
```

Verify that every CPF is anonymized:

```sql
SELECT cpf_aluno
FROM fato_vendas
WHERE cpf_aluno !~ '^\*{3}\.\d{3}\.\d{3}-\*{2}$';
```

## Stopping the environment

Stop the containers without deleting the data:

```bash
docker compose down
```

Delete the PostgreSQL volume as well:

```bash
docker compose down -v
```
