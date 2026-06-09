# NogTech Airflow ETL

Pipeline didatico de Big Data para orquestrar uma ETL com Apache Airflow,
Docker Compose, BrasilAPI e PostgreSQL.

A NogTech e uma plataforma ficticia de cursos de tecnologia. O objetivo do
projeto e consolidar transacoes financeiras com dados de engajamento dos
alunos, enriquecer as vendas com informacoes publicas de CEP e feriados
nacionais, anonimizar dados pessoais e persistir o resultado de forma
idempotente.

## Tecnologias

- Apache Airflow 2.9.3
- PostgreSQL 13
- Docker Compose
- Python, pandas, requests e psycopg2
- BrasilAPI

## Estrutura do projeto

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
`-- README.md
```

> As pastas `data/output`, `data/cache` e `logs` sao geradas/usadas em tempo de
> execucao. O `docker-compose.yml` monta `./data` em `/opt/airflow/data` dentro
> dos containers do Airflow.

## Pre-requisitos

- Docker
- Docker Compose
- Portas locais livres: `8080` e `5432`

## Como executar

Na raiz do projeto, suba o ambiente:

```bash
docker compose up -d
```

Para acompanhar os logs do Airflow:

```bash
docker compose logs -f airflow-scheduler airflow-webserver
```

Em Linux, caso exista problema de permissao nos volumes, defina o usuario do
Airflow antes de subir os containers:

```bash
export AIRFLOW_UID=50000
docker compose up -d
```

## Como executar os testes

Instale as dependencias e execute a suite automatizada:

```bash
pip install -r requirements.txt
python -m pytest -q
```

Os testes cobrem cenarios de sucesso, tratamento de erros e falhas simuladas
sem depender do Airflow ou do PostgreSQL em execucao.

## Portas e acessos

| Servico | URL/porta | Credenciais |
| --- | --- | --- |
| Airflow Webserver | http://localhost:8080 | `airflow` / `airflow` |
| PostgreSQL | `localhost:5432` | `airflow` / `airflow` |

Banco PostgreSQL usado pela aplicacao: `airflow`.

## Como executar a DAG

1. Abra http://localhost:8080.
2. Entre com usuario `airflow` e senha `airflow`.
3. Localize a DAG `dag_nogtech_etl`.
4. Ative a DAG pelo toggle.
5. Clique em `Trigger DAG` para executar manualmente.

A DAG tambem esta agendada com `@daily`, `start_date=datetime(2024, 1, 1)` e
`catchup=False`, evitando execucoes historicas automaticas.

## Fluxo da DAG

```text
extract_local_files >> transform_and_enrich >> load_to_postgres >> validate_result
```

### 1. Extract

A task `extract_local_files` le as fontes locais:

- `data/input/transacoes_nogtech.csv`
  - encoding `latin-1`
  - delimitador `;`
- `data/input/engajamento_alunos.json`
  - encoding `utf-8`

Nesta etapa tambem sao padronizados CPF, CEP, datas e valores monetarios. A
coluna `mes_referencia` e calculada a partir de `data_transacao`.

Os dados normalizados sao gravados como arquivos intermediarios em:

```text
data/output/_stage_transacoes.json
data/output/_stage_engajamento.json
```

### 2. Transform e enriquecimento

A task `transform_and_enrich` faz um `LEFT JOIN` entre transacoes e engajamento
usando:

```text
cpf_padronizado + mes_referencia
```

Com isso, toda transacao e mantida mesmo quando nao existe engajamento no mes
correspondente. Nesses casos, as metricas de engajamento ficam nulas.

Depois, a task consulta a BrasilAPI:

- CEP: `https://brasilapi.com.br/api/cep/v2/{CEP}`
- Feriados: `https://brasilapi.com.br/api/feriados/v1/{ANO}`

Campos adicionados ao resultado:

- `cidade`
- `uf`
- `bairro`
- `venda_em_feriado`

Antes da saida final, o CPF e anonimizado no formato:

```text
***.456.789-**
```

O campo `nome_aluno` nao e carregado no dataset final.

Arquivo final gerado:

```text
data/output/relatorio_final.csv
```

### 3. Load

A task `load_to_postgres` cria a tabela `fato_vendas`, se ela ainda nao existir,
e grava os registros no PostgreSQL.

Tabela destino:

```text
fato_vendas
```

Principais campos:

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

### 4. Validacao

A task `validate_result` executa validacoes depois da carga:

- nao pode existir duplicidade por `id_transacao`
- a coluna `nome_aluno` nao pode existir na tabela final
- todos os CPFs carregados devem estar anonimizados

Se alguma regra falhar, a task falha e o erro aparece nos logs do Airflow.

## Estrategia de idempotencia

A idempotencia foi implementada com chave natural e UPSERT.

O campo `id_transacao` e a chave primaria da tabela `fato_vendas`. Durante a
carga, o pipeline usa:

```sql
ON CONFLICT (id_transacao) DO UPDATE
```

Assim, executar a DAG varias vezes para o mesmo lote nao duplica linhas. Se uma
transacao ja existir, os campos sao atualizados com os valores reprocessados.

Essa estrategia foi escolhida porque `id_transacao` identifica naturalmente uma
venda e permite reprocessamento simples, previsivel e seguro.

## Cache da BrasilAPI

O cache local fica em `data/cache`:

- `ceps_cache.json`: evita consultar o mesmo CEP mais de uma vez.
- `feriados_cache.json`: evita consultar feriados mais de uma vez por ano.

O cache e persistido em arquivo JSON e reaproveitado entre execucoes da DAG,
desde que a pasta local seja mantida.

## Tratamento de falhas

A DAG possui retry configurado no Airflow:

- `retries=3`
- `retry_delay=2 minutos`

As chamadas HTTP usam timeout de 10 segundos e retry para erros transientes:

- `429`
- `500`
- `502`
- `503`
- `504`

CEP inexistente (`404`) nao interrompe o pipeline; os campos de localizacao sao
preenchidos com nulo. Erros de rede, timeout ou indisponibilidade da API fazem a
task falhar, permitindo que o Airflow aplique a politica de retry.

Os caches JSON sao gravados de forma atomica para reduzir o risco de arquivo
parcial em caso de falha durante a escrita.

## Observabilidade

Pela interface do Airflow e possivel acompanhar:

- grafo da DAG
- status de cada task
- historico de execucoes
- duracao das tarefas
- logs detalhados por task
- tentativas de retry

## Como verificar no PostgreSQL

Acesse o container do PostgreSQL:

```bash
docker compose exec postgres psql -U airflow -d airflow
```

Consulte a tabela final:

```sql
SELECT * FROM fato_vendas ORDER BY id_transacao;
```

Confira a quantidade de registros:

```sql
SELECT COUNT(*) FROM fato_vendas;
```

Confira se nao houve duplicidade:

```sql
SELECT id_transacao, COUNT(*)
FROM fato_vendas
GROUP BY id_transacao
HAVING COUNT(*) > 1;
```

Verifique as colunas gravadas:

```sql
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'fato_vendas'
ORDER BY ordinal_position;
```

Verifique se todos os CPFs estao anonimizados:

```sql
SELECT cpf_aluno
FROM fato_vendas
WHERE cpf_aluno !~ '^\*{3}\.\d{3}\.\d{3}-\*{2}$';
```

## Como parar o ambiente

Para parar os containers sem apagar os dados:

```bash
docker compose down
```

Para apagar tambem o volume do PostgreSQL:

```bash
docker compose down -v
```
