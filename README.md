# NogTech Airflow ETL

Projeto didatico de Big Data para orquestrar um pipeline de ETL com Apache Airflow, Docker Compose, BrasilAPI e PostgreSQL.

A NogTech e uma plataforma ficticia de cursos de tecnologia. A DAG consolida transacoes financeiras com engajamento dos alunos, enriquece cada venda com dados publicos de CEP e feriados nacionais, anonimiza dados pessoais e grava o resultado em uma tabela idempotente.

## Estrutura

```text
.
|-- dags/
|   `-- dag_nogtech_etl.py
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
`-- README.md
```

## Servicos

- Airflow Webserver: http://localhost:8080
- PostgreSQL: `localhost:5432`
- Usuario do Airflow: `airflow`
- Senha do Airflow: `airflow`
- Banco usado no PostgreSQL: `airflow`

## Como executar

Na raiz do projeto, suba os containers:

```bash
docker compose up -d
```

Se preferir acompanhar os logs:

```bash
docker compose logs -f airflow-scheduler airflow-webserver
```

Em Windows com Docker Desktop, normalmente nao e necessario definir `AIRFLOW_UID`. Em Linux, se houver problema de permissao nos volumes, execute antes:

```bash
export AIRFLOW_UID=50000
```

## Como acessar o Airflow

1. Abra http://localhost:8080.
2. Entre com usuario `airflow` e senha `airflow`.
3. Localize a DAG `dag_nogtech_etl`.
4. Ative a DAG no botao de toggle.
5. Clique em `Trigger DAG` para executar manualmente.

A DAG tambem esta configurada com agenda diaria `@daily` e `catchup=False`, entao ela nao tenta executar datas historicas automaticamente.

## Fluxo da DAG

```text
extract_local_files >> transform_and_enrich >> load_to_postgres >> validate_result
```

### extract_local_files

Le os arquivos locais:

- `data/input/transacoes_nogtech.csv`, com encoding `latin-1` e delimitador `;`
- `data/input/engajamento_alunos.json`, com encoding `utf-8`

Tambem padroniza CPF, datas, valores monetarios e calcula `mes_referencia` com base em `data_transacao`.

### transform_and_enrich

Faz `LEFT JOIN` entre transacoes e engajamento usando `cpf_aluno` padronizado e `mes_referencia`.

Depois consulta a BrasilAPI:

- CEP: `https://brasilapi.com.br/api/cep/v2/{CEP}`
- Feriados: `https://brasilapi.com.br/api/feriados/v1/{ANO}`

O resultado recebe:

- `cidade`
- `uf`
- `bairro`
- `venda_em_feriado`

Antes da carga final, o CPF e anonimizado no formato `***.456.789-**` e o campo `nome_aluno` e removido.

### load_to_postgres

Cria, se necessario, a tabela `fato_vendas` no PostgreSQL e grava os dados com chave primaria em `id_transacao`.

A estrategia de idempotencia usa:

```sql
ON CONFLICT (id_transacao) DO UPDATE
```

Assim, reprocessar o mesmo lote atualiza os registros existentes em vez de duplicar linhas.

### validate_result

Valida se:

- nao existem duplicidades por `id_transacao`
- `nome_aluno` nao existe na tabela final
- todos os CPFs gravados estao anonimizados

## Cache local

Os caches ficam em `data/cache`.

- `ceps_cache.json`: evita consultar o mesmo CEP mais de uma vez.
- `feriados_cache.json`: evita consultar feriados mais de uma vez por ano.

Esses arquivos sao montados dentro do container em `/opt/airflow/data/cache`, portanto o cache sobrevive a novas execucoes da DAG enquanto a pasta local for mantida.

## Retry e tratamento de falhas

A DAG possui:

- `retries=3`
- `retry_delay=2 minutos`

As chamadas HTTP usam timeout e retry para erros transientes como `429`, `500`, `502`, `503` e `504`. CEP inexistente retorna campos de localizacao nulos e segue o fluxo. Erros de rede ou indisponibilidade da API fazem a tarefa falhar, permitindo que o Airflow aplique a politica de retry.

## Observabilidade

Pelo Airflow e possivel acompanhar:

- status geral da DAG
- duracao das tarefas
- logs por task
- historico de execucoes
- tentativas de retry

O arquivo final tambem pode ser conferido em:

```text
data/output/relatorio_final.csv
```

## Como verificar no PostgreSQL

Entre no container do PostgreSQL:

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

Verifique se o campo `nome_aluno` nao existe no destino:

```sql
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'fato_vendas'
ORDER BY ordinal_position;
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

## Roteiro de video de ate 5 minutos

1. Apresentacao rapida do problema, da empresa ficticia NogTech e da escolha do Apache Airflow.
2. Mostrar a estrutura do projeto: `dags`, `data/input`, `data/cache`, `data/output`, `docker-compose.yml`.
3. Subir o ambiente com `docker compose up -d` e abrir o Airflow em http://localhost:8080.
4. Explicar as quatro tarefas da DAG e disparar `dag_nogtech_etl` manualmente.
5. Abrir os logs de uma task para mostrar observabilidade, retry configurado e chamadas para BrasilAPI.
6. Mostrar os arquivos de cache preenchidos em `data/cache`.
7. Verificar o arquivo `data/output/relatorio_final.csv`, destacando CPF anonimizado e ausencia de `nome_aluno`.
8. Abrir o PostgreSQL com `psql` e executar `SELECT * FROM fato_vendas ORDER BY id_transacao;`.
9. Executar a DAG novamente e mostrar que a contagem nao duplica, explicando o `ON CONFLICT`.
10. Encerrar retomando LGPD, idempotencia, cache e orquestracao pelo Airflow.
