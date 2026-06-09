# Atividade Prática (Grupo com máximo de 4 participantes)

## Orquestração de Pipelines de ETL com Ferramentas Open Source (Airflow, NiFi ou Luigi)

**Disciplina:** Big Data

---

# 1. Contexto e Objetivo

No dia a dia da Engenharia de Software e de Dados, o gerenciamento de fluxos de informação exige automação, resiliência e monitoramento. Em vez de desenvolvermos motores de agendamento do zero, utilizamos ferramentas consolidadas de mercado para gerenciar a dependência e a execução de grafos de tarefas — conhecidos como DAGs (*Directed Acyclic Graphs*).

O objetivo desta atividade é projetar, containerizar e monitorar um pipeline de ETL (*Extract, Transform, Load*) real, utilizando uma das tecnologias open source homologadas. O fluxo integrará arquivos estáticos gerados localmente com dados dinâmicos consumidos em tempo real da BrasilAPI.

---

# 2. O Desafio — Cenário NogTech

A **NogTech**, plataforma de cursos de tecnologia, precisa consolidar um relatório diário cruzando transações financeiras com engajamento dos alunos.

A diretoria quer saber, em uma única visão:

> Quem está pagando pelos cursos, de onde vem (geograficamente), em quais datas (incluindo feriados) e se essas pessoas estão realmente consumindo a plataforma.

Cada equipe deverá escolher uma das ferramentas de orquestração e construir um pipeline automatizado que entregue essa visão.

O ecossistema deve ser:

- Tolerante a falhas
- Idempotente (reexecuções sem duplicar dados)
- Compatível com a LGPD através de anonimização dos dados

---

# 3. Escopo do Pipeline de ETL (Requisitos de Negócio)

O pipeline deve consolidar um relatório de vendas diárias integrando dados locais e governamentais.

O fluxo deve seguir rigorosamente as três etapas:

---

## 🟩 Extract (Extração)

O pipeline deve extrair dados de duas fontes distintas.

### Fonte A — Arquivos Estáticos

Arquivos locais:

#### `transacoes_nogtech.csv`

- Exportado pelo ERP financeiro
- Encoding: `latin-1`
- Delimitador: `;`

#### `engajamento_alunos.json`

- Exportado pela plataforma de vídeo
- Encoding: `utf-8`

### Fonte B — API em Tempo Real

Consumir dados públicos diretamente da BrasilAPI (sem necessidade de autenticação ou tokens) para enriquecer o contexto do pipeline.

---

## 🟨 Transform (Transformação)

Os dados extraídos devem ser cruzados, validados e higienizados conforme as regras abaixo.

**Todas as regras são obrigatórias.**

### 3.1 Junção das Fontes Locais

Cruzar transações com engajamento utilizando `cpf_aluno` como chave.

Para cada transação:

- Anexar o engajamento do mês correspondente
- `mes_referencia = mês(data_transacao)`

Quando não houver engajamento:

- Manter a transação
- Campos de engajamento devem permanecer nulos

**Tipo de junção:**

```sql
LEFT JOIN
```

---

### 3.2 Padronização de CPF

Os CPFs podem aparecer em dois formatos:

```text
123.456.789-00
12345678900
```

Todos devem ser convertidos para:

```text
123.456.789-00
```

A padronização deve ocorrer antes da anonimização.

---

### 3.3 Enriquecimento de Localização (BrasilAPI)

Utilizar:

```http
https://brasilapi.com.br/api/cep/v2/{CEP}
```

Para converter:

```text
cep_cobranca
```

Em:

- Cidade
- Estado (UF)
- Bairro

Esses campos devem ser adicionados ao registro.

#### Cache obrigatório

Implementar cache local utilizando:

- Dicionário em memória
- Arquivo JSON
- SQLite

**Não consultar a API duas vezes para o mesmo CEP.**

---

### 3.4 Análise de Calendário (BrasilAPI)

Utilizar:

```http
https://brasilapi.com.br/api/feriados/v1/{ANO}
```

Para verificar se:

```text
data_transacao
```

Ocorreu em feriado nacional.

Criar a coluna:

```text
venda_em_feriado
```

Valores possíveis:

```text
true
false
```

#### Cache obrigatório

A lista de feriados deve ser armazenada por ano.

Uma única chamada por ano deve atender todas as transações daquele período.

---

### 3.5 Anonimização (LGPD)

Antes da gravação no destino final:

#### CPF

Entrada:

```text
123.456.789-00
```

Saída:

```text
***.456.789-**
```

Apenas os 6 dígitos centrais devem permanecer visíveis.

#### Nome do aluno

O campo:

```text
nome_aluno
```

Deve ser removido completamente do dataset final.

> Não basta mascarar. Nomes são identificadores diretos.

---

### 3.6 Idempotência

O pipeline deve ser executado múltiplas vezes para o mesmo lote sem gerar duplicidade.

Estratégias aceitas:

#### Opção 1

```text
Chave natural (id_transacao) + UPSERT
```

#### Opção 2

```text
Particionamento por data_transacao
com overwrite da partição
```

#### Opção 3

```text
Hash da linha
+
Tabela de controle de lotes
```

A estratégia escolhida deve ser justificada no README.

---

## 🟥 Load (Carga)

O dado final transformado e enriquecido deve ser gravado em uma das opções:

### Opção A — Banco Relacional

- PostgreSQL
- MySQL

Com tabela:

```text
fato_vendas
```

Modelada adequadamente.

### Opção B — Data Lake

Arquivos:

```text
Parquet
```

Particionados por:

```text
ano/mês
```

Simulando uma camada de Data Lake.

---

# 4. Requisitos de Engenharia de Software e Infraestrutura

> ⚠️ Fluxos manuais ou scripts isolados que funcionam apenas na máquina do aluno serão desconsiderados.

O foco é a engenharia e a sustentabilidade do ecossistema.

---

## Containerização (Docker)

A ferramenta escolhida:

- Airflow
- NiFi
- Luigi

Bem como bancos auxiliares, devem ser inicializados exclusivamente através de:

```yaml
docker-compose.yml
```

---

## Idempotência

O pipeline deve suportar múltiplas execuções sem:

- Duplicidade
- Inconsistência
- Corrupção dos dados

---

## Tratamento de Erros e Resiliência

Caso a BrasilAPI apresente:

- Instabilidade
- Timeout
- Erro de rede

O pipeline não pode corromper os dados.

Deve existir:

- Retry automático
- Ou fluxo alternativo de tratamento

Além de logs estruturados para falhas críticas.

---

## Observabilidade

A interface da ferramenta escolhida deve exibir:

- Grafo de execução
- Histórico de execuções
- Tempo de execução de cada nó

---

### Observação

A BrasilAPI é pública e gratuita.

Boas práticas:

- Utilizar cache
- Agrupar requisições
- Evitar loops infinitos durante o desenvolvimento

---

# 5. Opções de Implementação

A equipe deve escolher apenas uma opção.

---

## Opção A — Apache Airflow

### Requisitos

- PythonOperator
- BashOperator
- SqlOperator

Dependências:

```python
task_extract >> task_transform >> task_load
```

Uso de XComs quando necessário.

---

## Opção B — Apache NiFi

### Requisitos

Processors como:

- InvokeHTTP
- EvaluateJsonPath
- UpdateAttribute
- PutSQL

Além de:

- Process Groups
- Backpressure
- Filas configuradas adequadamente

---

## Opção C — Luigi

### Requisitos

Modelagem utilizando:

```python
luigi.Task
```

Dependências utilizando:

```python
requires()
```

Controle de conclusão utilizando:

```python
output()
```

e

```python
luigi.Target
```

Execução monitorada através do:

```text
Luigi Central Scheduler
```

Porta:

```text
8082
```

---

# 6. Entregáveis

## Repositório Git (GitHub ou GitLab)

Deve conter:

- Código-fonte completo
- DAGs (Airflow)
- Configurações do NiFi
- Scripts Luigi
- docker-compose.yml

---

## README.md

Deve conter:

### Inicialização

```bash
docker-compose up
```

### Portas de acesso

Interfaces visuais utilizadas.

### Estratégia de Idempotência

Explicação sucinta da solução adotada.

### Tratamento de Falhas

Explicação sucinta da estratégia implementada.

---

## Demonstração em Vídeo

**Duração máxima:** 5 minutos

O vídeo deve mostrar:

1. Ambiente subindo
2. Pipeline sendo executado
3. Execução pela interface visual
4. Resultado persistido no destino final

---

# 7. Formato de Entrega e Prazo

**Data de entrega:**

```text
09/06/2026
```