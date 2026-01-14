# Sparkle – Spark OpenAlex → OpenSearch

Projeto Spark unificado para ingestão de dados do **OpenAlex** no **OpenSearch**, com configuração centralizada via variáveis de ambiente (ENV) e **com job reutilizável** para múltiplos índices.

---

## 🎯 Objetivo

- Eliminar duplicação de scripts Spark  
- Centralizar configurações (OpenSearch, input, Spark)  
- Permitir execução flexível por **ENV**  
- Facilitar uso com Docker

---

## 📁 Estrutura do Projeto

```text
sparkle/
├── config/
│   └── settings.py          # Configurações centralizadas (ENV)
├── jobs/
│   └── send_to_opensearch.py    # Job Spark único
├── spark/
│   └── session.py           # Criação da SparkSession
├── utils/
│   └── opensearch.py        # Escrita no OpenSearch
└── README.md
```

---

## ⚙️ Configuração via Variáveis de Ambiente

Todas as configurações são definidas via **ENV** e centralizadas em `config/settings.py`.

### 📥 Entrada de dados
```bash
INPUT_DIR=/data
```

### 🔎 OpenSearch
```bash
OS_INDEX=raw_openalex_publishers
OS_NODES=https://200.136.72.107
OS_PORT=9200
OS_USER=admin
OS_PASS=********
```

### ⚡ Spark
```bash
SPARK_MASTER=local[*]
SPARK_APP_NAME=sparkle-raw_openalex_publishers
```

---

## ▶️ Exemplos de Execução

### 🔹 Publishers
```bash
export INPUT_DIR=/data/publishers
export OS_INDEX=raw_openalex_publishers

spark-submit jobs/send_to_opensearch.py
```

### 🔹 Works
```bash
export INPUT_DIR=/data/works
export OS_INDEX=raw_openalex_works

spark-submit jobs/send_to_opensearch.py
```

### 🔹 Institutions
```bash
export INPUT_DIR=/data/institutions
export OS_INDEX=raw_openalex_institutions

spark-submit jobs/send_to_opensearch.py
```

➡️ O mesmo job atende múltiplos índices apenas alterando as variáveis de ambiente.

---

## 🐳 Exemplo com Docker

```bash
docker run --rm \
  -e INPUT_DIR=/data/works \
  -e OS_INDEX=raw_openalex_works \
  -e OS_NODES=https://200.136.72.107 \
  -e OS_USER=admin \
  -e OS_PASS=******** \
  -v $(pwd)/data:/data \
  sparkle \
  spark-submit jobs/send_to_opensearch.py
```

---

## 🧠 Funcionamento

1. O Spark lê arquivos JSON/JSONL do `INPUT_DIR`
2. O job Spark único é executado
3. A escrita no OpenSearch ocorre via conector Spark
4. O índice de destino é definido por `OS_INDEX`

---

## 🔒 Segurança

⚠️ Não versionar credenciais sensíveis.

Recomenda-se:
- `.env` (não versionado)
- Secrets (Docker / Kubernetes)

---

## 🛠️ Requisitos

- Apache Spark 3.x  
- Java 11+  
- OpenSearch  
- Conector: `org.opensearch:opensearch-spark-30_2.12`  

---

## 🚀 Roadmap

- [ ] Bronze / Silver / Gold  
- [ ] Split por ano (`publication_year`)  
- [ ] Remoção de campos pesados (`inverted_abstract`, `fulltext`)    
- [ ] Métricas e logs estruturados  
## Exemplo para dividir um arquivo grande em partes

## Exemplo para dividir um arquivo grande em partes
```
zcat works-2019.jsonl.gz | split -l 2000000 - works-2019-part-
for f in works-2019-part-*; do gzip -9 "$f"; done
```

## Para baixar a base de dados completa (snapshot) do OpenAlex:

```
aws s3 sync "s3://openalex/data/works" "openalex-snapshot" --no-sign-request
```
Isso fará o download de todos os arquivos de trabalhos acadêmicos (works) para o diretório openalex-snapshot.


## Executar o job
```bash
docker compose run --rm spark-submit \
  spark-submit \
  --master ${SPARK_MASTER_URL:-spark://spark-master:7077} \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.sql.files.maxPartitionBytes=268435456 \
  --conf spark.executor.memory=3g \
  --conf spark.executor.cores=3 \
  --conf spark.executor.instances=4 \
  --packages "${OS_CONNECTOR:-org.opensearch.client:opensearch-spark-30_2.12:1.3.0}" \
  /app/works.py
```
