# Real-Time Credit Card Transaction Pipeline

> A production-grade streaming data pipeline for credit card fraud detection — built on **Apache Kafka**, **Apache Spark Structured Streaming**, **Apache Hadoop (HDFS)**, **Apache Airflow**, and **Microsoft Power BI**.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Pipeline Components](#pipeline-components)
- [Data Schema](#data-schema)
- [Analytics Pushed to Power BI](#analytics-pushed-to-power-bi)
- [Setup and Running](#setup-and-running)
- [Notes](#notes)

---

## Overview

This project implements an **end-to-end real-time data pipeline** that ingests credit card transaction records, streams them through Apache Kafka, processes them with Apache Spark Structured Streaming, stores the results in HDFS, and finally pushes analytical summaries to a **live Power BI dashboard** on a daily automated schedule via Apache Airflow.

Key capabilities include:

- **Real-time ingestion** — CSV records streamed row-by-row with simulated 1–3 second delays
- **Stream processing** — Spark parses, transforms, and enriches every micro-batch
- **Distributed storage** — Processed records persisted to HDFS as partitioned CSV files
- **Live dashboards** — Five separate Power BI streaming datasets updated daily
- **Fraud analytics** — Automatic detection of high-fraud cities/merchants, short-gap transactions, and abnormal user behaviour

---

## Architecture

```
credit_card.csv
      |
      v
 +-----------+       Kafka Topic         +--------------------------+
 | producer  | ---- "transactions" ----> |  Spark Structured Stream |
 |    .py    |                           |      (consumer.py)        |
 +-----------+                           +------------+-------------+
                                                      |  foreachBatch
                                                      v
                                           +----------------------+
                                           |        HDFS          |
                                           |  /user/vivian/       |
                                           |  credit_card_all/    |
                                           +----------+-----------+
                                                      |
                                           +----------v-----------+
                                           |   Apache Airflow     |
                                           |  (airflow.py DAG)    |
                                           |   @daily schedule    |
                                           +----------+-----------+
                                                      |  merge CSV -> run powerbi.py
                                                      v
                                           +----------------------+
                                           |      Power BI        |
                                           |  Streaming Datasets  |
                                           |    (5 dashboards)    |
                                           +----------------------+
```

**Pipeline summary:**

```
Producer -> Kafka -> Spark Streaming -> HDFS -> Airflow -> Power BI Dashboard
```

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Message Broker | Apache Kafka | 3.x |
| Stream Processing | Apache Spark (PySpark) | 3.5.5 |
| Distributed Storage | Apache Hadoop HDFS | 3.x |
| Orchestration | Apache Airflow | 2.x |
| Analytics Dashboard | Microsoft Power BI | Web Streaming API |
| Language | Python | 3.x |
| Runtime | WSL (Ubuntu) on Windows | — |

---

## Project Structure

```
odap/
├── producer.py          # Kafka producer — reads CSV, publishes rows to Kafka
├── consumer.py          # Spark Structured Streaming consumer — Kafka -> HDFS
├── powerbi.py           # Analytics aggregator & Power BI push script
├── airflow.py           # Airflow DAG — orchestrates daily merge + Power BI push
├── credit_card.csv      # Source dataset (credit card transactions)
└── README.md            # This file
```

---

## Pipeline Components

### 1. producer.py — Kafka Producer

Reads `credit_card.csv` row by row and publishes each record as a plain-text CSV string to the Kafka topic `transactions`.

- **Topic:** `transactions`
- **Broker:** `localhost:9092`
- **Delay:** Random 1–3 seconds between messages to simulate a real-time feed
- **Serialization:** UTF-8 string values

```python
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = 'transactions'
CSV_FILE_PATH = '/home/vivian/project/kafka-code/credit_card.csv'
```

---

### 2. consumer.py — Spark Structured Streaming Consumer

Consumes the `transactions` Kafka topic, parses and enriches each CSV record, and writes processed micro-batches to HDFS.

**Processing steps:**
1. Read stream from Kafka (binary -> string)
2. Split raw CSV line into 15 typed columns
3. Parse `Amount` field (e.g. `"$134.09"` -> `134.09` float)
4. Combine separate `Day/Month/Year/Time` columns into a single `datetime` timestamp
5. Extract `hour` from datetime for time-of-day analytics
6. Write each micro-batch to HDFS in append mode (no per-file header)

**Output location:** `hdfs://localhost:9000/user/vivian/credit_card_all/`  
**Checkpoint:** `/home/vivian/checkpoint/consumer_no_header`

```bash
spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  /home/vivian/project/kafka-code/consumer.py
```

---

### 3. powerbi.py — Analytics & Power BI Push Script

Reads the merged HDFS CSV, computes five analytical aggregations, and pushes new rows to separate Power BI Streaming Datasets via the REST API.

**Analytics computed:**

| Dataset | Description |
|---|---|
| **Transactions** | Raw transaction records (incremental push, tracks last pushed row count) |
| **Fraud Summary** | Fraud rate grouped by merchant city and merchant name |
| **Short Time Gaps** | Users with 3 or more consecutive transactions within 5-minute windows |
| **Weekday vs Weekend** | Transaction volume and total amount grouped by hour and day type |
| **Abnormal Users** | Users with above-average error counts or above-average fraud occurrences |

**Incremental push:** Tracks previously pushed row count in `/home/vivian/airflow/last_row_count.txt` — only new rows are sent on each run, preventing duplicates.

---

### 4. airflow.py — Airflow DAG

Defines the `merge_and_powerbi_dag` DAG that runs **daily** and orchestrates three sequential tasks:

```
delete_merged_csv_task  ->  merge_csv_task  ->  run_powerbi_task
```

| Task | Type | Description |
|---|---|---|
| `delete_merged_csv_task` | PythonOperator | Removes the old merged CSV from HDFS (`hadoop fs -rm -f`) |
| `merge_csv_task` | PythonOperator | Concatenates all partitioned HDFS files into a single merged CSV |
| `run_powerbi_task` | BashOperator | Executes `powerbi.py` to push incremental data to all Power BI datasets |

---

## Data Schema

### Raw CSV columns (from credit_card.csv)

| Column | Type | Description |
|---|---|---|
| `User` | Integer | User identifier |
| `Card` | Integer | Card identifier |
| `Year` | Integer | Transaction year |
| `Month` | Integer | Transaction month |
| `Day` | Integer | Transaction day |
| `Time` | String | Transaction time (H:MM or H:MM:SS) |
| `Amount` | String | Transaction amount (e.g. `$134.09`) |
| `Use Chip` | String | Transaction method |
| `Merchant Name` | String | Name of merchant |
| `Merchant City` | String | City of merchant |
| `Merchant State` | String | State of merchant |
| `Zip` | String | ZIP code |
| `MCC` | String | Merchant Category Code |
| `Errors?` | String | Transaction errors |
| `Is Fraud?` | String | Fraud label (`Yes` / `No`) |

### Processed output columns (written to HDFS)

| Column | Type | Description |
|---|---|---|
| `user_id` | Integer | User identifier |
| `card_id` | Integer | Card identifier |
| `datetime` | Timestamp | Combined datetime from Day/Month/Year/Time |
| `hour` | Integer | Hour of transaction (0–23) |
| `amount_usd` | Double | Parsed USD amount (float) |
| `merchant_name` | String | Merchant name |
| `merchant_city` | String | Merchant city |
| `merchant_state` | String | Merchant state |
| `zip` | String | ZIP code |
| `mcc` | String | Merchant category code |
| `year` | Integer | Year |
| `month` | Integer | Month |
| `day` | Integer | Day |
| `errors` | String | Error flags |
| `is_fraud` | String | Fraud label |

---

## Analytics Pushed to Power BI

| Power BI Dataset | Fields Pushed |
|---|---|
| Transactions | `datetime`, `hour`, `user_id`, `amount_usd`, `merchant_name`, `merchant_city`, `is_fraud`, `errors` |
| Fraud Summary | `type`, `merchant_name`, `merchant_city`, `total_transactions`, `total_fraud`, `fraud_rate` |
| Short Time Gaps | `user_id`, `short_gap_count` |
| Weekday vs Weekend | `is_weekend`, `hour`, `total_transactions`, `total_amount` |
| Abnormal Users | `user_id`, `total_errors`, `total_fraud` |

---

## Setup and Running

### Prerequisites

- WSL (Ubuntu) with Java 8+
- Apache Kafka installed at `~/kafka`
- Apache Hadoop 3.x installed and configured
- Apache Spark 3.5.5 installed
- Apache Airflow 2.x installed
- Python 3.x with packages: `kafka-python`, `pyspark`, `pandas`, `numpy`, `pyarrow`, `fsspec`, `hdfs`, `requests`
- Microsoft Power BI account with Streaming Datasets configured

---

### Step 1 — Start Zookeeper & Kafka Broker

```bash
# Terminal 1: Start Zookeeper
cd ~/kafka
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2: Start Kafka Broker
cd ~/kafka
bin/kafka-server-start.sh config/server.properties
```

---

### Step 2 — Start HDFS & YARN

```bash
# First-time only: format the NameNode
hdfs namenode -format

# Start HDFS and YARN
start-dfs.sh
start-yarn.sh

# Verify services are running
jps
```

> **HDFS Web UI:** http://localhost:9870/

---

### Step 3 — Run the Kafka Producer

```bash
cd ~/project/kafka-code
python3 producer.py
```

The producer reads `credit_card.csv` and publishes each row to the `transactions` topic with a random 1–3 second delay between messages.

---

### Step 4 — Run the Spark Streaming Consumer

```bash
# Activate the PySpark virtual environment
cd ~/pyspark-env
source bin/activate

# Submit the Spark job
spark-submit \
  --master local[*] \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  /home/vivian/project/kafka-code/consumer.py
```

Spark reads from Kafka, transforms data, and appends processed records to HDFS at `/user/vivian/credit_card_all/`.

---

### Step 5 — (Optional) Clean Up HDFS Files

```bash
# Remove the merged file
hadoop fs -rm /user/vivian/credit_card_all_merged.csv

# Remove all partitioned batch files
hadoop fs -rm /user/vivian/credit_card_all/*
```

---

### Step 6 — Run the Power BI Push Script (Manual)

```bash
# Install required packages if not already installed
pip install pyarrow fsspec hdfs requests

# Run the script
cd ~/project/kafka-code
python3 powerbi.py
```

The script reads the merged CSV from HDFS, computes analytics, and pushes new records to all configured Power BI Streaming Datasets.

---

### Step 7 — Start Apache Airflow (Automated Daily Scheduling)

```bash
# Terminal 1: Start the scheduler
airflow scheduler

# Terminal 2: Start the web server
airflow webserver
```

> **Airflow Web UI:** http://localhost:8080/

**DAG:** `merge_and_powerbi_dag` — runs daily and automatically:
1. Deletes the old merged CSV from HDFS
2. Merges all batch partition files into a single CSV
3. Executes `powerbi.py` to push incremental data to Power BI

---

## Notes

- The Spark consumer writes batch files **without headers** to HDFS. Headers are re-applied when reading the merged file in `powerbi.py`.
- `powerbi.py` uses **incremental pushing** — it tracks the last pushed row count and only sends new records on each run, avoiding duplicates.
- Airflow's `catchup=False` setting ensures that only the most recent daily run executes when the scheduler first starts.
- Power BI Streaming Dataset API keys are currently hardcoded in `powerbi.py`. For production use, move them to environment variables or a secrets manager.

