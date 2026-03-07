# Project 1: Real-Time E-Commerce Order Analytics Pipeline

## Overview

Build an end-to-end data engineering pipeline US traffic accidents analytics from 2016-2023. that ingest accident events processes them using PySpark, and orchestrates batch 
 and streaming workflows using Apache Airflow

---

## Traffic Scenario

A safety-aware individual wants to know:

1. **Stream** traffic accident events (severity, start time, timezone, weather, temperature, visivility, traffic signal, junction, roundabout, stop, traffic_singal, sunrise, sunset, railway, bump, precipitation) in real time.
2. **Process** the raw events to compute average temperature,  .
3. **Persist** both raw and transformed data to storage (local filesystem or S3).
4. **Orchestrate** the batch and streaming jobs on a daily schedule with retry and alerting.

---

## Architecture

```
┌──────────────┐       ┌─────────────┐       ┌─────────────────────┐
│  Accident    |       |             |
│  Event       │       │             |       │  PySpark Streaming    │
│  Simulator   │──────▶│   Kafka     │──────▶│  Consumer / ETL      │
│  (Producer)  │       │  (Topic:    │       │  (Spark Structured    │
│              │       │  Accidents) │       │   Streaming)          │
└──────────────┘       └─────────────┘       └──────────┬────────────┘
                                                        │
                                                        ▼
                                              ┌─────────────────────┐
                                              │ Raw Accident Layer  │
                                              │  (Parquet / JSON)   │
                                              └──────────┬──────────┘
                                                         │
                                                         ▼
                                              ┌─────────────────────┐
                                              │PySpark Analytics ETL│
                                              │  (RDD + SQL)        │
                                              └──────────┬──────────┘
                                                         │
                                                         ▼
                                              ┌─────────────────────┐
                                              │  Curated Analytics  │
                                              │  Traffic Insights   │
                                              └──────────┬──────────┘
                                                         │
                                                         ▼
                                              ┌─────────────────────┐
                                              │  Airflow DAG        │
                                              │  (Orchestration)    │
                                              └─────────────────────┘
```

---

## Tech Stack

| Technology     | Purpose                                      | Curriculum Week |
|----------------|----------------------------------------------|:---------------:|
| PySpark (RDDs) | Low-level data processing & custom transforms| Week 1          |
| PySpark (SQL)  | DataFrame operations, aggregations, joins    | Week 2          |
| Apache Kafka   | Real-time event ingestion (producer/consumer)| Week 3          |
| Spark Streaming| Consuming Kafka topics in near real-time     | Week 3          |
| Apache Airflow | DAG-based job orchestration & scheduling     | Week 4          |

---

## Detailed Requirements

### Module 1 — Kafka Producer (Week 3)

**Goal:** Simulate traffic accident reports.

- Create a Kafka topic named `traffic_accidents`.
- Write a Python Kafka producer (`producer.py`) that generates JSON order events:
  ```json
  {
    "accident_id": "ORD-10042",
    "severity": "3",
    "City": "Norfolk",
    "State": "Va",
    "start_time": "2026-02-19T10:32:00Z",
    "temperature": 41.2,
    "visibility": 2.5,
    "weather_condition": "fog",
    "traffic_signal": "true",
    "junction": "true",
    "distance_mi": 0.7
  }
  ```
- Generate 500 randomized events
- use sampled Kaggle data
- Simulate realistic weather + severity distributions 

---

### Module 2 — Spark Streaming Consumer (Week 3)

**Goal:** Consume accident stream and persist raw data.

- Write a PySpark Structured Streaming job (`stream_consumer.py`).
- Read accident event messages produced by the US accidents dataset  
- Deserialize JSON messages into a Spark DataFrame.
- Write the raw data to a **Parquet** sink partitioned by `date` (derived from `timestamp`).
- Implement a 1-minute micro-batch trigger.

---

### Module 3 — Batch ETL with PySpark (Weeks 1 & 2)

**Goal:** Transform raw data into analytics-ready datasets.

#### 3A — RDD-Based Processing (Week 1)

- Load the raw Parquet data as an RDD.
- Use RDD transformations (`map`, `filter`, `reduceByKey`) to:
  - Filter out null rows `Weather_Condition`.
  - Validate quantitative column values make sense.
  - Clean:
    - Trim `ID`
    - Standardize dates and timestamps
- Save the result as a text file.

#### 3B — DataFrame / Spark SQL Processing (Week 2)

- Load the raw Parquet data into a Spark DataFrame.
- Perform the following transformations:
  1. **Hour of days Summary** — Group by hour of day, compute `total_accidents`, `avg_duration_mintues`, `avg_serverity`.
  2. **Top 10 Weather Conditions** — Identify Weather conditions with the highest accident concentrations using Spark SQL window functions.
  3. **Weather Severity Impact** — Join orders with a static `weather.csv` reference dataset to enrich weather condition, then aggregate severity statistics per weather conditions.
  4. **Weather Conditions Breakdown** — Under stand accident distribution across enviromental conditions 
- Write each output to Parquet, partitioned and bucketed where appropriate.
- Use **caching** on the base DataFrame to speed up multiple downstream transformations.

---

### Module 4 — Airflow Orchestration (Week 4)

**Goal:** Schedule and manage the full pipeline.

- Create an Airflow DAG named `accident_pipeline` in a file called `accident_dag.py`.
- Define the following tasks with proper dependencies:

  ```
  start >> check_kafka_topic >> run_streaming_job >> wait_for_raw_data
        >> run_rdd_etl >> run_df_etl >> validate_output >> end
  ```

- **Task details:**

  | Task                 | Operator Type       | Description                                      |
  |----------------------|---------------------|--------------------------------------------------|
  | `start`              | DummyOperator       | Pipeline entry point                             |
  | `check_kafka_topic`  | PythonOperator      | Verify the Kafka topic exists and has messages   |
  | `run_streaming_job`  | BashOperator        | Submit the Spark Streaming job via `spark-submit` |
  | `wait_for_raw_data`  | FileSensor          | Wait until raw Parquet files appear              |
  | `run_rdd_etl`        | BashOperator        | Submit the RDD batch job                         |
  | `run_df_etl`         | BashOperator        | Submit the DataFrame batch job                   |
  | `validate_output`    | PythonOperator      | Check row counts & schema of output files        |
  | `end`                | DummyOperator       | Pipeline exit point                              |

- Configure:
  - `schedule_interval`: `@daily`
  - `retries`: 2, `retry_delay`: 5 minutes
  - `email_on_failure`: `true`
  - Use **Connections** for Kafka broker and Spark cluster settings.
  - Create at least one **parameterized DAG** that accepts `execution_date` as a parameter.

---

## Deliverables

| #  | Deliverable                        | Format              |
|----|------------------------------------|----------------------|
| 1  | `producer.py`                      | Python script        |
| 2  | `stream_consumer.py`               | PySpark script       |
| 3  | `batch_rdd_etl.py`                 | PySpark script       |
| 4  | `batch_df_etl.py`                  | PySpark script       |
| 5  | `accidents_dag.py`                 | Airflow DAG          |
| 6  | `regions.csv`                      | Reference data       |
| 7  | `README.md`                        | Setup & run guide    |
| 8  | Sample output screenshots          | PNG / Markdown       |

---

## Folder Structure

```
project1/
├── README.md
├── data/
│   ├── regions.csv
│   ├── raw/                  # Raw Parquet output from streaming
│   └── transformed/          # Aggregated Parquet output from batch ETL
├── kafka/
│   └── producer.py
├── spark/
│   ├── stream_consumer.py
│   ├── batch_rdd_etl.py
│   └── batch_df_etl.py
├── airflow/
│   └── dags/
│       └── accidents_dag.py
└── config/
    └── spark-defaults.conf
```

---

## Evaluation Criteria

| Area                     | Weight | What We Look For                                              |
|--------------------------|:------:|---------------------------------------------------------------|
| Kafka Integration        | 20%    | Proper topic setup, message schema, producer reliability      |
| Spark Streaming          | 15%    | Correct consumption, deserialization, partitioned Parquet sink |
| RDD Processing           | 15%    | Use of transformations, key-value RDDs, accumulators          |
| DataFrame / Spark SQL    | 20%    | Aggregations, joins, window functions, caching, bucketing     |
| Airflow DAG              | 20%    | Task dependencies, operator usage, parameterization, retries  |
| Code Quality & Docs      | 10%    | Clean code, README, inline comments, reproducibility          |

---

## Stretch Goals (Optional)

- Deploy the Spark jobs on an **AWS EMR** cluster (Week 1 - Friday).
- Use **Spark accumulators** to track bad/malformed records during RDD processing.
- Add a second Kafka topic (`order_updates`) for status changes and join both streams.
- Implement **dynamic DAGs** in Airflow that auto-generate tasks based on a config file.
- Add data quality checks using assertions in the `validate_output` task.