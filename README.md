# End-to-End Weather Analytics Pipeline (Medallion Architecture)

An automated, event-driven data engineering pipeline that ingests real-time weather metrics, streams them through a message broker, runs multi-layer transformations, and serves a consolidated analytical reporting dashboard.

<video src="assets/demo.mp4" controls width="100%" max-width="800px"></video>

## Tech Stack & Core Technologies

- **Orchestration**: Apache Airflow
- **Ingestion & Streaming**: OpenWeather API, Apache Kafka
- **Processing Engines**: Apache Spark (Structured Streaming for incremental writes + Spark SQL Batch for analytics)
- **Storage Layer**: Local Data Lake using compressed Apache Parquet formats
- **Data Visualization**: Power BI Desktop
- **Containerization**: Docker & Docker Compose

---

## Data Flow & Architecture

```text
[ OpenWeather API ]
        │ (hourly requests)
        ▼
[ ingesting.py ] ➔ (Producer) ➔ [ Apache Kafka: weather-raw topic ]
                                              │
       ┌──────────────────────────────────────┘
       ▼ (Incremental Stream read)
[ bronze_to_silver.py ] ➔ (Spark Structured Streaming) ➔ [ Data Lake: Silver Layer (Parquet Append) ]
                                                              │
       ┌──────────────────────────────────────────────────────┘
       ▼ (Batch read & Daily aggregations)
[ silver_to_gold.py ] ➔ (Spark SQL Batch) ➔ [ Data Lake: Gold Layer (Parquet Partitioned Overwrite) ]
                                                              │
                                                              ▼
                                                     [ Power BI Dashboard ]
```

### Data Layer Lifecycle

- **Bronze (Raw Ingestion)**: Weather JSON metrics for 10 major US cities are retrieved, adhering to API rate limits, and streamed directly into Kafka.
- **Silver (Cleaned & Incremental)**: PySpark consumes the streaming source incrementally using checkpoints. The JSON payloads are explicitly parsed, cast to a structured schema, and appended to the Silver Parquet folder.
- **Gold (Analytical)**: A batch Spark process groups the historical Silver data by city and date, calculating daily averages, maximums, and minimums. The final state is rewritten safely utilizing partition pruning.

---

## Technical Challenges & Production Solutions
Below is a log of the main production roadblocks encountered and engineered during development:

### 1. Docker Isolation & Kafka Advertised Listeners

- **Issue**: The Spark script running inside the Airflow container crashed with a `TimeoutException` trying to connect to `localhost:9092`.
- **Cause**: Inside a container, `localhost` maps to its own isolated workspace rather than the host machine hosting the Kafka broker.
- **Solution**: Configured dual-listeners in docker-compose.yml: an INTERNAL listener at kafka:29092 for secure inter-container traffic (Airflow to Kafka) and an EXTERNAL listener at localhost:9092 for local machine tasks and Power BI consumption.

### 2. Spark Parquet Schema Inference Failures

- **Issue**: The Gold layer batch script aborted with an AnalysisException: Unable to infer schema for Parquet during clean runs.
- **Cause**: Spark's default .read.parquet() triggers auto-inference for column names, which fails instantly if the destination directory is pristine or holds only 0-byte operational metadata files.
- **Solution**: Enforced an explicit strict schema design using PySpark's `StructType` and `StructField`. Applying `.schema(data_schema).parquet(path)` bypassed inference completely, increasing performance and protecting the DAG execution line.

### 3. Windows Native Spark Execution and Missing Hadoop Binaries (`winutils.exe`)
* **Issue**: Local testing of PySpark scripts natively on Windows caused a java.io.IOException: Cannot run program "winutils.exe" crash during file writes.
* **Cause**: Spark is built for Linux and expects POSIX-compliant file permissions (chmod). Windows (NTFS) lacks these native endpoints and requires translation tools (winutils.exe and hadoop.dll) to interact with local disk states.
* **Solution**: Staged the minimal required Windows binaries from the public cdarlint/winutils repository in a local project folder. Injected a Python bootstrap script using the platform library to dynamically set HADOOP_HOME and system paths only when a Windows host is detected, maintaining a fully environment-agnostic pipeline.

## 💻 How to Run This Project Locally

### Prerequisites

- Docker & Docker Compose installed.
- An active OpenWeather API Key saved inside a `.env` file in the root folder (`OPENWEATHER_API_KEY=your_key_here`).

### Setup Instructions

1. Clone this repository to your local machine.

2. Compose the entire system in detached mode:

   ```bash
   docker compose up -d
   ```

3. Extract your unique Airflow web UI admin credentials:

   ```bash
   docker exec -it airflow_medallion cat /opt/airflow/standalone_admin_password.txt
   ```

4. Access the Airflow control center at [http://localhost:8080](http://localhost:8080), log in with the user `admin`, and activate the `pipeline_clima_medallion` DAG.

---
