# End-to-End Weather Analytics Pipeline (Medallion Architecture)

An automated, event-driven data engineering pipeline that ingests real-time weather metrics, streams them through a message broker, runs incremental multi-layer transformations, and serves a consolidated analytical reporting dashboard.

This project implements a fully containerized **Medallion Architecture** (Bronze ➔ Silver ➔ Gold Data Lake layers) engineered for scalability, idempotency, and high-performance querying.

---

## 🛠️ Tech Stack & Core Technologies

- **Orchestration**: Apache Airflow (Configured in an optimized standalone execution environment inside Docker)
- **Ingestion & Streaming**: OpenWeather API, Apache Kafka
- **Processing Engines**: Apache Spark (Structured Streaming for incremental writes + Spark SQL Batch for analytics)
- **Storage Layer**: Local Data Lake using compressed Apache Parquet formats
- **Data Visualization**: Power BI Desktop (Connected directly to partitioned analytical gold layer outputs)
- **Containerization**: Docker & Docker Compose

---

## 📐 Data Flow & Architecture

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

- **Bronze (Raw Ingestion)**: Weather JSON metrics for 10 major US cities are retrieved sequentially, adhering to API rate limits, and streamed directly into Kafka brokers.
- **Silver (Cleaned & Incremental)**: PySpark consumes the streaming source incrementally using checkpoints (`availableNow=True`). The JSON payloads are explicitly parsed, cast to a structured schema, and appended to the Silver Parquet folder.
- **Gold (Analytical)**: A batch Spark process groups the historical Silver data by city and date, calculating daily averages, maximums, and minimums. The final state is rewritten safely utilizing partition pruning.

---

## 🚀 Technical Challenges & Production Solutions

Building an integrated distributed system locally introduces real-world infrastructure and data configuration friction. Below is a log of the main production roadblocks encountered and engineered during development:

### 1. Docker Isolation & Kafka Advertised Listeners

- **Issue**: The Spark script running inside the Airflow container crashed with a `TimeoutException` trying to connect to `localhost:9092`.
- **Cause**: Inside a container, `localhost` maps to its own isolated workspace rather than the host machine hosting the Kafka broker.
- **Solution**: Configured a dual-listener network setup in `docker-compose.yml`. An `INTERNAL` listener was mapped to `kafka:29092` for secure inter-container communication (Airflow to Kafka), while an `EXTERNAL` port was kept at `localhost:9092` for local machine tasks and Power BI consumption.

### 2. Spark Parquet Schema Inference Failures

- **Issue**: During initial runs or directory clears, the Gold layer batch script aborted with an `AnalysisException: Unable to infer schema for Parquet`.
- **Cause**: Spark's default `.read.parquet()` triggers an automatic scan to infer column names. If the directory is pristine or holds partial 0-byte operational metadata files, inference fails instantly.
- **Solution**: Enforced an explicit strict schema design using PySpark's `StructType` and `StructField`. Applying `.schema(data_schema).parquet(path)` bypassed inference completely, increasing performance and protecting the DAG execution line.
- 
### 3. Windows Native Spark Execution and Missing Hadoop Binaries (`winutils.exe`)
* **Issue**: Local testing of PySpark scripts natively on Windows caused an immediate crash during file write operations, throwing a Java runtime exception: `java.io.IOException: Cannot run program "winutils.exe": CreateProcess error=2`.
* **Cause**: Apache Spark was natively built to run on Linux environments and inherently relies on Hadoop client libraries to handle low-level disk tasks and file permissions (`chmod`). When running Spark directly on Windows, the underlying Java virtual machine requires a specialized Windows native translation layout—specifically `winutils.exe` and `hadoop.dll`—to execute these system access routines. Without them, Spark panics and aborts execution.
* **Solution**: The required lightweight Windows binary files were extracted from the public [cdarlint/winutils](https://github.com/cdarlint/winutils) repository and safely contained inside a localized `/hadoop/bin` directory within the workspace. Then, a bootstrap sequence using Python's `platform` library was injected at the script's entry point. It intercepts execution to dynamically set `HADOOP_HOME` and update the runtime environment variables **only** when a Windows host is detected.

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
