# NYC Taxi Data Engineering Pipeline

An end-to-end data engineering pipeline built using **Apache Kafka, Apache Spark, Apache Airflow, PostgreSQL, Docker, and Python**.

The project processes NYC Yellow Taxi trip data through a **Bronze → Silver → Gold** architecture and demonstrates incremental processing, Kafka offset tracking, batch processing, idempotency, orchestration, and failure/recovery concepts.

## Architecture

```text
NYC Taxi Dataset
       │
       ▼
Kafka Producer
       │
       ▼
Apache Kafka
       │
       ▼
Bronze Layer
Raw Kafka Records
       │
       ▼
Silver Layer
Structured / Transformed Data
       │
       ▼
Gold Layer
Daily Business Aggregates
       │
       ▼
PostgreSQL
```

Apache Airflow orchestrates the processing pipeline:

```text
Kafka Topic Check
       │
       ▼
Bronze Load
       │
       ▼
Silver Load
       │
       ▼
Gold Load
```

The entire environment is containerized using Docker and runs locally through WSL/Ubuntu.

## Technology Stack

| Technology     | Purpose                                    |
| -------------- | ------------------------------------------ |
| Python         | Kafka producer and Spark processing logic  |
| Apache Kafka   | Event ingestion and message transport      |
| Apache Spark   | Data processing and transformation         |
| Apache Airflow | Pipeline orchestration and scheduling      |
| PostgreSQL     | Bronze, Silver, Gold, and metadata storage |
| Docker         | Containerization and service management    |
| WSL / Ubuntu   | Local Linux development environment        |
| Git / GitHub   | Version control and project management     |

## Data Processing Layers

### Bronze Layer

The Bronze layer captures raw records consumed from Kafka with minimal transformation.

The Bronze Spark job:

* Reads Kafka records incrementally.
* Tracks the last processed Kafka offset.
* Reads only the available Kafka range.
* Stores Kafka partition and offset alongside the raw JSON payload.
* Checks PostgreSQL for previously processed Kafka records.
* Uses a `left_anti` join to prevent duplicate records.
* Writes new records to `bronze_taxi`.
* Updates the Bronze Kafka checkpoint only after the database write succeeds.

The Bronze checkpoint is maintained in the `pipeline_metadata` table.

```text
Kafka
  │
  │ partition + offset
  ▼
Bronze Spark Job
  │
  ├── Duplicate Check
  │
  ▼
bronze_taxi
  │
  ▼
pipeline_metadata
```

The current Kafka topic uses one partition. The current checkpoint implementation therefore tracks the Bronze offset for partition `0`.

### Silver Layer

The Silver layer converts the raw JSON records into structured taxi data.

Silver processing is incremental and uses the Bronze PostgreSQL ID as its checkpoint.

The job:

* Reads Bronze records where `id > last_processed_id`.
* Processes records in batches of 50,000.
* Parses JSON using an explicit Spark schema.
* Converts timestamp fields.
* Renames fields into the Silver schema.
* Writes processed records to `silver_taxi`.
* Updates the Silver checkpoint after a successful write.

```text
Bronze
  │
  ▼
Incremental Bronze IDs
  │
  ▼
Batch Processing
  │
  ▼
JSON Parsing
  │
  ▼
Structured Taxi Data
  │
  ▼
silver_taxi
```

### Gold Layer

The Gold layer contains business-ready daily taxi aggregates.

Gold processing:

* Reads new Silver records incrementally.
* Processes Silver data in batches of 50,000.
* Identifies the affected trip dates.
* Recalculates complete daily aggregates for those dates.
* Calculates total trips, revenue, average fare, average trip distance, average tip, and total passengers.
* Uses PostgreSQL `UPSERT` logic based on `trip_date`.
* Updates the Gold checkpoint after the Gold write succeeds.

Example Gold metrics:

```text
trip_date
total_trips
total_revenue
average_fare
average_trip_distance
average_tip
total_passengers
```

The UPSERT strategy allows an existing daily aggregate to be recalculated safely when additional Silver records for that date arrive.

## Incremental Processing

The pipeline uses different checkpoint mechanisms at each layer:

```text
Kafka
  │
  │ Kafka partition + offset
  ▼
Bronze
  │
  │ Bronze PostgreSQL ID
  ▼
Silver
  │
  │ Silver PostgreSQL ID
  ▼
Gold
```

### Bronze Checkpoint

Bronze stores the next Kafka offset to process.

```text
Kafka offset
     │
     ▼
Bronze write
     │
     ▼
Checkpoint update
```

The checkpoint is updated only after a successful Bronze database write.

### Silver Checkpoint

Silver stores the highest successfully processed Bronze record ID.

```text
Bronze ID
    │
    ▼
Silver batch
    │
    ▼
Silver write
    │
    ▼
Metadata update
```

### Gold Checkpoint

Gold stores the highest successfully processed Silver record ID.

```text
Silver ID
    │
    ▼
Gold batch
    │
    ▼
Gold UPSERT
    │
    ▼
Metadata update
```

This allows the pipeline to continue from its previous processing position rather than reprocessing the entire dataset on every run.

## Idempotency and Failure Recovery

The pipeline contains several mechanisms to reduce duplicate processing.

### Bronze

Bronze records contain:

```text
kafka_partition
kafka_offset
```

Before writing new records, the job checks PostgreSQL for existing partition/offset combinations.

This protects against duplicate inserts if a job succeeds in writing data but fails before updating its checkpoint.

### Gold

Gold uses:

```sql
ON CONFLICT (trip_date)
DO UPDATE
```

This means daily aggregate records can be recalculated without creating duplicate rows.

### Checkpoint Ordering

The general pattern is:

```text
Read data
    │
    ▼
Process data
    │
    ▼
Write data
    │
    ▼
Update checkpoint
```

The checkpoint is intentionally updated after the corresponding data write.

This prevents the pipeline from advancing its checkpoint before the data has actually been persisted.

## Airflow Orchestration

The main Airflow DAG is:

```text
airflow/dags/taxi_pipeline_dag.py
```

The DAG coordinates:

```text
Check / Create Kafka Topic
          │
          ▼
      Bronze Load
          │
          ▼
      Silver Load
          │
          ▼
       Gold Load
```

The current DAG configuration is:

```text
Schedule:        */5 * * * *
Catchup:         False
Max active runs: 1
Retries:         1
```

Therefore, Airflow checks for a new scheduled run every five minutes while preventing overlapping pipeline runs.

The DAG is currently paused when the local environment is not being used.

The Kafka producer is currently run separately from the Airflow processing DAG.

## Project Structure

```text
nyc-taxi-streaming-pipeline/
│
├── airflow/
│   └── dags/
│       └── taxi_pipeline_dag.py
│
├── docker/
│   └── spark/
│       ├── Dockerfile
│       └── jars/
│
├── jars/
│   ├── kafka-clients-3.5.1.jar
│   ├── postgresql-42.7.3.jar
│   ├── spark-catalyst_2.12-3.5.0.jar
│   ├── spark-sql-kafka-0-10_2.12-3.5.0.jar
│   └── spark-token-provider-kafka-0-10_2.12-3.5.0.jar
│
├── producer/
│   ├── download_dataset.py
│   └── kafka_producer.py
│
├── spark/
│   ├── config/
│   │   └── db_config.py
│   │
│   ├── jobs/
│   │   ├── bronze_batch.py
│   │   ├── bronze_streaming.py
│   │   ├── silver_job.py
│   │   └── gold_job.py
│   │
│   └── utils/
│       ├── jdbc.py
│       └── metadata.py
│
├── docker-compose.yml
├── dockerfile
├── .gitignore
└── README.md
```

## Directory Overview

| Directory / File     | Purpose                                            |
| -------------------- | -------------------------------------------------- |
| `airflow/`           | Airflow DAGs and orchestration                     |
| `producer/`          | Dataset download and Kafka producer logic          |
| `spark/jobs/`        | Bronze, Silver, and Gold processing jobs           |
| `spark/config/`      | Database and application configuration             |
| `spark/utils/`       | Reusable Spark/JDBC and metadata utilities         |
| `docker/`            | Spark Docker configuration and dependencies        |
| `jars/`              | Required Spark, Kafka, and PostgreSQL dependencies |
| `docker-compose.yml` | Defines and manages project services               |
| `.gitignore`         | Prevents local/runtime files from being committed  |

## Running the Project

### Prerequisites

Make sure the following are installed:

* WSL2 with Ubuntu
* Docker Desktop with WSL integration enabled
* Python 3
* Git

### Clone the Repository

```bash
git clone https://github.com/AshishDatapipes/nyc-taxi-data-pipeline.git
cd nyc-taxi-data-pipeline
```

### Start the Docker Environment

```bash
docker compose up -d
```

Check the running containers:

```bash
docker ps
```

### Start the Kafka Producer

The current producer reads the January 2024 NYC Taxi dataset and sends a test batch of records to Kafka.

```bash
python producer/kafka_producer.py
```

The current producer configuration sends up to 1,000 records with a one-second delay between records.

### Open Airflow

Open the Airflow web interface and locate:

```text
nyc_taxi_pipeline
```

The DAG can be unpaused and allowed to run according to its five-minute schedule.

### Monitor the Pipeline

The pipeline can be monitored through:

* **Airflow UI** — DAG and task status
* **Spark UI** — Spark job execution
* **Kafka** — topic and message flow
* **PostgreSQL** — Bronze, Silver, Gold, and metadata tables

### Stop the Environment

When finished:

```bash
docker compose down
```

The DAG should also be paused when scheduled execution is not required.

## Database Layers

The PostgreSQL database contains the main processing layers:

```text
bronze_taxi
      │
      ▼
silver_taxi
      │
      ▼
gold_daily_summary
```

Pipeline checkpoints are maintained separately in:

```text
pipeline_metadata
```

The metadata table tracks processing progress for the Bronze, Silver, and Gold stages.

## Challenges and Learnings

Building the pipeline involved several practical engineering challenges.

### Spark and Airflow Compatibility

The Spark jobs were not initially executing correctly through Airflow because of compatibility and environment issues.

The environment was rebuilt and the Spark/Airflow setup was aligned so that Airflow could submit Spark jobs successfully.

### Kafka and Spark Integration

The pipeline required the correct Spark-Kafka connector dependencies and Kafka client libraries to allow Spark to consume messages from Kafka.

This reinforced the importance of dependency and version compatibility when integrating distributed systems.

### JDBC Integration

Spark communicates with PostgreSQL through JDBC.

The PostgreSQL JDBC driver had to be correctly available inside the Spark environment.

### Docker Networking

The services communicate through the Docker network using container service names.

For example:

```text
kafka:9092
postgres:5432
spark-master:7077
```

Host-side applications such as the Kafka producer use the host-mapped Kafka port.

### Incremental Processing

A major part of the project was understanding how to prevent every scheduled run from processing the entire dataset again.

The pipeline therefore introduced:

* Kafka offset tracking
* Bronze duplicate detection
* Silver incremental IDs
* Gold incremental IDs
* Batch processing
* Metadata checkpoints
* Gold UPSERT logic

### Failure and Recovery

The project also explores what happens when a processing step fails.

A key design principle is:

```text
Process
   ↓
Write
   ↓
Checkpoint
```

rather than advancing the checkpoint before the data has been successfully persisted.

This provides a foundation for retry and recovery behavior.

## Future Improvements

The current project is functional in a local Docker environment. Planned improvements include:

* Add a `requirements.txt` file for reproducible Python dependencies
* Move database credentials and configuration to environment variables
* Improve configuration management across environments
* Add automated data quality checks
* Add unit and integration tests
* Improve structured logging and error handling
* Add monitoring and alerting
* Generalize Kafka checkpointing for multiple partitions
* Improve Gold processing so large Silver tables do not need to be scanned repeatedly
* Add CI/CD using GitHub Actions
* Add dashboards for Gold-layer data
* Deploy the pipeline to a cloud environment

## Project Status

The pipeline is currently functional and has been tested end-to-end in the local Docker environment.

The project demonstrates:

* Kafka-based ingestion
* Spark data processing
* Bronze/Silver/Gold architecture
* Airflow orchestration
* Incremental processing
* Kafka offset tracking
* Batch processing
* PostgreSQL JDBC integration
* Idempotency techniques
* Checkpoint management
* Failure/recovery concepts
* Docker-based development

The project is actively being improved as part of my journey toward a Data Engineer role.

## Author

**Ashish**
