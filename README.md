# NYC Taxi Data Streaming Pipeline

An end-to-end real-time data engineering pipeline for processing NYC taxi trip data using Kafka, Apache Spark, Apache Airflow, PostgreSQL, and Docker.

This project demonstrates a complete streaming data workflow from ingestion to analytics-ready data, using a Bronze → Silver → Gold architecture.

## Architecture

The pipeline follows a Bronze → Silver → Gold architecture:

```text
NYC Taxi Data
      │
      ▼
Kafka Producer
      │
      ▼
Apache Kafka
      │
      ▼
Bronze Layer
      │
      ▼
Silver Layer
      │
      ▼
Gold Layer
      │
      ▼
PostgreSQL
```

Apache Airflow orchestrates the pipeline and coordinates the execution of the Kafka, Bronze, Silver, and Gold tasks.

The entire environment is containerized using Docker and runs locally through WSL/Ubuntu.

## Technology Stack

| Technology                 | Purpose                                        |
| -------------------------- | ---------------------------------------------- |
| Python                     | Data ingestion, Kafka producer, and Spark jobs |
| Apache Kafka               | Streaming data ingestion and message transport |
| Apache Spark               | Data processing and transformation             |
| Spark Structured Streaming | Processing streaming data from Kafka           |
| Apache Airflow             | Pipeline orchestration and task scheduling     |
| PostgreSQL                 | Data storage and analytics layer               |
| Docker                     | Containerization and service management        |
| WSL / Ubuntu               | Local Linux development environment            |
| Git / GitHub               | Version control and project management         |

## Data Processing Layers

### Bronze Layer

The Bronze layer captures the raw data coming from Kafka with minimal transformation.

The Spark job reads messages from the Kafka topic and writes the incoming records to PostgreSQL using JDBC in append mode.

The Bronze layer is intended to preserve the incoming data as close to the source format as practical.

### Silver Layer

The Silver layer processes the Bronze data and applies the required transformations and data cleaning.

This layer converts the raw records into a more structured and usable format for downstream processing.

### Gold Layer

The Gold layer contains the final business-ready data produced from the Silver layer.

The transformed data can then be consumed for analytics, reporting, or other downstream use cases.

### Data Flow

```text
Kafka
  │
  ▼
Bronze
Raw / minimally transformed data
  │
  ▼
Silver
Cleaned and transformed data
  │
  ▼
Gold
Analytics-ready data
  │
  ▼
PostgreSQL
```

## Airflow Orchestration

Apache Airflow is used to orchestrate the pipeline workflow.

The main DAG is:

```text
airflow/dags/taxi_pipeline_dag.py
```

The DAG coordinates the major stages of the pipeline:

```text
Check / Create Kafka Topic
          │
          ▼
   Kafka Data Ingestion
          │
          ▼
      Bronze Load
          │
          ▼
      Silver Transform
          │
          ▼
       Gold Transform
```

The DAG uses Docker commands to execute the required Spark jobs inside the Spark environment.

The current DAG is configured with:

* `catchup=False`
* One retry for failed tasks
* Manual triggering through the Airflow UI
* Task dependencies to ensure the Bronze, Silver, and Gold stages execute in the correct order

This allows the complete data pipeline to be monitored from the Airflow UI.

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

### Directory Overview

| Directory / File     | Purpose                                                            |
| -------------------- | ------------------------------------------------------------------ |
| `airflow/`           | Airflow DAGs and orchestration                                     |
| `producer/`          | Dataset download and Kafka producer logic                          |
| `spark/jobs/`        | Bronze, Silver, and Gold processing jobs                           |
| `spark/config/`      | Database and application configuration                             |
| `spark/utils/`       | Reusable Spark/JDBC utilities                                      |
| `docker/`            | Spark Docker configuration and dependencies                        |
| `jars/`              | Required Spark, Kafka, and PostgreSQL dependencies                 |
| `docker-compose.yml` | Defines and manages the project services                           |
| `.gitignore`         | Prevents runtime files and local environments from being committed |

## Running the Project

### Prerequisites

Make sure the following are installed:

* WSL2 with Ubuntu
* Docker Desktop with WSL integration enabled
* Python 3
* Git

Clone the repository:

```bash
git clone https://github.com/AshishDatapipes/nyc-taxi-data-pipeline.git
cd nyc-taxi-data-pipeline

### Start the Docker Environment

Start the required services:

```bash
docker compose up -d
```

Check the running containers:

```bash
docker ps
```

### Open Airflow

Open the Airflow web interface and trigger the DAG:

```text
taxi_pipeline_dag
```

The DAG coordinates the pipeline execution from Kafka ingestion through the Bronze, Silver, and Gold processing stages.

### Monitor the Pipeline

The pipeline can be monitored through:

* Airflow UI — DAG and task status
* Spark UI — Spark job execution
* Kafka — streaming messages
* PostgreSQL — processed data

### Stop the Environment

When finished:

```bash
docker compose down
```

## Challenges and Learnings

Building the pipeline involved several practical engineering challenges.

### Spark and Airflow Compatibility

During development, the Spark jobs were not initially executing correctly through Airflow because of a Spark version compatibility issue between the Airflow environment and the Spark environment.

The issue was resolved by aligning the Spark environment and rebuilding the Docker setup.

### Kafka and Spark Integration

The pipeline required the correct Spark-Kafka connector dependencies and Kafka client libraries to allow Spark to consume messages from Kafka.

This reinforced the importance of dependency and version compatibility when integrating distributed systems.

### JDBC Integration

Spark writes the processed data to PostgreSQL through JDBC.

The PostgreSQL JDBC driver had to be correctly available inside the Spark environment for the database connection to work.

### Docker Networking

The services communicate through the Docker network using container service names rather than localhost.

Understanding Docker networking was important for connecting Kafka, Spark, PostgreSQL, and Airflow correctly.

### Debugging Distributed Components

A significant part of the project involved checking different components independently and then verifying the complete pipeline through Airflow and Spark.

This reinforced an important lesson:

> A data pipeline is not just a collection of individual tools. The real engineering challenge is making those tools work reliably together.

## Future Improvements

The project is currently running successfully in a local Docker-based environment. Planned improvements include:

* Add a `requirements.txt` file for reproducible Python dependencies
* Move database credentials and other configuration values to environment variables
* Improve configuration management across development and production environments
* Add automated data quality checks
* Add unit and integration tests
* Improve logging and error handling
* Add monitoring and alerting
* Add CI/CD using GitHub Actions
* Deploy the pipeline to a cloud environment
* Add dashboards for the Gold-layer data

## Project Status

The pipeline is currently functional and has been tested end-to-end in the local Docker environment.

The project is actively being improved as part of my journey toward a Junior Data Engineer role.

## Author

**Ashish**

[GitHub](https://github.com/AshishDatapipes)