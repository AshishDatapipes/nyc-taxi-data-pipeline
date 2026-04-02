from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="nyc_taxi_bronze_streaming",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["nyc", "taxi", "bronze", "streaming", "spark", "kafka"],
) as dag:

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command="""
        sleep 20 && \
        /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
          /opt/spark/jobs/bronze_ingestion.py
        """
    )

    bronze_ingestion
