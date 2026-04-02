from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="nyc_taxi_batch_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["nyc", "taxi", "silver", "gold", "batch", "spark"],
) as dag:

    silver_transformation = BashOperator(
        task_id="silver_transformation",
        bash_command="""
        /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          /opt/spark/jobs/silver_transformation.py
        """
    )

    gold_aggregation = BashOperator(
        task_id="gold_aggregation",
        bash_command="""
        /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          /opt/spark/jobs/gold_aggregation.py
        """
    )

    silver_transformation >> gold_aggregation
