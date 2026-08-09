from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "ashish",
    "retries": 1,
}

with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=default_args,
    description="NYC Taxi Bronze Silver Gold Pipeline",
    start_date=datetime(2026, 8, 5),
    schedule=None,
    catchup=False,
) as dag:

    # -------------------------------------------------
    # Kafka Topic Check / Creation
    # -------------------------------------------------

    kafka_topic_check = BashOperator(
        task_id="kafka_topic_check",
        bash_command="""
        echo "Checking Kafka topic..."

        if docker exec kafka kafka-topics \
            --bootstrap-server kafka:9092 \
            --list | grep -q "^taxi-rides$"; then

            echo "Kafka topic taxi-rides already exists."

        else

            echo "Kafka topic taxi-rides does not exist."
            echo "Creating topic..."

            docker exec kafka kafka-topics \
                --bootstrap-server kafka:9092 \
                --create \
                --topic taxi-rides \
                --partitions 1 \
                --replication-factor 1

            echo "Kafka topic created."

        fi
        """,
    )

    # -------------------------------------------------
    # Bronze
    # -------------------------------------------------

    bronze_task = BashOperator(
        task_id="bronze_load",
        bash_command="""
        docker exec spark-master \
        /opt/spark/bin/spark-submit \
        --master spark://spark-master:7077 \
        --jars /opt/spark/external-jars/postgresql-42.7.3.jar,\
/opt/spark/external-jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,\
/opt/spark/external-jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,\
/opt/spark/external-jars/kafka-clients-3.5.1.jar,\
/opt/spark/external-jars/commons-pool2-2.11.1.jar \
        /opt/spark/jobs/bronze_batch.py
        """,
    )

    # -------------------------------------------------
    # Silver
    # -------------------------------------------------

    silver_task = BashOperator(
    task_id="silver_load",
    bash_command="""
    docker exec spark-master \
    bash -c 'PYTHONPATH=/opt/spark /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/spark/external-jars/postgresql-42.7.3.jar \
    /opt/spark/jobs/silver_job.py'
    """,

    )

    # -------------------------------------------------
    # Gold
    # -------------------------------------------------

    gold_task = BashOperator(
    task_id="gold_load",
    bash_command="""
    docker exec spark-master \
    bash -c 'PYTHONPATH=/opt/spark /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --jars /opt/spark/external-jars/postgresql-42.7.3.jar \
    /opt/spark/jobs/gold_job.py'
    """,

    )

    # -------------------------------------------------
    # Pipeline Dependency
    # -------------------------------------------------

    kafka_topic_check >> bronze_task >> silver_task >> gold_task
