FROM apache/airflow:2.9.0

USER root

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    procps \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.8.txt
