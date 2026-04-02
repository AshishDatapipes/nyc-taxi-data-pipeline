FROM apache/airflow:2.9.0

USER root

ENV PIP_USER=false

# System deps
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    ca-certificates \
    procps \
    openjdk-17-jdk \
    && update-ca-certificates

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3

RUN curl -L --retry 5 https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -o spark.tgz && \
    tar -xvzf spark.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark.tgz

ENV PATH="/opt/spark/bin:${PATH}"

# ✅ Switch user BEFORE pip
USER airflow

# ✅ Match Airflow constraints EXACTLY
RUN pip install apache-airflow-providers-apache-spark==4.7.1 \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.0/constraints-3.10.txt"
