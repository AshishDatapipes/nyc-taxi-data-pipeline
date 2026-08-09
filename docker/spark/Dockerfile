FROM apache/spark:3.5.0

USER root

RUN apt-get update && \
    apt-get install -y \
        curl \
        wget \
        procps \
        python3 \
        python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    pandas \
    pyarrow \
    kafka-python

# Create writable Ivy cache
RUN mkdir -p /opt/spark/.ivy2 && \
    chown -R spark:spark /opt/spark/.ivy2

# PostgreSQL JDBC driver only
COPY docker/spark/jars/postgresql-42.7.3.jar /opt/spark/jars/

ENV HOME=/opt/spark
ENV IVY_HOME=/opt/spark/.ivy2
ENV SPARK_LOCAL_DIRS=/tmp

USER spark
