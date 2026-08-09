from pyspark.sql import SparkSession


# -----------------------------------
# Create Spark Session
# -----------------------------------
spark = (
    SparkSession.builder
    .appName("Taxi Bronze Streaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# -----------------------------------
# Read data from Kafka
# -----------------------------------
kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "kafka:9092"
    )
    .option(
        "subscribe",
        "taxi-rides"
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)


# -----------------------------------
# Bronze layer
# Keep raw JSON as string
# -----------------------------------
bronze_df = (
    kafka_df
    .selectExpr(
        "CAST(value AS STRING) AS json_data"
    )
)


# -----------------------------------
# Write each micro batch to PostgreSQL
# -----------------------------------
def write_to_postgres(batch_df, batch_id):

    print(f"Writing batch {batch_id} to PostgreSQL")

    (
        batch_df.write
        .format("jdbc")
        .option(
            "url",
            "jdbc:postgresql://postgres:5432/taxi"
        )
        .option(
            "dbtable",
            "bronze_taxi"
        )
        .option(
            "user",
            "airflow"
        )
        .option(
            "password",
            "airflowpassword"
        )
        .option(
            "driver",
            "org.postgresql.Driver"
        )
        .mode("append")
        .save()
    )


# -----------------------------------
# Start streaming query
# -----------------------------------
query = (
    bronze_df.writeStream
    .foreachBatch(write_to_postgres)
    .option(
        "checkpointLocation",
        "/tmp/checkpoints/bronze_postgres"
    )
    .outputMode("append")
    .start()
)


query.awaitTermination()
