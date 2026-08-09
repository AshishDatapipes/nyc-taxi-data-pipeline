from pyspark.sql import SparkSession


# -----------------------------------
# Spark Session
# -----------------------------------

spark = (
    SparkSession.builder
    .appName("Taxi Bronze Batch Load")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# -----------------------------------
# Read available messages from Kafka
# -----------------------------------

print("Reading Kafka messages...")

kafka_df = (
    spark.read
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
    .option(
        "endingOffsets",
        "latest"
    )
    .load()
)


# -----------------------------------
# Convert Kafka value to JSON string
# -----------------------------------

bronze_df = (
    kafka_df
    .selectExpr(
        "CAST(value AS STRING) AS json_data"
    )
)


count = bronze_df.count()

print(
    f"Kafka records received: {count}"
)


# -----------------------------------
# Handle no data
# -----------------------------------

if count == 0:

    print(
        "No Kafka data available"
    )

else:

    print(
        "Writing to bronze_taxi..."
    )


    (
        bronze_df.write
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


    print(
        "Bronze batch load completed successfully!"
    )


spark.stop()
