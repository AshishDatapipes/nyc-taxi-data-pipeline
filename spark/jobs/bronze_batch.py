from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from utils.metadata import (
    get_last_processed_offset,
    update_bronze_offset
)

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
# Read last processed Kafka offset
# -----------------------------------

last_processed_offset = get_last_processed_offset()

print(
    f"Last processed Kafka offset: {last_processed_offset}"
)

# -----------------------------------
# Determine Kafka starting offset
# -----------------------------------

if last_processed_offset == 0:

    starting_offsets = "earliest"

    print(
        "First Bronze run - reading from earliest available Kafka offset."
    )

else:

    starting_offsets = (
        f'{{"taxi-rides":{{"0":{last_processed_offset}}}}}'
    )

    print(
        f"Reading Kafka from offset {last_processed_offset}"
    )

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
        starting_offsets
    )
    .option(
        "endingOffsets",
        "latest"
    )
    .load()
)

# -----------------------------------
# Count records
# -----------------------------------

count = kafka_df.count()

print(
    f"Kafka records received: {count}"
)

# -----------------------------------
# Handle no data
# -----------------------------------

if count == 0:

    print(
        "No new Kafka data available."
    )

else:

    # -----------------------------------
    # Find maximum Kafka offset
    # -----------------------------------

    max_offset = (
        kafka_df
        .selectExpr(
            "MAX(offset) AS max_offset"
        )
        .collect()[0]["max_offset"]
    )

    print(
        f"Maximum Kafka offset in this batch: {max_offset}"
    )

    # -----------------------------------
    # Convert Kafka value to Bronze format
    # -----------------------------------

    bronze_df = (
        kafka_df
        .selectExpr(
            "CAST(value AS STRING) AS json_data",
            "partition AS kafka_partition",
            "offset AS kafka_offset"
        )
    )

    # -----------------------------------
    # Check for existing Kafka records
    # -----------------------------------

    print(
        "Checking for existing Kafka records..."
    )

    existing_query = f"""
    (
        SELECT
            kafka_partition,
            kafka_offset
        FROM bronze_taxi
        WHERE kafka_offset >= {last_processed_offset}
          AND kafka_offset <= {max_offset}
    ) AS existing_records
    """

    existing_df = (
        spark.read
        .format("jdbc")
        .option(
            "url",
            "jdbc:postgresql://postgres:5432/taxi"
        )
        .option(
            "dbtable",
            existing_query
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
        .load()
    )

    # -----------------------------------
    # Remove already processed records
    # -----------------------------------

    new_bronze_df = (
        bronze_df.alias("kafka")
        .join(
            existing_df.alias("existing"),
            on=[
                col("kafka.kafka_partition")
                == col("existing.kafka_partition"),

                col("kafka.kafka_offset")
                == col("existing.kafka_offset")
            ],
            how="left_anti"
        )
    )

    new_count = new_bronze_df.count()

    print(
        f"New Bronze records after duplicate check: {new_count}"
    )

    # -----------------------------------
    # Write only new records
    # -----------------------------------

    if new_count > 0:

        print(
            "Writing new records to bronze_taxi..."
        )

        (
            new_bronze_df.write
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
            "Bronze database write completed successfully."
        )

    else:

        print(
            "All Kafka records in this batch already exist. "
            "Nothing to write."
        )

    # -----------------------------------
    # Update Kafka offset
    # -----------------------------------

    next_offset = max_offset + 1

    update_bronze_offset(next_offset)

    print(
        f"Bronze metadata updated. Next Kafka offset: {next_offset}"
    )

    print(
        "Bronze batch load completed successfully!"
    )

# -----------------------------------
# Stop Spark
# -----------------------------------

spark.stop()
