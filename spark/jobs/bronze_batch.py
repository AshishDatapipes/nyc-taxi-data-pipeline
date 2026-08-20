from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from utils.metadata import (
    get_bronze_metadata,
    update_bronze_metadata
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
# Read Bronze metadata
# -----------------------------------

(
    last_processed_offset,
    kafka_generation
) = get_bronze_metadata()

print(
    f"Last processed Kafka offset: "
    f"{last_processed_offset}"
)

print(
    f"Current Kafka generation: "
    f"{kafka_generation}"
)


# -----------------------------------
# Read Kafka current end offset
# -----------------------------------

print(
    "Checking current Kafka end offset..."
)

kafka_latest_df = (
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

latest_count = kafka_latest_df.count()

print(
    f"Kafka records currently available: "
    f"{latest_count}"
)


# -----------------------------------
# Handle empty Kafka topic
# -----------------------------------

if latest_count == 0:

    print(
        "Kafka topic currently contains no records."
    )

    print(
        "Nothing to process."
    )

    spark.stop()

    raise SystemExit(0)


# -----------------------------------
# Find current Kafka maximum offset
# -----------------------------------

current_max_offset = (
    kafka_latest_df
    .selectExpr(
        "MAX(offset) AS max_offset"
    )
    .collect()[0]["max_offset"]
)

print(
    f"Current Kafka maximum offset: "
    f"{current_max_offset}"
)


# -----------------------------------
# Calculate Kafka end offset
# -----------------------------------
#
# Kafka record offsets are zero-based.
#
# Example:
#
# records:
#   0 ... 999
#
# maximum offset:
#   999
#
# next/end offset:
#   1000
#
# The metadata checkpoint stores this
# exclusive next offset.
# -----------------------------------

current_end_offset = current_max_offset + 1

print(
    f"Current Kafka end offset: "
    f"{current_end_offset}"
)


# -----------------------------------
# Determine Kafka generation
# -----------------------------------
#
# IMPORTANT:
#
# last_processed_offset represents the
# NEXT offset that Bronze should consume.
#
# Therefore we compare it against the
# Kafka END offset, not MAX(offset).
#
# Example:
#
# Stored checkpoint = 1000
# Kafka end offset  = 1000
#
# This is normal.
#
# It does NOT mean Kafka was reset.
#
# A reset is only detected when:
#
# current_end_offset < last_processed_offset
#
# Example:
#
# Stored checkpoint = 2000
# Kafka end offset  = 1000
#
# This means Kafka moved backwards.
# -----------------------------------

if (
    last_processed_offset > 0
    and current_end_offset < last_processed_offset
):

    print(
        "WARNING: Kafka end offset has moved backwards."
    )

    print(
        f"Stored next offset: "
        f"{last_processed_offset}"
    )

    print(
        f"Current Kafka end offset: "
        f"{current_end_offset}"
    )

    kafka_generation += 1

    print(
        "Kafka reset detected."
    )

    print(
        f"Starting new Kafka generation: "
        f"{kafka_generation}"
    )

    starting_offsets = "earliest"

else:

    if last_processed_offset == 0:

        starting_offsets = "earliest"

        print(
            "First Bronze run - "
            "reading from earliest available Kafka offset."
        )

    else:

        starting_offsets = (
            f'{{"taxi-rides":{{"0":'
            f'{last_processed_offset}}}}}'
        )

        print(
            f"Reading Kafka from offset "
            f"{last_processed_offset}"
        )


# -----------------------------------
# Read Kafka batch
# -----------------------------------

print(
    "Reading Kafka messages..."
)

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
        f"Maximum Kafka offset in this batch: "
        f"{max_offset}"
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
        .withColumn(
            "kafka_generation",
            lit(kafka_generation)
        )
    )


    # -----------------------------------
    # Check existing Kafka records
    # -----------------------------------

    print(
        "Checking for existing Kafka records..."
    )

    existing_query = f"""
    (
        SELECT
            kafka_generation,
            kafka_partition,
            kafka_offset
        FROM bronze_taxi
        WHERE kafka_generation = {kafka_generation}
          AND kafka_offset >= 0
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
    # Remove duplicate records
    # -----------------------------------

    new_bronze_df = (
        bronze_df.alias("kafka")
        .join(
            existing_df.alias("existing"),
            on=[
                col("kafka.kafka_generation")
                == col("existing.kafka_generation"),

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
        f"New Bronze records after duplicate check: "
        f"{new_count}"
    )


    # -----------------------------------
    # Write new records
    # -----------------------------------

    if new_count > 0:

        print(
            "Writing new records to bronze_taxi..."
        )

        (
            new_bronze_df
            .select(
                "json_data",
                "kafka_partition",
                "kafka_offset",
                "kafka_generation"
            )
            .write
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
            "All Kafka records in this batch "
            "already exist. Nothing to write."
        )


    # -----------------------------------
    # Update metadata
    # -----------------------------------
    #
    # Kafka offsets are zero-based.
    #
    # If the last processed record is offset 999,
    # the next offset to consume is 1000.
    # -----------------------------------

    next_offset = max_offset + 1

    update_bronze_metadata(
        next_offset,
        kafka_generation
    )

    print(
        "Bronze metadata updated."
    )

    print(
        f"Next Kafka offset: {next_offset}"
    )

    print(
        f"Kafka generation: {kafka_generation}"
    )


# -----------------------------------
# Stop Spark
# -----------------------------------

spark.stop()

print(
    "Bronze batch completed successfully."
)
