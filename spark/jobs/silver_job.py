from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp

from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType
)

from utils.jdbc import read_query, write_table
from utils.metadata import (
    get_last_processed_id,
    update_metadata,
)


# ---------------------------------------
# Configuration
# ---------------------------------------

BATCH_SIZE = 50000


# ---------------------------------------
# Spark Session
# ---------------------------------------

spark = (
    SparkSession.builder
    .appName("Silver Taxi Job")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------
# Read Metadata
# ---------------------------------------

last_processed_id = get_last_processed_id(spark, "silver")

print(f"Starting Silver load.")
print(f"Last processed Bronze ID: {last_processed_id}")
print(f"Silver batch size: {BATCH_SIZE}")


# ---------------------------------------
# Taxi Schema
# ---------------------------------------

taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", StringType(), True),
    StructField("tpep_dropoff_datetime", StringType(), True),
    StructField("passenger_count", DoubleType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", DoubleType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True)
])


# ---------------------------------------
# Batch Processing Loop
# ---------------------------------------

batch_number = 0

while True:

    batch_number += 1

    print("---------------------------------------")
    print(f"Starting Silver batch {batch_number}")
    print(f"Reading Bronze records with id > {last_processed_id}")
    print("---------------------------------------")


    # ---------------------------------------
    # Read Next Bronze Batch
    # ---------------------------------------

    bronze_query = f"""
        SELECT *
        FROM bronze_taxi
        WHERE id > {last_processed_id}
        ORDER BY id
        LIMIT {BATCH_SIZE}
    """

    bronze_df = read_query(spark, bronze_query)


    # ---------------------------------------
    # Check Whether More Data Exists
    # ---------------------------------------

    bronze_count = bronze_df.count()

    if bronze_count == 0:

        print("---------------------------------------")
        print("No new Bronze records available.")
        print(f"Silver load completed after {batch_number - 1} batches.")
        print(f"Final processed Bronze ID: {last_processed_id}")
        print("---------------------------------------")

        break


    print(
        f"Bronze records received in batch {batch_number}: "
        f"{bronze_count}"
    )


    # ---------------------------------------
    # Parse JSON
    # ---------------------------------------

    print("Parsing JSON...")

    parsed_df = bronze_df.withColumn(
        "parsed",
        from_json(col("json_data"), taxi_schema)
    )


    # ---------------------------------------
    # Flatten JSON
    # ---------------------------------------

    silver_df = parsed_df.select(

        col("parsed.VendorID").alias("vendorid"),

        to_timestamp(
            col("parsed.tpep_pickup_datetime")
        ).alias("tpep_pickup_datetime"),

        to_timestamp(
            col("parsed.tpep_dropoff_datetime")
        ).alias("tpep_dropoff_datetime"),

        col("parsed.passenger_count"),

        col("parsed.trip_distance"),

        col("parsed.RatecodeID").alias("ratecodeid"),

        col("parsed.store_and_fwd_flag"),

        col("parsed.PULocationID").alias("pulocationid"),

        col("parsed.DOLocationID").alias("dolocationid"),

        col("parsed.payment_type"),

        col("parsed.fare_amount"),

        col("parsed.extra"),

        col("parsed.mta_tax"),

        col("parsed.tip_amount"),

        col("parsed.tolls_amount"),

        col("parsed.improvement_surcharge"),

        col("parsed.total_amount"),

        col("parsed.congestion_surcharge"),

        col("parsed.Airport_fee").alias("airport_fee"),

        col("kafka_partition"),

        col("kafka_offset")
    )


    # ---------------------------------------
    # Write Silver Batch
    # ---------------------------------------

    print(
        f"Writing Silver batch {batch_number} "
        f"with {bronze_count} records..."
    )

    write_table(
        silver_df,
        "silver_taxi",
        mode="append"
    )

    print(
        f"Silver batch {batch_number} written successfully."
    )


    # ---------------------------------------
    # Find Highest Bronze ID
    # ---------------------------------------

    max_id = (
        bronze_df
        .selectExpr("MAX(id) AS max_id")
        .collect()[0]["max_id"]
    )


    # ---------------------------------------
    # Update Metadata
    # ---------------------------------------

    update_metadata(
        pipeline_name="silver",
        last_processed_id=max_id,
        status="SUCCESS"
    )

    last_processed_id = max_id

    print(
        f"Silver metadata updated."
        f" Last processed Bronze ID: {last_processed_id}"
    )

    print(
        f"Batch {batch_number} completed successfully."
    )


# ---------------------------------------
# Stop Spark
# ---------------------------------------

spark.stop()

print("Silver job finished successfully.")


