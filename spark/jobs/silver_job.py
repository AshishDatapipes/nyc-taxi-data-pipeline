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
    get_max_id,
    update_metadata,
)

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

print(f"Last processed Bronze ID: {last_processed_id}")

# ---------------------------------------
# Read Incremental Bronze Records
# ---------------------------------------
bronze_query = f"""
SELECT *
FROM bronze_taxi
WHERE id > {last_processed_id}
ORDER BY id
"""

bronze_df = read_query(spark, bronze_query)

bronze_count = bronze_df.count()

print(f"New Bronze records: {bronze_count}")

if bronze_count == 0:
    print("No new Bronze records found.")
    spark.stop()
    exit(0)

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
    col("parsed.Airport_fee").alias("airport_fee")
)

print("Silver Data Preview")
silver_df.show(5, truncate=False)

# ---------------------------------------
# Write to Silver
# ---------------------------------------
print("Writing to silver_taxi...")

write_table(
    silver_df,
    "silver_taxi",
    mode="append"
)

# ---------------------------------------
# Update Metadata
# ---------------------------------------
max_id = get_max_id(bronze_df)

update_metadata(
    pipeline_name="silver",
    last_processed_id=max_id,
    status="SUCCESS"
)

print(f"Metadata updated. Last processed ID: {max_id}")

print("Silver load completed successfully!")

spark.stop()
