from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    count,
    sum,
    avg,
    max as spark_max
)

from utils.metadata import (
    get_last_processed_id,
    update_metadata,
    get_max_id
)

from config.db_config import (
    JDBC_URL,
    DB_PROPERTIES
)


# -------------------------------------------------
# Spark Session
# -------------------------------------------------

spark = (
    SparkSession.builder
    .appName("Gold Taxi Job")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# -------------------------------------------------
# Read Metadata
# -------------------------------------------------

pipeline_name = "gold"

last_processed_id = get_last_processed_id(
    spark,
    pipeline_name
)

print(
    f"Last processed Silver ID for Gold: {last_processed_id}"
)


# -------------------------------------------------
# Read only new Silver records
# -------------------------------------------------

print("Reading new Silver records...")

silver_query = f"""
(
    SELECT *
    FROM silver_taxi
    WHERE id > {last_processed_id}
) AS silver_increment
"""


silver_df = (
    spark.read
    .jdbc(
        url=JDBC_URL,
        table=silver_query,
        properties=DB_PROPERTIES
    )
)


record_count = silver_df.count()

print(
    f"New Silver records: {record_count}"
)


# -------------------------------------------------
# No new data handling
# -------------------------------------------------

if record_count == 0:

    print("No new data available for Gold processing")

    update_metadata(
        pipeline_name="gold",
        last_processed_id=last_processed_id,
        status="NO_DATA"
    )

    spark.stop()

else:


    # ---------------------------------------------
    # Create trip date
    # ---------------------------------------------

    gold_df = (
        silver_df
        .withColumn(
            "trip_date",
            to_date("tpep_pickup_datetime")
        )
    )


    # ---------------------------------------------
    # Daily Aggregation
    # ---------------------------------------------

    gold_summary = (
        gold_df
        .groupBy("trip_date")
        .agg(
            count("*").alias("total_trips"),
            sum("total_amount").alias("total_revenue"),
            avg("fare_amount").alias("average_fare"),
            avg("trip_distance").alias("average_trip_distance"),
            avg("tip_amount").alias("average_tip"),
            sum("passenger_count").alias("total_passengers")
        )
    )


    print("Gold Preview")

    gold_summary.show(
        truncate=False
    )


    # ---------------------------------------------
    # Write Gold
    # ---------------------------------------------

    print(
        "Writing to gold_daily_summary..."
    )


    (
        gold_summary.write
        .mode("append")
        .jdbc(
            url=JDBC_URL,
            table="gold_daily_summary",
            properties=DB_PROPERTIES
        )
    )


    # ---------------------------------------------
    # Update Metadata
    # ---------------------------------------------

    max_id = get_max_id(
        silver_df
    )


    update_metadata(
        pipeline_name="gold",
        last_processed_id=max_id,
        status="SUCCESS"
    )


    print(
        f"Gold load completed. Metadata updated: {max_id}"
    )


spark.stop()
