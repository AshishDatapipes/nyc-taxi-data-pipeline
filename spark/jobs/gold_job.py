from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    count,
    sum,
    avg
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
# Configuration
# -------------------------------------------------

BATCH_SIZE = 50000
PIPELINE_NAME = "gold"

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
# Read Gold Metadata
# -------------------------------------------------

last_processed_id = get_last_processed_id(
    spark,
    PIPELINE_NAME
)

print(
    f"Last processed Silver ID for Gold: {last_processed_id}"
)

# -------------------------------------------------
# Process Silver in batches
# -------------------------------------------------

current_id = last_processed_id
total_processed = 0

while True:

    batch_end_id = current_id + BATCH_SIZE

    print("---------------------------------------")
    print(
        f"Reading Silver IDs > {current_id} "
        f"and <= {batch_end_id}"
    )

    # -------------------------------------------------
    # Read current Silver batch
    # -------------------------------------------------

    silver_query = f"""
    (
        SELECT *
        FROM silver_taxi
        WHERE id > {current_id}
          AND id <= {batch_end_id}
        ORDER BY id
    ) AS silver_batch
    """

    silver_df = (
        spark.read
        .jdbc(
            url=JDBC_URL,
            table=silver_query,
            properties=DB_PROPERTIES
        )
    )

    batch_count = silver_df.count()

    print(
        f"Silver records in batch: {batch_count}"
    )

    # -------------------------------------------------
    # No more data
    # -------------------------------------------------

    if batch_count == 0:

        print("No new Silver records available.")
        break

    # -------------------------------------------------
    # Determine affected dates
    # -------------------------------------------------

    affected_dates = (
        silver_df
        .select(
            to_date(
                col("tpep_pickup_datetime")
            ).alias("trip_date")
        )
        .where(
            col("trip_date").isNotNull()
        )
        .distinct()
    )

    affected_date_count = affected_dates.count()

    print(
        f"Affected trip dates: {affected_date_count}"
    )

    # -------------------------------------------------
    # Get affected date range
    # -------------------------------------------------

    affected_dates.createOrReplaceTempView(
        "affected_dates"
    )

    # -------------------------------------------------
    # Recalculate complete daily aggregates
    #
    # IMPORTANT:
    # We calculate from ALL Silver records for the
    # affected dates, not just the current batch.
    # -------------------------------------------------

    print("Recalculating affected Gold dates...")

    silver_for_gold = (
        spark.read
        .jdbc(
            url=JDBC_URL,
            table="""
            (
                SELECT *
                FROM silver_taxi
                WHERE tpep_pickup_datetime IS NOT NULL
            ) AS silver_all
            """,
            properties=DB_PROPERTIES
        )
    )

    gold_df = (
        silver_for_gold
        .withColumn(
            "trip_date",
            to_date(
                col("tpep_pickup_datetime")
            )
        )
        .join(
            affected_dates,
            on="trip_date",
            how="inner"
        )
        .groupBy("trip_date")
        .agg(
            count("*").alias("total_trips"),
            sum("total_amount").alias("total_revenue"),
            avg("fare_amount").alias("average_fare"),
            avg("trip_distance").alias(
                "average_trip_distance"
            ),
            avg("tip_amount").alias("average_tip"),
            sum("passenger_count").alias(
                "total_passengers"
            )
        )
    )

    print("Gold preview:")

    gold_df.show(
        10,
        truncate=False
    )

    # -------------------------------------------------
    # Collect Gold rows
    #
    # We use PostgreSQL UPSERT because trip_date is
    # the PRIMARY KEY.
    # -------------------------------------------------

    gold_rows = gold_df.collect()

    print(
        f"Gold rows to upsert: {len(gold_rows)}"
    )

    # -------------------------------------------------
    # UPSERT into PostgreSQL
    # -------------------------------------------------

    if len(gold_rows) > 0:

        import psycopg2

        connection = psycopg2.connect(
            host="postgres",
            port=5432,
            database="taxi",
            user="airflow",
            password="airflowpassword"
        )

        cursor = connection.cursor()

        upsert_sql = """
        INSERT INTO gold_daily_summary (
            trip_date,
            total_trips,
            total_revenue,
            average_fare,
            average_trip_distance,
            average_tip,
            total_passengers
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        ON CONFLICT (trip_date)
        DO UPDATE SET
            total_trips = EXCLUDED.total_trips,
            total_revenue = EXCLUDED.total_revenue,
            average_fare = EXCLUDED.average_fare,
            average_trip_distance =
                EXCLUDED.average_trip_distance,
            average_tip = EXCLUDED.average_tip,
            total_passengers =
                EXCLUDED.total_passengers
        """

        for row in gold_rows:

            cursor.execute(
                upsert_sql,
                (
                    row["trip_date"],
                    row["total_trips"],
                    row["total_revenue"],
                    row["average_fare"],
                    row["average_trip_distance"],
                    row["average_tip"],
                    row["total_passengers"]
                )
            )

        connection.commit()

        cursor.close()
        connection.close()

        print(
            "Gold UPSERT completed successfully."
        )

    # -------------------------------------------------
    # Update checkpoint
    # -------------------------------------------------

    max_id = get_max_id(
        silver_df
    )

    update_metadata(
        pipeline_name=PIPELINE_NAME,
        last_processed_id=max_id,
        status="SUCCESS"
    )

    current_id = max_id

    total_processed += batch_count

    print(
        f"Gold checkpoint updated: {max_id}"
    )

    print(
        f"Total Silver records processed: "
        f"{total_processed}"
    )


# -------------------------------------------------
# Final Status
# -------------------------------------------------

print("---------------------------------------")
print(
    f"Gold load completed after processing "
    f"{total_processed} Silver records."
)

print(
    f"Final Gold checkpoint: {current_id}"
)

print("---------------------------------------")

spark.stop()

