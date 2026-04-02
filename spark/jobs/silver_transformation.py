from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

spark = (
    SparkSession.builder
    .appName("NYC_Taxi_Silver_Transformation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

bronze_path = "/data/bronze/nyc_taxi"
silver_path = "/data/silver/nyc_taxi"

schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("fare", DoubleType(), True)
])

df_bronze = spark.read.parquet(bronze_path)

df_silver = df_bronze.select(
    from_json(col("kafka_value"), schema).alias("data"),
    col("kafka_timestamp")
).select(
    col("data.trip_id"),
    col("data.fare"),
    col("kafka_timestamp")
)

df_silver = df_silver.filter(col("trip_id").isNotNull())

df_silver.write.mode("append").parquet(silver_path)

print(f"✅ Silver transformation completed → {silver_path}")

spark.stop()
