from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg

spark = (
    SparkSession.builder
    .appName("NYC_Taxi_Gold_Aggregation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

silver_path = "/data/silver/nyc_taxi"
gold_path = "/data/gold/nyc_taxi"

df_silver = spark.read.parquet(silver_path)

df_gold = df_silver.agg(
    count("*").alias("total_trips"),
    sum("fare").alias("total_fare"),
    avg("fare").alias("avg_fare")
)

df_gold.write.mode("append").parquet(gold_path)

print(f"✅ Gold aggregation completed → {gold_path}")

spark.stop()
