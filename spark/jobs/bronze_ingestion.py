from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NYC_Taxi_Bronze_Ingestion")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "nyc_taxi")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("kafka.metadata.max.age.ms", "10000")
    .load()
)

df_parsed = df.selectExpr(
    "CAST(key AS STRING) AS kafka_key",
    "CAST(value AS STRING) AS kafka_value",
    "timestamp AS kafka_timestamp"
)

query = (
    df_parsed.writeStream
    .format("parquet")
    .option("path", "/data/bronze/nyc_taxi")
    .option("checkpointLocation", "/data/checkpoints/bronze")
    .outputMode("append")
    .start()
)

query.awaitTermination()
