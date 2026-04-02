from pyspark.sql import SparkSession
import requests
import os
from datetime import datetime

spark = SparkSession.builder.appName("Bronze Ingestion").getOrCreate()

# ------------------ CONFIG ------------------

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
FILE_NAME = "yellow_tripdata_2023-01.parquet"

download_path = "/data/raw"
output_path = "/data/bronze/yellow_tripdata"

# Create folder
os.makedirs(download_path, exist_ok=True)

# Timestamp for uniqueness
run_time = datetime.now().strftime("%Y%m%d_%H%M%S")

local_file = f"{download_path}/{run_time}_{FILE_NAME}"

# ------------------ DOWNLOAD ------------------

url = f"{BASE_URL}/{FILE_NAME}"

print(f"Downloading: {url}")

response = requests.get(url)

with open(local_file, "wb") as f:
    f.write(response.content)

print(f"Saved to: {local_file}")

# ------------------ LOAD TO SPARK ------------------

df = spark.read.parquet(local_file)

# ------------------ WRITE TO BRONZE ------------------

df.write.mode("append").parquet(output_path)

print("Bronze ingestion complete")
