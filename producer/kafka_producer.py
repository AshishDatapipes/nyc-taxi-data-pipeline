import json
import time

import pandas as pd
from kafka import KafkaProducer


# -----------------------------------
# Kafka Configuration
# -----------------------------------
KAFKA_BROKER = "localhost:29092"
TOPIC_NAME = "taxi-rides"


# -----------------------------------
# Read january dataset
# -----------------------------------
df = pd.read_parquet(
    "data/raw/yellow_tripdata_2024-01.parquet"
)

df = df.head(1000)


print(f"Loaded {len(df)} rows")


# -----------------------------------
# Create Kafka Producer
# -----------------------------------
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(
        v,
        default=str
    ).encode("utf-8")
)


print("Starting Kafka producer...")


# -----------------------------------
# Streaming configuration
# -----------------------------------
TOTAL_RECORDS = len(df)
STREAM_DURATION = 600  # 10 minutes in seconds

delay = STREAM_DURATION / TOTAL_RECORDS

print(f"Sending one record every {delay:.2f} seconds")


# -----------------------------------
# Send records to Kafka
# -----------------------------------
for index, row in df.iterrows():

    producer.send(
        TOPIC_NAME,
        value=row.to_dict()
    )

    print(
        f"Sent record {index + 1}/{TOTAL_RECORDS}"
    )

    time.sleep(1)


producer.flush()
producer.close()


print("All records sent successfully!")
