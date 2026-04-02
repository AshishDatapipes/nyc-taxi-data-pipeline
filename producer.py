import requests
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

topic = "api-topic"

url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"

print("🚀 Starting API Producer...")

while True:
    try:
        response = requests.get(url)
        data = response.json()

        record = {
            "symbol": data["symbol"],
            "price": float(data["price"])
        }

        print(f"📤 Sending: {record}")
        producer.send(topic, value=record)
        producer.flush()

    except Exception as e:
        print("❌ Error:", e)

    time.sleep(5)
