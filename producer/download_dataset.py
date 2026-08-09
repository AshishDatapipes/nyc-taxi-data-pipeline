import os
import requests

url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

output_path = "data/raw/yellow_tripdata_2024-01.parquet"

os.makedirs("data/raw", exist_ok=True)

print("Downloading dataset...")

response = requests.get(url, stream=True)

with open(output_path, "wb") as file:
    for chunk in response.iter_content(chunk_size=1024 * 1024):
        if chunk:
            file.write(chunk)

print(f"Dataset saved to: {output_path}")
