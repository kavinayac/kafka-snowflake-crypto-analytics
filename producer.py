import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

API_KEY = "CG-Fxm4YNRNwkw1eSkgTub7JbiE"

URL = "https://api.coingecko.com/api/v3/simple/price"

PARAMS = {
    "ids": "bitcoin,ethereum",
    "vs_currencies": "usd",
    "include_24hr_change": "true"
}

HEADERS = {
    "x-cg-demo-api-key": API_KEY
}

# Retry logic to wait for Kafka to be ready
producer = None
retries = 0
max_retries = 30

while producer is None and retries < max_retries:
    try:
        producer = KafkaProducer(
            bootstrap_servers="kafka:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5000
        )
        print("✅ Successfully connected to Kafka")
    except KafkaError as e:
        retries += 1
        wait_time = min(2 ** retries, 30)
        print(f"⏳ Kafka not ready, retry {retries}/{max_retries} in {wait_time}s...")
        time.sleep(wait_time)

print("🚀 Kafka Producer started")

while True:
    try:
        response = requests.get(URL, params=PARAMS, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()

        message = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "data": data
        }

        producer.send("crypto-prices", value=message)
        print("📤 Sent:", message)

    except requests.exceptions.RequestException as e:
        print(f"⚠️ API Error: {e}")
        print("⏳ Retrying in 30 seconds...")
        time.sleep(30)
        continue
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        print("⏳ Retrying in 30 seconds...")
        time.sleep(30)
        continue
    
    time.sleep(10)
