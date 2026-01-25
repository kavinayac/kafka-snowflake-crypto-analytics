import json
import os
import time
import snowflake.connector
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Retry logic to wait for Kafka to be ready
consumer = None
retries = 0
max_retries = 30

while consumer is None and retries < max_retries:
    try:
        consumer = KafkaConsumer(
            "crypto-prices",
            bootstrap_servers="kafka:9092",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="snowflake-consumer",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            request_timeout_ms=5000
        )
        print("🟢 Kafka consumer started")
    except KafkaError as e:
        retries += 1
        wait_time = min(2 ** retries, 30)
        print(f"⏳ Kafka not ready, retry {retries}/{max_retries} in {wait_time}s...")
        time.sleep(wait_time)

# Check for required Snowflake environment variables
required_env_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
missing_vars = [var for var in required_env_vars if var not in os.environ]

if missing_vars:
    print(f"⚠️ Missing Snowflake env vars: {', '.join(missing_vars)}")
    print("Continuing in Kafka-only mode...")
    sf_conn = None
else:
    # Try to connect to Snowflake
    try:
        sf_conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "CRYPTO_DB"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
        )
        
        cursor = sf_conn.cursor()
        
        # Create Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            event_timestamp STRING,
            coin STRING,
            price_usd FLOAT,
            change_24h FLOAT
        )
        """)
        
        print("❄️ Connected to Snowflake and table ready")
    except Exception as e:
        print(f"❌ Snowflake connection error: {e}")
        print("Continuing in Kafka-only mode...")
        sf_conn = None

# ---------------- Consume Loop ----------------
for msg in consumer:
    payload = msg.value
    ts = payload["timestamp"]

    if sf_conn:
        try:
            for coin, values in payload["data"].items():
                cursor.execute(
                    """
                    INSERT INTO crypto_prices (event_timestamp, coin, price_usd, change_24h)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (
                        ts,
                        coin,
                        values["usd"],
                        values.get("usd_24h_change", 0.0)
                    )
                )

            sf_conn.commit()
            print(f"✅ Inserted crypto prices at {ts}")
        except Exception as e:
            print(f"❌ Error inserting to Snowflake: {e}")
    else:
        # Just log the message if Snowflake is not connected
        print(f"📨 Received message at {ts}: {payload['data']}")

