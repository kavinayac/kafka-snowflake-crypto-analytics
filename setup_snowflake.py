import os
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote
import requests
import time

# Snowflake credentials
snowflake_creds = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "user": os.environ.get("SNOWFLAKE_USER"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.environ.get("SNOWFLAKE_DATABASE", "NEO_DB"),
    "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
}

# CoinGecko API credentials
API_KEY = "CG-Fxm4YNRNwkw1eSkgTub7JbiE"
COINGECKO_URL = "https://api.coingecko.com/api/v3/coins"

# Check Snowflake credentials
if not all([snowflake_creds["account"], snowflake_creds["user"], snowflake_creds["password"]]):
    print("❌ Snowflake credentials not set")
    exit(1)

try:
    # URL-encode password to handle special characters
    encoded_password = quote(snowflake_creds['password'], safe='')
    
    # Connect to Snowflake
    engine = create_engine(
        f"snowflake://{snowflake_creds['user']}:{encoded_password}@{snowflake_creds['account']}/{snowflake_creds['database']}/{snowflake_creds['schema']}?warehouse={snowflake_creds['warehouse']}"
    )
    
    print("✅ Connected to Snowflake")
    
    # Create table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS crypto_prices (
        event_timestamp TIMESTAMP,
        coin VARCHAR,
        price_usd FLOAT,
        change_24h FLOAT
    )
    """
    
    with engine.connect() as conn:
        conn.execute(text(create_table_sql))
        conn.commit()
        print("✅ Table 'crypto_prices' created")
    
    # Fetch real data from CoinGecko
    print("\n📡 Fetching cryptocurrency data from CoinGecko API...")
    
    coins = ["bitcoin", "ethereum"]
    all_data = []
    
    headers = {"x-cg-demo-api-key": API_KEY}
    
    for coin in coins:
        try:
            # Fetch market data
            url = f"{COINGECKO_URL}/{coin}/market_chart"
            params = {
                "vs_currency": "usd",
                "days": "30",
                "interval": "daily"
            }
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            prices = data.get("prices", [])
            market_caps = data.get("market_caps", [])
            
            print(f"✅ Fetched {len(prices)} price points for {coin}")
            
            for i, price_data in enumerate(prices):
                timestamp_ms, price = price_data
                timestamp = datetime.fromtimestamp(timestamp_ms / 1000)
                
                # Get 24h change from market data
                change_24h = 0  # Default value
                if i > 0:
                    prev_price = prices[i-1][1]
                    change_24h = ((price - prev_price) / prev_price) * 100
                
                all_data.append({
                    'event_timestamp': timestamp,
                    'coin': coin,
                    'price_usd': price,
                    'change_24h': change_24h
                })
            
            time.sleep(2)  # Rate limiting
            
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Failed to fetch {coin}: {e}")
            continue
    
    if not all_data:
        print("❌ No data fetched from CoinGecko API")
        exit(1)
    
    df = pd.DataFrame(all_data)
    
    print(f"\n📊 Total records to insert: {len(df)}")
    
    # Insert data using raw Snowflake connector
    import snowflake.connector
    conn = snowflake.connector.connect(
        account=snowflake_creds['account'],
        user=snowflake_creds['user'],
        password=snowflake_creds['password'],
        warehouse=snowflake_creds['warehouse'],
        database=snowflake_creds['database'],
        schema=snowflake_creds['schema']
    )
    
    cursor = conn.cursor()
    
    # Insert data row by row
    for idx, row in df.iterrows():
        insert_sql = f"""
        INSERT INTO crypto_prices (event_timestamp, coin, price_usd, change_24h)
        VALUES ('{row['event_timestamp']}', '{row['coin']}', {row['price_usd']}, {row['change_24h']})
        """
        cursor.execute(insert_sql)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"✅ Loaded {len(df)} rows into crypto_prices table")
    
    engine.dispose()
    print("\n🎉 Real cryptocurrency data loaded successfully!")
    print("Run: python train_crypto_model.py")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    exit(1)

