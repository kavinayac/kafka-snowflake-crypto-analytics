import os
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
import joblib
from datetime import datetime, timedelta

# Check for Snowflake credentials
snowflake_creds = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "user": os.environ.get("SNOWFLAKE_USER"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
    "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database": os.environ.get("SNOWFLAKE_DATABASE", "CRYPTO_DB"),
    "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
}

# Try to load from Snowflake, fallback to demo data
if all([snowflake_creds["account"], snowflake_creds["user"], snowflake_creds["password"]]):
    try:
        import snowflake.connector
        conn = snowflake.connector.connect(**snowflake_creds)
        
        # Try multiple table names and schemas
        queries = [
            """
            SELECT
                TO_TIMESTAMP(event_timestamp) AS ts,
                coin,
                price_usd,
                change_24h
            FROM crypto_prices
            ORDER BY ts
            """,
            """
            SELECT
                TO_TIMESTAMP(event_timestamp) AS ts,
                coin,
                price_usd,
                change_24h
            FROM CRYPTO_DB.PUBLIC.crypto_prices
            ORDER BY ts
            """,
            """
            SELECT
                event_timestamp AS ts,
                coin,
                price_usd,
                change_24h
            FROM crypto_prices
            ORDER BY event_timestamp
            """
        ]
        
        df = None
        for query in queries:
            try:
                df = pd.read_sql(query, conn)
                if df is not None and len(df) > 0:
                    # Normalize column names to lowercase
                    df.columns = [col.lower() for col in df.columns]
                    print(f"✅ Data loaded from Snowflake ({len(df)} rows)")
                    break
            except:
                continue
        
        conn.close()
        
        if df is None or len(df) == 0:
            raise Exception("No data found in Snowflake tables")
            
    except Exception as e:
        print(f"⚠️ Snowflake connection/query failed: {e}")
        print("📊 Using demo data instead...\n")
        df = None
else:
    print("ℹ️ Snowflake credentials not set")
    print("📊 Using demo data instead...\n")
    df = None

# Generate demo data if Snowflake is unavailable
if df is None:
    np.random.seed(42)
    dates = [datetime.now() - timedelta(hours=i) for i in range(100, 0, -1)]
    data = {
        'ts': dates,
        'coin': ['bitcoin'] * 50 + ['ethereum'] * 50,
        'price_usd': np.concatenate([
            np.linspace(89000, 89500, 50) + np.random.normal(0, 50, 50),
            np.linspace(2950, 3000, 50) + np.random.normal(0, 5, 50)
        ]),
        'change_24h': np.random.uniform(-2, 1, 100)
    }
    df = pd.DataFrame(data)
    print("✅ Demo data generated (100 samples)")

# -------------------------------
# Basic cleaning
# -------------------------------
df["ts"] = pd.to_datetime(df["ts"])
df = df.dropna()

# -------------------------------
# Feature engineering
# -------------------------------
df["hour"] = df["ts"].dt.hour
df["day"] = df["ts"].dt.day
df["minute"] = df["ts"].dt.minute

# Sort by coin & time
df = df.sort_values(["coin", "ts"])

# Lag feature (previous price)
df["price_lag_1"] = df.groupby("coin")["price_usd"].shift(1)
df["price_lag_2"] = df.groupby("coin")["price_usd"].shift(2)

df = df.dropna()

# One-hot encode coin
df = pd.get_dummies(df, columns=["coin"])

# -------------------------------
# Train / test split
# -------------------------------
X = df.drop(columns=["price_usd", "ts"])
y = df["price_usd"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, shuffle=False
)

# -------------------------------
# Model training
# -------------------------------
model = RandomForestRegressor(
    n_estimators=200,
    random_state=42,
    n_jobs=-1
)

model.fit(X_train, y_train)

# -------------------------------
# Evaluation
# -------------------------------
preds = model.predict(X_test)
mae = mean_absolute_error(y_test, preds)

print(f"📉 MAE: {mae:.2f}")

# -------------------------------
# Save model
# -------------------------------
joblib.dump(model, "crypto_price_model.pkl")
print("💾 Model saved as crypto_price_model.pkl")
