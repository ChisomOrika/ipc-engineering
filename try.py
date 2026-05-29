import os, sys
import psycopg2
import pandas as pd
from dotenv import load_dotenv

print("1. Script started", flush=True)

load_dotenv()
print(f"2. PG_HOST={os.getenv('PG_HOST')}, PG_USER={os.getenv('PG_USER')}, PG_PORT={os.getenv('PG_PORT')}", flush=True)

try:
    print("3. Connecting...", flush=True)
    conn = psycopg2.connect(
        database="PROD_ANALYTICS_DB",
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT", "25060"),
        connect_timeout=10,
    )
    print("4. Connected", flush=True)

    df = pd.read_sql('SELECT * FROM raw_lenco.accounts;', conn)
    print(f"5. Got {len(df)} rows", flush=True)
    print(df.to_string(), flush=True)
    conn.close()
except Exception as e:
    print(f"ERROR: {type(e).__name__}: {e}", flush=True)
    sys.exit(1)