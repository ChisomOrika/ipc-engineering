import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------



BASE_URL = "https://api.paystack.co"
HEADERS = {
    "Authorization": f"Bearer {os.getenv('PAYSTACK_SECRET_KEY')}",
    "Content-Type": "application/json",
}

PAGE_SIZE = 100
MAX_RETRIES = 3
BACKOFF_BASE = 2
REQUEST_TIMEOUT = 30
SCHEMA = "raw_paystack"

INGESTION_JOBS = [
    {"endpoint": "/transaction", "pg_table": "transactions", "pk_col": "id"},
]


# ---------------------------------------------------------------------------
# Checkpointing
# ---------------------------------------------------------------------------

def _get_conn(pg_conn_params):
    return psycopg2.connect(**pg_conn_params)


def ensure_checkpoint_table(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS "{SCHEMA}"."ingestion_checkpoint" (
                table_name    TEXT PRIMARY KEY,
                last_page     INT,
                total_fetched INT,
                updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
    conn.commit()


def save_checkpoint(conn, table_name, page, total_fetched):
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO "{SCHEMA}"."ingestion_checkpoint" (table_name, last_page, total_fetched, updated_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (table_name) DO UPDATE SET
                last_page = EXCLUDED.last_page,
                total_fetched = EXCLUDED.total_fetched,
                updated_at = CURRENT_TIMESTAMP;
        """, (table_name, page, total_fetched))
    conn.commit()


def load_checkpoint(conn, table_name):
    ensure_checkpoint_table(conn)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT last_page, total_fetched FROM "{SCHEMA}"."ingestion_checkpoint"
            WHERE table_name = %s;
        """, (table_name,))
        result = cur.fetchone()
    if result:
        print(f"  Resuming from page {result[0]}, {result[1]:,} already fetched")
        return {"last_page": result[0], "total_fetched": result[1]}
    return {"last_page": 1, "total_fetched": 0}


def clear_checkpoint(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(f'DELETE FROM "{SCHEMA}"."ingestion_checkpoint" WHERE table_name = %s;', (table_name,))
    conn.commit()


# ---------------------------------------------------------------------------
# API fetch with retry
# ---------------------------------------------------------------------------

def fetch_page(endpoint, page):
    url = f"{BASE_URL}{endpoint}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS,
                                params={"perPage": PAGE_SIZE, "page": page},
                                timeout=REQUEST_TIMEOUT)

            if resp.status_code == 200:
                return resp.json().get("data", [])
            elif resp.status_code == 429:
                print(f"  Rate limited on page {page}. Waiting 60s...")
                time.sleep(60)
            else:
                print(f"  Attempt {attempt}/{MAX_RETRIES} — page {page}: HTTP {resp.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"  Attempt {attempt}/{MAX_RETRIES} — page {page}: {e}")

        if attempt < MAX_RETRIES:
            time.sleep(BACKOFF_BASE ** attempt)

    print(f"  All retries failed for page {page}. Skipping.")
    return None


def fetch_all(endpoint, table_name, conn, resume=True):
    all_data = []
    failed_pages = []

    checkpoint = load_checkpoint(conn, table_name) if resume else {"last_page": 1, "total_fetched": 0}
    page = checkpoint["last_page"]

    while True:
        result = fetch_page(endpoint, page)

        if result is None:
            failed_pages.append(page)
            page += 1
            continue
        if not result:
            break

        all_data.extend(result)
        save_checkpoint(conn, table_name, page + 1, len(all_data))
        print(f"  Page {page}: {len(result)} records (total: {len(all_data):,})")
        page += 1

    if failed_pages:
        print(f"  ⚠️  Failed pages: {failed_pages}")

    print(f"  Fetched {len(all_data):,} records total")
    return pd.DataFrame(all_data)


# ---------------------------------------------------------------------------
# Serialization (vectorized — no applymap)
# ---------------------------------------------------------------------------

def serialize_dataframe(df):
    df = df.copy()

    for col in df.columns:
        sample = df[col].dropna()
        if sample.empty:
            continue
        first = sample.iloc[0]

        if isinstance(first, (dict, list)):
            mask = df[col].notna()
            df.loc[mask, col] = df.loc[mask, col].apply(
                lambda x: json.dumps(x, default=str) if isinstance(x, (dict, list)) else x
            )
        elif isinstance(first, datetime):
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S")

    df = df.where(df.notna(), None)
    return df


# ---------------------------------------------------------------------------
# Postgres helpers
# ---------------------------------------------------------------------------

def map_dtype(dtype):
    return {"object": "TEXT", "int64": "BIGINT", "float64": "FLOAT",
            "datetime64[ns]": "TIMESTAMP", "bool": "BOOLEAN"}.get(str(dtype), "TEXT")


def setup_table(df, table_name, pk_col, conn):
    cols = ", ".join(f'"{c}" {map_dtype(df[c].dtype)}' for c in df.columns)
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{table_name}" ({cols});')

        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
            (SCHEMA, table_name))
        existing = {r[0] for r in cur.fetchall()}
        for c in set(df.columns) - existing:
            cur.execute(f'ALTER TABLE "{SCHEMA}"."{table_name}" ADD COLUMN "{c}" TEXT;')

        constraint = f"uq_{table_name}_{pk_col}"
        cur.execute("""
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_schema=%s AND table_name=%s AND constraint_name=%s;
        """, (SCHEMA, table_name, constraint))
        if not cur.fetchone():
            cur.execute(f'ALTER TABLE "{SCHEMA}"."{table_name}" ADD CONSTRAINT {constraint} UNIQUE ("{pk_col}");')

    conn.commit()
    print(f"  Table '{SCHEMA}.{table_name}' ready")


def upsert_dataframe(df, table_name, pk_col, conn):
    print(f"  Serializing {len(df):,} rows...")
    df_clean = serialize_dataframe(df)

    columns_str = ", ".join(f'"{c}"' for c in df_clean.columns)
    update_str = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in df_clean.columns if c != pk_col)
    sql = f'INSERT INTO "{SCHEMA}"."{table_name}" ({columns_str}) VALUES %s ON CONFLICT ("{pk_col}") DO UPDATE SET {update_str};'

    rows = [tuple(r) for r in df_clean.itertuples(index=False, name=None)]

    start = time.time()
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=5000)
    conn.commit()
    print(f"  ✅ {len(rows):,} rows upserted in {time.time() - start:.1f}s")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_job(endpoint, table_name, pk_col, pg_conn_params, resume=True):
    print(f"\n--- {endpoint} -> {table_name} ---")
    start = time.time()

    conn = _get_conn(pg_conn_params)
    try:
        df = fetch_all(endpoint, table_name, conn, resume=resume)
        if df.empty:
            print("  No data, skipping.")
            return

        before = len(df)
        df = df.drop_duplicates(subset=[pk_col], keep="last")
        if len(df) < before:
            print(f"  Dropped {before - len(df)} dupes on '{pk_col}'")

        setup_table(df, table_name, pk_col, conn)
        upsert_dataframe(df, table_name, pk_col, conn)
        clear_checkpoint(conn, table_name)

        print(f"  Done in {time.time() - start:.1f}s")
    except Exception as e:
        print(f"  ❌ Fatal: {e}")
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    PG_PARAMS = {
        "database": os.getenv("PG_DB", "PROD_ANALYTICS_DB"),
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT", "25060"),
    }

    for job in INGESTION_JOBS:
        run_job(job["endpoint"], job["pg_table"], job["pk_col"], PG_PARAMS, resume=True)
