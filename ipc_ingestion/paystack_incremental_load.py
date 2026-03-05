import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import json
import time
import os
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("paystack_ingestion.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

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
LOOKBACK_DAYS = 3

INGESTION_JOBS = [
    {"endpoint": "/transaction", "pg_table": "transactions", "pk_col": "id", "timestamp_col": "created_at"},
]


# ---------------------------------------------------------------------------
# Postgres — get last max timestamp for incremental load
# ---------------------------------------------------------------------------

def get_last_max_timestamp(conn, table_name, timestamp_col):
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT MAX("{timestamp_col}") FROM "{SCHEMA}"."{table_name}"
                WHERE "{timestamp_col}" IS NOT NULL
                  AND "{timestamp_col}" NOT IN ('NaN', 'nan', 'NaT', '');
            """)
            result = cur.fetchone()
            raw = result[0] if result and result[0] else None
            log.info(f"MAX('{timestamp_col}') raw value from DB: {repr(raw)}")
            if raw is None:
                return None
            # psycopg2 may return a string (TEXT column) or datetime (TIMESTAMP column)
            if isinstance(raw, datetime):
                return raw.replace(tzinfo=None)
            try:
                return pd.to_datetime(str(raw), utc=False).to_pydatetime().replace(tzinfo=None)
            except Exception as e:
                log.warning(f"Could not parse timestamp '{raw}': {e}")
                return None
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        log.info(f"Table '{table_name}' does not exist yet — running full load.")
        return None
    except psycopg2.errors.UndefinedColumn:
        conn.rollback()
        log.warning(f"Column '{timestamp_col}' not found — running full load.")
        return None
    except Exception as e:
        conn.rollback()
        log.warning(f"Could not read max timestamp: {e} — running full load.")
        return None


# ---------------------------------------------------------------------------
# Parse a timestamp value from a record to a naive datetime for comparison
# ---------------------------------------------------------------------------

def parse_ts(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    try:
        return pd.to_datetime(str(value), utc=False).to_pydatetime().replace(tzinfo=None)
    except Exception:
        return None


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

def fetch_page(endpoint, page, from_date=None):
    url = f"{BASE_URL}{endpoint}"
    params = {"perPage": PAGE_SIZE, "page": page}

    # Use Paystack's `from` param to filter at API level
    if from_date:
        params["from"] = from_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=REQUEST_TIMEOUT)

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


def fetch_all(endpoint, table_name, conn, from_date=None, resume=True, timestamp_col=None):
    all_data = []
    failed_pages = []

    checkpoint = load_checkpoint(conn, table_name) if resume else {"last_page": 1, "total_fetched": 0}
    page = checkpoint["last_page"]

    while True:
        result = fetch_page(endpoint, page, from_date=from_date)

        if result is None:
            failed_pages.append(page)
            page += 1
            continue
        if not result:
            break

        # Client-side date filter + early exit (safety net in case API `from` param is imperfect)
        if from_date and timestamp_col:
            new_records = []
            old_count   = 0
            for rec in result:
                ts = parse_ts(rec.get(timestamp_col))
                if ts is not None and ts >= from_date:
                    new_records.append(rec)
                else:
                    old_count += 1
            print(f"  Page {page}: {len(new_records)} in window, {old_count} older")
            all_data.extend(new_records)
            save_checkpoint(conn, table_name, page + 1, len(all_data))
            # Newest-first: if ALL records on this page are older than from_date, stop
            if old_count == len(result):
                print(f"  All records on page {page} older than {from_date.date()}. Stopping early.")
                break
        else:
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

    def _clean(x):
        if isinstance(x, (dict, list, bytes)):
            return x
        if isinstance(x, str):
            return None if x in ("NaT", "nan", "NaN") else x
        try:
            if pd.isna(x):
                return None
        except (TypeError, ValueError):
            pass
        return x

    df = df.map(_clean)
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

    def _safe_none(x):
        if isinstance(x, (dict, list, str, bytes)):
            return x
        try:
            return None if pd.isna(x) else x
        except (TypeError, ValueError):
            return x

    rows = [tuple(_safe_none(x) for x in r) for r in df_clean.itertuples(index=False, name=None)]

    start = time.time()
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=5000)
    conn.commit()
    print(f"  ✅ {len(rows):,} rows upserted in {time.time() - start:.1f}s")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_job(endpoint, table_name, pk_col, timestamp_col, pg_conn_params, mode="incremental", resume=True):
    print(f"\n--- {endpoint} -> {table_name} ({mode}) ---")
    start = time.time()

    conn = _get_conn(pg_conn_params)
    try:
        # Determine from_date for incremental
        from_date = None
        if mode == "incremental":
            last_ts = get_last_max_timestamp(conn, table_name, timestamp_col)
            if last_ts:
                from_date = last_ts - timedelta(days=LOOKBACK_DAYS)
                print(f"  Last '{timestamp_col}' in Postgres: {last_ts}")
                print(f"  Fetching from {from_date} ({LOOKBACK_DAYS}d lookback)")
            else:
                print(f"  No existing data — running full load")

        df = fetch_all(endpoint, table_name, conn, from_date=from_date, resume=resume if not from_date else False, timestamp_col=timestamp_col)
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

    # Change to "full" to reload everything from scratch
    MODE = "incremental"

    for job in INGESTION_JOBS:
        run_job(
            endpoint=job["endpoint"],
            table_name=job["pg_table"],
            pk_col=job["pk_col"],
            timestamp_col=job["timestamp_col"],
            pg_conn_params=PG_PARAMS,
            mode=MODE,
            resume=True,
        )