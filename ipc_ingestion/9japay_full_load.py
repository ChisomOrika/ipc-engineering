import requests
import urllib3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import json
import time
import logging
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("ninepay_ingestion.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# API Config
# ---------------------------------------------------------------------------

URL = "https://developer.9japay.com/v1/api/transactions"
HEADERS = {
    "secret":       "sk_EDVcSlkkSQ8c8RDvUEihWoswqK51mKmmxJJvk+FHt0Q=",
    "api-key":      "3e08a422-b01d-46bc-8b11-8f71a5557b7f",
    "Content-Type": "application/json",
}

PAGE_SIZE       = 500
MAX_RETRIES     = 3
BACKOFF_BASE    = 2
REQUEST_TIMEOUT = 30
CHUNK_SIZE      = 10_000
SCHEMA          = "raw_9japay"
TABLE_NAME      = "transactions"
PK_COL          = "transactionId"


# ---------------------------------------------------------------------------
# Postgres Checkpointing
# ---------------------------------------------------------------------------

def ensure_checkpoint_table(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS "{SCHEMA}"."ingestion_checkpoint" (
                id           SERIAL PRIMARY KEY,
                table_name   TEXT UNIQUE,
                last_page    INT,
                total_fetched INT,
                updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
    conn.commit()


def save_checkpoint(pg_conn_params: dict, page: int, total_fetched: int):
    conn = psycopg2.connect(**pg_conn_params)
    try:
        ensure_checkpoint_table(conn)
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO "{SCHEMA}"."ingestion_checkpoint" (table_name, last_page, total_fetched, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (table_name) DO UPDATE SET
                    last_page     = EXCLUDED.last_page,
                    total_fetched = EXCLUDED.total_fetched,
                    updated_at    = CURRENT_TIMESTAMP;
            """, (TABLE_NAME, page, total_fetched))
        conn.commit()
        log.info(f"Checkpoint saved — page {page}, {total_fetched:,} records so far")
    finally:
        conn.close()


def load_checkpoint(pg_conn_params: dict) -> dict:
    conn = psycopg2.connect(**pg_conn_params)
    try:
        ensure_checkpoint_table(conn)
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT last_page, total_fetched FROM "{SCHEMA}"."ingestion_checkpoint"
                WHERE table_name = %s;
            """, (TABLE_NAME,))
            result = cur.fetchone()
        if result:
            log.info(f"Resuming from checkpoint — page {result[0]}, {result[1]:,} already fetched")
            return {"last_page": result[0], "total_fetched": result[1]}
        return {"last_page": 1, "total_fetched": 0}
    finally:
        conn.close()


def clear_checkpoint(pg_conn_params: dict):
    conn = psycopg2.connect(**pg_conn_params)
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                DELETE FROM "{SCHEMA}"."ingestion_checkpoint" WHERE table_name = %s;
            """, (TABLE_NAME,))
        conn.commit()
        log.info("Checkpoint cleared after successful run")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Fetch single page with retry + exponential backoff
# ---------------------------------------------------------------------------

def fetch_page(page: int) -> list:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                URL,
                headers=HEADERS,
                params={"page-size": PAGE_SIZE, "page-number": page},
                verify=False,
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                return response.json().get("data", [])

            elif response.status_code == 429:
                log.warning(f"Rate limited on page {page}. Waiting 60s...")
                time.sleep(60)

            else:
                log.warning(f"Attempt {attempt}/{MAX_RETRIES} — page {page}: HTTP {response.status_code}")

        except requests.exceptions.Timeout:
            log.warning(f"Attempt {attempt}/{MAX_RETRIES} — page {page}: Timed out")
        except requests.exceptions.RequestException as e:
            log.warning(f"Attempt {attempt}/{MAX_RETRIES} — page {page}: {e}")

        if attempt < MAX_RETRIES:
            wait = BACKOFF_BASE ** attempt
            log.info(f"Retrying in {wait}s...")
            time.sleep(wait)

    log.error(f"All {MAX_RETRIES} attempts failed for page {page}. Skipping.")
    return None  # None = failed, [] = no more data


# ---------------------------------------------------------------------------
# Fetch all pages
# ---------------------------------------------------------------------------

def fetch_all_transactions(pg_conn_params: dict, resume: bool = True) -> pd.DataFrame:
    all_data = []
    failed_pages = []

    checkpoint = load_checkpoint(pg_conn_params) if resume else {"last_page": 1, "total_fetched": 0}
    page = checkpoint["last_page"]

    while True:
        log.info(f"Fetching page {page}...")
        result = fetch_page(page)

        if result is None:
            failed_pages.append(page)
            page += 1
            continue

        if not result:
            log.info(f"No more data at page {page}. Fetch complete.")
            break

        all_data.extend(result)
        save_checkpoint(pg_conn_params, page + 1, len(all_data))
        log.info(f"Page {page}: {len(result)} records (total: {len(all_data):,})")
        page += 1

    if failed_pages:
        log.warning(f"Failed pages (skipped): {failed_pages}")

    log.info(f"Total records fetched: {len(all_data):,}")
    return pd.DataFrame(all_data)


# ---------------------------------------------------------------------------
# Serialize for Postgres — no transformations, raw data only
# ---------------------------------------------------------------------------

def serialize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in df.columns:
        sample = df[col].dropna()
        if sample.empty:
            continue
        first = sample.iloc[0]

        if isinstance(first, (dict, list)):
            df[col] = df[col].apply(
                lambda x: json.dumps(x, default=str) if isinstance(x, (dict, list)) else x
            )
        elif isinstance(first, datetime):
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df[col] = df[col].apply(lambda x: None if pd.isnull(x) else x.isoformat())

    df = df.where(pd.notnull(df), None)
    df = df.map(lambda x: None if isinstance(x, str) and x == "NaT" else x)

    return df


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------

def map_dtype_to_postgres(dtype) -> str:
    mapping = {
        "object":         "TEXT",
        "int64":          "BIGINT",
        "float64":        "FLOAT",
        "datetime64[ns]": "TIMESTAMP",
        "bool":           "BOOLEAN",
    }
    return mapping.get(str(dtype), "TEXT")


def create_table_if_missing(df: pd.DataFrame, conn):
    cols = ", ".join(
        f'"{col}" {map_dtype_to_postgres(df[col].dtype)}'
        for col in df.columns
    )
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{TABLE_NAME}" ({cols});')
    conn.commit()
    log.info(f"Table '{SCHEMA}.{TABLE_NAME}' ready.")


def add_missing_columns(df: pd.DataFrame, conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s;",
            (SCHEMA, TABLE_NAME),
        )
        existing = {row[0] for row in cur.fetchall()}

    missing = set(df.columns) - existing
    if missing:
        with conn.cursor() as cur:
            for col in missing:
                cur.execute(f'ALTER TABLE "{SCHEMA}"."{TABLE_NAME}" ADD COLUMN "{col}" TEXT;')
                log.info(f"Added column '{col}'")
        conn.commit()


def ensure_unique_constraint(conn):
    constraint_name = f"uq_{TABLE_NAME}_{PK_COL}".lower()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.table_constraints
            WHERE table_schema = %s AND table_name = %s AND constraint_name = %s;
        """, (SCHEMA, TABLE_NAME, constraint_name))
        exists = cur.fetchone()[0]

    if not exists:
        with conn.cursor() as cur:
            cur.execute(f'ALTER TABLE "{SCHEMA}"."{TABLE_NAME}" ADD CONSTRAINT {constraint_name} UNIQUE ("{PK_COL}");')
        conn.commit()
        log.info(f"Unique constraint added on '{PK_COL}'")

def ensure_indexes(conn):
    """Create indexes on columns most likely to be filtered or joined on."""
    indexes = [
        ("idx_transactions_transactionDate",      "transactionDate"),
        ("idx_transactions_transactionReference", "transactionReference"),
        ("idx_transactions_accountNumber",        "accountNumber"),
        ("idx_transactions_transactionType",      "transactionType"),
    ]
    with conn.cursor() as cur:
        for index_name, col in indexes:
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS {index_name}
                ON "raw_9japay"."transactions" ("{col}");
            """)
            log.info(f"Index ensured: {index_name}")
    conn.commit()



# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def upsert_dataframe(df: pd.DataFrame, pg_conn_params: dict):
    log.info(f"Serializing {len(df):,} rows...")
    df_clean = serialize_dataframe(df)

    columns_str = ", ".join(f'"{c}"' for c in df_clean.columns)
    update_str  = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in df_clean.columns if c != PK_COL)
    upsert_sql  = f"""
        INSERT INTO "{SCHEMA}"."{TABLE_NAME}" ({columns_str}) VALUES %s
        ON CONFLICT ("{PK_COL}") DO UPDATE SET {update_str};
    """

    total    = len(df_clean)
    upserted = 0
    start    = time.time()

    for i in range(0, total, CHUNK_SIZE):
        chunk = df_clean.iloc[i : i + CHUNK_SIZE]
        rows  = [tuple(row) for row in chunk.itertuples(index=False, name=None)]

        conn = psycopg2.connect(**pg_conn_params)
        try:
            with conn.cursor() as cur:
                execute_values(cur, upsert_sql, rows, page_size=CHUNK_SIZE)
            conn.commit()
        finally:
            conn.close()

        upserted += len(rows)
        log.info(f"⏳ {upserted:,}/{total:,} rows upserted...")

    log.info(f"✅ {total:,} rows upserted in {round(time.time() - start, 2)}s")


# ---------------------------------------------------------------------------
# Run metadata logging
# ---------------------------------------------------------------------------

def log_run(pg_conn_params: dict, rows_fetched: int, status: str, error: str = None):
    try:
        conn = psycopg2.connect(**pg_conn_params)
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{SCHEMA}"."ingestion_log" (
                    id           SERIAL PRIMARY KEY,
                    table_name   TEXT,
                    rows_fetched INT,
                    status       TEXT,
                    error        TEXT,
                    run_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute(f"""
                INSERT INTO "{SCHEMA}"."ingestion_log" (table_name, rows_fetched, status, error)
                VALUES (%s, %s, %s, %s);
            """, (TABLE_NAME, rows_fetched, status, error))
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"Could not write run log: {e}")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_full_ingestion(pg_conn_params: dict, resume: bool = True):
    rows_fetched = 0
    start = time.time()

    try:
        log.info("=" * 60)
        log.info("Starting 9japay transactions ingestion")
        log.info("=" * 60)

        df = fetch_all_transactions(pg_conn_params, resume=resume)
        rows_fetched = len(df)

        if df.empty:
            log.warning("No data returned from API.")
            log_run(pg_conn_params, 0, "empty")
            return

        # Deduplicate on PK
        before = len(df)
        df = df.drop_duplicates(subset=[PK_COL], keep="last")
        if len(df) < before:
            log.warning(f"Dropped {before - len(df)} duplicate rows on '{PK_COL}'")

        # Table setup
        conn = psycopg2.connect(**pg_conn_params)
        try:
            create_table_if_missing(df, conn)
            add_missing_columns(df, conn)
            ensure_unique_constraint(conn)
            ensure_indexes(conn)
        finally:
            conn.close()

        # Upsert
        upsert_dataframe(df, pg_conn_params)

        # Clear checkpoint on success
        clear_checkpoint(pg_conn_params)

        log.info(f"✅ Done in {round(time.time() - start, 2)}s")
        log_run(pg_conn_params, rows_fetched, "success")

    except Exception as e:
        log.error(f"❌ Fatal error: {e}")
        log_run(pg_conn_params, rows_fetched, "failed", str(e))
        raise


if __name__ == "__main__":
    import os

    PG_PARAMS = {
        "database": os.getenv("PG_DB",       "PROD_ANALYTICS_DB"),
        "user":     os.getenv("PG_USER",      "doadmin"),
        "password": os.getenv("PG_PASSWORD",  "AVNS_N8t5eaEFwaRi2nAe-qG"),
        "host":     os.getenv("PG_HOST",      "postgres-db-do-user-14235175-0.c.db.ondigitalocean.com"),
        "port":     os.getenv("PG_PORT",      "25060"),
    }


    # resume=True  → continue from last checkpoint if script crashed
    # resume=False → start fresh from page 1
    run_full_ingestion(PG_PARAMS, resume=False)
