import requests
import urllib3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import json
import time
import logging
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

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
    "secret":       os.getenv("9JAPAY_SECRET"),
    "api-key":      os.getenv("9JAPAY_API_KEY"),
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
TIMESTAMP_COL   = "transactionDate"
LOOKBACK_DAYS   = 3


# ---------------------------------------------------------------------------
# Postgres — get last max timestamp for incremental load
# ---------------------------------------------------------------------------

def get_last_max_timestamp(pg_conn_params: dict):
    """Returns the max transactionDate from the existing table, or None if table doesn't exist."""
    conn = psycopg2.connect(**pg_conn_params)
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT MAX("{TIMESTAMP_COL}") FROM "{SCHEMA}"."{TABLE_NAME}";')
            result = cur.fetchone()
            raw = result[0] if result and result[0] else None
            log.info(f"MAX('{TIMESTAMP_COL}') raw value from DB: {repr(raw)}")
            return raw
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        log.info("Table does not exist yet — will run full load.")
        return None
    except psycopg2.errors.UndefinedColumn:
        conn.rollback()
        log.warning(f"Column '{TIMESTAMP_COL}' not found in table — will run full load.")
        return None
    except Exception as e:
        conn.rollback()
        log.warning(f"Could not read max timestamp: {e} — will run full load.")
        return None
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
# Parse transactionDate to comparable datetime
# ---------------------------------------------------------------------------

def parse_tx_date(value) -> datetime | None:
    """Parse 9japay transactionDate string to datetime for comparison."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    try:
        ts = pd.to_datetime(str(value), utc=False)
        return ts.to_pydatetime().replace(tzinfo=None)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Fetch pages incrementally — stop early when we hit records older than from_date
# ---------------------------------------------------------------------------

def fetch_incremental(from_date: datetime) -> pd.DataFrame:
    """
    Fetches only records at or after from_date.
    Assumes newest-first ordering from the API (standard for financial APIs).
    Stops pagination when all records on a page are older than from_date.
    """
    all_data = []
    page = 1
    stopped_early = False

    while True:
        log.info(f"Fetching page {page}...")
        result = fetch_page(page)

        if result is None:
            log.warning(f"Page {page} failed after all retries. Skipping.")
            page += 1
            continue

        if not result:
            log.info(f"No more data at page {page}. Fetch complete.")
            break

        # Filter records newer than from_date
        new_records = []
        old_count = 0
        for record in result:
            tx_date = parse_tx_date(record.get(TIMESTAMP_COL))
            if tx_date is not None and tx_date >= from_date:
                new_records.append(record)
            else:
                old_count += 1

        all_data.extend(new_records)
        log.info(f"Page {page}: {len(result)} fetched, {len(new_records)} within window, {old_count} older (skipped)")

        # Early-exit: if every record on this page is older than from_date, stop paginating
        if old_count == len(result):
            log.info(f"All records on page {page} are older than {from_date.date()}. Stopping early.")
            stopped_early = True
            break

        page += 1

    if not stopped_early:
        log.info("Paginated through all available records.")

    log.info(f"Total records in window: {len(all_data):,}")
    return pd.DataFrame(all_data)


# ---------------------------------------------------------------------------
# Full fetch (no date filter) — used when table is empty
# ---------------------------------------------------------------------------

def fetch_all() -> pd.DataFrame:
    """Full load — fetches all pages. Used when no existing data in DB."""
    log.info("Running full load (no existing data)...")
    all_data = []
    page = 1

    while True:
        log.info(f"Fetching page {page}...")
        result = fetch_page(page)

        if result is None:
            page += 1
            continue

        if not result:
            log.info(f"No more data at page {page}. Fetch complete.")
            break

        all_data.extend(result)
        log.info(f"Page {page}: {len(result)} records (total: {len(all_data):,})")
        page += 1

    log.info(f"Total records fetched: {len(all_data):,}")
    return pd.DataFrame(all_data)


# ---------------------------------------------------------------------------
# Serialize for Postgres
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

    # Safety net — handles numpy.float64 NaN in NumPy 2.x (no longer subclass of float)
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
# PostgreSQL helpers
# ---------------------------------------------------------------------------

def map_dtype_to_postgres(dtype) -> str:
    return {
        "object":         "TEXT",
        "int64":          "BIGINT",
        "float64":        "FLOAT",
        "datetime64[ns]": "TIMESTAMP",
        "bool":           "BOOLEAN",
    }.get(str(dtype), "TEXT")


def setup_table(df: pd.DataFrame, conn):
    cols = ", ".join(
        f'"{col}" {map_dtype_to_postgres(df[col].dtype)}'
        for col in df.columns
    )
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{TABLE_NAME}" ({cols});')

        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s;",
            (SCHEMA, TABLE_NAME),
        )
        existing = {row[0] for row in cur.fetchall()}
        for col in set(df.columns) - existing:
            cur.execute(f'ALTER TABLE "{SCHEMA}"."{TABLE_NAME}" ADD COLUMN "{col}" TEXT;')
            log.info(f"Added column '{col}'")

        constraint = f"uq_{TABLE_NAME}_{PK_COL}".lower()
        cur.execute("""
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_schema = %s AND table_name = %s AND constraint_name = %s;
        """, (SCHEMA, TABLE_NAME, constraint))
        if not cur.fetchone():
            cur.execute(
                f'ALTER TABLE "{SCHEMA}"."{TABLE_NAME}" '
                f'ADD CONSTRAINT {constraint} UNIQUE ("{PK_COL}");'
            )
            log.info(f"Unique constraint added on '{PK_COL}'")

    conn.commit()
    log.info(f"Table '{SCHEMA}.{TABLE_NAME}' ready.")


def ensure_indexes(conn):
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
                ON "{SCHEMA}"."{TABLE_NAME}" ("{col}");
            """)
    conn.commit()
    log.info("Indexes ensured.")


# ---------------------------------------------------------------------------
# Upsert with chunked writes
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

    def _safe_none(x):
        if isinstance(x, (dict, list, str, bytes)):
            return x
        try:
            return None if pd.isna(x) else x
        except (TypeError, ValueError):
            return x

    for i in range(0, total, CHUNK_SIZE):
        chunk = df_clean.iloc[i : i + CHUNK_SIZE]
        rows  = [tuple(_safe_none(x) for x in row) for row in chunk.itertuples(index=False, name=None)]

        conn = psycopg2.connect(**pg_conn_params)
        try:
            with conn.cursor() as cur:
                execute_values(cur, upsert_sql, rows, page_size=CHUNK_SIZE)
            conn.commit()
        finally:
            conn.close()

        upserted += len(rows)
        log.info(f"Progress: {upserted:,}/{total:,} rows upserted")

    log.info(f"Done: {total:,} rows in {round(time.time() - start, 2)}s")


# ---------------------------------------------------------------------------
# Run metadata log
# ---------------------------------------------------------------------------

def log_run(pg_conn_params: dict, rows_fetched: int, status: str, mode: str, error: str = None):
    try:
        conn = psycopg2.connect(**pg_conn_params)
        with conn.cursor() as cur:
            # Create table (matches existing schema from full load — no mode column)
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
            # Add mode column if missing (incremental adds it, full load doesn't have it)
            cur.execute(f"""
                ALTER TABLE "{SCHEMA}"."ingestion_log"
                ADD COLUMN IF NOT EXISTS mode TEXT;
            """)
            cur.execute(f"""
                INSERT INTO "{SCHEMA}"."ingestion_log" (table_name, mode, rows_fetched, status, error)
                VALUES (%s, %s, %s, %s, %s);
            """, (TABLE_NAME, mode, rows_fetched, status, error))
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"Could not write run log: {e}")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_incremental(pg_conn_params: dict):
    rows_fetched = 0
    start = time.time()
    mode = "incremental"

    try:
        log.info("=" * 60)
        log.info("Starting 9japay transactions INCREMENTAL load")
        log.info("=" * 60)

        # Determine window
        last_ts = get_last_max_timestamp(pg_conn_params)

        if last_ts:
            # Parse to datetime if returned as string — pd.to_datetime handles any ISO format
            if not isinstance(last_ts, datetime):
                try:
                    last_ts = pd.to_datetime(last_ts, utc=False).to_pydatetime()
                    last_ts = last_ts.replace(tzinfo=None)  # strip tz for timedelta math
                except Exception as e:
                    log.warning(f"Could not parse timestamp '{last_ts}': {e} — falling back to full load")
                    last_ts = None

        if last_ts:
            from_date = last_ts - timedelta(days=LOOKBACK_DAYS)
            log.info(f"Last '{TIMESTAMP_COL}' in DB: {last_ts}")
            log.info(f"Fetching from {from_date} ({LOOKBACK_DAYS}d lookback)")
            df = fetch_incremental(from_date)
        else:
            mode = "full"
            log.info("No existing data (or unparseable timestamp) — falling back to full load")
            df = fetch_all()

        rows_fetched = len(df)

        if df.empty:
            log.info("No new data to load.")
            log_run(pg_conn_params, 0, "empty", mode)
            return

        # Deduplicate on PK
        before = len(df)
        df = df.drop_duplicates(subset=[PK_COL], keep="last")
        if len(df) < before:
            log.warning(f"Dropped {before - len(df)} duplicate rows on '{PK_COL}'")

        # Table setup
        conn = psycopg2.connect(**pg_conn_params)
        try:
            setup_table(df, conn)
            ensure_indexes(conn)
        finally:
            conn.close()

        # Upsert
        upsert_dataframe(df, pg_conn_params)

        log.info(f"Completed in {round(time.time() - start, 2)}s")
        log_run(pg_conn_params, rows_fetched, "success", mode)

    except Exception as e:
        log.error(f"Fatal error: {e}")
        log_run(pg_conn_params, rows_fetched, "failed", mode, str(e))
        raise


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    PG_PARAMS = {
        "database": os.getenv("PG_DB",      "PROD_ANALYTICS_DB"),
        "user":     os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host":     os.getenv("PG_HOST"),
        "port":     os.getenv("PG_PORT",     "25060"),
    }

    run_incremental(PG_PARAMS)
