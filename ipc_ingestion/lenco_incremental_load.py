import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import json
import time
import logging
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from categorizer import categorize_dataframe

load_dotenv()

# ---------------------------------------------------------------------------
# Logging — plain text only, no emojis (Windows encoding safe)
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("lenco_ingestion.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

authorization_token = os.getenv("LENCO_API_TOKEN")


# ---------------------------------------------------------------------------
# API Config
# ---------------------------------------------------------------------------

BASE_URL = "https://api.lenco.ng/access/v1"
HEADERS  = {
    "Authorization": authorization_token,
    "Content-Type":  "application/json",
}

# ---------------------------------------------------------------------------
# Architecture notes:
#
# Lenco pagination uses:
#   - params: page=N, perPage=N
#   - response: data.meta.pageCount tells us total pages
#   - We use pageCount to know when to stop — much more reliable than
#     checking empty data or comparing lengths like the naive approach
#
# Endpoints and their PKs:
#   accounts                    → no pagination (small list) → PK: id
#   recipients                  → paginated                  → PK: id
#   transactions                → paginated                  → PK: id
#   virtual-accounts            → paginated                  → PK: id
#   virtual-accounts/all-transactions → paginated            → PK: id
#   bills                       → paginated                  → PK: id
#   point-of-sale/terminals     → paginated                  → PK: id
#   point-of-sale/transactions  → paginated                  → PK: id
#
# Nested JSON (dict/list) is stored as TEXT in Postgres — raw, no flattening
# ---------------------------------------------------------------------------

PER_PAGE        = 500
MAX_RETRIES     = 3
BACKOFF_BASE    = 2
REQUEST_TIMEOUT = 30
CHUNK_SIZE      = 10_000
SCHEMA          = "raw_lenco"
LOOKBACK_DAYS   = 1


# ---------------------------------------------------------------------------
# Get last max timestamp for incremental load
# ---------------------------------------------------------------------------

def get_last_max_timestamp(pg_conn_params: dict, table_name: str, timestamp_col: str):
    conn = psycopg2.connect(**pg_conn_params)
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT MAX("{timestamp_col}") FROM "{SCHEMA}"."{table_name}"
                WHERE "{timestamp_col}" IS NOT NULL
                  AND "{timestamp_col}" NOT IN ('NaN', 'nan', 'NaT', '');
            """)
            result = cur.fetchone()
            raw = result[0] if result and result[0] else None
            log.info(f"[{table_name}] MAX('{timestamp_col}') from DB: {repr(raw)}")
            if raw is None:
                return None
            if isinstance(raw, datetime):
                return raw.replace(tzinfo=None)
            try:
                parsed = pd.to_datetime(str(raw), utc=False)
                if pd.isnull(parsed):
                    log.warning(f"[{table_name}] MAX returned unparseable value '{raw}' — running full load")
                    return None
                return parsed.to_pydatetime().replace(tzinfo=None)
            except Exception as e:
                log.warning(f"[{table_name}] Could not parse timestamp '{raw}': {e}")
                return None
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        log.info(f"[{table_name}] Table does not exist yet — running full load.")
        return None
    except psycopg2.errors.UndefinedColumn:
        conn.rollback()
        log.warning(f"[{table_name}] Column '{timestamp_col}' not found — running full load.")
        return None
    except Exception as e:
        conn.rollback()
        log.warning(f"[{table_name}] Could not read max timestamp: {e} — running full load.")
        return None
    finally:
        conn.close()


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
# Postgres Checkpointing — one row per table in ingestion_checkpoint
# ---------------------------------------------------------------------------

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


def save_checkpoint(pg_conn_params: dict, table_name: str, page: int, total_fetched: int):
    conn = psycopg2.connect(**pg_conn_params)
    try:
        ensure_checkpoint_table(conn)
        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO "{SCHEMA}"."ingestion_checkpoint"
                    (table_name, last_page, total_fetched, updated_at)
                VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (table_name) DO UPDATE SET
                    last_page     = EXCLUDED.last_page,
                    total_fetched = EXCLUDED.total_fetched,
                    updated_at    = CURRENT_TIMESTAMP;
            """, (table_name, page, total_fetched))
        conn.commit()
        log.info(f"[{table_name}] Checkpoint saved — page {page}, {total_fetched:,} records")
    finally:
        conn.close()


def load_checkpoint(pg_conn_params: dict, table_name: str) -> dict:
    conn = psycopg2.connect(**pg_conn_params)
    try:
        ensure_checkpoint_table(conn)
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT last_page, total_fetched
                FROM "{SCHEMA}"."ingestion_checkpoint"
                WHERE table_name = %s;
            """, (table_name,))
            result = cur.fetchone()
        if result:
            log.info(f"[{table_name}] Resuming from page {result[0]}, {result[1]:,} already fetched")
            return {"last_page": result[0], "total_fetched": result[1]}
        return {"last_page": 1, "total_fetched": 0}
    finally:
        conn.close()


def clear_checkpoint(pg_conn_params: dict, table_name: str):
    conn = psycopg2.connect(**pg_conn_params)
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                DELETE FROM "{SCHEMA}"."ingestion_checkpoint"
                WHERE table_name = %s;
            """, (table_name,))
        conn.commit()
        log.info(f"[{table_name}] Checkpoint cleared")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Single page fetch with retry + exponential backoff
# ---------------------------------------------------------------------------

def fetch_page(endpoint: str, page: int, extra_params: dict = None) -> dict:
    """
    Returns the full JSON response dict on success, None on failure.
    """
    params = {"page": page, "perPage": PER_PAGE}
    if extra_params:
        params.update(extra_params)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                f"{BASE_URL}/{endpoint}",
                headers=HEADERS,
                params=params,
                timeout=REQUEST_TIMEOUT
            )

            if response.status_code == 200:
                return response.json()

            elif response.status_code == 429:
                log.warning(f"[{endpoint}] Rate limited. Waiting 60s...")
                time.sleep(60)

            elif response.status_code == 401:
                log.error(f"[{endpoint}] Unauthorized — check your API token")
                return None

            else:
                log.warning(f"[{endpoint}] Attempt {attempt}/{MAX_RETRIES}: HTTP {response.status_code} — {response.text[:200]}")

        except requests.exceptions.Timeout:
            log.warning(f"[{endpoint}] Attempt {attempt}/{MAX_RETRIES}: Timed out")
        except requests.exceptions.RequestException as e:
            log.warning(f"[{endpoint}] Attempt {attempt}/{MAX_RETRIES}: {e}")

        if attempt < MAX_RETRIES:
            wait = BACKOFF_BASE ** attempt  # 2s, 4s
            log.info(f"Retrying in {wait}s...")
            time.sleep(wait)

    log.error(f"[{endpoint}] All {MAX_RETRIES} attempts failed for page {page}")
    return None


# ---------------------------------------------------------------------------
# Paginated fetch — uses meta.pageCount to know total pages
# This is much more reliable than checking empty data or comparing lengths
# ---------------------------------------------------------------------------

def fetch_paginated(
    endpoint: str,
    data_key: str,
    pg_conn_params: dict,
    table_name: str,
    resume: bool = True,
    extra_params: dict = None,
    from_date: datetime = None,
    timestamp_col: str = None,
) -> pd.DataFrame:
    all_data     = []
    failed_pages = []

    checkpoint = load_checkpoint(pg_conn_params, table_name) if resume else {"last_page": 1, "total_fetched": 0}
    start_page = checkpoint["last_page"]
    page_count = None   # Will be set from first response

    page = start_page
    while True:
        log.info(f"[{table_name}] Fetching page {page}...")
        result = fetch_page(endpoint, page, extra_params)

        if result is None:
            # Failed after all retries — skip and continue
            failed_pages.append(page)
            page += 1
            # If we've never gotten a page_count and already failing, stop
            if page_count is None and len(failed_pages) >= 3:
                log.error(f"[{table_name}] Too many failures before first successful page. Stopping.")
                break
            continue

        # Extract data — handle both list and dict wrapper structures
        raw = result.get("data", {})
        if isinstance(raw, list):
            records = raw
        elif isinstance(raw, dict):
            records = raw.get(data_key, [])
        else:
            records = []

        # Get pagination meta from first successful response
        if page_count is None:
            meta = result.get("data", {})
            if isinstance(meta, dict):
                meta = meta.get("meta", {})
            else:
                meta = result.get("meta", {})
            page_count = meta.get("pageCount", 1)
            total      = meta.get("total", 0)
            log.info(f"[{table_name}] Total records: {total:,} across {page_count} pages")

        if not records:
            log.info(f"[{table_name}] No records on page {page}. Done.")
            break

        # Client-side date filter + early exit (API may not honour startDate)
        if from_date and timestamp_col:
            new_records = []
            old_count   = 0
            for rec in records:
                ts = parse_ts(rec.get(timestamp_col))
                if ts is not None and ts >= from_date:
                    new_records.append(rec)
                else:
                    old_count += 1
            log.info(f"[{table_name}] Page {page}/{page_count}: {len(new_records)} in window, {old_count} older")
            all_data.extend(new_records)
            # Newest-first: if ALL records on this page are older than from_date, stop
            if old_count == len(records):
                log.info(f"[{table_name}] All records on page {page} are older than {from_date.date()}. Stopping early.")
                break
        else:
            all_data.extend(records)
            log.info(f"[{table_name}] Page {page}/{page_count}: {len(records)} records (total so far: {len(all_data):,})")

        save_checkpoint(pg_conn_params, table_name, page + 1, len(all_data))

        # Stop when we've hit the last page
        if page >= page_count:
            log.info(f"[{table_name}] Reached last page ({page_count}). Done.")
            break

        page += 1

    if failed_pages:
        log.warning(f"[{table_name}] Failed pages (skipped): {failed_pages}")

    log.info(f"[{table_name}] Total fetched: {len(all_data):,}")
    return pd.DataFrame(all_data)


# ---------------------------------------------------------------------------
# Non-paginated fetch (accounts — small list, no pagination needed)
# ---------------------------------------------------------------------------

def fetch_single(endpoint: str, data_key: str) -> pd.DataFrame:
    log.info(f"Fetching {endpoint}...")
    result = fetch_page(endpoint, page=1)

    if result is None:
        log.error(f"Failed to fetch {endpoint}")
        return pd.DataFrame()

    raw = result.get("data", [])
    if isinstance(raw, list):
        records = raw
    elif isinstance(raw, dict):
        records = raw.get(data_key, [])
    else:
        records = []

    log.info(f"[{endpoint}] {len(records)} records fetched")
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Serialize for Postgres — raw data only, no transformations
# Nested dicts/lists stored as JSON text
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
    mapping = {
        "object":         "TEXT",
        "int64":          "BIGINT",
        "float64":        "FLOAT",
        "datetime64[ns]": "TIMESTAMP",
        "bool":           "BOOLEAN",
    }
    return mapping.get(str(dtype), "TEXT")


def create_table_if_missing(df: pd.DataFrame, table_name: str, conn):
    cols = ", ".join(
        f'"{col}" {map_dtype_to_postgres(df[col].dtype)}'
        for col in df.columns
    )
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{table_name}" ({cols});')
    conn.commit()
    log.info(f"Table '{SCHEMA}.{table_name}' ready.")


def add_missing_columns(df: pd.DataFrame, table_name: str, conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s;",
            (SCHEMA, table_name),
        )
        existing = {row[0] for row in cur.fetchall()}

    missing = set(df.columns) - existing
    if missing:
        with conn.cursor() as cur:
            for col in missing:
                cur.execute(f'ALTER TABLE "{SCHEMA}"."{table_name}" ADD COLUMN "{col}" TEXT;')
                log.info(f"[{table_name}] Added column '{col}'")
        conn.commit()


def ensure_unique_constraint(table_name: str, pk_col: str, conn):
    constraint_name = f"uq_{table_name}_{pk_col}"
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.table_constraints
            WHERE table_schema = %s AND table_name = %s AND constraint_name = %s;
        """, (SCHEMA, table_name, constraint_name))
        exists = cur.fetchone()[0]

    if not exists:
        with conn.cursor() as cur:
            cur.execute(f'ALTER TABLE "{SCHEMA}"."{table_name}" ADD CONSTRAINT {constraint_name} UNIQUE ("{pk_col}");')
        conn.commit()
        log.info(f"[{table_name}] Unique constraint added on '{pk_col}'")


def ensure_indexes(table_name: str, index_cols: list, conn):
    with conn.cursor() as cur:
        for col in index_cols:
            index_name = f"idx_{table_name}_{col}"
            cur.execute(f"""
                CREATE INDEX IF NOT EXISTS {index_name}
                ON "{SCHEMA}"."{table_name}" ("{col}");
            """)
            log.info(f"[{table_name}] Index ensured: {index_name}")
    conn.commit()


# ---------------------------------------------------------------------------
# Upsert with fresh connection per chunk
# ---------------------------------------------------------------------------

def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    pk_col: str,
    pg_conn_params: dict,
):
    log.info(f"[{table_name}] Serializing {len(df):,} rows...")
    df_clean = serialize_dataframe(df)

    columns_str = ", ".join(f'"{c}"' for c in df_clean.columns)
    update_str  = ", ".join(f'"{c}"=EXCLUDED."{c}"' for c in df_clean.columns if c != pk_col)
    upsert_sql  = f"""
        INSERT INTO "{SCHEMA}"."{table_name}" ({columns_str}) VALUES %s
        ON CONFLICT ("{pk_col}") DO UPDATE SET {update_str};
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
        log.info(f"[{table_name}] Progress: {upserted:,}/{total:,} rows upserted")

    log.info(f"[{table_name}] DONE: {total:,} rows in {round(time.time() - start, 2)}s")


# ---------------------------------------------------------------------------
# Run metadata log
# ---------------------------------------------------------------------------

def log_run(pg_conn_params: dict, table_name: str, rows_fetched: int, status: str, error: str = None):
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
                INSERT INTO "{SCHEMA}"."ingestion_log"
                    (table_name, rows_fetched, status, error)
                VALUES (%s, %s, %s, %s);
            """, (table_name, rows_fetched, status, error))
        conn.commit()
        conn.close()
    except Exception as e:
        log.warning(f"Could not write run log: {e}")


# ---------------------------------------------------------------------------
# Generic ingestion job runner
# ---------------------------------------------------------------------------

def run_job(
    endpoint: str,
    table_name: str,
    pk_col: str,
    data_key: str,
    index_cols: list,
    pg_conn_params: dict,
    paginated: bool = True,
    resume: bool = True,
    extra_params: dict = None,
    from_date: datetime = None,
    timestamp_col: str = None,
):
    rows_fetched = 0
    start = time.time()
    log.info(f"--- Starting: {table_name} ---")

    try:
        # Fetch
        if paginated:
            df = fetch_paginated(endpoint, data_key, pg_conn_params, table_name, resume, extra_params, from_date, timestamp_col)
        else:
            df = fetch_single(endpoint, data_key)

        rows_fetched = len(df)

        if df.empty:
            log.warning(f"[{table_name}] No data returned. Skipping.")
            log_run(pg_conn_params, table_name, 0, "empty")
            return

        # Deduplicate on PK
        if pk_col in df.columns:
            before = len(df)
            df = df.drop_duplicates(subset=[pk_col], keep="last")
            dropped = before - len(df)
            if dropped > 0:
                log.warning(f"[{table_name}] Dropped {dropped} duplicate rows on '{pk_col}'")

        # Categorize transactions narration
        if table_name == "transactions" and "narration" in df.columns:
            df = categorize_dataframe(df, narration_col="narration", txn_type_col="type")
            log.info(f"[{table_name}] Narration categorization applied.")

        # Table setup
        conn = psycopg2.connect(**pg_conn_params)
        try:
            create_table_if_missing(df, table_name, conn)
            add_missing_columns(df, table_name, conn)
            if pk_col in df.columns:
                ensure_unique_constraint(table_name, pk_col, conn)
            ensure_indexes(table_name, index_cols, conn)
        finally:
            conn.close()

        # Upsert
        upsert_dataframe(df, table_name, pk_col, pg_conn_params)

        # Clear checkpoint on success
        if paginated:
            clear_checkpoint(pg_conn_params, table_name)

        log.info(f"[{table_name}] Completed in {round(time.time() - start, 2)}s")
        log_run(pg_conn_params, table_name, rows_fetched, "success")

    except Exception as e:
        log.error(f"[{table_name}] FATAL ERROR: {e}")
        log_run(pg_conn_params, table_name, rows_fetched, "failed", str(e))
        raise


# ---------------------------------------------------------------------------
# Job definitions — one entry per table
#
# endpoint:     Lenco API path after base URL
# table_name:   Postgres table name in lenco schema
# pk_col:       Primary key for upsert dedup
# data_key:     Key inside data{} that holds the list
# index_cols:   Columns to index for query performance
# paginated:    True for list endpoints, False for single-response endpoints
# ---------------------------------------------------------------------------

INGESTION_JOBS = [
    {
        "endpoint":   "accounts",
        "table_name": "accounts",
        "pk_col":     "id",
        "data_key":   "accounts",
        "index_cols": ["createdAt"],
        "paginated":  False,   # Small list, no pagination needed
    },
    {
        "endpoint":     "transactions",
        "table_name":   "transactions",
        "pk_col":       "id",
        "data_key":     "transactions",
        "index_cols":   ["initiatedAt", "completedAt", "accountId", "status"],
        "paginated":    True,
        "timestamp_col": "initiatedAt",   # Used to compute startDate for incremental
        "date_param":   "startDate",       # Lenco API param name for date filtering
    },
    {
        "endpoint":   "virtual-accounts",
        "table_name": "virtual_accounts",
        "pk_col":     "id",
        "data_key":   "virtualAccounts",
        "index_cols": ["createdAt", "status"],
        "paginated":  True,
    },
    {
        "endpoint":     "virtual-accounts/all-transactions",
        "table_name":   "virtual_account_transactions",
        "pk_col":       "id",
        "data_key":     "transactions",
        "index_cols":   ["datetime", "accountReference", "status"],
        "paginated":    True,
        "timestamp_col": "datetime",
        "date_param":   "startDate",
    },
    {
        "endpoint":   "point-of-sale/terminals",
        "table_name": "pos_terminals",
        "pk_col":     "id",
        "data_key":   "terminals",
        "index_cols": ["status", "assignedAt"],
        "paginated":  True,
    },
]


# ---------------------------------------------------------------------------
# Run all jobs
# ---------------------------------------------------------------------------

def run_all(pg_conn_params: dict, resume: bool = True):
    log.info("=" * 60)
    log.info("Starting Lenco ingestion (all endpoints)")
    log.info("=" * 60)

    for job in INGESTION_JOBS:
        try:
            # For tables with a timestamp column, compute startDate for incremental load
            extra_params  = None
            from_date     = None
            timestamp_col = job.get("timestamp_col")
            date_param    = job.get("date_param")
            if timestamp_col and date_param:
                last_ts = get_last_max_timestamp(pg_conn_params, job["table_name"], timestamp_col)
                if last_ts:
                    from_date    = last_ts - timedelta(days=LOOKBACK_DAYS)
                    extra_params = {date_param: from_date.strftime("%Y-%m-%d")}
                    log.info(f"[{job['table_name']}] Incremental from {from_date.date()} ({LOOKBACK_DAYS}d lookback)")
                else:
                    log.info(f"[{job['table_name']}] No existing data — running full load")

            run_job(
                endpoint       = job["endpoint"],
                table_name     = job["table_name"],
                pk_col         = job["pk_col"],
                data_key       = job["data_key"],
                index_cols     = job["index_cols"],
                pg_conn_params = pg_conn_params,
                paginated      = job.get("paginated", True),
                resume         = resume if not from_date else False,
                extra_params   = extra_params,
                from_date      = from_date,
                timestamp_col  = timestamp_col,
            )
        except Exception as e:
            # Log and continue to next job — don't let one failure stop everything
            log.error(f"Job failed for '{job['table_name']}': {e}. Moving to next job.")
            continue

    log.info("=" * 60)
    log.info("Lenco ingestion complete")
    log.info("=" * 60)


if __name__ == "__main__":
    PG_PARAMS = {
    "database": "PROD_ANALYTICS_DB",
    "user":     os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "host":     os.getenv("PG_HOST"),
    "port":     os.getenv("PG_PORT", "25060"),
}


    # resume=True  → continue from last checkpoint if script crashed mid-run
    # resume=False → start everything fresh from page 1
    run_all(PG_PARAMS, resume=True)