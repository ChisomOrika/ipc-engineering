import ssl

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

import json
from datetime import datetime, timedelta
import pandas as pd
from bson import ObjectId
from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
import os
from dotenv import load_dotenv
import certifi
load_dotenv()


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------

def serialize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in df.columns:
        sample = df[col].dropna()
        if sample.empty:
            continue

        first = sample.iloc[0]

        if isinstance(first, ObjectId):
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, ObjectId) else x)

        elif isinstance(first, (dict, list)):
            df[col] = df[col].apply(
                lambda x: json.dumps(x, default=str) if isinstance(x, (dict, list)) else x
            )

        elif isinstance(first, datetime):
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Convert datetime columns to isoformat strings — NaT becomes None
    for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df[col] = df[col].apply(lambda x: None if pd.isnull(x) else x.isoformat())

    # Replace NaN/NaT with None
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
# PostgreSQL — get last max timestamp for incremental load
# ---------------------------------------------------------------------------

def get_last_max_updated_at(conn, table_name, schema="raw_dash", timestamp_column="updatedAt"):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                );
            """, (schema, table_name))
            if not cur.fetchone()[0]:
                print(f"  Table doesn't exist yet — will run full load")
                return None
            sql = f'SELECT MAX("{timestamp_column}") FROM "{schema}"."{table_name}";'
            cur.execute(sql)
            result = cur.fetchone()
            return result[0] if result and result[0] else None
    except Exception as e:
        conn.rollback()
        print(f"  Could not read max timestamp: {e} — running full load")
        return None


# ---------------------------------------------------------------------------
# MongoDB — fetch only new/updated records with lookback window
# ---------------------------------------------------------------------------

def retrieve_new_records(mongo_uri: str, db_name: str, collection_name: str,
                         last_updated_at=None, lookback_days: int = 2) -> pd.DataFrame:
    print(f"  Connecting to MongoDB...")
    client = MongoClient(mongo_uri, 
                         serverSelectionTimeoutMS=90000,
                         tls=True,
                        tlsAllowInvalidCertificates=True)
    db = client[db_name]

    if last_updated_at:
        query_start = last_updated_at - timedelta(days=lookback_days)
        query = {"updatedAt": {"$gte": query_start}}
        print(f"  Fetching records updated since {query_start} (with {lookback_days}d lookback)")
    else:
        query = {}
        print(f"  No existing data found — running full load")

    count = db[collection_name].count_documents(query)
    print(f"  {count:,} documents matched in '{collection_name}'")

    docs = list(db[collection_name].find(query))
    client.close()
    return pd.DataFrame(docs)


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------

def map_dtype_to_postgres(dtype) -> str:
    mapping = {
        "object": "TEXT",
        "int64": "BIGINT",
        "float64": "FLOAT",
        "datetime64[ns]": "TIMESTAMP",
        "bool": "BOOLEAN",
    }
    return mapping.get(str(dtype), "TEXT")


def create_table_if_missing(df: pd.DataFrame, table_name: str, conn, schema: str = "raw_dash"):
    cols = ", ".join(
        f'"{col}" {map_dtype_to_postgres(df[col].dtype)}'
        for col in df.columns
    )
    sql = f'CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" ({cols});'
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"  Table '{schema}.{table_name}' ready.")


def add_missing_columns(df: pd.DataFrame, table_name: str, conn, schema: str = "raw_dash"):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s;",
            (schema, table_name),
        )
        existing = {row[0] for row in cur.fetchall()}

    missing = set(df.columns) - existing
    if missing:
        with conn.cursor() as cur:
            for col in missing:
                cur.execute(f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{col}" TEXT;')
                print(f"  Added column '{col}' to {table_name}.")
        conn.commit()


def fix_column_types(df: pd.DataFrame, table_name: str, conn, schema: str = "raw_dash") -> list:
    """Alter numeric DB columns to TEXT when the DataFrame has object-dtype data for them.
    Returns list of columns that could NOT be altered (e.g. view dependency) — caller drops them from df."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s;",
            (schema, table_name),
        )
        col_types = {row[0]: row[1] for row in cur.fetchall()}

    numeric_pg_types = {"double precision", "real", "bigint", "integer", "smallint", "numeric"}
    to_alter = [
        col for col in df.columns
        if col in col_types
        and col_types[col] in numeric_pg_types
        and str(df[col].dtype) == "object"
    ]
    failed = []
    for col in to_alter:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'ALTER TABLE "{schema}"."{table_name}" ALTER COLUMN "{col}" TYPE TEXT USING "{col}"::TEXT;'
                )
            conn.commit()
            print(f"  Altered column '{col}' to TEXT in {table_name}.")
        except Exception as e:
            conn.rollback()
            print(f"  Skipping ALTER on '{col}' (view dependency — will exclude from upsert): {e}")
            failed.append(col)
    return failed


def ensure_unique_constraint(conn, table_name: str, pk_columns: list, schema: str = "raw_dash"):
    constraint_name = f"uq_{table_name}_{pk_columns[0].replace('.', '_').replace(' ', '_')}"
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM information_schema.table_constraints
            WHERE table_schema = %s AND table_name = %s AND constraint_name = %s;
        """, (schema, table_name, constraint_name))
        if cur.fetchone()[0]:
            return  # already exists
    # Try to add; if existing rows have duplicates, deduplicate the table first
    cols_sql = ", ".join(f'"{pk}"' for pk in pk_columns)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'ALTER TABLE "{schema}"."{table_name}" ADD CONSTRAINT {constraint_name} UNIQUE ({cols_sql});'
            )
        conn.commit()
        print(f"  Unique constraint added on {pk_columns} in {table_name}.")
    except Exception:
        conn.rollback()
        # Deduplicate existing rows (keep row with highest ctid per pk), then retry
        print(f"  Deduplicating '{table_name}' on {pk_columns} before adding constraint...")
        with conn.cursor() as cur:
            cur.execute(f"""
                DELETE FROM "{schema}"."{table_name}" t1
                USING "{schema}"."{table_name}" t2
                WHERE t1.ctid < t2.ctid
                  AND {" AND ".join(f't1."{pk}" = t2."{pk}"' for pk in pk_columns)};
            """)
        conn.commit()
        with conn.cursor() as cur:
            cur.execute(
                f'ALTER TABLE "{schema}"."{table_name}" ADD CONSTRAINT {constraint_name} UNIQUE ({cols_sql});'
            )
        conn.commit()
        print(f"  Unique constraint added on {pk_columns} in {table_name} (after dedup).")


# ---------------------------------------------------------------------------
# Upsert with execute_values — fresh connection per chunk to avoid SSL drops
# ---------------------------------------------------------------------------

def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    pg_conn_params: dict,
    pk_columns: list,
    schema: str = "raw_dash",
    chunk_size: int = 10_000,
):
    print(f"  Serializing {len(df):,} rows...")
    df_clean = serialize_dataframe(df)

    columns = [f'"{c}"' for c in df_clean.columns]
    columns_str = ", ".join(columns)
    conflict_str = ", ".join(f'"{pk}"' for pk in pk_columns)
    update_str = ", ".join(
        f'"{c}"=EXCLUDED."{c}"' for c in df_clean.columns if c not in pk_columns
    )

    upsert_sql = f"""
        INSERT INTO "{schema}"."{table_name}" ({columns_str}) VALUES %s
        ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str};
    """

    total = len(df_clean)
    upserted = 0

    def _safe_none(x):
        if isinstance(x, (dict, list, str, bytes)):
            return x
        try:
            return None if pd.isna(x) else x
        except (TypeError, ValueError):
            return x

    for start in range(0, total, chunk_size):
        chunk = df_clean.iloc[start : start + chunk_size]
        rows = [
            tuple(_safe_none(x) for x in row)
            for row in chunk.itertuples(index=False, name=None)
        ]

        # Fresh connection per chunk — prevents SSL EOF on large collections
        conn = psycopg2.connect(**pg_conn_params)
        try:
            with conn.cursor() as cur:
                execute_values(cur, upsert_sql, rows, page_size=chunk_size)
            conn.commit()
        finally:
            conn.close()

        upserted += len(rows)
        print(f"  ⏳ {upserted:,}/{total:,} rows upserted...")

    print(f"  ✅ {total:,} rows upserted into {schema}.{table_name}")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_incremental_ingestion(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    pg_conn_params: dict,
    table_name: str,
    pk_columns: list,
    schema: str = "raw_dash",
    timestamp_column: str = "updatedAt",
    lookback_days: int = 2,
):
    try:
        # Get last max timestamp from Postgres
        conn = psycopg2.connect(**pg_conn_params)
        try:
            last_updated_at = get_last_max_updated_at(conn, table_name, schema, timestamp_column)
            print(f"  Last '{timestamp_column}' in Postgres: {last_updated_at}")
        finally:
            conn.close()

        # Fetch from MongoDB
        df = retrieve_new_records(mongo_uri, db_name, collection_name, last_updated_at, lookback_days)
        if df.empty:
            print(f"  No new data for '{collection_name}', skipping.")
            return

        # Table setup
        conn = psycopg2.connect(**pg_conn_params)
        try:
            create_table_if_missing(df, table_name, conn, schema)
            add_missing_columns(df, table_name, conn, schema)
            failed_cols = fix_column_types(df, table_name, conn, schema)
            if failed_cols:
                df = df.drop(columns=[c for c in failed_cols if c in df.columns])
            ensure_unique_constraint(conn, table_name, pk_columns, schema)
        finally:
            conn.close()

        # Upsert
        upsert_dataframe(df, table_name, pg_conn_params, pk_columns, schema, chunk_size=10_000)

    except Exception as e:
        print(f"  ❌ Fatal error for '{collection_name}': {e}")


# --- Config for multiple collections/tables ---
INGESTION_JOBS = [
    {"mongo_collection": "orders",            "pg_table": "orders",            "pk_columns": ["_id"]},
    {"mongo_collection": "branches",          "pg_table": "branches",          "pk_columns": ["_id"]},
    {"mongo_collection": "customers",         "pg_table": "customers",         "pk_columns": ["_id"]},
    {"mongo_collection": "discounts",         "pg_table": "discounts",         "pk_columns": ["_id"]},
    {"mongo_collection": "menucategories",    "pg_table": "menucategories",    "pk_columns": ["_id"]},
    {"mongo_collection": "users",             "pg_table": "users",             "pk_columns": ["_id"]},
    {"mongo_collection": "wallets",           "pg_table": "wallets",           "pk_columns": ["_id"]},
    {"mongo_collection": "revenueledgers",    "pg_table": "revenueledgers",    "pk_columns": ["_id"]},
    {"mongo_collection": "products",          "pg_table": "products",          "pk_columns": ["_id"]},
    {"mongo_collection": "subscriptions",     "pg_table": "subscriptions",     "pk_columns": ["_id"]},
    #{"mongo_collection": "deliveries",        "pg_table": "deliveries",        "pk_columns": ["_id"]},
    #{"mongo_collection": "members",           "pg_table": "members",           "pk_columns": ["_id"]},
    #{"mongo_collection": "productcategories", "pg_table": "productcategories", "pk_columns": ["_id"]},
    #{"mongo_collection": "refunds",           "pg_table": "refunds",           "pk_columns": ["_id"]},
    #{"mongo_collection": "relaytransactions", "pg_table": "relaytransactions", "pk_columns": ["_id"]},
    #{"mongo_collection": "relaywallets",      "pg_table": "relaywallets",      "pk_columns": ["_id"]},
    #{"mongo_collection": "subscriptionplans", "pg_table": "subscriptionplans", "pk_columns": ["_id"]},
    #{"mongo_collection": "taxes",             "pg_table": "taxes",             "pk_columns": ["_id"]},
    #{"mongo_collection": "taxledgers",        "pg_table": "taxledgers",        "pk_columns": ["_id"]},
    #{"mongo_collection": "taxtransactions",   "pg_table": "taxtransactions",   "pk_columns": ["_id"]},
    #{"mongo_collection": "transactions",      "pg_table": "transactions",      "pk_columns": ["_id"]},
    #{"mongo_collection": "virtualaccounts",   "pg_table": "virtualaccounts",   "pk_columns": ["_id"]},
]

def run_all_ingestions(mongo_uri, db_name, pg_conn_params, schema="raw_dash"):
    for job in INGESTION_JOBS:
        print(f"\n--- Starting ingestion for '{job['mongo_collection']}' ---")
        try:
            run_incremental_ingestion(
                mongo_uri=mongo_uri,
                db_name=db_name,
                collection_name=job['mongo_collection'],
                pg_conn_params=pg_conn_params,
                table_name=job['pg_table'],
                pk_columns=job['pk_columns'],
                schema=schema,
            )
        except Exception as e:
            print(f"Error in ingestion job for {job['mongo_collection']}: {e}")

if __name__ == "__main__":
    mongo_uri = os.getenv("DASH_URL")
    pg_conn_params = {
        "database": "PROD_ANALYTICS_DB",
        "user":     os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host":     os.getenv("PG_HOST"),
        "port":     os.getenv("PG_PORT", "25060"),
    }
    run_all_ingestions(mongo_uri, "daash", pg_conn_params)
