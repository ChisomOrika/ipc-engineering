import json
from datetime import datetime
import pandas as pd
from bson import ObjectId
import pymongo
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
import os
from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------------
# Serialization — only touches columns that actually need it
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

    # Safety net — catch any "NaT" strings that slipped through
    df = df.map(lambda x: None if isinstance(x, str) and x == "NaT" else x)

    return df


# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------

def retrieve_all_records(mongo_uri: str, db_name: str, collection_name: str) -> pd.DataFrame:
    print(f"  Connecting to MongoDB...")
    client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=90000)
    db = client[db_name]

    count = db[collection_name].count_documents({})
    print(f"  {count:,} documents found in '{collection_name}'")

    docs = list(db[collection_name].find())
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


def create_table_if_missing(df: pd.DataFrame, table_name: str, conn, schema: str = "DASH"):
    cols = ", ".join(
        f'"{col}" {map_dtype_to_postgres(df[col].dtype)}'
        for col in df.columns
    )
    sql = f'CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" ({cols});'
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"  Table '{schema}.{table_name}' ready.")


def add_missing_columns(df: pd.DataFrame, table_name: str, conn, schema: str = "DASH"):
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


# ---------------------------------------------------------------------------
# Fast bulk insert — fresh connection per chunk to avoid SSL timeouts
# ---------------------------------------------------------------------------

def insert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    pg_conn_params: dict,
    schema: str = "DASH",
    chunk_size: int = 10_000,
):
    print(f"  Serializing {len(df):,} rows...")
    df_clean = serialize_dataframe(df)

    columns_str = ", ".join(f'"{c}"' for c in df_clean.columns)
    insert_sql = f'INSERT INTO "{schema}"."{table_name}" ({columns_str}) VALUES %s'

    total = len(df_clean)
    inserted = 0

    for start in range(0, total, chunk_size):
        chunk = df_clean.iloc[start : start + chunk_size]
        rows = [tuple(row) for row in chunk.itertuples(index=False, name=None)]

        # Fresh connection per chunk — prevents SSL EOF on large collections
        conn = psycopg2.connect(**pg_conn_params)
        try:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, rows, page_size=chunk_size)
            conn.commit()
        finally:
            conn.close()

        inserted += len(rows)
        print(f"  ⏳ {inserted:,}/{total:,} rows inserted...")

    print(f"  ✅ {total:,} rows loaded into {schema}.{table_name}")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_full_ingestion(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    pg_conn_params: dict,
    table_name: str,
    schema: str = "DASH",
):
    try:
        df = retrieve_all_records(mongo_uri, db_name, collection_name)
        if df.empty:
            print(f"  No data in '{collection_name}', skipping.")
            return

        # Single connection for table setup only
        conn = psycopg2.connect(**pg_conn_params)
        try:
            create_table_if_missing(df, table_name, conn, schema)
            add_missing_columns(df, table_name, conn, schema)
        finally:
            conn.close()

        # Chunked inserts with fresh connections to handle SSL timeouts
        insert_dataframe(df, table_name, pg_conn_params, schema)

    except Exception as e:
        print(f"  ❌ Fatal error for '{collection_name}': {e}")


INGESTION_JOBS = [
    {"mongo_collection": "orders",        "pg_table": "orders"},
    {"mongo_collection": "menuitems",     "pg_table": "menuitems"},
    {"mongo_collection": "subscriptions", "pg_table": "subscriptions"},
    {"mongo_collection": "users",         "pg_table": "users"},
]


def run_all_full_ingestion(mongo_uri: str, db_name: str, pg_conn_params: dict, schema: str = "DASH"):
    for job in INGESTION_JOBS:
        print(f"\n--- {job['mongo_collection']} ---")
        run_full_ingestion(
            mongo_uri=mongo_uri,
            db_name=db_name,
            collection_name=job["mongo_collection"],
            pg_conn_params=pg_conn_params,
            table_name=job["pg_table"],
            schema=schema,
        )


# Usage example
if __name__ == "__main__":
    mongo_uri = os.getenv("DASH_URL")
    pg_conn_params = {
        "database": "PROD_ANALYTICS_DB",
        "user":     os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host":     os.getenv("PG_HOST"),
        "port":     os.getenv("PG_PORT"),
    }
    run_all_full_ingestion(mongo_uri, "daash", pg_conn_params, schema="raw_dash")
