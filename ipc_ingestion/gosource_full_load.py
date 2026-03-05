import json
from datetime import datetime
import pandas as pd
from bson import ObjectId
import pymongo
import psycopg2
from psycopg2.extras import execute_values
import os
from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------

def transform_orders(df):
    df = df.rename(columns={"quantity": "totalquantity"})
    df = df.explode("products").reset_index(drop=True)
    products = pd.json_normalize(df.pop("products"))
    products = products.reset_index(drop=True)
    df = pd.concat([df, products], axis=1)

    drop_cols = ["product.images", "product.slug", "product.__v", "address", "phoneNumbers"]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def transform_products(df):
    def safe_parse_unit(v):
        if isinstance(v, str):
            try:
                return json.loads(v.replace("'", '"'))
            except (json.JSONDecodeError, TypeError):
                return {}
        return v if isinstance(v, dict) else {}

    if "unit" in df.columns:
        parsed = df["unit"].apply(safe_parse_unit)
        df["unit_key"] = parsed.apply(lambda x: next(iter(x), None) if x else None)
        df["unit_value"] = parsed.apply(lambda x: next(iter(x.values()), None) if x else None)
        df = df.drop(columns=["unit"])

    if "specialPrices" in df.columns:
        df = df.explode("specialPrices").reset_index(drop=True)
        sp = pd.json_normalize(df.pop("specialPrices"))
        sp = sp.reset_index(drop=True)
        df = pd.concat([df, sp.add_suffix("_specialPrices")], axis=1)

    if "images" in df.columns:
        df = df.explode("images").reset_index(drop=True)
        img = pd.json_normalize(df.pop("images"))
        img = img.reset_index(drop=True)
        df = pd.concat([df, img.add_suffix("_images")], axis=1)

    df = df.reset_index(drop=True)

    if "name" in df.columns:
        df["product_name"] = df["name"].str.split("(").str[0].str.strip()
        df["product_detail"] = df["name"].str.extract(r"\((.*?)\)")

    df = df.drop(columns=["url", "id", "email"], errors="ignore")
    return df


# ---------------------------------------------------------------------------
# Serialization — handles ObjectId in ALL cells, not just first sample
# ---------------------------------------------------------------------------

def serialize_dataframe(df):
    df = df.copy()

    # First pass: blanket convert ALL ObjectId values in every cell
    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x) if isinstance(x, ObjectId) else x)

    # Second pass: handle dicts, lists, datetimes
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

    # Vectorized datetime to string
    for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]).columns:
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S")

    df = df.where(df.notna(), None)
    return df


# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------

def retrieve_all_records(mongo_uri, db_name, collection_name):
    print(f"  Connecting to MongoDB...")
    client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=30000)
    db = client[db_name]
    count = db[collection_name].count_documents({})
    print(f"  {count:,} documents in '{collection_name}'")

    projection = {"__v": 0}
    docs = list(db[collection_name].find({}, projection))
    client.close()
    print(f"  Fetched {len(docs):,} docs")
    return pd.DataFrame(docs)


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------

def map_dtype(dtype):
    return {"object": "TEXT", "int64": "BIGINT", "float64": "FLOAT",
            "datetime64[ns]": "TIMESTAMP", "bool": "BOOLEAN"}.get(str(dtype), "TEXT")


def create_table_if_missing(df, table_name, conn, schema="GOSOURCE"):
    cols = ", ".join(f'"{c}" {map_dtype(df[c].dtype)}' for c in df.columns)
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" ({cols});')
    conn.commit()


def add_missing_columns(df, table_name, conn, schema="GOSOURCE"):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
            (schema, table_name))
        existing = {r[0] for r in cur.fetchall()}
    missing = set(df.columns) - existing
    if missing:
        with conn.cursor() as cur:
            for c in missing:
                cur.execute(f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{c}" TEXT;')
        conn.commit()


# ---------------------------------------------------------------------------
# Insert — single connection, chunked execute_values
# ---------------------------------------------------------------------------

def insert_dataframe(df, table_name, pg_conn_params, schema="GOSOURCE"):
    print(f"  Serializing {len(df):,} rows...")
    df_clean = serialize_dataframe(df)

    cols = ", ".join(f'"{c}"' for c in df_clean.columns)
    sql = f'INSERT INTO "{schema}"."{table_name}" ({cols}) VALUES %s'
    rows = [tuple(r) for r in df_clean.itertuples(index=False, name=None)]

    conn = psycopg2.connect(**pg_conn_params)
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)
        conn.commit()
        print(f"  ✅ {len(rows):,} rows loaded into {schema}.{table_name}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_full_ingestion(mongo_uri, db_name, collection_name, pg_conn_params,
                       table_name, schema="GOSOURCE", transform=None):
    df = retrieve_all_records(mongo_uri, db_name, collection_name)
    if df.empty:
        print(f"  No data, skipping.")
        return

    if transform == "transform_orders":
        df = transform_orders(df)
    elif transform == "transform_products":
        df = transform_products(df)

    conn = psycopg2.connect(**pg_conn_params)
    try:
        create_table_if_missing(df, table_name, conn, schema)
        add_missing_columns(df, table_name, conn, schema)
    finally:
        conn.close()

    insert_dataframe(df, table_name, pg_conn_params, schema)


INGESTION_JOBS = [
    {"mongo_collection": "orders",   "pg_table": "orders",   "transform": "transform_orders"},
]


if __name__ == "__main__":
    mongo_uri = os.getenv("GOSOURCE_URL")
    pg_params = {
        "database": "PROD_ANALYTICS_DB",
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host": os.getenv("PG_HOST"),
        "port": os.getenv("PG_PORT"),
    }
    for job in INGESTION_JOBS:
        print(f"\n--- {job['mongo_collection']} -> {job['pg_table']} ---")
        run_full_ingestion(mongo_uri, "ipc_db", job["mongo_collection"],
                           pg_params, job["pg_table"], transform=job.get("transform"))
