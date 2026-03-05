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
load_dotenv()


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------

def transform_orders(orders_df):
    # Renaming 'quantity' to 'totalquantity' as per the request
    orders_df.rename(columns={'quantity': 'totalquantity'}, inplace=True)

    # Exploding 'products' column to create individual rows for each product
    filtered_df_exploded = orders_df.explode('products')

    # Normalize nested product details
    products_df = pd.json_normalize(filtered_df_exploded['products'])

    # Reset indices of both DataFrames to avoid reindexing issues
    filtered_df_exploded = filtered_df_exploded.reset_index(drop=True)
    products_df = products_df.reset_index(drop=True)

    # Concatenate the normalized product details back to the original dataframe
    final_df = pd.concat([filtered_df_exploded.drop(columns=['products']), products_df], axis=1)

    # Drop unwanted columns
    columns_to_drop = ['product.images', 'product.slug', 'product.__v', 'address', 'phoneNumbers']
    final_df = final_df.drop(columns=[col for col in columns_to_drop if col in final_df.columns], errors='ignore')

    print(f"  Columns after dropping unwanted columns: {final_df.columns.tolist()}")

    # Drop duplicate '_id' columns that may appear after exploding
    final_df = final_df.loc[:, ~final_df.columns.duplicated()]

    # Replace NaN in all columns with None
    final_df = final_df.where(pd.notna(final_df), None)

    # Replace NaT in datetime columns with a default timestamp
    default_timestamp = pd.Timestamp('2020-01-01 00:00:00')
    final_df = final_df.apply(lambda col: col.fillna(default_timestamp) if col.dtypes == 'datetime64[ns]' else col)

    return final_df


def transform_products(products_df):
    # Parse and normalize the 'unit' field
    def safe_parse_unit(unit_value):
        try:
            if isinstance(unit_value, str):
                return json.loads(unit_value.replace("'", '"'))
            return unit_value
        except (json.JSONDecodeError, TypeError):
            return {}

    if 'unit' in products_df.columns:
        products_df['unit'] = products_df['unit'].apply(safe_parse_unit)
        products_df['unit_key'] = products_df['unit'].apply(lambda x: list(x.keys())[0] if isinstance(x, dict) and x else None)
        products_df['unit_value'] = products_df['unit'].apply(lambda x: list(x.values())[0] if isinstance(x, dict) and x else None)
        products_df = products_df.drop(columns=['unit'])

    # Explode and normalize 'specialPrices'
    if 'specialPrices' in products_df.columns:
        products_df = products_df.explode('specialPrices')
        # Replace NaN (products with no specialPrices) with empty dict before normalizing
        normalized_col = products_df['specialPrices'].apply(lambda x: x if isinstance(x, dict) else {})
        special_prices_df = pd.json_normalize(normalized_col)
        products_df = products_df.drop(columns=['specialPrices']).join(special_prices_df, rsuffix='_specialPrices')

    # Explode and normalize 'images'
    if 'images' in products_df.columns:
        products_df = products_df.explode('images')
        # Replace NaN (products with no images) with empty dict before normalizing
        normalized_col = products_df['images'].apply(lambda x: x if isinstance(x, dict) else {})
        images_df = pd.json_normalize(normalized_col)
        products_df = products_df.drop(columns=['images']).join(images_df, rsuffix='_images')

    # Remove duplicate indices
    products_df = products_df.loc[~products_df.index.duplicated(keep='first')]

    # Extract additional columns from the 'name' field
    if 'name' in products_df.columns:
        products_df['product_name'] = products_df['name'].str.split('(').str[0].str.strip()
        products_df['product_detail'] = products_df['name'].str.extract(r'\((.*?)\)')

    # Drop unnecessary columns
    columns_to_drop = ['url', 'id', 'email']
    products_df = products_df.drop(columns=columns_to_drop, errors='ignore')

    # Replace NaN with None
    products_df = products_df.where(pd.notna(products_df), None)

    # Replace NaT in datetime columns with a default timestamp
    default_timestamp = pd.Timestamp('2020-01-01 00:00:00')
    products_df = products_df.apply(lambda col: col.fillna(default_timestamp) if col.dtypes == 'datetime64[ns]' else col)

    return products_df


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

def get_last_max_updated_at(conn, table_name, schema="raw_gosource", timestamp_column="updatedAt"):
    with conn.cursor() as cur:
        # Check if table exists first
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            );
        """, (schema, table_name))
        exists = cur.fetchone()[0]
        if not exists:
            print(f"  Table doesn't exist yet — will run full load")
            return None

        sql = f'SELECT MAX("{timestamp_column}") FROM "{schema}"."{table_name}";'
        cur.execute(sql)
        result = cur.fetchone()
        return result[0] if result and result[0] else None


# ---------------------------------------------------------------------------
# MongoDB — fetch only new/updated records with lookback window
# ---------------------------------------------------------------------------

def retrieve_new_records(mongo_uri: str, db_name: str, collection_name: str,
                         last_updated_at=None, lookback_days: int = 7) -> pd.DataFrame:
    print(f"  Connecting to MongoDB...")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=90000)
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


def create_table_if_missing(df: pd.DataFrame, table_name: str, conn, schema: str = "raw_gosource"):
    cols = ", ".join(
        f'"{col}" {map_dtype_to_postgres(df[col].dtype)}'
        for col in df.columns
    )
    sql = f'CREATE TABLE IF NOT EXISTS "{schema}"."{table_name}" ({cols});'
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    print(f"  Table '{schema}.{table_name}' ready.")


def add_missing_columns(df: pd.DataFrame, table_name: str, conn, schema: str = "raw_gosource"):
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


def fix_column_types(df: pd.DataFrame, table_name: str, conn, schema: str = "raw_gosource") -> list:
    """Alter numeric DB columns to TEXT when the DataFrame has object-dtype data for them.
    Returns list of columns that could NOT be altered — caller drops them from df."""
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


def ensure_unique_constraint(conn, table_name: str, pk_columns: list, schema: str = "raw_gosource"):
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
    schema: str = "raw_gosource",
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
        chunk = df_clean.iloc[start: start + chunk_size]
        rows = [
            tuple(_safe_none(x) for x in row)
            for row in chunk.itertuples(index=False, name=None)
        ]

        conn = psycopg2.connect(**pg_conn_params)
        try:
            with conn.cursor() as cur:
                execute_values(cur, upsert_sql, rows, page_size=chunk_size)
            conn.commit()
        finally:
            conn.close()

        upserted += len(rows)
        print(f"  {upserted:,}/{total:,} rows upserted...")

    print(f"  Done — {total:,} rows upserted into {schema}.{table_name}")


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
    schema: str = "raw_gosource",
    timestamp_column: str = "updatedAt",
    lookback_days: int = 7,
    transform: str = None,
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

        # Apply transform if specified
        if transform == "transform_orders":
            print(f"  Applying transform_orders...")
            df = transform_orders(df)
        elif transform == "transform_products":
            print(f"  Applying transform_products...")
            df = transform_products(df)

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
        print(f"  Fatal error for '{collection_name}': {e}")


# ---------------------------------------------------------------------------
# Ingestion jobs
# ---------------------------------------------------------------------------

INGESTION_JOBS = [
    {"mongo_collection": "orders",            "pg_table": "receipts",   "pk_columns": ["_id"],                                        "timestamp_column": "updatedAt", "transform": None},
    {"mongo_collection": "timelines",         "pg_table": "timelines",  "pk_columns": ["_id"],                                        "timestamp_column": "updatedAt", "transform": None},
    {"mongo_collection": "businesscustomers", "pg_table": "customers",  "pk_columns": ["_id"],                                        "timestamp_column": "updatedAt", "transform": None},
    {"mongo_collection": "categories",        "pg_table": "categories", "pk_columns": ["_id"],                                        "timestamp_column": "updatedAt", "transform": None},
    {"mongo_collection": "products",          "pg_table": "products",   "pk_columns": ["_id"],                                        "timestamp_column": "updatedAt", "transform": "transform_products"},
    {"mongo_collection": "orders",            "pg_table": "orders",     "pk_columns": ["_id", "cartProduct._id", "product._id"],       "timestamp_column": "updatedAt", "transform": "transform_orders"},
]


def run_all(mongo_uri: str, db_name: str, pg_conn_params: dict, schema: str = "raw_gosource"):
    for job in INGESTION_JOBS:
        print(f"\n--- {job['mongo_collection']} -> {job['pg_table']} ---")
        run_incremental_ingestion(
            mongo_uri=mongo_uri,
            db_name=db_name,
            collection_name=job["mongo_collection"],
            pg_conn_params=pg_conn_params,
            table_name=job["pg_table"],
            pk_columns=job["pk_columns"],
            schema=schema,
            timestamp_column=job["timestamp_column"],
            transform=job.get("transform"),
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mongo_uri = os.getenv("GOSOURCE_URL")
    pg_conn_params = {
        "database": "PROD_ANALYTICS_DB",
        "user":     os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "host":     os.getenv("PG_HOST"),
        "port":     os.getenv("PG_PORT", "25060"),
    }
    run_all(mongo_uri, "ipc_db", pg_conn_params)