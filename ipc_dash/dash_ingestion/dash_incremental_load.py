
from datetime import timedelta
from pymongo import MongoClient
import pandas as pd
import psycopg2
import json
from datetime import datetime
import numpy as np
from bson import ObjectId
from db_credentials import db_connection_parameters_dash


def handle_non_serializable(x):
    try:
        if isinstance(x, dict):
            return {k: handle_non_serializable(v) for k, v in x.items()}
        elif isinstance(x, list):
            return [handle_non_serializable(i) for i in x]
        elif isinstance(x, ObjectId):
            return str(x)
        elif isinstance(x, (datetime, np.datetime64, pd.Timestamp)):
            if pd.isna(x):  # Check if the value is NaT (Not a Time)
                return None  # Convert NaT to NULL in PostgreSQL
            return pd.to_datetime(x).isoformat()
        else:
            return x
    except Exception as e:
        print(f"Error handling {x}: {e}")
        return None
    

def get_last_max_updated_at(conn, table_name, schema="DASH", timestamp_column="updatedAt"):
    with conn.cursor() as cur:
        sql = f'SELECT MAX("{timestamp_column}") FROM "{schema}"."{table_name}";'
        cur.execute(sql)
        result = cur.fetchone()
        return result[0] if result and result[0] else None



def retrieve_new_records_with_lookback(mongo_uri, db_name, collection_name, last_updated_at=None, lookback_days=1):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    
    if last_updated_at:
        # Subtract lookback_days from last_updated_at to create the query start time
        query_start = last_updated_at - timedelta(days=lookback_days)
        query = {"updatedAt": {"$gte": query_start}}
    else:
        # No last_updated_at means full load; no filter
        query = {}

    docs = list(db[collection_name].find(query))
    if not docs:
        print(f"No new or updated documents found in collection '{collection_name}' since {query.get('updatedAt')}.")
    return pd.DataFrame(docs)


def add_missing_columns(conn, df, table_name, schema="DASH"):
    cursor = conn.cursor()
    # Fetch the existing columns in the PostgreSQL table
    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table_name}';")
    existing_columns = {col[0] for col in cursor.fetchall()}

    # Identify missing columns in the DataFrame
    missing_columns = set(df.columns) - existing_columns

    for column in missing_columns:
        # Add the missing columns to the table (you may need to adjust the data type)
        cursor.execute(f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{column}" TEXT;')
        print(f"Added missing column '{column}' to {schema}.{table_name}.")

    cursor.close()

def upsert_data_into_postgres(df, table_name, conn, schema="DASH", pk_column="_id"):
    cursor = None
    try:
        cursor = conn.cursor()

        # Add missing columns if necessary
        add_missing_columns(conn, df, table_name, schema)

        # Clean data recursively
        df_cleaned = df.applymap(handle_non_serializable)

        # Serialize dict/list columns to JSON strings
        for col in df_cleaned.columns:
            if df_cleaned[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df_cleaned[col] = df_cleaned[col].apply(json.dumps)

        df_cleaned = df_cleaned.replace({np.nan: None})

        # Prepare columns for SQL
        quoted_schema = f'"{schema}"'
        columns = [f'"{col}"' for col in df_cleaned.columns]
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(df_cleaned.columns))

        # Prepare UPSERT SQL statement
        update_clause = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col != f'"{pk_column}"'])
        insert_sql = f"""
            INSERT INTO {quoted_schema}.{table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT ("{pk_column}") DO UPDATE SET {update_clause};
        """

        data_tuples = [tuple(row) for row in df_cleaned.values]
        cursor.executemany(insert_sql, data_tuples)
        conn.commit()
        print(f"✅ Upserted {len(data_tuples)} rows into {quoted_schema}.{table_name}.")

    except Exception as e:
        print(f"❌ Error during upsert into {table_name}: {e}")
        conn.rollback()
    finally:
        if cursor:
            cursor.close()


# --- Main incremental ingestion per collection ---
def run_incremental_ingestion(mongo_uri, db_name, collection_name, pg_conn_params,
                              table_name, schema="DASH", pk_column="_id", timestamp_column="updatedAt"):

    conn = None
    try:
        conn = psycopg2.connect(**pg_conn_params)

        last_updated_at = get_last_max_updated_at(conn, table_name, schema, timestamp_column)
        print(f"Last updatedAt in PostgreSQL for table '{table_name}': {last_updated_at}")

        df_new = retrieve_new_records_with_lookback(mongo_uri, db_name, collection_name, last_updated_at)
        if df_new.empty:
            print(f"No new data to ingest for collection '{collection_name}'.")
            return

        print(f"Fetched {len(df_new)} new/updated documents from '{collection_name}'.")

        upsert_data_into_postgres(df_new, table_name, conn, schema, pk_column)

    except Exception as e:
        print(f"❌ Fatal error in ingestion for collection '{collection_name}': {e}")
    finally:
        if conn:
            conn.close()
            print(f"PostgreSQL connection closed for table '{table_name}'.")

# --- Config for multiple collections/tables ---
ingestion_jobs = [
    {"mongo_collection": "orders", "pg_table": "orders", "pk_column": "_id", "timestamp_column": "updatedAt"},
    {"mongo_collection": "branches", "pg_table": "branches", "pk_column": "_id", "timestamp_column": "updatedAt"},
    {"mongo_collection": "customers", "pg_table": "customers", "pk_column": "_id", "timestamp_column": "updatedAt"},
    {"mongo_collection": "discounts", "pg_table": "discounts", "pk_column": "_id", "timestamp_column": "updatedAt"},
    {"mongo_collection": "menucategories", "pg_table" : "menucategories", "pk_column": "_id", "timestamp_column": "updatedAt"},
    {"mongo_collection": "users", "pg_table" : "users", "pk_column": "_id", "timestamp_column": "updatedAt"},
    {"mongo_collection": "wallets", "pg_table" : "wallets", "pk_column": "_id", "timestamp_column": "updatedAt"},
    {"mongo_collection": "revenueledgers", "pg_table" : "revenueledgers", "pk_column": "_id", "timestamp_column": "updatedAt"},
    {"mongo_collection": "products", "pg_table" : "products", "pk_column": "_id", "timestamp_column": "updatedAt"}
    
]

def run_all_ingestions(mongo_uri, db_name, pg_conn_params, schema="DASH"):
    for job in ingestion_jobs:
        print(f"\n--- Starting ingestion for '{job['mongo_collection']}' ---")
        try:
            run_incremental_ingestion(
                mongo_uri=mongo_uri,
                db_name=db_name,
                collection_name=job['mongo_collection'],
                pg_conn_params=pg_conn_params,
                table_name=job['pg_table'],
                schema=schema,
                pk_column=job['pk_column'],
                timestamp_column=job['timestamp_column']
            )
        except Exception as e:
            print(f"Error in ingestion job for {job['mongo_collection']}: {e}")

mongo_uri = "mongodb+srv://chisom:2d5PXqu68974lE1f@daash-db-85f32cd4.mongo.ondigitalocean.com/daash?authSource=admin&replicaSet=daash-db&tls=true"
pg_conn_params = {
    "database": "DEV_RAW_DASH_DB",
    "user": "postgres",
    "password": "Chisom33",
    "host": "localhost",
    "port": "5432"
} 

if __name__ == "__main__":
    run_all_ingestions(mongo_uri, "daash", pg_conn_params)
