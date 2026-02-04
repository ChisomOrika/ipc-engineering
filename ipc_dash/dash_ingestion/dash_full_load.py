import json
from datetime import datetime
import pandas as pd
from bson import ObjectId
import pymongo
import psycopg2
from psycopg2 import sql
import numpy as np


def handle_non_serializable(x):
    try:
        if isinstance(x, dict):
            return {k: handle_non_serializable(v) for k, v in x.items()}
        elif isinstance(x, list):
            return [handle_non_serializable(i) for i in x]
        elif isinstance(x, ObjectId):
            return str(x)
        elif pd.isna(x):  # Catches NaN, None, NaT, etc.
            return None
        elif isinstance(x, (datetime, np.datetime64, pd.Timestamp)):
            return pd.to_datetime(x).isoformat()
        else:
            return x
    except Exception as e:
        print(f"Error handling {x}: {e}")
        return None


def map_dtype_to_postgres(dtype):
    if dtype == 'object':
        return 'TEXT'
    elif dtype == 'int64':
        return 'BIGINT'
    elif dtype == 'float64':
        return 'FLOAT'
    elif dtype == 'datetime64[ns]':
        return 'TIMESTAMP'
    elif dtype == 'bool':
        return 'BOOLEAN'
    else:
        return 'TEXT'  # Default to TEXT for unknown types


def create_postgres_table_from_df(df, table_name, conn, schema="DASH"):
    columns = []
    quoted_schema = f'"{schema}"'  # Preserve case

    for col in df.columns:
        quoted_col = f'"{col}"'  # Always quote
        dtype = df[col].dtype
        postgres_dtype = map_dtype_to_postgres(dtype)
        columns.append(f"{quoted_col} {postgres_dtype}")

    create_table_sql = f"CREATE TABLE IF NOT EXISTS {quoted_schema}.{table_name} ({', '.join(columns)});"

    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    conn.commit()
    cursor.close()
    print(f"Table '{quoted_schema}.{table_name}' ensured to exist (created if missing).")


def retrieve_all_records(mongo_uri, db_name, collection_name):
    client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=90000)
    db = client[db_name]
    docs = list(db[collection_name].find())
    return pd.DataFrame(docs)


def insert_data_into_postgres(df, table_name, conn, schema="DASH"):
    cursor = None
    try:
        cursor = conn.cursor()

        # Clean data recursively
        df_cleaned = df.applymap(handle_non_serializable)

        # Serialize dict/list columns to JSON strings
        for col in df_cleaned.columns:
            if df_cleaned[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df_cleaned[col] = df_cleaned[col].apply(json.dumps)

        df_cleaned = df_cleaned.replace({np.nan: None})

        quoted_schema = f'"{schema}"'
        columns = [f'"{col}"' for col in df_cleaned.columns]
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(df_cleaned.columns))
        insert_sql = f"INSERT INTO {quoted_schema}.{table_name} ({columns_str}) VALUES ({placeholders})"

        data_tuples = [tuple(row) for row in df_cleaned.values]
        cursor.executemany(insert_sql, data_tuples)
        conn.commit()
        print(f"✅ Inserted {len(data_tuples)} rows into {quoted_schema}.{table_name}.")
    except Exception as e:
        print(f"❌ Error during insert into {table_name}: {e}")
        conn.rollback()
    finally:
        if cursor:
            cursor.close()


def run_full_ingestion(mongo_uri, db_name, collection_name, pg_conn_params,
                       table_name, schema="DASH"):
    conn = None
    try:
        conn = psycopg2.connect(**pg_conn_params)

        df = retrieve_all_records(mongo_uri, db_name, collection_name)

        if df.empty:
            print(f"No data found in MongoDB collection '{collection_name}'.")
            return

        print(f"Fetched {len(df)} records from MongoDB collection '{collection_name}'.")

        # Create table if not exists before inserting
        create_postgres_table_from_df(df, table_name, conn, schema)

        insert_data_into_postgres(df, table_name, conn, schema)

    finally:
        if conn:
            conn.close()
            print(f"PostgreSQL connection closed for table '{table_name}'.")


# Configuration for collections to do full loads
ingestion_jobs = [
    {"mongo_collection": "wallets", "pg_table": "wallets"}
]

def run_all_full_ingestions(mongo_uri, db_name, pg_conn_params, schema="DASH"):
    for job in ingestion_jobs:
        print(f"\n--- Starting full ingestion for '{job['mongo_collection']}' ---")
        try:
            run_full_ingestion(
                mongo_uri=mongo_uri,
                db_name=db_name,
                collection_name=job['mongo_collection'],
                pg_conn_params=pg_conn_params,
                table_name=job['pg_table'],
                schema=schema
            )
        except Exception as e:
            print(f"Error in ingestion job for {job['mongo_collection']}: {e}")


# Usage example
if __name__ == "__main__":
    mongo_uri = "mongodb+srv://chisom:2d5PXqu68974lE1f@daash-db-85f32cd4.mongo.ondigitalocean.com/daash?authSource=admin&replicaSet=daash-db&tls=true"
    pg_conn_params = {
        "database": "DEV_RAW_DASH_DB",
        "user": "postgres",
        "password": "Chisom33",
        "host": "localhost",
        "port": "5432"
    }
    run_all_full_ingestions(mongo_uri, "daash", pg_conn_params)
