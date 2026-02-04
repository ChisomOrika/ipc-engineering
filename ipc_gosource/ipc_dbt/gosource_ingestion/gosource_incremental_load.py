from datetime import timedelta
from pymongo import MongoClient
import pandas as pd
import psycopg2
import json
from datetime import datetime
import numpy as np
from bson import ObjectId
from db_credentials import db_connection_parameters_gosource

# Function to handle non-serializable data types (ObjectId, datetime, NaT, etc.)
def handle_non_serializable(x):
    try:
        if isinstance(x, dict):
            return {k: handle_non_serializable(v) for k, v in x.items()}
        elif isinstance(x, list):
            return [handle_non_serializable(i) for i in x]
        elif isinstance(x, ObjectId):
            return str(x)  # Convert ObjectId to string
        elif isinstance(x, (datetime, np.datetime64, pd.Timestamp)):
            if pd.isna(x):  # Check if the value is NaT (Not a Time)
                return None  # Convert NaT to NULL in PostgreSQL
            return pd.to_datetime(x).isoformat()  # Ensure datetime is in ISO format
        else:
            return x
    except Exception as e:
        print(f"Error handling {x}: {e}")
        return None

# Get the last updated timestamp in PostgreSQL
def get_last_max_updated_at(conn, table_name, schema="GOSOURCE", timestamp_column="updatedat"):
    with conn.cursor() as cur:
        sql = f'SELECT MAX("{timestamp_column}") FROM "{schema}"."{table_name}";'
        cur.execute(sql)
        result = cur.fetchone()
        return result[0] if result and result[0] else None

# Convert PostgreSQL timestamp to datetime for comparison
def convert_to_datetime(value):
    if isinstance(value, str):
        try:
            # Try converting string to datetime
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
    elif isinstance(value, datetime):
        return value
    return None

# Retrieve new records from MongoDB based on the `updatedAt` field (incremental load)
def retrieve_new_records_with_lookback(mongo_uri, db_name, collection_name, last_updated_at=None, lookback_days=1):
    client = MongoClient(mongo_uri)
    db = client[db_name]
    
    # Convert the PostgreSQL last_updated_at to datetime if it is not None
    last_updated_at = convert_to_datetime(last_updated_at)
    
    if last_updated_at:
        query_start = last_updated_at - timedelta(days=lookback_days)
        query = {"updatedAt": {"$gte": query_start}}  # MongoDB query to fetch updated records
    else:
        query = {}

    print(f"Querying MongoDB for records since {query_start}")  # Debug print to check the query timestamp
    docs = list(db[collection_name].find(query))
    if not docs:
        print(f"No new or updated documents found in collection '{collection_name}' since {last_updated_at}.")
    return pd.DataFrame(docs)

# Add missing columns in the PostgreSQL table if they don't already exist
def add_missing_columns(conn, df, table_name, schema="GOSOURCE"):
    cursor = conn.cursor()

    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_schema = '{schema}' AND table_name = '{table_name}';")
    # Fetch column names and convert them to lowercase for comparison
    existing_columns = {col[0].lower() for col in cursor.fetchall()}
    
    # Check for missing columns and convert df columns to lowercase
    missing_columns = {col.lower() for col in df.columns} - existing_columns
    
    for column in missing_columns:
        original_column_name = [col for col in df.columns if col.lower() == column][0]
        dtype = df[original_column_name].dtype
        
        # Dynamically assign column types based on DataFrame column types
        if dtype == 'object':
            col_type = 'TEXT'
        elif dtype == 'int64':
            col_type = 'BIGINT'
        elif dtype == 'float64':
            col_type = 'DOUBLE PRECISION'
        elif dtype == 'datetime64[ns]':
            col_type = 'TIMESTAMP'
        else:
            col_type = 'TEXT'  # Default to TEXT if not identified
        
        cursor.execute(f'ALTER TABLE "{schema}"."{table_name}" ADD COLUMN "{original_column_name}" {col_type};')
        print(f"Added missing column '{original_column_name}' with type {col_type} to {schema}.{table_name}.")
    cursor.close()


# Upsert data into PostgreSQL
def upsert_data_into_postgres(df, table_name, conn, schema="GOSOURCE", pk_columns=["_id"]):
    cursor = None
    try:
        cursor = conn.cursor()
        
        # Add missing columns if necessary
        add_missing_columns(conn, df, table_name, schema)
        
        # Clean data to handle non-serializable types
        df_cleaned = df.applymap(handle_non_serializable)
        
        # Serialize dict/list columns to JSON strings
        for col in df_cleaned.columns:
            if df_cleaned[col].apply(lambda x: isinstance(x, (dict, list))).any():
                df_cleaned[col] = df_cleaned[col].apply(json.dumps)
        
        df_cleaned = df_cleaned.replace({np.nan: None})  # Replace NaN with None for PostgreSQL
        
        # Prepare columns for SQL
        quoted_schema = f'"{schema}"'
        columns = [f'"{col}"' for col in df_cleaned.columns]
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(df_cleaned.columns))
        
        # Prepare ON CONFLICT clause for multiple PKs
        conflict_columns_str = ', '.join([f'"{col}"' for col in pk_columns])
        update_clause = ', '.join([f"{col}=EXCLUDED.{col}" for col in columns if col not in pk_columns])

        insert_sql = f"""
            INSERT INTO {quoted_schema}.{table_name} ({columns_str})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_columns_str}) DO UPDATE SET {update_clause};
        """
        
        # Execute the upsert
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
    
    # Drop unwanted columns including cartProduct.name explicitly
    columns_to_drop = [
        'product.images', 'product.slug', 'product.__v', 'address', 'phoneNumbers'    ]
    
    final_df = final_df.drop(columns=[col for col in columns_to_drop if col in final_df.columns], errors='ignore')

    # Debug: Check if 'cartproduct.name' is removed
    print(f"Columns after dropping unwanted columns: {final_df.columns.tolist()}")
    
    # Drop duplicate '_id' columns that may appear after exploding
    final_df = final_df.loc[:, ~final_df.columns.duplicated()]
    
    # Replace NaN in all columns with None (empty)
    final_df = final_df.where(pd.notna(final_df), None)
    
    # Replace NaT in datetime columns with a default timestamp
    default_timestamp = pd.Timestamp('2020-01-01 00:00:00')
    final_df = final_df.apply(lambda col: col.fillna(default_timestamp) if col.dtypes == 'datetime64[ns]' else col)
    
    return final_df




def transform_products(products_df):
    """
    Transforms the 'products' DataFrame by normalizing nested fields and applying necessary transformations.
    
    Args:
        products_df (pd.DataFrame): The products DataFrame fetched from MongoDB.

    Returns:
        pd.DataFrame: Transformed products DataFrame.
    """
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
        special_prices_df = pd.json_normalize(products_df['specialPrices'])
        products_df = products_df.drop(columns=['specialPrices']).join(special_prices_df, rsuffix='_specialPrices')

    # Explode and normalize 'images'
    if 'images' in products_df.columns:
        products_df = products_df.explode('images')
        images_df = pd.json_normalize(products_df['images'])
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

    # Replace NaN in all columns with None (empty)
    products_df = products_df.where(pd.notna(products_df), None)

    # Replace NaT in datetime columns with a default timestamp
    default_timestamp = pd.Timestamp('2020-01-01 00:00:00')
    products_df = products_df.apply(lambda col: col.fillna(default_timestamp) if col.dtypes == 'datetime64[ns]' else col)

    return products_df


def run_incremental_ingestion(mongo_uri, db_name, collection_name, pg_conn_params,
                              table_name, schema="GOSOURCE", pk_column="_id", timestamp_column="updatedat"):
    conn = None
    try:
        conn = psycopg2.connect(**pg_conn_params)

        last_updated_at = get_last_max_updated_at(conn, table_name, schema, timestamp_column)
        print(f"Last updatedAt in PostgreSQL for table '{table_name}': {last_updated_at}")

        # Fetch new records from MongoDB
        df_new = retrieve_new_records_with_lookback(mongo_uri, db_name, collection_name, last_updated_at)
        if df_new.empty:
            print(f"No new data to ingest for collection '{collection_name}'.")
            return

        print(f"Fetched {len(df_new)} new/updated documents from '{collection_name}'.")

        # Apply transformation if the PostgreSQL table name is 'orders'
        if table_name == "orders":
            df_new = transform_orders(df_new)  # Apply the transformation for 'orders' table

        if table_name == "products":
            df_new = transform_orders(df_new)  # Apply the transformation for 'orders' table
    
        # Clean data to handle non-serializable types
        df_cleaned = df_new.applymap(handle_non_serializable)

        # Convert column names to lowercase before processing further
        df_cleaned.columns = [col.lower() for col in df_cleaned.columns]

        # Upsert the data into PostgreSQL
        upsert_data_into_postgres(df_cleaned, table_name, conn, schema, pk_column)

    except Exception as e:
        print(f"❌ Fatal error in ingestion for collection '{collection_name}': {e}")
    finally:
        if conn:
            conn.close()
            print(f"PostgreSQL connection closed for table '{table_name}'.")





# Config for multiple collections/tables
ingestion_jobs = [
    {"mongo_collection": "orders", "pg_table": "receipts", "pk_column": ["_id"], "timestamp_column": "updatedat"},
    {"mongo_collection": "timelines", "pg_table": "timelines", "pk_column": ["_id"], "timestamp_column": "updatedat"},
    {"mongo_collection": "businesscustomers", "pg_table": "customers", "pk_column": ["_id"], "timestamp_column": "updatedat"},
    {"mongo_collection": "categories", "pg_table": "categories", "pk_column": ["_id"], "timestamp_column": "updatedat"},
    {"mongo_collection": "products", "pg_table": "products", "pk_column": ["_id"], "timestamp_column": "updatedat"},
    {"mongo_collection": "orders", "pg_table": "orders", "pk_column": ["_id", "cartproduct._id","product._id"], "timestamp_column": "updatedat"}
]


def run_all_ingestions(mongo_uri, db_name, pg_conn_params, schema="GOSOURCE"):
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


# MongoDB URI and PostgreSQL connection params
mongo_uri = "mongodb+srv://ipc_user:93i8N2o4e1HvAP65@db-mongodb-sfo3-51186-a3cdbca3.mongo.ondigitalocean.com/ipc_db?tls=true&authSource=admin&replicaSet=db-mongodb-sfo3-51186"
pg_conn_params = {
    "database": "PROD_RAW_GOSOURCE_DB",
    "user": "postgres",
    "password": "Chisom33",
    "host": "localhost",
    "port": "5432"
}

if __name__ == "__main__":
    run_all_ingestions(mongo_uri, "ipc_db", pg_conn_params)
