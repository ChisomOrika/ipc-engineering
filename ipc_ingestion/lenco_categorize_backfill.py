"""
Lenco Transaction Categorization Backfill
==========================================
Reads existing raw_lenco.transactions from the DB,
applies the categorizer, and writes the category column back.

Run once after initial ingestion:
    python ipc_ingestion/lenco_categorize_backfill.py
"""

import os
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from categorizer import categorize_dataframe

load_dotenv()

SCHEMA     = "raw_lenco"
TABLE      = "transactions"
CHUNK_SIZE = 5_000

PG_PARAMS = {
    "database": os.getenv("PG_DB", "PROD_ANALYTICS_DB"),
    "user":     os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "host":     os.getenv("PG_HOST"),
    "port":     os.getenv("PG_PORT", "25060"),
}


def ensure_category_column(conn):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s AND column_name = 'category';
        """, (SCHEMA, TABLE))
        if not cur.fetchone():
            cur.execute(f'ALTER TABLE "{SCHEMA}"."{TABLE}" ADD COLUMN category TEXT;')
            print("Added 'category' column.")
        else:
            print("'category' column already exists.")
    conn.commit()


def run_backfill():
    conn = psycopg2.connect(**PG_PARAMS)
    try:
        ensure_category_column(conn)

        print(f"Reading {SCHEMA}.{TABLE}...")
        df = pd.read_sql(
            f'SELECT id, narration, type FROM "{SCHEMA}"."{TABLE}";',
            conn
        )
        print(f"  {len(df):,} rows loaded.")

        # Apply categorization
        # Lenco uses 'narration' and 'type' (debit/credit)
        df = categorize_dataframe(df, narration_col='narration', txn_type_col='type')

        counts = df['category'].value_counts()
        print("\nCategory breakdown:")
        for cat, count in counts.items():
            print(f"  {cat:<28} {count:>6}  ({count/len(df)*100:.1f}%)")
        print(f"  {'TOTAL':<28} {len(df):>6}\n")

        # Write category back in chunks
        total    = len(df)
        updated  = 0
        for i in range(0, total, CHUNK_SIZE):
            chunk = df.iloc[i : i + CHUNK_SIZE][['id', 'category']]
            rows  = list(chunk.itertuples(index=False, name=None))

            with conn.cursor() as cur:
                execute_values(
                    cur,
                    f"""
                    UPDATE "{SCHEMA}"."{TABLE}" AS t
                    SET category = v.category
                    FROM (VALUES %s) AS v(id, category)
                    WHERE t.id = v.id;
                    """,
                    rows
                )
            conn.commit()
            updated += len(rows)
            print(f"  Updated {updated:,}/{total:,} rows...")

        print(f"\nDone. {total:,} rows categorized.")

    finally:
        conn.close()


if __name__ == "__main__":
    run_backfill()
