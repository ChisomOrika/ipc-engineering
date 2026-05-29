#!/usr/bin/env python3
"""Fix queries 6, 7, 9 — find correct customer/business identifier"""

import psycopg2, os
from dotenv import load_dotenv
load_dotenv('/Users/sapaleague/Downloads/ipc_analytics/.env')

conn = psycopg2.connect(
    host=os.getenv('PG_HOST','').strip('\r'),
    port=os.getenv('PG_PORT','').strip('\r'),
    user=os.getenv('PG_USER','').strip('\r'),
    password=os.getenv('PG_PASSWORD','').strip('\r'),
    dbname='PROD_ANALYTICS_DB', sslmode='require'
)
cur = conn.cursor()

# Check which customer identifiers are populated in March orders
cur.execute("""
WITH orders AS (
    SELECT DISTINCT ON (_id) *
    FROM raw_gosource.orders
    ORDER BY _id, "updatedAt" DESC
)
SELECT
    COUNT(*) AS total,
    COUNT("business._id") AS has_business_id,
    COUNT("customerId") AS has_customer_id,
    COUNT("businessName") AS has_business_name,
    COUNT("business.businessName") AS has_biz_biz_name,
    COUNT("business") AS has_business
FROM orders
WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
""")
cols = [d[0] for d in cur.description]
row = cur.fetchone()
for c, v in zip(cols, row):
    print(f"  {c}: {v}")

# Sample some rows to see what's populated
print("\n--- Sample March orders (customer fields) ---")
cur.execute("""
WITH orders AS (
    SELECT DISTINCT ON (_id) *
    FROM raw_gosource.orders
    ORDER BY _id, "updatedAt" DESC
)
SELECT _id, "customerId", "businessName", "business._id", "business.businessName", "business"
FROM orders
WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
LIMIT 10
""")
cols = [d[0] for d in cur.description]
print(" | ".join(cols))
for r in cur.fetchall():
    print(" | ".join(str(v)[:40] if v else '-' for v in r))

cur.close()
conn.close()
