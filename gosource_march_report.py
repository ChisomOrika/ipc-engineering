#!/usr/bin/env python3
"""GoSource March 2026 Comprehensive Report"""

import psycopg2
import os
from dotenv import load_dotenv

load_dotenv('/Users/sapaleague/Downloads/ipc_analytics/.env')

conn = psycopg2.connect(
    host=os.getenv('PG_HOST', '').strip('\r'),
    port=os.getenv('PG_PORT', '').strip('\r'),
    user=os.getenv('PG_USER', '').strip('\r'),
    password=os.getenv('PG_PASSWORD', '').strip('\r'),
    dbname='PROD_ANALYTICS_DB',
    sslmode='require'
)
cur = conn.cursor()

def run(sql, label=None):
    if label:
        print(f"\n{'='*80}")
        print(f"  {label}")
        print(f"{'='*80}")
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return cols, rows

def print_table(cols, rows, fmt=None):
    """fmt is a dict of col_index -> format function"""
    fmt = fmt or {}
    # Calculate widths
    str_rows = []
    for r in rows:
        sr = []
        for i, v in enumerate(r):
            if i in fmt:
                sr.append(fmt[i](v))
            elif isinstance(v, float):
                sr.append(f"{v:,.2f}")
            elif isinstance(v, int):
                sr.append(f"{v:,}")
            else:
                sr.append(str(v) if v is not None else '-')
        str_rows.append(sr)
    widths = [max(len(c), *(len(sr[i]) for sr in str_rows)) if str_rows else len(c) for i, c in enumerate(cols)]
    header = " | ".join(c.ljust(w) for c, w in zip(cols, widths))
    print(header)
    print("-+-".join("-"*w for w in widths))
    for sr in str_rows:
        print(" | ".join(s.ljust(w) for s, w in zip(sr, widths)))

def naira(v):
    if v is None: return '-'
    return f"₦{v:,.2f}"

def pct(v):
    if v is None: return '-'
    return f"{v:.1f}%"

def comma(v):
    if v is None: return '-'
    if isinstance(v, float): return f"{v:,.2f}"
    return f"{v:,}"

# ─── STEP 0: Table structures ───
print("\n" + "="*80)
print("  TABLE STRUCTURES")
print("="*80)

for tbl in ['orders', 'receipts', 'customers', 'products']:
    cols, rows = run(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='raw_gosource' AND table_name='{tbl}'
        ORDER BY ordinal_position
    """)
    print(f"\n--- raw_gosource.{tbl} ---")
    for r in rows:
        print(f"  {r[0]:40s} {r[1]}")

# ─── CTE base for all deduped order queries ───
ORDERS_CTE = """
WITH orders AS (
    SELECT DISTINCT ON (_id) *
    FROM raw_gosource.orders
    ORDER BY _id, "updatedAt" DESC
)
"""

# ─── 1. Order Volume — March vs Feb 2026 ───
cols, rows = run(ORDERS_CTE + """
SELECT
    to_char("createdAt", 'YYYY-MM') AS month,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (WHERE lower(status) = 'delivered') AS delivered,
    COUNT(*) FILTER (WHERE lower(status) = 'cancelled') AS cancelled,
    COUNT(*) FILTER (WHERE lower(status) NOT IN ('delivered','cancelled')) AS other
FROM orders
WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
GROUP BY 1 ORDER BY 1
""", "1. ORDER VOLUME — March vs Feb 2026")
print_table(cols, rows)

# ─── 2. Revenue — March vs Feb (delivered + paid) ───
cols, rows = run(ORDERS_CTE + """
SELECT
    to_char("createdAt", 'YYYY-MM') AS month,
    COUNT(*) AS paid_delivered_orders,
    SUM("totalPrice") AS total_revenue,
    AVG("totalPrice") AS avg_order_value
FROM orders
WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
  AND lower(status) = 'delivered'
  AND lower("paymentStatus") = 'paid'
GROUP BY 1 ORDER BY 1
""", "2. REVENUE — March vs Feb (delivered + paid)")
print_table(cols, rows, {2: naira, 3: naira})

# ─── 3. Revenue by Payment Method (March vs Feb) — percentages ───
cols, rows = run(ORDERS_CTE + """
SELECT
    to_char("createdAt", 'YYYY-MM') AS month,
    "paymentMethod",
    COUNT(*) AS orders,
    ROUND((100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY to_char("createdAt", 'YYYY-MM')))::numeric, 1) AS pct_of_orders,
    ROUND((100.0 * SUM("totalPrice") / SUM(SUM("totalPrice")) OVER (PARTITION BY to_char("createdAt", 'YYYY-MM')))::numeric, 1) AS pct_of_revenue
FROM orders
WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
  AND lower(status) = 'delivered'
  AND lower("paymentStatus") = 'paid'
GROUP BY 1, 2 ORDER BY 1, 3 DESC
""", "3. REVENUE BY PAYMENT METHOD — % breakdown (March vs Feb)")
print_table(cols, rows, {3: pct, 4: pct})

# ─── 4. Service Charge collected (March vs Feb) ───
cols, rows = run(ORDERS_CTE + """
SELECT
    to_char("createdAt", 'YYYY-MM') AS month,
    SUM("serviceCharge") AS total_service_charge,
    COUNT(*) AS delivered_orders
FROM orders
WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
  AND lower(status) = 'delivered'
GROUP BY 1 ORDER BY 1
""", "4. SERVICE CHARGE COLLECTED (March vs Feb)")
print_table(cols, rows, {1: naira})

# ─── 5. Delivery Fee collected (March vs Feb) ───
cols, rows = run(ORDERS_CTE + """
SELECT
    to_char("createdAt", 'YYYY-MM') AS month,
    SUM("deliveryFee") AS total_delivery_fee,
    COUNT(*) AS delivered_orders
FROM orders
WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
  AND lower(status) = 'delivered'
GROUP BY 1 ORDER BY 1
""", "5. DELIVERY FEE COLLECTED (March vs Feb)")
print_table(cols, rows, {1: naira})

# ─── 6. Top 10 Customers by order count (March) ───
# First check what business-related columns exist
cols, rows = run("""
    SELECT column_name FROM information_schema.columns
    WHERE table_schema='raw_gosource' AND table_name='orders'
    AND lower(column_name) LIKE '%business%'
    ORDER BY ordinal_position
""")
biz_cols = [r[0] for r in rows]
print(f"\n[DEBUG] Business columns in orders: {biz_cols}")

# Check customers columns too
cols, rows = run("""
    SELECT column_name FROM information_schema.columns
    WHERE table_schema='raw_gosource' AND table_name='customers'
    ORDER BY ordinal_position
""")
cust_cols = [r[0] for r in rows]
print(f"[DEBUG] Customer columns: {cust_cols}")

# Try to find business name — check if there's a denormalized field
cols, rows = run("""
    SELECT column_name FROM information_schema.columns
    WHERE table_schema='raw_gosource' AND table_name='orders'
    AND (lower(column_name) LIKE '%name%' OR lower(column_name) LIKE '%business%')
    ORDER BY ordinal_position
""")
name_cols = [r[0] for r in rows]
print(f"[DEBUG] Name/business columns in orders: {name_cols}")

# Now run top 10 — will adapt based on available columns
# Try joining with customers first
cols, rows = run(ORDERS_CTE + """
, march_orders AS (
    SELECT * FROM orders
    WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
      AND lower(status) = 'delivered'
),
total AS (SELECT COUNT(*) AS cnt FROM march_orders),
top_cust AS (
    SELECT
        o."business._id" AS business_id,
        COALESCE(o."business.businessName", o."businessName", o."business._id") AS business_name,
        COUNT(*) AS order_count
    FROM march_orders o
    GROUP BY 1, 2
    ORDER BY 3 DESC
    LIMIT 10
)
SELECT
    business_name,
    order_count,
    ROUND((100.0 * order_count / t.cnt)::numeric, 1) AS pct_of_total
FROM top_cust, total t
ORDER BY order_count DESC
""", "6. TOP 10 CUSTOMERS BY ORDER COUNT (March 2026)")
print_table(cols, rows, {2: pct})

# ─── 7. Customer Activity — new vs returning (March vs Feb) ───
cols, rows = run(ORDERS_CTE + """
, first_order AS (
    SELECT "business._id", MIN("createdAt") AS first_delivered_date
    FROM orders
    WHERE lower(status) = 'delivered'
    GROUP BY 1
),
monthly AS (
    SELECT
        to_char(o."createdAt", 'YYYY-MM') AS month,
        COUNT(DISTINCT CASE WHEN to_char(f.first_delivered_date, 'YYYY-MM') = to_char(o."createdAt", 'YYYY-MM') THEN o."business._id" END) AS new_customers,
        COUNT(DISTINCT CASE WHEN to_char(f.first_delivered_date, 'YYYY-MM') < to_char(o."createdAt", 'YYYY-MM') THEN o."business._id" END) AS returning_customers,
        COUNT(DISTINCT o."business._id") AS total_active
    FROM orders o
    JOIN first_order f ON o."business._id" = f."business._id"
    WHERE o."createdAt" >= '2026-02-01' AND o."createdAt" < '2026-04-01'
      AND lower(o.status) = 'delivered'
    GROUP BY 1
)
SELECT
    month, new_customers, returning_customers, total_active,
    ROUND((100.0 * new_customers / total_active)::numeric, 1) AS new_pct,
    ROUND((100.0 * returning_customers / total_active)::numeric, 1) AS returning_pct
FROM monthly ORDER BY 1
""", "7. CUSTOMER ACTIVITY — New vs Returning (March vs Feb)")
print_table(cols, rows, {4: pct, 5: pct})

# ─── 8. AR Aging snapshot ───
cols, rows = run(ORDERS_CTE + """
SELECT
    CASE
        WHEN CURRENT_DATE - "createdAt"::date BETWEEN 0 AND 30 THEN '0-30 days'
        WHEN CURRENT_DATE - "createdAt"::date BETWEEN 31 AND 60 THEN '31-60 days'
        WHEN CURRENT_DATE - "createdAt"::date BETWEEN 61 AND 90 THEN '61-90 days'
        ELSE '90+ days'
    END AS aging_bucket,
    COUNT(*) AS order_count,
    SUM("totalPrice") AS outstanding_amount,
    ROUND((100.0 * SUM("totalPrice") / SUM(SUM("totalPrice")) OVER ())::numeric, 1) AS pct_of_total
FROM orders
WHERE lower("paymentMethod") = 'credit'
  AND lower(status) = 'delivered'
  AND COALESCE(lower("paymentStatus"), '') != 'paid'
GROUP BY 1
ORDER BY MIN(CURRENT_DATE - "createdAt"::date)
""", "8. AR AGING SNAPSHOT — Outstanding Credit Orders")
print_table(cols, rows, {2: naira, 3: pct})

# ─── 9. Weekly trend within March ───
cols, rows = run(ORDERS_CTE + """
SELECT
    'W' || EXTRACT(WEEK FROM "createdAt")::int AS week,
    MIN("createdAt"::date) AS week_start,
    MAX("createdAt"::date) AS week_end,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (WHERE lower(status)='delivered' AND lower("paymentStatus")='paid') AS paid_delivered,
    SUM("totalPrice") FILTER (WHERE lower(status)='delivered' AND lower("paymentStatus")='paid') AS revenue,
    COUNT(DISTINCT "business._id") AS active_customers
FROM orders
WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
GROUP BY 1
ORDER BY 2
""", "9. WEEKLY TREND — March 2026")
print_table(cols, rows, {5: naira})

# ─── 10. Product mix — top 10 products (March) — use RAW table, not deduped ───
# First find product name columns
cols, rows = run("""
    SELECT column_name FROM information_schema.columns
    WHERE table_schema='raw_gosource' AND table_name='orders'
    AND (lower(column_name) LIKE '%product%' OR lower(column_name) LIKE '%cart%')
    ORDER BY ordinal_position
""")
prod_cols = [r[0] for r in rows]
print(f"\n[DEBUG] Product/cart columns in orders: {prod_cols}")

# Try with likely column names
try:
    cols, rows = run("""
    WITH march_items AS (
        SELECT * FROM raw_gosource.orders
        WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
    ),
    total AS (SELECT COUNT(*) AS cnt FROM march_items)
    SELECT
        COALESCE("cartProduct.name", "cartProduct.productName", 'Unknown') AS product_name,
        COUNT(*) AS line_items,
        ROUND((100.0 * COUNT(*) / t.cnt)::numeric, 1) AS pct_of_total
    FROM march_items, total t
    GROUP BY 1, t.cnt
    ORDER BY 2 DESC
    LIMIT 10
    """, "10. TOP 10 PRODUCTS BY ORDER FREQUENCY (March — line items)")
    print_table(cols, rows, {2: pct})
except Exception as e:
    print(f"  [Trying alternative column names due to: {e}]")
    conn.rollback()
    # Try different column name patterns
    try:
        cols, rows = run("""
        WITH march_items AS (
            SELECT * FROM raw_gosource.orders
            WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
        ),
        total AS (SELECT COUNT(*) AS cnt FROM march_items)
        SELECT
            "cartProduct.name" AS product_name,
            COUNT(*) AS line_items,
            ROUND((100.0 * COUNT(*) / t.cnt)::numeric, 1) AS pct_of_total
        FROM march_items, total t
        GROUP BY 1, t.cnt
        ORDER BY 2 DESC
        LIMIT 10
        """, "10. TOP 10 PRODUCTS BY ORDER FREQUENCY (March — line items)")
        print_table(cols, rows, {2: pct})
    except Exception as e2:
        conn.rollback()
        # Last resort: check what product columns actually exist
        cols, rows = run("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='raw_gosource' AND table_name='orders'
            AND column_name LIKE '%roduct%'
        """)
        available = [r[0] for r in rows]
        print(f"  Available product columns: {available}")
        if available:
            col = available[0]
            cols, rows = run(f"""
            WITH march_items AS (
                SELECT * FROM raw_gosource.orders
                WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
            ),
            total AS (SELECT COUNT(*) AS cnt FROM march_items)
            SELECT
                "{col}" AS product_name,
                COUNT(*) AS line_items,
                ROUND((100.0 * COUNT(*) / t.cnt)::numeric, 1) AS pct_of_total
            FROM march_items, total t
            GROUP BY 1, t.cnt
            ORDER BY 2 DESC
            LIMIT 10
            """, f"10. TOP 10 PRODUCTS (using column: {col})")
            print_table(cols, rows, {2: pct})

# ─── 11. Order fulfillment rate (March vs Feb) ───
cols, rows = run(ORDERS_CTE + """
SELECT
    to_char("createdAt", 'YYYY-MM') AS month,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (WHERE lower(status) = 'delivered') AS delivered,
    ROUND((100.0 * COUNT(*) FILTER (WHERE lower(status) = 'delivered') / COUNT(*))::numeric, 1) AS fulfillment_rate
FROM orders
WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
GROUP BY 1 ORDER BY 1
""", "11. ORDER FULFILLMENT RATE (March vs Feb)")
print_table(cols, rows, {3: pct})

# ─── 12. Average items per order (March vs Feb) ───
cols, rows = run("""
WITH raw_items AS (
    SELECT
        to_char("createdAt", 'YYYY-MM') AS month,
        _id,
        COUNT(*) AS items
    FROM raw_gosource.orders
    WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
    GROUP BY 1, 2
)
SELECT
    month,
    COUNT(*) AS distinct_orders,
    SUM(items) AS total_line_items,
    ROUND(AVG(items)::numeric, 2) AS avg_items_per_order
FROM raw_items
GROUP BY 1 ORDER BY 1
""", "12. AVERAGE ITEMS PER ORDER (March vs Feb)")
print_table(cols, rows)

# ─── 13. Credit vs Cash split trend (monthly) ───
cols, rows = run(ORDERS_CTE + """
SELECT
    to_char("createdAt", 'YYYY-MM') AS month,
    COUNT(*) AS total_orders,
    ROUND((100.0 * COUNT(*) FILTER (WHERE lower("paymentMethod") = 'credit') / COUNT(*))::numeric, 1) AS credit_pct,
    ROUND((100.0 * COUNT(*) FILTER (WHERE lower("paymentMethod") != 'credit') / COUNT(*))::numeric, 1) AS non_credit_pct,
    COUNT(*) FILTER (WHERE lower("paymentMethod") = 'credit') AS credit_orders,
    COUNT(*) FILTER (WHERE lower("paymentMethod") != 'credit') AS non_credit_orders
FROM orders
WHERE lower(status) = 'delivered'
  AND "createdAt" >= '2025-01-01'
GROUP BY 1 ORDER BY 1
""", "13. CREDIT vs NON-CREDIT SPLIT TREND (Monthly, delivered orders)")
print_table(cols, rows, {2: pct, 3: pct})

print("\n" + "="*80)
print("  REPORT COMPLETE")
print("="*80)

cur.close()
conn.close()
