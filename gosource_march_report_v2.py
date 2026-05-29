#!/usr/bin/env python3
"""GoSource March 2026 Comprehensive Report — v2 (fixed customer ID)"""

import psycopg2, os
from dotenv import load_dotenv
from decimal import Decimal

load_dotenv('/Users/sapaleague/Downloads/ipc_analytics/.env')

conn = psycopg2.connect(
    host=os.getenv('PG_HOST','').strip('\r'),
    port=os.getenv('PG_PORT','').strip('\r'),
    user=os.getenv('PG_USER','').strip('\r'),
    password=os.getenv('PG_PASSWORD','').strip('\r'),
    dbname='PROD_ANALYTICS_DB', sslmode='require'
)
cur = conn.cursor()

def run(sql, label=None):
    if label:
        print(f"\n{'='*90}")
        print(f"  {label}")
        print(f"{'='*90}")
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    return cols, rows

def fmt_val(v):
    if v is None: return '-'
    if isinstance(v, Decimal): v = float(v)
    if isinstance(v, float): return f"{v:,.2f}"
    if isinstance(v, int): return f"{v:,}"
    return str(v)

def print_table(cols, rows, fmt=None):
    fmt = fmt or {}
    str_rows = []
    for r in rows:
        sr = []
        for i, v in enumerate(r):
            if i in fmt: sr.append(fmt[i](v))
            else: sr.append(fmt_val(v))
        str_rows.append(sr)
    if not str_rows:
        print("  (no rows)")
        return
    widths = [max(len(c), *(len(sr[i]) for sr in str_rows)) for i, c in enumerate(cols)]
    print(" | ".join(c.ljust(w) for c, w in zip(cols, widths)))
    print("-+-".join("-"*w for w in widths))
    for sr in str_rows:
        print(" | ".join(s.ljust(w) for s, w in zip(sr, widths)))

def naira(v):
    if v is None: return '-'
    if isinstance(v, Decimal): v = float(v)
    return f"₦{v:,.2f}"

def pct(v):
    if v is None: return '-'
    if isinstance(v, Decimal): v = float(v)
    return f"{v:.1f}%"

# The correct customer identifier: "business" column holds the customer ObjectId
# "business._id" is only populated for some records (denormalized embed)
# Use COALESCE("business._id", business) to get customer ID reliably
CUST_ID = """COALESCE("business._id", business)"""

ORDERS_CTE = f"""
WITH orders AS (
    SELECT DISTINCT ON (_id) *
    FROM raw_gosource.orders
    ORDER BY _id, "updatedAt" DESC
)
"""

# ═══════════════════════════════════════════════════════════════════════
# 1. ORDER VOLUME — March vs Feb 2026
# ═══════════════════════════════════════════════════════════════════════
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

# MoM change
if len(rows) == 2:
    feb, mar = rows[0], rows[1]
    print(f"\n  MoM Change:")
    print(f"    Total orders:     {mar[1]} vs {feb[1]} ({(mar[1]-feb[1])/feb[1]*100:+.1f}%)")
    print(f"    Delivered orders: {mar[2]} vs {feb[2]} ({(mar[2]-feb[2])/feb[2]*100:+.1f}%)")

# ═══════════════════════════════════════════════════════════════════════
# 2. REVENUE — March vs Feb (delivered + paid)
# ═══════════════════════════════════════════════════════════════════════
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

if len(rows) == 2:
    feb, mar = rows[0], rows[1]
    rev_chg = (mar[2]-feb[2])/feb[2]*100
    print(f"\n  MoM Revenue Change: {rev_chg:+.1f}%")
    print(f"  MoM AOV Change:    {(mar[3]-feb[3])/feb[3]*100:+.1f}%")

# ═══════════════════════════════════════════════════════════════════════
# 3. REVENUE BY PAYMENT METHOD — % breakdown
# ═══════════════════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════════════════
# 4. SERVICE CHARGE COLLECTED
# ═══════════════════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════════════════
# 5. DELIVERY FEE COLLECTED
# ═══════════════════════════════════════════════════════════════════════
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

# ═══════════════════════════════════════════════════════════════════════
# 6. TOP 10 CUSTOMERS BY ORDER COUNT (March)
# ═══════════════════════════════════════════════════════════════════════
cols, rows = run(ORDERS_CTE + f"""
, march_orders AS (
    SELECT *,
        {CUST_ID} AS cust_id
    FROM orders
    WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
      AND lower(status) = 'delivered'
),
total AS (SELECT COUNT(*) AS cnt FROM march_orders),
top_cust AS (
    SELECT
        o.cust_id,
        COALESCE(c."businessName", o."business.businessName", o."businessName", o.cust_id) AS business_name,
        COUNT(*) AS order_count
    FROM march_orders o
    LEFT JOIN raw_gosource.customers c ON o.cust_id = c._id
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

# ═══════════════════════════════════════════════════════════════════════
# 7. CUSTOMER ACTIVITY — New vs Returning
# ═══════════════════════════════════════════════════════════════════════
cols, rows = run(ORDERS_CTE + f"""
, ord_with_cust AS (
    SELECT *, {CUST_ID} AS cust_id FROM orders
),
first_order AS (
    SELECT cust_id, MIN("createdAt") AS first_delivered_date
    FROM ord_with_cust
    WHERE lower(status) = 'delivered'
    GROUP BY 1
),
monthly AS (
    SELECT
        to_char(o."createdAt", 'YYYY-MM') AS month,
        COUNT(DISTINCT CASE WHEN to_char(f.first_delivered_date, 'YYYY-MM') = to_char(o."createdAt", 'YYYY-MM') THEN o.cust_id END) AS new_customers,
        COUNT(DISTINCT CASE WHEN to_char(f.first_delivered_date, 'YYYY-MM') < to_char(o."createdAt", 'YYYY-MM') THEN o.cust_id END) AS returning_customers,
        COUNT(DISTINCT o.cust_id) AS total_active
    FROM ord_with_cust o
    JOIN first_order f ON o.cust_id = f.cust_id
    WHERE o."createdAt" >= '2026-02-01' AND o."createdAt" < '2026-04-01'
      AND lower(o.status) = 'delivered'
    GROUP BY 1
)
SELECT
    month, new_customers, returning_customers, total_active,
    ROUND((100.0 * new_customers / NULLIF(total_active,0))::numeric, 1) AS new_pct,
    ROUND((100.0 * returning_customers / NULLIF(total_active,0))::numeric, 1) AS returning_pct
FROM monthly ORDER BY 1
""", "7. CUSTOMER ACTIVITY — New vs Returning (March vs Feb)")
print_table(cols, rows, {4: pct, 5: pct})

# ═══════════════════════════════════════════════════════════════════════
# 8. AR AGING SNAPSHOT
# ═══════════════════════════════════════════════════════════════════════
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

# Also show total
cur.execute(ORDERS_CTE + """
SELECT COUNT(*) AS total_orders, SUM("totalPrice") AS total_outstanding
FROM orders
WHERE lower("paymentMethod") = 'credit'
  AND lower(status) = 'delivered'
  AND COALESCE(lower("paymentStatus"), '') != 'paid'
""")
r = cur.fetchone()
print(f"\n  TOTAL AR OUTSTANDING: {r[0]} orders, ₦{r[1]:,.2f}")

# ═══════════════════════════════════════════════════════════════════════
# 9. WEEKLY TREND WITHIN MARCH
# ═══════════════════════════════════════════════════════════════════════
cols, rows = run(ORDERS_CTE + f"""
SELECT
    'W' || EXTRACT(WEEK FROM "createdAt")::int AS week,
    MIN("createdAt"::date) AS week_start,
    MAX("createdAt"::date) AS week_end,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (WHERE lower(status)='delivered') AS delivered,
    COUNT(*) FILTER (WHERE lower(status)='delivered' AND lower("paymentStatus")='paid') AS paid_delivered,
    SUM("totalPrice") FILTER (WHERE lower(status)='delivered' AND lower("paymentStatus")='paid') AS revenue,
    COUNT(DISTINCT {CUST_ID}) AS active_customers
FROM orders
WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
GROUP BY 1
ORDER BY 2
""", "9. WEEKLY TREND — March 2026")
print_table(cols, rows, {6: naira})

# ═══════════════════════════════════════════════════════════════════════
# 10. TOP 10 PRODUCTS BY ORDER FREQUENCY (March — line items, NOT deduped)
# ═══════════════════════════════════════════════════════════════════════
cols, rows = run("""
WITH march_items AS (
    SELECT * FROM raw_gosource.orders
    WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
),
total AS (SELECT COUNT(*) AS cnt FROM march_items)
SELECT
    COALESCE("cartProduct.name", "product.name", 'Unknown') AS product_name,
    COUNT(*) AS line_items,
    ROUND((100.0 * COUNT(*) / t.cnt)::numeric, 1) AS pct_of_total
FROM march_items, total t
GROUP BY 1, t.cnt
ORDER BY 2 DESC
LIMIT 10
""", "10. TOP 10 PRODUCTS BY ORDER FREQUENCY (March — line items)")
print_table(cols, rows, {2: pct})

# ═══════════════════════════════════════════════════════════════════════
# 11. ORDER FULFILLMENT RATE
# ═══════════════════════════════════════════════════════════════════════
cols, rows = run(ORDERS_CTE + """
SELECT
    to_char("createdAt", 'YYYY-MM') AS month,
    COUNT(*) AS total_orders,
    COUNT(*) FILTER (WHERE lower(status) = 'delivered') AS delivered,
    COUNT(*) FILTER (WHERE lower(status) = 'cancelled') AS cancelled,
    COUNT(*) FILTER (WHERE lower(status) NOT IN ('delivered','cancelled')) AS pending,
    ROUND((100.0 * COUNT(*) FILTER (WHERE lower(status) = 'delivered') / COUNT(*))::numeric, 1) AS fulfillment_rate_pct
FROM orders
WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
GROUP BY 1 ORDER BY 1
""", "11. ORDER FULFILLMENT RATE (March vs Feb)")
print_table(cols, rows, {5: pct})

if len(rows) == 2:
    print(f"\n  Fulfillment rate improvement: {float(rows[1][5]) - float(rows[0][5]):+.1f} pp")

# ═══════════════════════════════════════════════════════════════════════
# 12. AVERAGE ITEMS PER ORDER
# ═══════════════════════════════════════════════════════════════════════
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

if len(rows) == 2:
    print(f"\n  MoM Change in avg items/order: {float(rows[1][3]) - float(rows[0][3]):+.2f} ({(float(rows[1][3])-float(rows[0][3]))/float(rows[0][3])*100:+.1f}%)")

# ═══════════════════════════════════════════════════════════════════════
# 13. CREDIT vs NON-CREDIT SPLIT TREND (Monthly)
# ═══════════════════════════════════════════════════════════════════════
cols, rows = run(ORDERS_CTE + """
SELECT
    to_char("createdAt", 'YYYY-MM') AS month,
    COUNT(*) AS total_delivered,
    COUNT(*) FILTER (WHERE lower("paymentMethod") = 'credit') AS credit_orders,
    COUNT(*) FILTER (WHERE lower("paymentMethod") != 'credit') AS non_credit_orders,
    ROUND((100.0 * COUNT(*) FILTER (WHERE lower("paymentMethod") = 'credit') / COUNT(*))::numeric, 1) AS credit_pct,
    ROUND((100.0 * COUNT(*) FILTER (WHERE lower("paymentMethod") != 'credit') / COUNT(*))::numeric, 1) AS non_credit_pct
FROM orders
WHERE lower(status) = 'delivered'
  AND "createdAt" >= '2025-01-01'
GROUP BY 1 ORDER BY 1
""", "13. CREDIT vs NON-CREDIT SPLIT TREND (Monthly, delivered orders)")
print_table(cols, rows, {4: pct, 5: pct})

# ═══════════════════════════════════════════════════════════════════════
# SUMMARY
# ═══════════════════════════════════════════════════════════════════════
print(f"\n{'='*90}")
print("  MARCH 2026 EXECUTIVE SUMMARY")
print(f"{'='*90}")

# Re-fetch key numbers
cur.execute(ORDERS_CTE + """
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE lower(status)='delivered') AS delivered,
    COUNT(*) FILTER (WHERE lower(status)='delivered' AND lower("paymentStatus")='paid') AS paid_del,
    SUM("totalPrice") FILTER (WHERE lower(status)='delivered' AND lower("paymentStatus")='paid') AS revenue,
    AVG("totalPrice") FILTER (WHERE lower(status)='delivered' AND lower("paymentStatus")='paid') AS aov,
    SUM("serviceCharge") FILTER (WHERE lower(status)='delivered') AS svc,
    SUM("deliveryFee") FILTER (WHERE lower(status)='delivered') AS del_fee
FROM orders WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
""")
m = cur.fetchone()

cur.execute(ORDERS_CTE + """
SELECT
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE lower(status)='delivered') AS delivered,
    COUNT(*) FILTER (WHERE lower(status)='delivered' AND lower("paymentStatus")='paid') AS paid_del,
    SUM("totalPrice") FILTER (WHERE lower(status)='delivered' AND lower("paymentStatus")='paid') AS revenue,
    AVG("totalPrice") FILTER (WHERE lower(status)='delivered' AND lower("paymentStatus")='paid') AS aov
FROM orders WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-03-01'
""")
f = cur.fetchone()

print(f"""
  MARCH 2026:
    Total Orders:          {m[0]:,}
    Delivered Orders:      {m[1]:,}  (fulfillment rate: {m[1]/m[0]*100:.1f}%)
    Paid+Delivered Orders: {m[2]:,}
    Revenue:               ₦{m[3]:,.2f}
    Avg Order Value:       ₦{m[4]:,.2f}
    Service Charges:       ₦{m[5]:,.2f}
    Delivery Fees:         ₦{m[6]:,.2f}

  vs FEBRUARY 2026:
    Total Orders:          {f[0]:,} -> {m[0]:,}  ({(m[0]-f[0])/f[0]*100:+.1f}%)
    Delivered Orders:      {f[1]:,} -> {m[1]:,}  ({(m[1]-f[1])/f[1]*100:+.1f}%)
    Revenue:               ₦{f[3]:,.2f} -> ₦{m[3]:,.2f}  ({(m[3]-f[3])/f[3]*100:+.1f}%)
    AOV:                   ₦{f[4]:,.2f} -> ₦{m[4]:,.2f}  ({(m[4]-f[4])/f[4]*100:+.1f}%)
""")

print("="*90)
print("  REPORT COMPLETE")
print("="*90)

cur.close()
conn.close()
