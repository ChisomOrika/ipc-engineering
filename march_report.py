#!/usr/bin/env python3
"""DAASH March 2026 Comprehensive Report"""

import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv('/Users/sapaleague/Downloads/ipc_analytics/.env')

conn = psycopg2.connect(
    host=os.getenv('PG_HOST','').strip('\r'),
    port=os.getenv('PG_PORT','').strip('\r'),
    user=os.getenv('PG_USER','').strip('\r'),
    password=os.getenv('PG_PASSWORD','').strip('\r'),
    dbname='PROD_ANALYTICS_DB',
    sslmode='require'
)
cur = conn.cursor()

def fmt(n):
    """Format number with commas"""
    if n is None: return 'N/A'
    if isinstance(n, float): return f"{n:,.2f}"
    return f"{n:,}"

def naira(n):
    if n is None: return 'N/A'
    return f"₦{n:,.2f}"

def pct(n):
    if n is None: return 'N/A'
    return f"{n:.2f}%"

print("="*80)
print("DAASH MARCH 2026 COMPREHENSIVE REPORT")
print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
print("="*80)

# ── TABLE STRUCTURES ──
print("\n" + "─"*80)
print("TABLE STRUCTURES")
print("─"*80)

for tbl in ['orders', 'branches', 'revenueledgers', 'customers']:
    cur.execute(f"""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_schema='raw_dash' AND table_name='{tbl}'
        ORDER BY ordinal_position LIMIT 50
    """)
    rows = cur.fetchall()
    print(f"\n  raw_dash.{tbl} ({len(rows)} columns):")
    for col, dtype in rows:
        print(f"    {col:<40} {dtype}")

# Check if deliveries table exists
cur.execute("""
    SELECT column_name, data_type FROM information_schema.columns
    WHERE table_schema='raw_dash' AND table_name='deliveries'
    ORDER BY ordinal_position LIMIT 30
""")
del_cols = cur.fetchall()
if del_cols:
    print(f"\n  raw_dash.deliveries ({len(del_cols)} columns):")
    for col, dtype in del_cols:
        print(f"    {col:<40} {dtype}")
else:
    print("\n  raw_dash.deliveries: TABLE NOT FOUND")

# ── 0. DISTINCT STATUS VALUES ──
print("\n" + "─"*80)
print("DISTINCT ORDER STATUSES")
print("─"*80)
cur.execute("SELECT DISTINCT status, count(*) FROM raw_dash.orders GROUP BY status ORDER BY count(*) DESC")
for s, c in cur.fetchall():
    print(f"  {str(s):<25} {fmt(c)}")

# Check distinct values for channel/source columns
print("\n  Checking for channel/source columns...")
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_schema='raw_dash' AND table_name='orders'
    AND lower(column_name) IN ('channel','ordertype','source','platform','order_type','ordersource','order_source')
""")
chan_cols = cur.fetchall()
print(f"  Channel-like columns found: {[c[0] for c in chan_cols]}")

# Check orderType values
cur.execute("SELECT DISTINCT \"orderType\", count(*) FROM raw_dash.orders GROUP BY \"orderType\" ORDER BY count(*) DESC LIMIT 20")
rows = cur.fetchall()
if rows:
    print("\n  orderType values:")
    for v, c in rows:
        print(f"    {str(v):<30} {fmt(c)}")

# Check source values if exists
try:
    cur.execute("SELECT DISTINCT source, count(*) FROM raw_dash.orders GROUP BY source ORDER BY count(*) DESC LIMIT 20")
    rows = cur.fetchall()
    if rows:
        print("\n  source values:")
        for v, c in rows:
            print(f"    {str(v):<30} {fmt(c)}")
except:
    conn.rollback()

# Check revenueledgers type/category
print("\n  Revenue ledger type/category values:")
cur.execute("""SELECT DISTINCT type, count(*) FROM raw_dash.revenueledgers GROUP BY type ORDER BY count(*) DESC LIMIT 20""")
rows = cur.fetchall()
for v, c in rows:
    print(f"    type: {str(v):<30} {fmt(c)}")

try:
    cur.execute("""SELECT DISTINCT category, count(*) FROM raw_dash.revenueledgers GROUP BY category ORDER BY count(*) DESC LIMIT 20""")
    rows = cur.fetchall()
    for v, c in rows:
        print(f"    category: {str(v):<30} {fmt(c)}")
except:
    conn.rollback()

# ── 1. ORDER VOLUME — March vs Feb ──
print("\n" + "="*80)
print("1. ORDER VOLUME — March vs February 2026")
print("="*80)
cur.execute("""
    SELECT
        to_char(date_trunc('month', "createdAt"), 'YYYY-MM') as month,
        count(*) as total_orders,
        count(*) FILTER (WHERE lower(status)='delivered') as delivered,
        count(*) FILTER (WHERE lower(status)='cancelled') as cancelled,
        count(*) FILTER (WHERE lower(status)='rejected') as rejected,
        count(*) FILTER (WHERE lower(status) IN ('void','voided')) as voided,
        count(*) FILTER (WHERE lower(status) NOT IN ('delivered','cancelled','rejected','void','voided')) as other
    FROM raw_dash.orders
    WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
    GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall()
print(f"\n  {'Month':<12} {'Total':>10} {'Delivered':>10} {'Cancelled':>10} {'Rejected':>10} {'Voided':>10} {'Other':>10}")
print(f"  {'-'*12} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10} {'-'*10}")
for r in rows:
    print(f"  {r[0]:<12} {fmt(r[1]):>10} {fmt(r[2]):>10} {fmt(r[3]):>10} {fmt(r[4]):>10} {fmt(r[5]):>10} {fmt(r[6]):>10}")

if len(rows) == 2:
    feb, mar = rows[0], rows[1]
    chg = ((mar[1] - feb[1]) / feb[1] * 100) if feb[1] else 0
    print(f"\n  MoM Change: {pct(chg)} ({fmt(mar[1] - feb[1])} orders)")

# ── 2. REVENUE — GMV (March vs Feb) ──
print("\n" + "="*80)
print("2. REVENUE — GMV from Delivered Orders (March vs Feb)")
print("="*80)

# Check which column has the order value
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_schema='raw_dash' AND table_name='orders'
    AND lower(column_name) LIKE '%total%' OR lower(column_name) LIKE '%amount%' OR lower(column_name) LIKE '%value%' OR lower(column_name) LIKE '%price%'
""")
val_cols = cur.fetchall()
print(f"  Value columns: {[c[0] for c in val_cols]}")

cur.execute("""
    SELECT
        to_char(date_trunc('month', "createdAt"), 'YYYY-MM') as month,
        count(*) FILTER (WHERE lower(status)='delivered') as delivered_orders,
        coalesce(sum("totalPrice") FILTER (WHERE lower(status)='delivered'), 0) as gmv_totalamount
    FROM raw_dash.orders
    WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
    GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall()
print(f"\n  {'Month':<12} {'Delivered':>12} {'GMV (totalAmount)':>22}")
print(f"  {'-'*12} {'-'*12} {'-'*22}")
for r in rows:
    print(f"  {r[0]:<12} {fmt(r[1]):>12} {naira(float(r[2])):>22}")

if len(rows) == 2:
    feb_gmv, mar_gmv = float(rows[0][2]), float(rows[1][2])
    chg = ((mar_gmv - feb_gmv) / feb_gmv * 100) if feb_gmv else 0
    print(f"\n  GMV MoM Change: {pct(chg)}")

# ── 3. PLATFORM FEES from Revenue Ledgers ──
print("\n" + "="*80)
print("3. PLATFORM FEES — Revenue Ledgers (March vs Feb)")
print("="*80)

# First get a sample to understand the data
cur.execute("""
    SELECT * FROM raw_dash.revenueledgers LIMIT 5
""")
sample_cols = [desc[0] for desc in cur.description]
sample_rows = cur.fetchall()
print(f"\n  Columns: {sample_cols}")
print("  Sample rows:")
for r in sample_rows:
    print(f"    {r}")

# Platform fees by month
cur.execute("""
    SELECT
        to_char(date_trunc('month', "createdAt"), 'YYYY-MM') as month,
        type,
        count(*) as entries,
        sum(amount) as total_amount
    FROM raw_dash.revenueledgers
    WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
    GROUP BY 1, 2
    ORDER BY 1, 2
""")
rows = cur.fetchall()
print(f"\n  Revenue Ledger by Month & Type:")
print(f"  {'Month':<12} {'Type':<25} {'Entries':>10} {'Total Amount':>18}")
print(f"  {'-'*12} {'-'*25} {'-'*10} {'-'*18}")
for r in rows:
    print(f"  {r[0]:<12} {str(r[1]):<25} {fmt(r[2]):>10} {naira(float(r[3])):>18}")

# Total platform fees by month (all types summed)
cur.execute("""
    SELECT
        to_char(date_trunc('month', "createdAt"), 'YYYY-MM') as month,
        count(*) as entries,
        sum(amount) as total_platform_fees
    FROM raw_dash.revenueledgers
    WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
    GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall()
print(f"\n  Total Platform Fees by Month:")
print(f"  {'Month':<12} {'Entries':>10} {'Total Fees':>18}")
print(f"  {'-'*12} {'-'*10} {'-'*18}")
for r in rows:
    print(f"  {r[0]:<12} {fmt(r[1]):>10} {naira(float(r[2])):>18}")

if len(rows) == 2:
    feb_fee, mar_fee = float(rows[0][2]), float(rows[1][2])
    chg = ((mar_fee - feb_fee) / feb_fee * 100) if feb_fee else 0
    print(f"\n  Platform Fee MoM Change: {pct(chg)}")

# ── 4. CHANNEL BREAKDOWN ──
print("\n" + "="*80)
print("4. CHANNEL BREAKDOWN — March vs Feb")
print("="*80)
cur.execute("""
    SELECT
        to_char(date_trunc('month', "createdAt"), 'YYYY-MM') as month,
        coalesce("orderType", 'Unknown') as channel,
        count(*) as total_orders,
        count(*) FILTER (WHERE lower(status)='delivered') as delivered
    FROM raw_dash.orders
    WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
    GROUP BY 1, 2 ORDER BY 1, 3 DESC
""")
rows = cur.fetchall()
print(f"\n  {'Month':<12} {'Channel':<20} {'Total':>10} {'Delivered':>10}")
print(f"  {'-'*12} {'-'*20} {'-'*10} {'-'*10}")
for r in rows:
    print(f"  {r[0]:<12} {str(r[1]):<20} {fmt(r[2]):>10} {fmt(r[3]):>10}")

# ── 5. TOP 10 BRANDS by Order Volume (March) ──
print("\n" + "="*80)
print("5. TOP 10 BRANDS by Order Volume (March 2026)")
print("="*80)

# Check branch join field
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_schema='raw_dash' AND table_name='orders'
    AND lower(column_name) LIKE '%branch%'
""")
branch_cols = cur.fetchall()
print(f"  Branch columns in orders: {[c[0] for c in branch_cols]}")

cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_schema='raw_dash' AND table_name='branches'
    AND lower(column_name) IN ('_id','id','name','brandname','brand_name','brand')
""")
br_id_cols = cur.fetchall()
print(f"  ID/name columns in branches: {[c[0] for c in br_id_cols]}")

# Get total delivered for March for % calc
cur.execute("""
    SELECT count(*) FROM raw_dash.orders
    WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
""")
mar_total = cur.fetchone()[0]

cur.execute("""
    SELECT
        coalesce(b.name, 'Unknown') as brand_name,
        count(*) as order_count,
        round(count(*)::numeric / %s * 100, 2) as pct_of_total
    FROM raw_dash.orders o
    LEFT JOIN raw_dash.branches b ON o.branch = b._id
    WHERE o."createdAt" >= '2026-03-01' AND o."createdAt" < '2026-04-01'
    GROUP BY 1
    ORDER BY 2 DESC
    LIMIT 10
""", (mar_total,))
rows = cur.fetchall()
print(f"\n  {'Rank':<6} {'Brand':<40} {'Orders':>10} {'% of Total':>12}")
print(f"  {'-'*6} {'-'*40} {'-'*10} {'-'*12}")
for i, r in enumerate(rows, 1):
    print(f"  {i:<6} {str(r[0])[:39]:<40} {fmt(r[1]):>10} {pct(float(r[2])):>12}")

# ── 6. CUSTOMER METRICS ──
print("\n" + "="*80)
print("6. CUSTOMER METRICS — March vs Feb")
print("="*80)
cur.execute("""
    WITH first_orders AS (
        SELECT customer, min("createdAt") as first_order_date
        FROM raw_dash.orders
        GROUP BY customer
    ),
    monthly AS (
        SELECT
            to_char(date_trunc('month', o."createdAt"), 'YYYY-MM') as month,
            count(DISTINCT o.customer) as active_customers,
            count(DISTINCT o.customer) FILTER (WHERE date_trunc('month', f.first_order_date) = date_trunc('month', o."createdAt")) as new_customers,
            count(DISTINCT o.customer) FILTER (WHERE date_trunc('month', f.first_order_date) < date_trunc('month', o."createdAt")) as returning_customers
        FROM raw_dash.orders o
        JOIN first_orders f ON o.customer = f.customer
        WHERE o."createdAt" >= '2026-02-01' AND o."createdAt" < '2026-04-01'
        GROUP BY 1
    )
    SELECT * FROM monthly ORDER BY month
""")
rows = cur.fetchall()
print(f"\n  {'Month':<12} {'Active':>12} {'New':>12} {'Returning':>12} {'New %':>10}")
print(f"  {'-'*12} {'-'*12} {'-'*12} {'-'*12} {'-'*10}")
for r in rows:
    new_pct = (r[2] / r[1] * 100) if r[1] else 0
    print(f"  {r[0]:<12} {fmt(r[1]):>12} {fmt(r[2]):>12} {fmt(r[3]):>12} {pct(new_pct):>10}")

# ── 7. AOV ──
print("\n" + "="*80)
print("7. AVERAGE ORDER VALUE — March vs Feb")
print("="*80)
cur.execute("""
    SELECT
        to_char(date_trunc('month', "createdAt"), 'YYYY-MM') as month,
        count(*) FILTER (WHERE lower(status)='delivered') as delivered,
        sum("totalPrice") FILTER (WHERE lower(status)='delivered') as gmv,
        avg("totalPrice") FILTER (WHERE lower(status)='delivered') as aov
    FROM raw_dash.orders
    WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
    GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall()
print(f"\n  {'Month':<12} {'Delivered':>12} {'GMV':>18} {'AOV':>14}")
print(f"  {'-'*12} {'-'*12} {'-'*18} {'-'*14}")
for r in rows:
    print(f"  {r[0]:<12} {fmt(r[1]):>12} {naira(float(r[2])):>18} {naira(float(r[3])):>14}")

if len(rows) == 2:
    aov_feb, aov_mar = float(rows[0][3]), float(rows[1][3])
    chg = ((aov_mar - aov_feb) / aov_feb * 100) if aov_feb else 0
    print(f"\n  AOV MoM Change: {pct(chg)}")

# ── 8. ORDER QUALITY ──
print("\n" + "="*80)
print("8. ORDER QUALITY — March vs Feb")
print("="*80)
cur.execute("""
    SELECT
        to_char(date_trunc('month', "createdAt"), 'YYYY-MM') as month,
        count(*) as total,
        round(count(*) FILTER (WHERE lower(status)='rejected')::numeric / count(*) * 100, 2) as rejection_rate,
        round(count(*) FILTER (WHERE lower(status) IN ('void','voided'))::numeric / count(*) * 100, 2) as void_rate,
        round(count(*) FILTER (WHERE lower(status)='cancelled')::numeric / count(*) * 100, 2) as cancellation_rate,
        round(count(*) FILTER (WHERE lower(status)='delivered')::numeric / count(*) * 100, 2) as delivery_rate
    FROM raw_dash.orders
    WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
    GROUP BY 1 ORDER BY 1
""")
rows = cur.fetchall()
print(f"\n  {'Month':<12} {'Total':>10} {'Delivery%':>12} {'Reject%':>10} {'Void%':>10} {'Cancel%':>10}")
print(f"  {'-'*12} {'-'*10} {'-'*12} {'-'*10} {'-'*10} {'-'*10}")
for r in rows:
    print(f"  {r[0]:<12} {fmt(r[1]):>10} {pct(float(r[5])):>12} {pct(float(r[2])):>10} {pct(float(r[3])):>10} {pct(float(r[4])):>10}")

# ── 9. WEEKLY TREND (March) ──
print("\n" + "="*80)
print("9. WEEKLY TREND — March 2026")
print("="*80)
cur.execute("""
    SELECT
        'W' || to_char(date_trunc('week', "createdAt"), 'WW') as week,
        min("createdAt"::date) as week_start,
        max("createdAt"::date) as week_end,
        count(*) as total_orders,
        count(*) FILTER (WHERE lower(status)='delivered') as delivered,
        coalesce(sum("totalPrice") FILTER (WHERE lower(status)='delivered'), 0) as gmv,
        count(DISTINCT customer) as active_customers
    FROM raw_dash.orders
    WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
    GROUP BY 1, date_trunc('week', "createdAt")
    ORDER BY 2
""")
rows = cur.fetchall()
print(f"\n  {'Week':<6} {'Period':<25} {'Total':>8} {'Delivered':>10} {'GMV':>18} {'Customers':>10}")
print(f"  {'-'*6} {'-'*25} {'-'*8} {'-'*10} {'-'*18} {'-'*10}")
for r in rows:
    period = f"{r[1]} to {r[2]}"
    print(f"  {r[0]:<6} {period:<25} {fmt(r[3]):>8} {fmt(r[4]):>10} {naira(float(r[5])):>18} {fmt(r[6]):>10}")

# ── 10. DAY OF WEEK PATTERN (March) ──
print("\n" + "="*80)
print("10. DAY-OF-WEEK PATTERN — March 2026")
print("="*80)
cur.execute("""
    SELECT
        to_char("createdAt", 'Day') as day_name,
        extract(isodow FROM "createdAt") as day_num,
        count(*) as orders,
        count(*) FILTER (WHERE lower(status)='delivered') as delivered,
        coalesce(sum("totalPrice") FILTER (WHERE lower(status)='delivered'), 0) as gmv
    FROM raw_dash.orders
    WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
    GROUP BY 1, 2 ORDER BY 2
""")
rows = cur.fetchall()
print(f"\n  {'Day':<12} {'Orders':>10} {'Delivered':>10} {'GMV':>18}")
print(f"  {'-'*12} {'-'*10} {'-'*10} {'-'*18}")
for r in rows:
    print(f"  {r[0].strip():<12} {fmt(r[2]):>10} {fmt(r[3]):>10} {naira(float(r[4])):>18}")

# ── 11. DELIVERY PERFORMANCE ──
print("\n" + "="*80)
print("11. DELIVERY PERFORMANCE")
print("="*80)
if del_cols:
    # Check for time columns
    time_cols = [c[0] for c in del_cols if 'time' in c[0].lower() or 'date' in c[0].lower() or 'created' in c[0].lower() or 'deliver' in c[0].lower() or 'picked' in c[0].lower() or 'pickup' in c[0].lower()]
    print(f"  Time-related columns: {time_cols}")

    cur.execute("""SELECT * FROM raw_dash.deliveries LIMIT 3""")
    cols = [desc[0] for desc in cur.description]
    sample = cur.fetchall()
    print(f"  All columns: {cols}")
    for s in sample:
        print(f"    {s}")

    # Try to get delivery time if possible
    try:
        cur.execute("""
            SELECT
                count(*) as total_deliveries,
                avg(EXTRACT(EPOCH FROM ("deliveredAt" - "createdAt"))/60) as avg_minutes,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM ("deliveredAt" - "createdAt"))/60) as median_minutes
            FROM raw_dash.deliveries
            WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
            AND "deliveredAt" IS NOT NULL
        """)
        r = cur.fetchone()
        if r:
            print(f"\n  March Deliveries: {fmt(r[0])}")
            if r[1]: print(f"  Avg Delivery Time: {r[1]:.1f} minutes")
            if r[2]: print(f"  Median Delivery Time: {r[2]:.1f} minutes")
    except Exception as e:
        conn.rollback()
        print(f"  Could not calculate delivery time: {e}")
        # Try with order-level timestamps
        try:
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_schema='raw_dash' AND table_name='orders'
                AND lower(column_name) LIKE '%deliver%' OR lower(column_name) LIKE '%picked%'
            """)
            print(f"  Order delivery columns: {[c[0] for c in cur.fetchall()]}")
        except:
            conn.rollback()
else:
    print("  Deliveries table not found. Checking order-level timestamps...")
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='raw_dash' AND table_name='orders'
        AND (lower(column_name) LIKE '%deliver%' OR lower(column_name) LIKE '%picked%' OR lower(column_name) LIKE '%accept%')
    """)
    ts_cols = cur.fetchall()
    print(f"  Delivery timestamp columns in orders: {[c[0] for c in ts_cols]}")
    if ts_cols:
        # Try to compute delivery time from order timestamps
        try:
            cur.execute("""
                SELECT
                    count(*) as orders,
                    avg(EXTRACT(EPOCH FROM ("deliveredAt" - "createdAt"))/60) FILTER (WHERE "deliveredAt" IS NOT NULL) as avg_min
                FROM raw_dash.orders
                WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
                AND lower(status) = 'delivered'
            """)
            r = cur.fetchone()
            if r and r[1]:
                print(f"  Avg order-to-delivery time: {r[1]:.1f} minutes ({fmt(r[0])} orders)")
        except Exception as e:
            conn.rollback()
            print(f"  Error: {e}")

# ── 12. CUSTOMER ORDER FREQUENCY (March) ──
print("\n" + "="*80)
print("12. CUSTOMER ORDER FREQUENCY DISTRIBUTION — March 2026")
print("="*80)
cur.execute("""
    WITH cust_freq AS (
        SELECT customer, count(*) as order_count
        FROM raw_dash.orders
        WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
        GROUP BY customer
    )
    SELECT
        CASE
            WHEN order_count = 1 THEN '1 order'
            WHEN order_count = 2 THEN '2 orders'
            WHEN order_count = 3 THEN '3 orders'
            WHEN order_count = 4 THEN '4 orders'
            WHEN order_count BETWEEN 5 AND 9 THEN '5-9 orders'
            WHEN order_count BETWEEN 10 AND 19 THEN '10-19 orders'
            ELSE '20+ orders'
        END as frequency_bucket,
        count(*) as num_customers,
        sum(order_count) as total_orders,
        min(order_count) as min_ord,
        max(order_count) as max_ord
    FROM cust_freq
    GROUP BY
        CASE
            WHEN order_count = 1 THEN 1
            WHEN order_count = 2 THEN 2
            WHEN order_count = 3 THEN 3
            WHEN order_count = 4 THEN 4
            WHEN order_count BETWEEN 5 AND 9 THEN 5
            WHEN order_count BETWEEN 10 AND 19 THEN 6
            ELSE 7
        END,
        CASE
            WHEN order_count = 1 THEN '1 order'
            WHEN order_count = 2 THEN '2 orders'
            WHEN order_count = 3 THEN '3 orders'
            WHEN order_count = 4 THEN '4 orders'
            WHEN order_count BETWEEN 5 AND 9 THEN '5-9 orders'
            WHEN order_count BETWEEN 10 AND 19 THEN '10-19 orders'
            ELSE '20+ orders'
        END
    ORDER BY min_ord
""")
rows = cur.fetchall()
total_custs = sum(r[1] for r in rows)
print(f"\n  {'Frequency':<16} {'Customers':>12} {'% Customers':>14} {'Orders':>10}")
print(f"  {'-'*16} {'-'*12} {'-'*14} {'-'*10}")
for r in rows:
    cust_pct = (r[1] / total_custs * 100) if total_custs else 0
    print(f"  {r[0]:<16} {fmt(r[1]):>12} {pct(cust_pct):>14} {fmt(r[2]):>10}")
print(f"  {'TOTAL':<16} {fmt(total_custs):>12} {'100.00%':>14} {fmt(sum(r[2] for r in rows)):>10}")

# ── BONUS: MARCH SUMMARY ──
print("\n" + "="*80)
print("MARCH 2026 EXECUTIVE SUMMARY")
print("="*80)

# Get March numbers
cur.execute("""
    SELECT
        count(*) as total,
        count(*) FILTER (WHERE lower(status)='delivered') as delivered,
        sum("totalPrice") FILTER (WHERE lower(status)='delivered') as gmv,
        avg("totalPrice") FILTER (WHERE lower(status)='delivered') as aov,
        count(DISTINCT customer) as active_customers,
        round(count(*) FILTER (WHERE lower(status)='delivered')::numeric / count(*) * 100, 2) as delivery_rate
    FROM raw_dash.orders
    WHERE "createdAt" >= '2026-03-01' AND "createdAt" < '2026-04-01'
""")
mar = cur.fetchone()

# Get Feb numbers
cur.execute("""
    SELECT
        count(*) as total,
        count(*) FILTER (WHERE lower(status)='delivered') as delivered,
        sum("totalPrice") FILTER (WHERE lower(status)='delivered') as gmv,
        avg("totalPrice") FILTER (WHERE lower(status)='delivered') as aov,
        count(DISTINCT customer) as active_customers,
        round(count(*) FILTER (WHERE lower(status)='delivered')::numeric / count(*) * 100, 2) as delivery_rate
    FROM raw_dash.orders
    WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-03-01'
""")
feb = cur.fetchone()

def mom_chg(m, f):
    if f and f != 0: return ((m - f) / f * 100)
    return 0

print(f"\n  {'Metric':<30} {'March':>18} {'February':>18} {'MoM Change':>12}")
print(f"  {'-'*30} {'-'*18} {'-'*18} {'-'*12}")
print(f"  {'Total Orders':<30} {fmt(mar[0]):>18} {fmt(feb[0]):>18} {pct(mom_chg(mar[0], feb[0])):>12}")
print(f"  {'Delivered Orders':<30} {fmt(mar[1]):>18} {fmt(feb[1]):>18} {pct(mom_chg(mar[1], feb[1])):>12}")
print(f"  {'GMV':<30} {naira(float(mar[2])):>18} {naira(float(feb[2])):>18} {pct(mom_chg(float(mar[2]), float(feb[2]))):>12}")
print(f"  {'AOV':<30} {naira(float(mar[3])):>18} {naira(float(feb[3])):>18} {pct(mom_chg(float(mar[3]), float(feb[3]))):>12}")
print(f"  {'Active Customers':<30} {fmt(mar[4]):>18} {fmt(feb[4]):>18} {pct(mom_chg(mar[4], feb[4])):>12}")
print(f"  {'Delivery Rate':<30} {pct(float(mar[5])):>18} {pct(float(feb[5])):>18} {'':>12}")

# Platform fees summary
cur.execute("""
    SELECT
        to_char(date_trunc('month', "createdAt"), 'YYYY-MM') as month,
        sum(amount) as total_fees
    FROM raw_dash.revenueledgers
    WHERE "createdAt" >= '2026-02-01' AND "createdAt" < '2026-04-01'
    GROUP BY 1 ORDER BY 1
""")
fee_rows = cur.fetchall()
if len(fee_rows) == 2:
    feb_fee, mar_fee = float(fee_rows[0][1]), float(fee_rows[1][1])
    print(f"  {'Platform Fees':<30} {naira(mar_fee):>18} {naira(feb_fee):>18} {pct(mom_chg(mar_fee, feb_fee)):>12}")
elif len(fee_rows) == 1:
    print(f"  {'Platform Fees':<30} {naira(float(fee_rows[0][1])):>18}")

cur.close()
conn.close()
print("\n" + "="*80)
print("Report complete.")
print("="*80)
