#!/usr/bin/env python3
"""DAASH March 2026 Monthly Report — data extraction (no PDF yet).

Computes every metric needed for the 8-slide monthly report and prints them
so we can sanity-check before generating the PDF.

Methodology
-----------
- sales / orders   → gold.fact_dash_orders WHERE lower(order_status)='delivered'
- service charge   → raw_dash.revenueledgers WHERE description LIKE 'Service charge%'
- channel          → fact_dash_orders.order_channel ('pos' / 'website')
- brand            → bv.bv_dash_customers.customer_business_name (joined on customer_id)
- order quality    → fact_dash_orders.order_status ('rejected' / 'voided')
"""
import os
import psycopg2
from datetime import date

PG = dict(
    database="PROD_ANALYTICS_DB",
    user=os.environ["PG_USER"].strip("\r"),
    password=os.environ["PG_PASSWORD"].strip("\r"),
    host=os.environ["PG_HOST"].strip("\r"),
    port=os.environ["PG_PORT"].strip("\r"),
)

conn = psycopg2.connect(**PG)
cur = conn.cursor()


def naira(n):
    if n is None:
        return "₦0"
    n = float(n)
    if abs(n) >= 1e6:
        return f"₦{n/1e6:,.1f}M"
    return f"₦{n:,.0f}"


def pct(a, b):
    if not b:
        return "n/a"
    p = (a - b) / b * 100
    sign = "+" if p >= 0 else ""
    return f"{sign}{p:.1f}%"


def hr(t):
    print("\n" + "=" * 78)
    print(t)
    print("=" * 78)


def section(t):
    print(f"\n--- {t} ---")


# ----------------------------------------------------------------------------
# Month definitions
# ----------------------------------------------------------------------------
MONTHS = {
    "Jan": ("2026-01-01", "2026-02-01", 31),
    "Feb": ("2026-02-01", "2026-03-01", 28),
    "Mar": ("2026-03-01", "2026-04-01", 31),
}


# ----------------------------------------------------------------------------
# 1. KPI cards: sales, orders, service charge, active brands
# ----------------------------------------------------------------------------
hr("1. KPI CARDS — Jan / Feb / Mar")

monthly = {}
for label, (start, end, days) in MONTHS.items():
    cur.execute(
        f"""
        SELECT count(*), coalesce(sum(total_sales),0)
        FROM gold.fact_dash_orders
        WHERE order_date >= '{start}' AND order_date < '{end}'
          AND lower(order_status) = 'delivered'
        """
    )
    orders, sales = cur.fetchone()

    cur.execute(
        f"""
        SELECT coalesce(sum(amount),0)
        FROM raw_dash.revenueledgers
        WHERE "createdAt" >= '{start}' AND "createdAt" < '{end}'
          AND description LIKE 'Service charge%'
        """
    )
    svc = cur.fetchone()[0]

    cur.execute(
        f"""
        SELECT count(DISTINCT c.customer_business_name)
        FROM gold.fact_dash_orders o
        JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
        WHERE o.order_date >= '{start}' AND o.order_date < '{end}'
          AND lower(o.order_status) = 'delivered'
          AND c.customer_business_name IS NOT NULL
        """
    )
    brands = cur.fetchone()[0]

    monthly[label] = {
        "orders": orders,
        "sales": float(sales),
        "svc": float(svc),
        "brands": brands,
        "days": days,
    }

print(f"  {'Month':<5} {'Orders':>10} {'Sales':>15} {'Svc Chg':>12} {'Brands':>8} {'Days':>5}")
for m in ["Jan", "Feb", "Mar"]:
    d = monthly[m]
    print(
        f"  {m:<5} {d['orders']:>10,} {naira(d['sales']):>15} {naira(d['svc']):>12} {d['brands']:>8} {d['days']:>5}"
    )

print("\n  March vs February:")
mom_sales = pct(monthly["Mar"]["sales"], monthly["Feb"]["sales"])
mom_orders = pct(monthly["Mar"]["orders"], monthly["Feb"]["orders"])
mom_svc = pct(monthly["Mar"]["svc"], monthly["Feb"]["svc"])
mom_brands = monthly["Mar"]["brands"] - monthly["Feb"]["brands"]
print(f"    Sales:    {mom_sales}")
print(f"    Orders:   {mom_orders}")
print(f"    Svc Chg:  {mom_svc}")
print(f"    Brands:   {mom_brands:+d}")


# ----------------------------------------------------------------------------
# 2. Channel split (POS vs Website)
# ----------------------------------------------------------------------------
hr("2. CHANNEL SPLIT — Feb vs Mar")

for label, (start, end, _) in [("Feb", MONTHS["Feb"][:2] + (28,)), ("Mar", MONTHS["Mar"][:2] + (31,))]:
    pass

for label in ["Feb", "Mar"]:
    start, end, _ = MONTHS[label]
    section(label)
    cur.execute(
        f"""
        SELECT order_channel, count(*), coalesce(sum(total_sales),0)
        FROM gold.fact_dash_orders
        WHERE order_date >= '{start}' AND order_date < '{end}'
          AND lower(order_status) = 'delivered'
        GROUP BY 1 ORDER BY 1
        """
    )
    for ch, n, s in cur.fetchall():
        print(f"  {ch:10s} {n:6,} orders  {naira(s):>12}")


# ----------------------------------------------------------------------------
# 3. Best / worst / average day in March + special days
# ----------------------------------------------------------------------------
hr("3. DAILY EXTREMES — March")

cur.execute(
    f"""
    SELECT order_date, count(*), sum(total_sales)::bigint
    FROM gold.fact_dash_orders
    WHERE order_date >= '2026-03-01' AND order_date < '2026-04-01'
      AND lower(order_status) = 'delivered'
    GROUP BY 1 ORDER BY sum(total_sales) DESC
    """
)
day_rows = cur.fetchall()
print(f"  Best day:    {day_rows[0]}")
print(f"  Worst day:   {day_rows[-1]}")
total_sales_mar = sum(r[2] for r in day_rows)
total_orders_mar = sum(r[1] for r in day_rows)
print(
    f"  Average day: ₦{total_sales_mar/len(day_rows):,.0f}, {total_orders_mar/len(day_rows):,.0f} orders ({len(day_rows)} days)"
)

# Special days
section("Special days")
for d, name in [("2026-03-08", "IWD"), ("2026-03-15", "Mother's Day")]:
    cur.execute(
        f"""
        SELECT count(*), sum(total_sales)::bigint,
               sum(total_sales) FILTER (WHERE order_channel='pos')::bigint,
               sum(total_sales) FILTER (WHERE order_channel='website')::bigint,
               count(*) FILTER (WHERE order_channel='pos'),
               count(*) FILTER (WHERE order_channel='website')
        FROM gold.fact_dash_orders
        WHERE order_date = '{d}' AND lower(order_status)='delivered'
        """
    )
    n, s, pos_s, web_s, pos_n, web_n = cur.fetchone()
    print(f"  {d} ({name}): {n} orders, ₦{s:,}  | POS {pos_n}/₦{pos_s:,}  Web {web_n}/₦{web_s:,}")


# ----------------------------------------------------------------------------
# 4. Day of Week — March
# ----------------------------------------------------------------------------
hr("4. DAY OF WEEK — March")

cur.execute(
    f"""
    SELECT to_char(order_date, 'Day') AS dow,
           extract(dow FROM order_date)::int AS dow_num,
           count(*) AS orders,
           sum(total_sales)::bigint AS sales
    FROM gold.fact_dash_orders
    WHERE order_date >= '2026-03-01' AND order_date < '2026-04-01'
      AND lower(order_status) = 'delivered'
    GROUP BY 1, 2 ORDER BY 2
    """
)
print(f"  {'Day':<10} {'Orders':>8} {'Sales':>15} {'AOV':>12}")
total_orders = 0
total_sales = 0
weekend_o = 0
weekend_s = 0
for dow, dnum, o, s in cur.fetchall():
    aov = s / o if o else 0
    total_orders += o
    total_sales += s
    if dnum in (0, 6):
        weekend_o += o
        weekend_s += s
    print(f"  {dow.strip():<10} {o:>8,} {naira(s):>15} ₦{aov:>10,.0f}")
print(
    f"\n  Weekend (Sat+Sun): {weekend_o:,} orders ({weekend_o/total_orders*100:.1f}%), {naira(weekend_s)} ({weekend_s/total_sales*100:.1f}%)"
)


# ----------------------------------------------------------------------------
# 5. Top 10 brands — Mar (with Feb comparison)
# ----------------------------------------------------------------------------
hr("5. TOP 10 BRANDS — March (vs Feb)")

cur.execute(
    """
    WITH mar AS (
      SELECT c.customer_business_name AS brand,
             count(*) AS orders,
             sum(o.total_sales)::bigint AS sales
      FROM gold.fact_dash_orders o
      JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
        AND lower(o.order_status)='delivered'
        AND c.customer_business_name IS NOT NULL
      GROUP BY 1
    ),
    feb AS (
      SELECT c.customer_business_name AS brand,
             sum(o.total_sales)::bigint AS sales
      FROM gold.fact_dash_orders o
      JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-02-01' AND o.order_date < '2026-03-01'
        AND lower(o.order_status)='delivered'
        AND c.customer_business_name IS NOT NULL
      GROUP BY 1
    )
    SELECT mar.brand, mar.orders, mar.sales,
           coalesce(feb.sales, 0) AS feb_sales
    FROM mar LEFT JOIN feb USING (brand)
    ORDER BY mar.sales DESC LIMIT 10
    """
)
top10 = cur.fetchall()
mar_total = sum(monthly["Mar"]["sales"] for _ in [1])  # placeholder
mar_total = monthly["Mar"]["sales"]
top3 = sum(r[2] for r in top10[:3])
print(f"  {'#':<3}{'Brand':<32}{'Orders':>8}{'Sales':>14}{'Share':>9}{'AOV':>12}{'vs Feb':>10}")
for i, (brand, o, s, fs) in enumerate(top10, 1):
    aov = s / o if o else 0
    share = s / mar_total * 100
    delta = pct(s, fs) if fs else "NEW"
    print(f"  {i:<3}{brand[:31]:<32}{o:>8,}{naira(s):>14}{share:>8.1f}%₦{aov:>10,.0f}{delta:>10}")
print(f"\n  Top 3 concentration: {top3/mar_total*100:.1f}% of total")


# ----------------------------------------------------------------------------
# 6. Brand health — retained / churned / new
# ----------------------------------------------------------------------------
hr("6. BRAND HEALTH — Feb → March")

cur.execute(
    """
    WITH feb_brands AS (
      SELECT DISTINCT c.customer_business_name AS brand
      FROM gold.fact_dash_orders o
      JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-02-01' AND o.order_date < '2026-03-01'
        AND lower(o.order_status)='delivered'
        AND c.customer_business_name IS NOT NULL
    ),
    mar_brands AS (
      SELECT DISTINCT c.customer_business_name AS brand
      FROM gold.fact_dash_orders o
      JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
        AND lower(o.order_status)='delivered'
        AND c.customer_business_name IS NOT NULL
    )
    SELECT
      (SELECT count(*) FROM feb_brands) AS feb_count,
      (SELECT count(*) FROM mar_brands) AS mar_count,
      (SELECT count(*) FROM (SELECT brand FROM feb_brands INTERSECT SELECT brand FROM mar_brands) i) AS retained,
      (SELECT count(*) FROM (SELECT brand FROM feb_brands EXCEPT    SELECT brand FROM mar_brands) e) AS churned,
      (SELECT count(*) FROM (SELECT brand FROM mar_brands EXCEPT    SELECT brand FROM feb_brands) n) AS new_b
    """
)
fb, mb, ret, churn, new_b = cur.fetchone()
print(f"  Feb active:       {fb}")
print(f"  Mar active:       {mb}")
print(f"  Retained:         {ret} ({ret/fb*100:.0f}%)")
print(f"  Churned (Feb→Mar):{churn}")
print(f"  New in Mar:       {new_b}")

section("Churned brands (with Feb revenue & last order date)")
cur.execute(
    """
    WITH feb_brands AS (
      SELECT c.customer_business_name AS brand,
             max(o.order_date) AS last_order,
             sum(o.total_sales)::bigint AS feb_revenue
      FROM gold.fact_dash_orders o
      JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-02-01' AND o.order_date < '2026-03-01'
        AND lower(o.order_status)='delivered'
        AND c.customer_business_name IS NOT NULL
      GROUP BY 1
    ),
    mar_brands AS (
      SELECT DISTINCT c.customer_business_name AS brand
      FROM gold.fact_dash_orders o
      JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
        AND lower(o.order_status)='delivered'
        AND c.customer_business_name IS NOT NULL
    )
    SELECT f.brand, f.last_order, f.feb_revenue
    FROM feb_brands f
    LEFT JOIN mar_brands m USING (brand)
    WHERE m.brand IS NULL
    ORDER BY f.feb_revenue DESC
    """
)
churn_total = 0
for b, lo, rev in cur.fetchall():
    churn_total += rev
    days_gone = (date(2026, 3, 31) - lo).days
    print(f"  {b[:30]:<30} last={lo}  Feb=₦{rev:>12,}  days_gone={days_gone}")
print(f"  Total Feb revenue from churned brands: ₦{churn_total:,}")

section("New brands in March (orders + revenue)")
cur.execute(
    """
    WITH feb_brands AS (
      SELECT DISTINCT c.customer_business_name AS brand
      FROM gold.fact_dash_orders o
      JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-02-01' AND o.order_date < '2026-03-01'
        AND lower(o.order_status)='delivered'
        AND c.customer_business_name IS NOT NULL
    ),
    mar_brands AS (
      SELECT c.customer_business_name AS brand,
             count(*) AS orders,
             sum(o.total_sales)::bigint AS revenue,
             min(o.order_date) AS first_order
      FROM gold.fact_dash_orders o
      JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
      WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
        AND lower(o.order_status)='delivered'
        AND c.customer_business_name IS NOT NULL
      GROUP BY 1
    )
    SELECT m.brand, m.orders, m.revenue, m.first_order
    FROM mar_brands m
    LEFT JOIN feb_brands f USING (brand)
    WHERE f.brand IS NULL
    ORDER BY m.revenue DESC
    """
)
for b, o, r, fo in cur.fetchall():
    print(f"  {b[:30]:<30}  first={fo}  orders={o}  rev=₦{r:,}")


# ----------------------------------------------------------------------------
# 7. Order quality — weekly trend
# ----------------------------------------------------------------------------
hr("7. ORDER QUALITY — Weekly Trend (Mar)")

cur.execute(
    """
    SELECT date_trunc('week', order_date)::date AS wk,
           count(*) AS total,
           count(*) FILTER (WHERE lower(order_status)='rejected') AS rejected,
           count(*) FILTER (WHERE lower(order_status)='voided') AS voided
    FROM gold.fact_dash_orders
    WHERE order_date >= '2026-02-23' AND order_date < '2026-04-01'
    GROUP BY 1 ORDER BY 1
    """
)
print(f"  {'Week':<14}{'Orders':>10}{'Rejected':>10}{'Voided':>10}{'Rate':>10}")
for wk, t, rej, vod in cur.fetchall():
    rate = (rej + vod) / t * 100 if t else 0
    print(f"  {str(wk):<14}{t:>10,}{rej:>10}{vod:>10}{rate:>9.2f}%")

section("Top issue brands (March)")
cur.execute(
    """
    SELECT c.customer_business_name, count(*) AS orders,
           count(*) FILTER (WHERE lower(o.order_status)='rejected') AS rej,
           count(*) FILTER (WHERE lower(o.order_status)='voided') AS vod
    FROM gold.fact_dash_orders o
    JOIN bv.bv_dash_customers c ON c.customer_id_pk = o.customer_id
    WHERE o.order_date >= '2026-03-01' AND o.order_date < '2026-04-01'
      AND c.customer_business_name IS NOT NULL
    GROUP BY 1
    HAVING count(*) >= 30
    ORDER BY (count(*) FILTER (WHERE lower(o.order_status) IN ('rejected','voided')))::numeric / count(*) DESC
    LIMIT 10
    """
)
for b, o, rej, vod in cur.fetchall():
    rate = (rej + vod) / o * 100
    print(f"  {b[:30]:<30}  orders={o:>6}  rej={rej:>3}  vod={vod:>3}  rate={rate:>5.2f}%")

cur.close()
conn.close()
