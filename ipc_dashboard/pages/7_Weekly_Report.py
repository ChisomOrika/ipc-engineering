import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import datetime as dt
import streamlit as st
import pandas as pd

from utils.db     import run_query
from utils.fmt    import naira, pct, count
from utils.styles import inject_css, page_header, section_title, COLOR_DAASH, COLOR_GOSOURCE

st.set_page_config(page_title="Weekly Report · IPC", page_icon="📋", layout="wide")
inject_css()

# ── Date ranges (always relative to the current Friday) ──────────────────────
today = dt.date.today()
# On a Friday: this_week = last 7 days (Fri→Thu), last_week = 7 days before that
this_week_end   = today - dt.timedelta(days=1)          # Yesterday (Thursday)
this_week_start = today - dt.timedelta(days=7)          # Last Friday
last_week_end   = today - dt.timedelta(days=8)          # Thursday before last
last_week_start = today - dt.timedelta(days=14)         # Friday before last

TW_S = this_week_start.strftime("%Y-%m-%d")
TW_E = this_week_end.strftime("%Y-%m-%d")
LW_S = last_week_start.strftime("%Y-%m-%d")
LW_E = last_week_end.strftime("%Y-%m-%d")

page_header(
    "IPC — Weekly Performance Report",
    f"Week: {this_week_start.strftime('%d %b')} – {this_week_end.strftime('%d %b %Y')}  ·  "
    f"vs Prior: {last_week_start.strftime('%d %b')} – {last_week_end.strftime('%d %b %Y')}"
)

# ─── Queries ─────────────────────────────────────────────────────────────────
AGG_CLAUSE = "LOWER(revenue_payment_method) IN ('chowdeck', 'glovo')"

daash_kpi = run_query(f"""
    SELECT
        period,
        COUNT(*)                                                AS orders,
        SUM(revenue_amount)                                     AS revenue,
        SUM(revenue_service_charge_amount)                      AS svc_charge,
        SUM(revenue_amount) / NULLIF(COUNT(*), 0)               AS aov,
        COUNT(*) FILTER (WHERE {AGG_CLAUSE})                   AS agg_orders,
        SUM(revenue_amount) FILTER (WHERE {AGG_CLAUSE})        AS agg_rev,
        COUNT(*) FILTER (WHERE NOT ({AGG_CLAUSE}))             AS direct_orders,
        SUM(revenue_amount) FILTER (WHERE NOT ({AGG_CLAUSE}))  AS direct_rev
    FROM (
        SELECT *, 'this_week' AS period
        FROM gold.fact_revenue
        WHERE service_line = 'DAASH'
          AND revenue_order_date BETWEEN '{TW_S}' AND '{TW_E}'
        UNION ALL
        SELECT *, 'last_week' AS period
        FROM gold.fact_revenue
        WHERE service_line = 'DAASH'
          AND revenue_order_date BETWEEN '{LW_S}' AND '{LW_E}'
    ) t
    GROUP BY period
""")

daash_brands_tw = run_query(f"""
    SELECT
        COALESCE(NULLIF(TRIM(revenue_customer_name), ''), 'Unknown') AS brand,
        COUNT(*)                    AS orders,
        SUM(revenue_amount)         AS revenue
    FROM gold.fact_revenue
    WHERE service_line = 'DAASH'
      AND revenue_order_date BETWEEN '{TW_S}' AND '{TW_E}'
    GROUP BY revenue_customer_name
    ORDER BY revenue DESC
    LIMIT 5
""")

daash_brands_lw = run_query(f"""
    SELECT
        COALESCE(NULLIF(TRIM(revenue_customer_name), ''), 'Unknown') AS brand,
        SUM(revenue_amount) AS revenue_lw
    FROM gold.fact_revenue
    WHERE service_line = 'DAASH'
      AND revenue_order_date BETWEEN '{LW_S}' AND '{LW_E}'
    GROUP BY revenue_customer_name
""")

daash_cancelled = run_query(f"""
    SELECT
        period,
        COUNT(*) FILTER (WHERE LOWER(order_status) = 'cancelled') AS cancelled,
        COUNT(*) FILTER (WHERE LOWER(order_status) = 'voided')    AS voided,
        COUNT(*)                                                    AS total
    FROM (
        SELECT DISTINCT ON (order_id_pk) order_id_pk, order_status, 'this_week' AS period
        FROM raw_gosource.orders
        WHERE created_at::date BETWEEN '{TW_S}' AND '{TW_E}'
        UNION ALL
        SELECT DISTINCT ON (order_id_pk) order_id_pk, order_status, 'last_week' AS period
        FROM raw_gosource.orders
        WHERE created_at::date BETWEEN '{LW_S}' AND '{LW_E}'
    ) t
    GROUP BY period
""") if False else pd.DataFrame()  # GoSource cancelled handled separately

daash_cancelled = run_query(f"""
    SELECT
        period,
        SUM(CASE WHEN LOWER(order_status)='cancelled' THEN 1 ELSE 0 END) AS cancelled,
        SUM(CASE WHEN LOWER(order_status)='voided'    THEN 1 ELSE 0 END) AS voided,
        COUNT(*) AS total
    FROM (
        SELECT order_status, 'this_week' AS period
        FROM raw_dash.orders
        WHERE created_at::date BETWEEN '{TW_S}' AND '{TW_E}'
        UNION ALL
        SELECT order_status, 'last_week' AS period
        FROM raw_dash.orders
        WHERE created_at::date BETWEEN '{LW_S}' AND '{LW_E}'
    ) t
    GROUP BY period
""")

gs_kpi = run_query(f"""
    SELECT
        period,
        COUNT(*)                                    AS orders,
        SUM(revenue_amount)                         AS revenue,
        COUNT(DISTINCT revenue_customer_id)         AS customers,
        SUM(revenue_amount) / NULLIF(COUNT(*),0)    AS aov
    FROM (
        SELECT *, 'this_week' AS period
        FROM gold.fact_revenue
        WHERE service_line = 'GoSource'
          AND revenue_order_date BETWEEN '{TW_S}' AND '{TW_E}'
        UNION ALL
        SELECT *, 'last_week' AS period
        FROM gold.fact_revenue
        WHERE service_line = 'GoSource'
          AND revenue_order_date BETWEEN '{LW_S}' AND '{LW_E}'
    ) t
    GROUP BY period
""")

gs_customers_tw = run_query(f"""
    SELECT
        COALESCE(NULLIF(TRIM(revenue_customer_name), ''), 'Unknown') AS customer,
        COUNT(*)            AS orders,
        SUM(revenue_amount) AS revenue
    FROM gold.fact_revenue
    WHERE service_line = 'GoSource'
      AND revenue_order_date BETWEEN '{TW_S}' AND '{TW_E}'
    GROUP BY revenue_customer_name
    ORDER BY revenue DESC
    LIMIT 5
""")

gs_customers_lw = run_query(f"""
    SELECT
        COALESCE(NULLIF(TRIM(revenue_customer_name), ''), 'Unknown') AS customer,
        SUM(revenue_amount) AS revenue_lw
    FROM gold.fact_revenue
    WHERE service_line = 'GoSource'
      AND revenue_order_date BETWEEN '{LW_S}' AND '{LW_E}'
    GROUP BY revenue_customer_name
""")

gs_ar = run_query("""
    SELECT
        ar_aging_bucket,
        COUNT(*)                   AS invoices,
        SUM(ar_outstanding_amount) AS amount
    FROM gold.fact_ar_aging
    GROUP BY ar_aging_bucket
    ORDER BY ar_aging_bucket
""")

# ─── Helpers ─────────────────────────────────────────────────────────────────
def _get(df, period, col, default=0):
    row = df[df["period"] == period]
    if row.empty or row.iloc[0][col] is None: return float(default)
    return float(row.iloc[0][col])

def _chg(curr, prev):
    if prev and prev > 0:
        return (curr - prev) / prev * 100
    return None

def _arrow(chg, good="up"):
    if chg is None: return "—"
    if chg > 0: color = "#22C55E" if good == "up" else "#EF4444"; sym = "▲"
    elif chg < 0: color = "#EF4444" if good == "up" else "#22C55E"; sym = "▼"
    else: color = "#94A3B8"; sym = "→"
    return f"<span style='color:{color};font-weight:700;'>{sym} {abs(chg):.1f}%</span>"

def _status(chg, good="up", warn=-5):
    if chg is None: return "—"
    if good == "up":
        if chg > 0: return "🟢"
        elif chg > warn: return "🟡"
        else: return "🔴"
    else:
        if chg < 0: return "🟢"
        elif chg < abs(warn): return "🟡"
        else: return "🔴"

def slide_header(title, color, subtitle=""):
    st.markdown(
        f"<div style='background:{color};border-radius:12px;padding:16px 24px;"
        f"margin-bottom:16px;'>"
        f"<div style='font-size:18px;font-weight:800;color:white;'>{title}</div>"
        f"{'<div style=\"font-size:12px;color:rgba(255,255,255,0.75);margin-top:2px;\">' + subtitle + '</div>' if subtitle else ''}"
        f"</div>",
        unsafe_allow_html=True,
    )

def kpi_table(rows, cols=("Metric", "This Week", "Last Week", "Change", "Status")):
    header = " | ".join(f"**{c}**" for c in cols)
    sep    = " | ".join("---" for _ in cols)
    body   = "\n".join(" | ".join(str(v) for v in r) for r in rows)
    st.markdown(f"{header}\n{sep}\n{body}", unsafe_allow_html=True)

def top5_table(df_tw, df_lw, name_col, total_rev):
    if df_tw.empty:
        st.info("No data for this period.")
        return
    merged = df_tw.merge(df_lw, on=name_col, how="left")
    merged["revenue_lw"] = merged["revenue_lw"].fillna(0)
    rows = []
    for i, r in enumerate(merged.itertuples(), 1):
        chg = _chg(r.revenue, r.revenue_lw)
        share = r.revenue / total_rev * 100 if total_rev > 0 else 0
        rows.append((
            f"**{i}**", f"_{r._asdict()[name_col]}_",
            count(int(r.orders)), naira(r.revenue),
            _arrow(chg), f"{share:.1f}%"
        ))
    header = "**#** | **Name** | **Orders** | **Revenue** | **vs Last Wk** | **Share**"
    sep    = "--- | --- | --- | --- | --- | ---"
    body   = "\n".join(" | ".join(str(v) for v in r) for r in rows)
    st.markdown(f"{header}\n{sep}\n{body}", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════════════════
#  DAASH
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown("---")
slide_header(
    f"🍔 DAASH — WEEK AT A GLANCE",
    COLOR_DAASH,
    f"{this_week_start.strftime('%d %b')} – {this_week_end.strftime('%d %b %Y')} vs {last_week_start.strftime('%d %b')} – {last_week_end.strftime('%d %b %Y')}"
)

# Scalars
d_tw_rev   = _get(daash_kpi, "this_week", "revenue")
d_lw_rev   = _get(daash_kpi, "last_week", "revenue")
d_tw_ord   = _get(daash_kpi, "this_week", "orders")
d_lw_ord   = _get(daash_kpi, "last_week", "orders")
d_tw_svc   = _get(daash_kpi, "this_week", "svc_charge")
d_lw_svc   = _get(daash_kpi, "last_week", "svc_charge")
d_tw_aov   = _get(daash_kpi, "this_week", "aov")
d_lw_aov   = _get(daash_kpi, "last_week", "aov")
d_tw_agg   = _get(daash_kpi, "this_week", "agg_orders")
d_lw_agg   = _get(daash_kpi, "last_week", "agg_orders")
d_tw_dir   = _get(daash_kpi, "this_week", "direct_orders")
d_lw_dir   = _get(daash_kpi, "last_week", "direct_orders")
d_tw_aggr  = _get(daash_kpi, "this_week", "agg_rev")
d_lw_aggr  = _get(daash_kpi, "last_week", "agg_rev")
d_tw_dirr  = _get(daash_kpi, "this_week", "direct_rev")
d_lw_dirr  = _get(daash_kpi, "last_week", "direct_rev")

# Issue rate
def _issue(df, period):
    row = df[df["period"] == period]
    if row.empty: return 0, 0, 0, 0
    r = row.iloc[0]
    c = float(r.get("cancelled", 0) or 0)
    v = float(r.get("voided", 0) or 0)
    t = float(r.get("total", 1) or 1)
    return c, v, c + v, (c + v) / t * 100

d_tw_canc, d_tw_void, d_tw_issues, d_tw_issue_rate = _issue(daash_cancelled, "this_week")
d_lw_canc, d_lw_void, d_lw_issues, d_lw_issue_rate = _issue(daash_cancelled, "last_week")

# ── SLIDE 1: Week at a Glance ────────────────────────────────────────────────
section_title("SLIDE 1 — WEEK AT A GLANCE")
kpi_table([
    ("Total Sales",    naira(d_tw_rev),                  naira(d_lw_rev),                  _arrow(_chg(d_tw_rev, d_lw_rev)),                _status(_chg(d_tw_rev, d_lw_rev))),
    ("Total Orders",   count(int(d_tw_ord)),              count(int(d_lw_ord)),              _arrow(_chg(d_tw_ord, d_lw_ord)),                _status(_chg(d_tw_ord, d_lw_ord))),
    ("Platform Fee",   naira(d_tw_svc),                  naira(d_lw_svc),                  _arrow(_chg(d_tw_svc, d_lw_svc)),                _status(_chg(d_tw_svc, d_lw_svc))),
    ("AOV",            naira(d_tw_aov),                  naira(d_lw_aov),                  _arrow(_chg(d_tw_aov, d_lw_aov)),                _status(_chg(d_tw_aov, d_lw_aov))),
    ("Issue Rate",     f"{d_tw_issue_rate:.1f}%",         f"{d_lw_issue_rate:.1f}%",         _arrow(_chg(d_tw_issue_rate, d_lw_issue_rate), good="down"), _status(_chg(d_tw_issue_rate, d_lw_issue_rate), good="down")),
])

st.markdown("---")

# ── SLIDE 2: Channel Breakdown ───────────────────────────────────────────────
section_title("SLIDE 2 — CHANNEL BREAKDOWN")
left, right = st.columns(2)
with left:
    kpi_table(
        [
            ("Direct Orders",     count(int(d_tw_dir)),  count(int(d_lw_dir)),  _arrow(_chg(d_tw_dir, d_lw_dir)),  _status(_chg(d_tw_dir, d_lw_dir))),
            ("Direct Revenue",    naira(d_tw_dirr),       naira(d_lw_dirr),       _arrow(_chg(d_tw_dirr, d_lw_dirr)), _status(_chg(d_tw_dirr, d_lw_dirr))),
            ("Aggregator Orders", count(int(d_tw_agg)),  count(int(d_lw_agg)),  _arrow(_chg(d_tw_agg, d_lw_agg)),  _status(_chg(d_tw_agg, d_lw_agg))),
            ("Aggregator Rev",    naira(d_tw_aggr),       naira(d_lw_aggr),       _arrow(_chg(d_tw_aggr, d_lw_aggr)), _status(_chg(d_tw_aggr, d_lw_aggr))),
        ],
        cols=("Channel", "This Week", "Last Week", "Change", "Status")
    )
with right:
    agg_share_tw = (d_tw_agg / d_tw_ord * 100) if d_tw_ord > 0 else 0
    agg_share_lw = (d_lw_agg / d_lw_ord * 100) if d_lw_ord > 0 else 0
    st.markdown(
        f"<div style='background:#F8FAFC;border-radius:10px;padding:16px 20px;'>"
        f"<div style='font-size:12px;color:#64748B;font-weight:600;text-transform:uppercase;'>Aggregator Mix</div>"
        f"<div style='font-size:32px;font-weight:800;color:{COLOR_DAASH};'>{agg_share_tw:.1f}%</div>"
        f"<div style='font-size:12px;color:#94A3B8;'>of orders via Chowdeck / Glovo</div>"
        f"<div style='margin-top:8px;font-size:12px;color:#64748B;'>vs {agg_share_lw:.1f}% last week  "
        f"{_arrow(_chg(agg_share_tw, agg_share_lw))}</div>"
        f"</div>",
        unsafe_allow_html=True
    )

st.markdown("---")

# ── SLIDE 3: Brand Performance ───────────────────────────────────────────────
section_title("SLIDE 3 — TOP 5 BRANDS")
top5_table(daash_brands_tw, daash_brands_lw, "brand", d_tw_rev)

st.markdown("---")

# ── SLIDE 4: Order Issues ────────────────────────────────────────────────────
section_title("SLIDE 4 — ORDER ISSUES & LOST REVENUE")
lost_rev_tw = d_tw_issues * d_tw_aov if d_tw_aov > 0 else 0
lost_rev_lw = d_lw_issues * d_lw_aov if d_lw_aov > 0 else 0

kpi_table([
    ("Cancelled",       count(int(d_tw_canc)),   count(int(d_lw_canc)),   _arrow(_chg(d_tw_canc, d_lw_canc), good="down"),   _status(_chg(d_tw_canc, d_lw_canc), good="down")),
    ("Voided",          count(int(d_tw_void)),   count(int(d_lw_void)),   _arrow(_chg(d_tw_void, d_lw_void), good="down"),   _status(_chg(d_tw_void, d_lw_void), good="down")),
    ("Total Issues",    count(int(d_tw_issues)), count(int(d_lw_issues)), _arrow(_chg(d_tw_issues, d_lw_issues), good="down"),_status(_chg(d_tw_issues, d_lw_issues), good="down")),
    ("Issue Rate",      f"{d_tw_issue_rate:.1f}%", f"{d_lw_issue_rate:.1f}%", _arrow(_chg(d_tw_issue_rate, d_lw_issue_rate), good="down"), _status(_chg(d_tw_issue_rate, d_lw_issue_rate), good="down")),
    ("Est. Lost Rev",   naira(lost_rev_tw),      naira(lost_rev_lw),      _arrow(_chg(lost_rev_tw, lost_rev_lw), good="down"), _status(_chg(lost_rev_tw, lost_rev_lw), good="down")),
])

st.markdown("---")
st.markdown("---")

# ═══════════════════════════════════════════════════════════════════════════════
#  GOSOURCE
# ═══════════════════════════════════════════════════════════════════════════════
slide_header(
    f"📦 GOSOURCE — WEEK AT A GLANCE",
    COLOR_GOSOURCE,
    f"{this_week_start.strftime('%d %b')} – {this_week_end.strftime('%d %b %Y')} vs {last_week_start.strftime('%d %b')} – {last_week_end.strftime('%d %b %Y')}"
)

g_tw_rev  = _get(gs_kpi, "this_week", "revenue")
g_lw_rev  = _get(gs_kpi, "last_week", "revenue")
g_tw_ord  = _get(gs_kpi, "this_week", "orders")
g_lw_ord  = _get(gs_kpi, "last_week", "orders")
g_tw_cust = _get(gs_kpi, "this_week", "customers")
g_lw_cust = _get(gs_kpi, "last_week", "customers")
g_tw_aov  = _get(gs_kpi, "this_week", "aov")
g_lw_aov  = _get(gs_kpi, "last_week", "aov")

# AR at risk
ar_90_amount = float(gs_ar[gs_ar["ar_aging_bucket"] == "90+ days"]["amount"].sum()) if not gs_ar.empty else 0
ar_total = float(gs_ar["amount"].sum()) if not gs_ar.empty else 0
ar_90_pct = (ar_90_amount / ar_total * 100) if ar_total > 0 else 0

# ── SLIDE 5: Week at a Glance ────────────────────────────────────────────────
section_title("SLIDE 5 — WEEK AT A GLANCE")
kpi_table([
    ("Total Revenue",   naira(g_tw_rev),        naira(g_lw_rev),        _arrow(_chg(g_tw_rev, g_lw_rev)),   _status(_chg(g_tw_rev, g_lw_rev))),
    ("Total Orders",    count(int(g_tw_ord)),    count(int(g_lw_ord)),    _arrow(_chg(g_tw_ord, g_lw_ord)),   _status(_chg(g_tw_ord, g_lw_ord))),
    ("Active Customers",count(int(g_tw_cust)),  count(int(g_lw_cust)),  _arrow(_chg(g_tw_cust, g_lw_cust)), _status(_chg(g_tw_cust, g_lw_cust))),
    ("AOV",             naira(g_tw_aov),         naira(g_lw_aov),         _arrow(_chg(g_tw_aov, g_lw_aov)),   _status(_chg(g_tw_aov, g_lw_aov))),
    ("AR 90+ Days",     naira(ar_90_amount),     "—",                    "—",                                 "🔴" if ar_90_pct > 30 else ("🟡" if ar_90_pct > 15 else "🟢")),
])

st.markdown(
    f"<div style='background:#FEF2F2;border-left:4px solid #EF4444;border-radius:8px;"
    f"padding:10px 16px;margin-top:12px;font-size:13px;color:#991B1B;'>"
    f"⚠️ AR at Risk: <b>{naira(ar_90_amount)}</b> ({ar_90_pct:.1f}% of total AR) is 90+ days overdue"
    f"</div>" if ar_90_pct > 0 else "",
    unsafe_allow_html=True
)

st.markdown("---")

# ── SLIDE 6: Top Customers ───────────────────────────────────────────────────
section_title("SLIDE 6 — TOP 5 CUSTOMERS")
top5_table(gs_customers_tw, gs_customers_lw, "customer", g_tw_rev)

st.markdown("---")

# ── SLIDE 7: AR Aging ────────────────────────────────────────────────────────
section_title("SLIDE 7 — AR AGING SUMMARY")
if not gs_ar.empty:
    bucket_colors = {"0-30 days": "🟢", "31-60 days": "🟡", "61-90 days": "🟠", "90+ days": "🔴"}
    ar_rows = []
    for _, row in gs_ar.iterrows():
        share = float(row["amount"]) / ar_total * 100 if ar_total > 0 else 0
        ar_rows.append((
            bucket_colors.get(row["ar_aging_bucket"], "⚪") + " " + row["ar_aging_bucket"],
            count(int(row["invoices"])),
            naira(float(row["amount"])),
            f"{share:.1f}%"
        ))
    kpi_table(ar_rows, cols=("Bucket", "Invoices", "Amount", "% of AR"))

st.markdown("---")

# ── Footer ───────────────────────────────────────────────────────────────────
st.markdown(
    f"<div style='text-align:center;font-size:11px;color:#94A3B8;padding:16px;'>"
    f"IPC Finance Dashboard · Weekly Report · Generated {dt.datetime.now().strftime('%d %b %Y, %H:%M WAT')}"
    f"</div>",
    unsafe_allow_html=True
)
