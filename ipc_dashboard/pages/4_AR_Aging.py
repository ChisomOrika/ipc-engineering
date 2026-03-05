import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import datetime as dt

from utils.db      import run_query
from utils.fmt     import naira, pct, count
from utils.styles  import (inject_css, page_header, section_title, CHART_LAYOUT,
                            COLOR_POSITIVE, COLOR_NEGATIVE)
from utils.periods import sidebar_filters

st.set_page_config(page_title="AR Aging · IPC", page_icon="📋", layout="wide")
inject_css()

_, _, _, _, _, _ = sidebar_filters()   # Keep sidebar consistent

page_header("Accounts Receivable — Aging Analysis",
            f"GoSource Credit Orders · Current Outstanding as of {dt.date.today().strftime('%d %b %Y')}")

# ─── Queries ──────────────────────────────────────────────────────────────────
ar_kpi = run_query("""
    SELECT
        SUM(ar_outstanding_amount)             AS total_ar,
        COUNT(*)                               AS invoices,
        COUNT(DISTINCT ar_customer_id_fk)      AS customers,
        MAX(ar_days_outstanding)               AS oldest_days,
        AVG(ar_days_outstanding)               AS avg_days
    FROM gold.fact_ar_aging
""")

by_bucket = run_query("""
    SELECT
        ar_aging_bucket,
        ar_aging_bucket_sort,
        COUNT(*)                    AS invoices,
        COUNT(DISTINCT ar_customer_id_fk) AS customers,
        SUM(ar_outstanding_amount)  AS amount
    FROM gold.fact_ar_aging
    GROUP BY ar_aging_bucket, ar_aging_bucket_sort
    ORDER BY ar_aging_bucket_sort
""")

# DSO: AR / (LTM Revenue / 365)
dso = run_query("""
    WITH ltm AS (
        SELECT SUM(revenue_amount) AS ltm_rev
        FROM gold.fact_revenue
        WHERE revenue_order_date >= CURRENT_DATE - INTERVAL '12 months'
    ),
    ar AS (SELECT SUM(ar_outstanding_amount) AS total_ar FROM gold.fact_ar_aging)
    SELECT
        ROUND((ar.total_ar / NULLIF(ltm.ltm_rev,0)) * 365, 0) AS dso_days
    FROM ar, ltm
""")

top_customers = run_query("""
    SELECT
        ar_customer_name                   AS customer,
        COUNT(*)                           AS invoices,
        SUM(ar_outstanding_amount)         AS outstanding,
        MAX(ar_days_outstanding)           AS max_days,
        MIN(ar_invoice_date)               AS oldest_invoice,
        SUM(CASE WHEN ar_aging_bucket='90+ days'
                 THEN ar_outstanding_amount ELSE 0 END) AS overdue_90
    FROM gold.fact_ar_aging
    GROUP BY ar_customer_name
    ORDER BY outstanding DESC
    LIMIT 20
""")

overdue_90 = run_query("""
    SELECT
        ar_customer_name AS customer,
        ar_order_reference AS reference,
        ar_invoice_date,
        ar_days_outstanding AS days,
        ar_outstanding_amount AS amount
    FROM gold.fact_ar_aging
    WHERE ar_aging_bucket = '90+ days'
    ORDER BY ar_days_outstanding DESC
""")

full_ar = run_query("""
    SELECT
        ar_customer_name       AS customer,
        ar_order_reference     AS reference,
        ar_invoice_date,
        ar_days_outstanding    AS days,
        ar_aging_bucket        AS bucket,
        ar_outstanding_amount  AS amount
    FROM gold.fact_ar_aging
    ORDER BY ar_days_outstanding DESC
""")

# ─── Scalars ──────────────────────────────────────────────────────────────────
def _v(df, col, default=0):
    if df.empty or df.iloc[0][col] is None: return float(default)
    return float(df.iloc[0][col])

total_ar  = _v(ar_kpi, "total_ar")
invoices  = int(_v(ar_kpi, "invoices"))
customers = int(_v(ar_kpi, "customers"))
oldest    = int(_v(ar_kpi, "oldest_days"))
avg_days  = _v(ar_kpi, "avg_days")
dso_days  = _v(dso, "dso_days")

overdue_90_amount = float(
    by_bucket[by_bucket["ar_aging_bucket"] == "90+ days"]["amount"].sum()
) if not by_bucket.empty else 0
overdue_90_pct = (overdue_90_amount / total_ar * 100) if total_ar > 0 else 0

# ─── KPIs ─────────────────────────────────────────────────────────────────────
section_title("RECEIVABLES OVERVIEW")

# Alert for high 90+ days
if overdue_90_pct > 30:
    st.error(
        f"⚠️ **{pct(overdue_90_pct)} of your AR ({naira(overdue_90_amount)}) is 90+ days overdue."
        f" Immediate collection action recommended.**"
    )
elif overdue_90_pct > 15:
    st.warning(
        f"🔔 {pct(overdue_90_pct)} of AR ({naira(overdue_90_amount)}) is 90+ days overdue."
    )

cols = st.columns(5)
cols[0].metric("📋 TOTAL AR",        naira(total_ar),
               help="All outstanding GoSource credit invoices")
cols[1].metric("📅 DSO",             f"{int(dso_days)} days",
               help="Days Sales Outstanding = AR / (Annual Revenue / 365). Lower is better.")
cols[2].metric("⚠️ AVG DAYS AGING",  f"{avg_days:.0f} days",
               delta=f"Oldest: {oldest} days", delta_color="off")
cols[3].metric("📂 OPEN INVOICES",   count(invoices),
               delta=f"{count(customers)} customers", delta_color="off")
cols[4].metric("🔴 90+ DAYS",        naira(overdue_90_amount),
               delta=f"{pct(overdue_90_pct)} of total AR",
               delta_color="inverse" if overdue_90_pct > 10 else "off")

st.markdown("---")

# ─── Bucket bar + donut ───────────────────────────────────────────────────────
bucket_colors = {
    "0-30 days":  "#22C55E",
    "31-60 days": "#F59E0B",
    "61-90 days": "#F97316",
    "90+ days":   "#EF4444",
}

left, right = st.columns([2, 1])

with left:
    section_title("AR BY AGING BUCKET")
    if not by_bucket.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=by_bucket["ar_aging_bucket"],
            y=by_bucket["amount"],
            marker_color=[bucket_colors.get(b, "#888") for b in by_bucket["ar_aging_bucket"]],
            text=by_bucket["amount"].apply(lambda x: naira(float(x))),
            textposition="outside",
            customdata=by_bucket[["invoices", "customers"]].values,
            hovertemplate=(
                "<b>%{x}</b><br>"
                "Amount: %{text}<br>"
                "Invoices: %{customdata[0]}<br>"
                "Customers: %{customdata[1]}<extra></extra>"
            ),
        ))
        fig.update_layout(**CHART_LAYOUT, height=300,
                          yaxis_title="₦", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

        # Summary stats per bucket
        bucket_display = by_bucket.copy()
        grand = float(bucket_display["amount"].sum() or 1)
        bucket_display["share"] = (bucket_display["amount"] / grand * 100).round(1)
        bucket_display["amount"] = bucket_display["amount"].apply(lambda x: naira(float(x)))
        bucket_display["invoices"]  = bucket_display["invoices"].apply(lambda x: count(int(x)))
        bucket_display["customers"] = bucket_display["customers"].apply(lambda x: count(int(x)))
        bucket_display["share"]     = bucket_display["share"].apply(lambda x: f"{x}%")
        bucket_display = bucket_display.drop(columns=["ar_aging_bucket_sort"])
        bucket_display.columns = ["Bucket", "Invoices", "Customers", "Amount", "% of AR"]
        st.dataframe(bucket_display, use_container_width=True, hide_index=True)

with right:
    section_title("AGING DISTRIBUTION")
    if not by_bucket.empty:
        fig2 = go.Figure(go.Pie(
            labels=by_bucket["ar_aging_bucket"],
            values=by_bucket["amount"],
            hole=0.55,
            marker_colors=[bucket_colors.get(b, "#888") for b in by_bucket["ar_aging_bucket"]],
            textinfo="percent",
            textfont_size=13,
            sort=False,
        ))
        fig2.add_annotation(
            text=f"<b>{naira(total_ar)}</b>",
            x=0.5, y=0.5, font_size=13, showarrow=False
        )
        fig2.update_layout(
            showlegend=True,
            legend=dict(orientation="v", x=1.02),
            margin=dict(t=0, b=0, l=0, r=80),
            height=300,
        )
        st.plotly_chart(fig2, use_container_width=True)

# ─── Critical 90+ day invoices ────────────────────────────────────────────────
if not overdue_90.empty:
    st.markdown("---")
    section_title(f"🔴 CRITICAL: 90+ DAY OVERDUE INVOICES ({len(overdue_90)} invoices)")
    st.markdown(
        "<div style='background:#FEF2F2;border-left:4px solid #EF4444;border-radius:8px;"
        "padding:12px 16px;margin-bottom:12px;font-size:13px;color:#991B1B;'>"
        f"These {len(overdue_90)} invoices totalling <b>{naira(float(overdue_90['amount'].sum()))}</b>"
        " require immediate collection follow-up.</div>",
        unsafe_allow_html=True,
    )
    c1, c2 = st.columns([5, 1])
    with c2:
        st.download_button("📥 Download CSV",
                           overdue_90.to_csv(index=False),
                           f"overdue_90_plus_{dt.date.today()}.csv", "text/csv")
    disp = overdue_90.copy()
    disp["amount"] = disp["amount"].apply(lambda x: naira(float(x)))
    disp["days"]   = disp["days"].apply(lambda x: f"{int(x)} days")
    disp.columns = ["Customer", "Order Reference", "Invoice Date", "Days Overdue", "Amount"]
    st.dataframe(disp, use_container_width=True, hide_index=True, height=280)

# ─── Top customers by AR ──────────────────────────────────────────────────────
st.markdown("---")
section_title("TOP CUSTOMERS BY OUTSTANDING BALANCE")
if not top_customers.empty:
    c1, c2 = st.columns([5, 1])
    with c2:
        st.download_button("📥 Download CSV",
                           top_customers.to_csv(index=False),
                           f"ar_top_customers_{dt.date.today()}.csv", "text/csv")
    display = top_customers.copy()
    display["outstanding"] = display["outstanding"].apply(lambda x: naira(float(x)))
    display["overdue_90"]  = display["overdue_90"].apply(lambda x: naira(float(x)))
    display["invoices"]    = display["invoices"].apply(lambda x: count(int(x)))
    display["max_days"]    = display["max_days"].apply(lambda x: f"{int(x)} days")
    display.columns = ["Customer", "Invoices", "Outstanding", "Max Days", "Oldest Invoice", "90+ Overdue"]
    st.dataframe(display, use_container_width=True, hide_index=True, height=400)

# ─── Full AR detail ────────────────────────────────────────────────────────────
with st.expander("📄 Full AR Detail (all open invoices)"):
    c1, c2 = st.columns([5, 1])
    with c2:
        st.download_button("📥 Download CSV",
                           full_ar.to_csv(index=False),
                           f"full_ar_{dt.date.today()}.csv", "text/csv")
    disp2 = full_ar.copy()
    disp2["amount"] = disp2["amount"].apply(lambda x: naira(float(x)))
    disp2["days"]   = disp2["days"].apply(lambda x: f"{int(x)} days")
    disp2.columns = ["Customer", "Reference", "Invoice Date", "Days", "Bucket", "Amount"]
    st.dataframe(disp2, use_container_width=True, hide_index=True, height=450)
