import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from utils.db  import run_query
from utils.fmt import naira, pct, count

st.set_page_config(page_title="AR Aging · IPC", page_icon="📋", layout="wide")

with st.sidebar:
    st.markdown("## IPC Finance Dashboard")
    st.markdown("---")
    st.info("AR Aging shows **current outstanding** credit invoices — no date filter applies.")
    st.markdown("---")
    st.caption("Data refreshes every hour")

# ─── Queries ──────────────────────────────────────────────────────────────────
ar_kpi = run_query("""
    SELECT
        SUM(ar_outstanding_amount)              AS total_ar,
        COUNT(*)                                AS total_invoices,
        COUNT(DISTINCT ar_customer_id_fk)       AS total_customers,
        MAX(ar_days_outstanding)                AS oldest_days
    FROM gold.fact_ar_aging
""")

by_bucket = run_query("""
    SELECT
        ar_aging_bucket,
        ar_aging_bucket_sort,
        COUNT(*)                                AS invoices,
        SUM(ar_outstanding_amount)              AS amount
    FROM gold.fact_ar_aging
    GROUP BY ar_aging_bucket, ar_aging_bucket_sort
    ORDER BY ar_aging_bucket_sort
""")

top_customers = run_query("""
    SELECT
        ar_customer_name                        AS customer,
        COUNT(*)                                AS invoices,
        SUM(ar_outstanding_amount)              AS total_outstanding,
        MAX(ar_days_outstanding)                AS max_days,
        MIN(ar_invoice_date)                    AS oldest_invoice
    FROM gold.fact_ar_aging
    GROUP BY ar_customer_name
    ORDER BY total_outstanding DESC
    LIMIT 20
""")

full_ar = run_query("""
    SELECT
        ar_customer_name                        AS customer,
        ar_order_reference                      AS reference,
        ar_invoice_date                         AS invoice_date,
        ar_days_outstanding                     AS days_outstanding,
        ar_aging_bucket                         AS aging_bucket,
        ar_outstanding_amount                   AS amount
    FROM gold.fact_ar_aging
    ORDER BY ar_days_outstanding DESC
""")

# ─── KPIs ─────────────────────────────────────────────────────────────────────
st.markdown("## 📋 Accounts Receivable Aging")

ak = ar_kpi.iloc[0] if not ar_kpi.empty else None
total_ar  = float(ak["total_ar"]       or 0) if ak is not None else 0
invoices  = int(ak["total_invoices"]   or 0) if ak is not None else 0
customers = int(ak["total_customers"]  or 0) if ak is not None else 0
oldest    = int(ak["oldest_days"]      or 0) if ak is not None else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total AR Outstanding", naira(total_ar))
col2.metric("Open Invoices",        count(invoices))
col3.metric("Customers with AR",    count(customers))
col4.metric("Oldest Invoice",       f"{oldest} days", delta_color="inverse")

st.markdown("---")

# ─── Bucket bars + pie ────────────────────────────────────────────────────────
left, right = st.columns(2)

bucket_colors = {
    "0-30 days":  "#2D9D5D",
    "31-60 days": "#F4A261",
    "61-90 days": "#E76F51",
    "90+ days":   "#E63946",
}

with left:
    st.markdown("#### AR by Aging Bucket")
    if not by_bucket.empty:
        by_bucket["color"] = by_bucket["ar_aging_bucket"].map(
            lambda x: bucket_colors.get(x, "#888")
        )
        fig = go.Figure(go.Bar(
            x=by_bucket["ar_aging_bucket"],
            y=by_bucket["amount"],
            marker_color=by_bucket["color"],
            text=by_bucket["amount"].apply(lambda x: naira(float(x))),
            textposition="outside",
        ))
        fig.update_layout(
            yaxis_title="₦",
            xaxis_title="",
            plot_bgcolor="white",
            height=320,
            margin=dict(t=30, b=0, l=0, r=0),
            showlegend=False,
        )
        fig.update_yaxes(gridcolor="#F0F0F0")
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.markdown("#### AR Distribution")
    if not by_bucket.empty:
        fig2 = px.pie(
            by_bucket,
            values="amount",
            names="ar_aging_bucket",
            color="ar_aging_bucket",
            color_discrete_map=bucket_colors,
            hole=0.5,
        )
        fig2.update_traces(textinfo="percent+label")
        fig2.update_layout(
            showlegend=False,
            margin=dict(t=10, b=0, l=0, r=0),
            height=320,
        )
        st.plotly_chart(fig2, use_container_width=True)

# ─── Top customers with AR ────────────────────────────────────────────────────
st.markdown("---")
st.markdown("#### Top Customers by Outstanding Balance")
if not top_customers.empty:
    display = top_customers.copy()
    display["total_outstanding"] = display["total_outstanding"].apply(lambda x: naira(float(x)))
    display["invoices"]          = display["invoices"].apply(lambda x: count(int(x)))
    display["max_days"]          = display["max_days"].apply(lambda x: f"{int(x)} days")
    display.columns = ["Customer", "Invoices", "Outstanding", "Max Days", "Oldest Invoice"]
    st.dataframe(display, use_container_width=True, height=350)

# ─── Full AR table ────────────────────────────────────────────────────────────
st.markdown("---")
with st.expander("Full AR Aging Detail"):
    if not full_ar.empty:
        display2 = full_ar.copy()
        display2["amount"] = display2["amount"].apply(lambda x: naira(float(x)))
        display2.columns   = ["Customer", "Reference", "Invoice Date", "Days Outstanding",
                               "Aging Bucket", "Amount"]
        st.dataframe(display2, use_container_width=True, height=500)
