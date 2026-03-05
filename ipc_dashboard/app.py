import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import datetime as dt
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from utils.db  import run_query
from utils.fmt import naira, pct, count

# ─── Page config ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="IPC Finance",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Sidebar ─────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://img.icons8.com/fluency/48/bank-building.png", width=48)
    st.markdown("## IPC Finance Dashboard")
    st.markdown("---")
    period = st.selectbox(
        "Time Period",
        ["Month to Date", "Last 30 days", "Last 90 days",
         "This Year", "Last 12 months", "All Time", "Custom"],
        index=3,
    )
    today = dt.date.today()
    if period == "Month to Date":
        start, end = today.replace(day=1), today
    elif period == "Last 30 days":
        start, end = today - dt.timedelta(days=30), today
    elif period == "Last 90 days":
        start, end = today - dt.timedelta(days=90), today
    elif period == "This Year":
        start, end = today.replace(month=1, day=1), today
    elif period == "Last 12 months":
        start, end = today - dt.timedelta(days=365), today
    elif period == "All Time":
        start, end = dt.date(2020, 1, 1), today
    else:
        start = st.date_input("From", today - dt.timedelta(days=365))
        end   = st.date_input("To",   today)

    st.markdown(f"**{start.strftime('%d %b %Y')} → {end.strftime('%d %b %Y')}**")
    st.markdown("---")
    st.caption("Data refreshes every hour")

# ─── Data queries ─────────────────────────────────────────────────────────────
revenue_kpi = run_query(f"""
    SELECT
        SUM(revenue_amount)                                         AS total_revenue,
        SUM(CASE WHEN service_line='DAASH'     THEN revenue_amount ELSE 0 END) AS daash_rev,
        SUM(CASE WHEN service_line='GoSource'  THEN revenue_amount ELSE 0 END) AS gosource_rev,
        COUNT(*)                                                    AS order_count
    FROM gold.fact_revenue
    WHERE revenue_order_date BETWEEN '{start}' AND '{end}'
""")

profit_kpi = run_query(f"""
    SELECT
        SUM(profit_revenue_amount)       AS revenue,
        SUM(profit_gross_profit_amount)  AS gross_profit,
        CASE WHEN SUM(profit_revenue_amount) > 0
             THEN ROUND(SUM(profit_gross_profit_amount) / SUM(profit_revenue_amount) * 100, 1)
             ELSE 0 END                  AS gross_margin_pct
    FROM gold.fact_profitability
    WHERE profit_date BETWEEN '{start}' AND '{end}'
""")

cash_kpi = run_query("""
    SELECT cumulative_net_movement_amount AS cash_position
    FROM gold.fact_cash_position
    ORDER BY cash_position_date DESC
    LIMIT 1
""")

ar_kpi = run_query("""
    SELECT
        SUM(ar_outstanding_amount)  AS total_ar,
        COUNT(DISTINCT ar_customer_id_fk)  AS ar_customers
    FROM gold.fact_ar_aging
""")

expense_kpi = run_query(f"""
    SELECT SUM(expense_amount) AS total_expenses
    FROM gold.fact_expenses
    WHERE expense_date BETWEEN '{start}' AND '{end}'
""")

# Monthly revenue trend (last 18 months)
rev_trend = run_query(f"""
    SELECT
        TO_CHAR(revenue_month, 'Mon YY') AS month_label,
        revenue_month,
        service_line,
        SUM(revenue_amount) / 1e6 AS revenue_m
    FROM gold.fact_revenue
    WHERE revenue_order_date BETWEEN '{start}' AND '{end}'
    GROUP BY revenue_month, service_line
    ORDER BY revenue_month
""")

# Cash position trend
cash_trend = run_query(f"""
    SELECT
        cash_position_date,
        cumulative_net_movement_amount / 1e6 AS cash_m
    FROM gold.fact_cash_position
    WHERE cash_position_date BETWEEN '{start}' AND '{end}'
    ORDER BY cash_position_date
""")

# ─── KPIs ─────────────────────────────────────────────────────────────────────
st.markdown("## Overview")

r = revenue_kpi.iloc[0]
p = profit_kpi.iloc[0]
c = float(cash_kpi.iloc[0]["cash_position"]) if not cash_kpi.empty else 0
ar = ar_kpi.iloc[0]
ex = float(expense_kpi.iloc[0]["total_expenses"] or 0)

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Total Revenue",    naira(r["total_revenue"]))
col2.metric("DAASH Revenue",    naira(r["daash_rev"]))
col3.metric("GoSource Revenue", naira(r["gosource_rev"]))
col4.metric("Gross Profit",     naira(p["gross_profit"]))
col5.metric("Gross Margin",     pct(p["gross_margin_pct"]))
col6.metric("Total Orders",     count(r["order_count"]))

st.markdown("")
col7, col8, col9 = st.columns(3)
col7.metric("💰 Cash Position",   naira(c),  help="Current running bank balance (Lenco)")
col8.metric("📋 AR Outstanding",  naira(ar["total_ar"]),
            help=f"{count(ar['ar_customers'])} customers with unpaid invoices")
col9.metric("💸 Total Expenses",  naira(ex))

st.markdown("---")

# ─── Charts ───────────────────────────────────────────────────────────────────
left, right = st.columns(2)

with left:
    st.markdown("#### Monthly Revenue by Service Line")
    if not rev_trend.empty:
        fig = px.bar(
            rev_trend,
            x="month_label", y="revenue_m",
            color="service_line",
            color_discrete_map={"DAASH": "#FF6B35", "GoSource": "#2D9D5D"},
            labels={"revenue_m": "Revenue (₦M)", "month_label": "", "service_line": ""},
            barmode="stack",
        )
        fig.update_layout(
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(t=10, b=0, l=0, r=0),
            plot_bgcolor="white",
            height=320,
        )
        fig.update_yaxes(gridcolor="#F0F0F0")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No revenue data for the selected period.")

with right:
    st.markdown("#### Running Cash Position")
    if not cash_trend.empty:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=cash_trend["cash_position_date"],
            y=cash_trend["cash_m"],
            mode="lines",
            fill="tozeroy",
            line=dict(color="#0A9396", width=2),
            fillcolor="rgba(10,147,150,0.15)",
            name="Cash Position",
        ))
        fig2.update_layout(
            yaxis_title="Balance (₦M)",
            xaxis_title="",
            showlegend=False,
            margin=dict(t=10, b=0, l=0, r=0),
            plot_bgcolor="white",
            height=320,
        )
        fig2.update_yaxes(gridcolor="#F0F0F0")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No cash position data for the selected period.")

# ─── Profit split ─────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("#### Revenue Split by Service Line")
split_data = run_query(f"""
    SELECT service_line, SUM(revenue_amount) AS revenue
    FROM gold.fact_revenue
    WHERE revenue_order_date BETWEEN '{start}' AND '{end}'
    GROUP BY service_line
""")
if not split_data.empty:
    c1, c2 = st.columns([1, 3])
    with c1:
        fig3 = px.pie(
            split_data, values="revenue", names="service_line",
            color="service_line",
            color_discrete_map={"DAASH": "#FF6B35", "GoSource": "#2D9D5D"},
            hole=0.5,
        )
        fig3.update_traces(textinfo="percent+label")
        fig3.update_layout(showlegend=False, margin=dict(t=0, b=0, l=0, r=0), height=280)
        st.plotly_chart(fig3, use_container_width=True)
    with c2:
        for _, row in split_data.iterrows():
            svc = row["service_line"]
            rev = float(row["revenue"])
            total = float(split_data["revenue"].sum())
            share = rev / total * 100 if total > 0 else 0
            color = "#FF6B35" if svc == "DAASH" else "#2D9D5D"
            st.markdown(
                f"<div style='margin-bottom:12px;'>"
                f"<span style='color:{color};font-weight:700;font-size:18px;'>● {svc}</span><br>"
                f"<span style='font-size:28px;font-weight:700;'>{naira(rev)}</span> "
                f"<span style='color:#888;font-size:16px;'>({share:.1f}%)</span>"
                f"</div>",
                unsafe_allow_html=True
            )
