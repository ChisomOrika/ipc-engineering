import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datetime as dt
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from utils.db  import run_query
from utils.fmt import naira, pct, count

st.set_page_config(page_title="Revenue · IPC", page_icon="📈", layout="wide")

# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
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

    service_filter = st.multiselect(
        "Service Line",
        ["DAASH", "GoSource"],
        default=["DAASH", "GoSource"],
    )
    filter_clause = ""
    if service_filter and len(service_filter) < 2:
        svc = service_filter[0]
        filter_clause = f"AND service_line = '{svc}'"

    st.markdown(f"**{start.strftime('%d %b %Y')} → {end.strftime('%d %b %Y')}**")
    st.markdown("---")
    st.caption("Data refreshes every hour")

# ─── Queries ──────────────────────────────────────────────────────────────────
rev_summary = run_query(f"""
    SELECT
        service_line,
        SUM(revenue_amount)                 AS total_revenue,
        COUNT(*)                            AS order_count
    FROM gold.fact_revenue
    WHERE revenue_order_date BETWEEN '{start}' AND '{end}'
    {filter_clause}
    GROUP BY service_line
""")

profit_summary = run_query(f"""
    SELECT
        service_line,
        SUM(profit_revenue_amount)          AS revenue,
        SUM(profit_gross_profit_amount)     AS gross_profit,
        CASE WHEN SUM(profit_revenue_amount) > 0
             THEN ROUND(SUM(profit_gross_profit_amount) / SUM(profit_revenue_amount) * 100, 1)
             ELSE 0 END                     AS gross_margin_pct
    FROM gold.fact_profitability
    WHERE profit_date BETWEEN '{start}' AND '{end}'
    {filter_clause}
    GROUP BY service_line
""")

monthly_rev = run_query(f"""
    SELECT
        TO_CHAR(revenue_month, 'Mon YY')    AS month_label,
        revenue_month,
        service_line,
        SUM(revenue_amount) / 1e6           AS revenue_m,
        COUNT(*)                            AS orders
    FROM gold.fact_revenue
    WHERE revenue_order_date BETWEEN '{start}' AND '{end}'
    {filter_clause}
    GROUP BY revenue_month, service_line
    ORDER BY revenue_month
""")

monthly_margin = run_query(f"""
    SELECT
        TO_CHAR(profit_month, 'Mon YY')         AS month_label,
        profit_month,
        service_line,
        CASE WHEN SUM(profit_revenue_amount) > 0
             THEN ROUND(SUM(profit_gross_profit_amount) / SUM(profit_revenue_amount) * 100, 1)
             ELSE 0 END                          AS margin_pct
    FROM gold.fact_profitability
    WHERE profit_date BETWEEN '{start}' AND '{end}'
    {filter_clause}
    GROUP BY profit_month, service_line
    ORDER BY profit_month
""")

top_customers = run_query(f"""
    SELECT
        COALESCE(revenue_customer_name, 'Unknown')  AS customer,
        service_line,
        SUM(revenue_amount)                         AS total_revenue,
        COUNT(*)                                    AS orders
    FROM gold.fact_revenue
    WHERE revenue_order_date BETWEEN '{start}' AND '{end}'
      AND revenue_customer_name IS NOT NULL
      AND TRIM(revenue_customer_name) != ''
    {filter_clause}
    GROUP BY revenue_customer_name, service_line
    ORDER BY total_revenue DESC
    LIMIT 20
""")

# ─── KPIs ─────────────────────────────────────────────────────────────────────
st.markdown("## 📈 Revenue & Profitability")

total_rev    = float(rev_summary["total_revenue"].sum() or 0)
total_orders = int(rev_summary["order_count"].sum() or 0)
total_gp     = float(profit_summary["gross_profit"].sum() or 0)
overall_margin = (total_gp / total_rev * 100) if total_rev > 0 else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Revenue",   naira(total_rev))
col2.metric("Gross Profit",    naira(total_gp))
col3.metric("Gross Margin",    pct(overall_margin))
col4.metric("Total Orders",    count(total_orders))

st.markdown("---")

# ─── Service line cards ───────────────────────────────────────────────────────
color_map = {"DAASH": "#FF6B35", "GoSource": "#2D9D5D"}
if not rev_summary.empty:
    cols = st.columns(len(rev_summary))
    for i, (_, row) in enumerate(rev_summary.iterrows()):
        svc = row["service_line"]
        col = color_map.get(svc, "#555")
        ps  = profit_summary[profit_summary["service_line"] == svc]
        gp  = float(ps["gross_profit"].iloc[0]) if not ps.empty else 0
        mg  = float(ps["gross_margin_pct"].iloc[0]) if not ps.empty else 0
        with cols[i]:
            st.markdown(
                f"<div style='border-left:4px solid {col}; padding:12px 16px; background:#FAFAFA; border-radius:6px;'>"
                f"<div style='font-weight:700; color:{col}; font-size:18px;'>{svc}</div>"
                f"<div style='font-size:26px; font-weight:700; margin:4px 0;'>{naira(float(row['total_revenue']))}</div>"
                f"<div style='color:#555;'>Gross Profit: <b>{naira(gp)}</b></div>"
                f"<div style='color:#555;'>Margin: <b>{pct(mg)}</b></div>"
                f"<div style='color:#888; font-size:13px;'>{count(int(row['order_count']))} orders</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

st.markdown("")

# ─── Charts ───────────────────────────────────────────────────────────────────
left, right = st.columns(2)

with left:
    st.markdown("#### Monthly Revenue")
    if not monthly_rev.empty:
        fig = px.bar(
            monthly_rev,
            x="month_label", y="revenue_m",
            color="service_line",
            color_discrete_map=color_map,
            labels={"revenue_m": "Revenue (₦M)", "month_label": "", "service_line": ""},
            barmode="stack",
        )
        fig.update_layout(
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            plot_bgcolor="white",
            height=320,
            margin=dict(t=10, b=0, l=0, r=0),
        )
        fig.update_yaxes(gridcolor="#F0F0F0")
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.markdown("#### Gross Margin % Over Time")
    if not monthly_margin.empty:
        fig2 = px.line(
            monthly_margin,
            x="month_label", y="margin_pct",
            color="service_line",
            color_discrete_map=color_map,
            labels={"margin_pct": "Gross Margin %", "month_label": "", "service_line": ""},
            markers=True,
        )
        fig2.update_layout(
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            plot_bgcolor="white",
            height=320,
            margin=dict(t=10, b=0, l=0, r=0),
        )
        fig2.update_yaxes(gridcolor="#F0F0F0", ticksuffix="%")
        st.plotly_chart(fig2, use_container_width=True)

# ─── Top customers table ──────────────────────────────────────────────────────
st.markdown("---")
st.markdown("#### Top Customers by Revenue")
if not top_customers.empty:
    display = top_customers.copy()
    display["total_revenue"] = display["total_revenue"].apply(lambda x: naira(float(x)))
    display["orders"]        = display["orders"].apply(lambda x: count(int(x)))
    display.columns          = ["Customer", "Service Line", "Revenue", "Orders"]
    st.dataframe(display, use_container_width=True, height=400)
else:
    st.info("No customer data for selected period.")
