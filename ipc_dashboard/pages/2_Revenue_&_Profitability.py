import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

from utils.db      import run_query
from utils.fmt     import naira, pct, count
from utils.styles  import (inject_css, page_header, section_title, CHART_LAYOUT,
                            COLOR_DAASH, COLOR_GOSOURCE, COLOR_POSITIVE, COLOR_NEGATIVE)
from utils.periods import sidebar_filters, svc_filter_sql

st.set_page_config(page_title="Revenue · IPC", page_icon="📈", layout="wide")
inject_css()

start, end, prev_start, prev_end, period_label, service_lines = sidebar_filters(extra_filters=True)
svc_clause = svc_filter_sql(service_lines)

page_header("Revenue & Profitability",
            f"{period_label} · DAASH + GoSource combined")

# ─── Queries ──────────────────────────────────────────────────────────────────
def _profit(s, e):
    # NULLIF guards against PostgreSQL NaN values in GP/COGS columns
    return run_query(f"""
        SELECT
            service_line,
            COALESCE(SUM(profit_revenue_amount), 0)                                AS revenue,
            COALESCE(SUM(NULLIF(profit_cogs_amount, 'NaN'::numeric)), 0)           AS cogs,
            COALESCE(SUM(NULLIF(profit_gross_profit_amount, 'NaN'::numeric)), 0)   AS gp,
            CASE WHEN SUM(profit_revenue_amount) > 0
                 THEN ROUND((
                     COALESCE(SUM(NULLIF(profit_gross_profit_amount, 'NaN'::numeric)), 0)
                     / SUM(profit_revenue_amount) * 100)::numeric, 1)
                 ELSE 0 END                                                         AS margin_pct,
            COUNT(DISTINCT profit_order_id)                                         AS orders
        FROM gold.fact_profitability
        WHERE profit_date BETWEEN '{s}' AND '{e}'
        {svc_clause}
        GROUP BY service_line
    """)

curr_profit = _profit(start, end)
prev_profit = _profit(prev_start, prev_end)

def _totals(df):
    if df.empty: return 0, 0, 0, 0, 0
    rev  = float(df["revenue"].sum() or 0)
    gp   = float(df["gp"].sum() or 0)
    cogs = float(df["cogs"].sum() or 0)
    ords = int(df["orders"].sum() or 0)
    marg = (gp / rev * 100) if rev > 0 else 0
    return rev, gp, cogs, ords, marg

c_rev, c_gp, c_cogs, c_ord, c_marg = _totals(curr_profit)
p_rev, p_gp, p_cogs, p_ord, p_marg = _totals(prev_profit)

def _d(c, p): return ((c-p)/p*100) if p and p > 0 else None

c_aov = c_rev / c_ord if c_ord > 0 else 0
p_aov = p_rev / p_ord if p_ord > 0 else 0

# Monthly trend (revenue + GP + margin)
monthly_pnl = run_query(f"""
    SELECT
        TO_CHAR(profit_month, 'Mon YY')      AS label,
        profit_month,
        service_line,
        COALESCE(SUM(profit_revenue_amount), 0)/1e6                                        AS revenue_m,
        COALESCE(SUM(NULLIF(profit_gross_profit_amount, 'NaN'::numeric)), 0)/1e6           AS gp_m,
        CASE WHEN SUM(profit_revenue_amount) > 0
             THEN ROUND((COALESCE(SUM(NULLIF(profit_gross_profit_amount, 'NaN'::numeric)), 0)
                        / SUM(profit_revenue_amount) * 100)::numeric, 1)
             ELSE 0 END                                                                     AS margin_pct
    FROM gold.fact_profitability
    WHERE profit_date BETWEEN '{start}' AND '{end}'
    {svc_clause}
    GROUP BY profit_month, service_line
    ORDER BY profit_month
""")

# MoM revenue growth %
mom_growth = run_query(f"""
    WITH monthly AS (
        SELECT
            revenue_month,
            TO_CHAR(revenue_month, 'Mon YY') AS label,
            SUM(revenue_amount)/1e6 AS rev_m
        FROM gold.fact_revenue
        WHERE revenue_order_date BETWEEN '{start}' AND '{end}'
        {svc_clause}
        GROUP BY revenue_month
    )
    SELECT
        label,
        revenue_month,
        rev_m,
        LAG(rev_m) OVER (ORDER BY revenue_month) AS prev_rev_m,
        CASE WHEN LAG(rev_m) OVER (ORDER BY revenue_month) > 0
             THEN ROUND(((rev_m - LAG(rev_m) OVER (ORDER BY revenue_month))
                        / LAG(rev_m) OVER (ORDER BY revenue_month) * 100)::numeric, 1)
             ELSE 0 END AS growth_pct
    FROM monthly
    ORDER BY revenue_month
""")

# Top customers
top_customers = run_query(f"""
    SELECT
        COALESCE(NULLIF(TRIM(profit_customer_name),''), 'Unknown') AS customer,
        service_line,
        SUM(profit_revenue_amount)      AS revenue,
        SUM(profit_gross_profit_amount) AS gp,
        COUNT(DISTINCT profit_order_id) AS orders,
        ROUND((SUM(profit_gross_profit_amount)/NULLIF(SUM(profit_revenue_amount),0)*100)::numeric,1) AS margin_pct
    FROM gold.fact_profitability
    WHERE profit_date BETWEEN '{start}' AND '{end}'
      AND profit_customer_name IS NOT NULL
    {svc_clause}
    GROUP BY profit_customer_name, service_line
    ORDER BY revenue DESC
    LIMIT 20
""")

# Customer concentration — derived from curr_profit (no extra query needed)
grand_total = float(curr_profit["revenue"].sum()) if not curr_profit.empty else 1.0

# ─── KPI Row ─────────────────────────────────────────────────────────────────
section_title("REVENUE & PROFITABILITY KPIs")
cols = st.columns(5)
kpis = [
    ("💰 REVENUE",       naira(c_rev),   _d(c_rev,  p_rev),  "normal"),
    ("📊 GROSS PROFIT",  naira(c_gp),    _d(c_gp,   p_gp),   "normal"),
    ("📈 GROSS MARGIN",  pct(c_marg),    None,               "off"),
    ("🛒 ORDERS",        count(c_ord),   _d(c_ord,  p_ord),  "normal"),
    ("🎯 AVG ORDER VALUE", naira(c_aov), _d(c_aov,  p_aov),  "normal"),
]
for col, (label, val, delta, dc) in zip(cols, kpis):
    col.metric(label, val,
               delta=(f"{'+' if (delta or 0)>=0 else ''}{delta:.1f}% vs prev"
                      if delta is not None else None),
               delta_color=dc)
st.markdown("")

# ─── Service line cards ───────────────────────────────────────────────────────
section_title("BY SERVICE LINE")
if not curr_profit.empty:
    svc_cols = st.columns(len(curr_profit))
    color_map = {"DAASH": COLOR_DAASH, "GoSource": COLOR_GOSOURCE}
    for i, (_, row) in enumerate(curr_profit.iterrows()):
        svc   = row["service_line"]
        color = color_map.get(svc, "#555")
        prev_row = prev_profit[prev_profit["service_line"] == svc]
        p_r = float(prev_row["revenue"].iloc[0]) if not prev_row.empty else 0
        delta_r = _d(float(row["revenue"]), p_r)
        arrow = (f"<span style='color:#22C55E'>▲ {delta_r:.1f}%</span>"
                 if delta_r and delta_r > 0
                 else (f"<span style='color:#EF4444'>▼ {abs(delta_r):.1f}%</span>"
                       if delta_r and delta_r < 0 else ""))
        svc_cols[i].markdown(
            f"""<div style='border-top:4px solid {color};background:white;border-radius:10px;
                padding:20px 24px;box-shadow:0 1px 4px rgba(0,0,0,0.06);'>
                <div style='font-weight:700;color:{color};font-size:16px;
                    text-transform:uppercase;letter-spacing:0.5px;'>{svc}</div>
                <div style='font-size:28px;font-weight:800;color:#0F172A;margin:8px 0 4px;'>
                    {naira(float(row['revenue']))}</div>
                <div style='font-size:12px;color:#64748B;'>{arrow}</div>
                <hr style='border:none;border-top:1px solid #F1F5F9;margin:10px 0;'>
                <div style='display:flex;gap:24px;'>
                    <div><div style='font-size:10px;color:#94A3B8;text-transform:uppercase;'>
                        GP</div><div style='font-weight:600;font-size:14px;'>
                        {naira(float(row['gp']))}</div></div>
                    <div><div style='font-size:10px;color:#94A3B8;text-transform:uppercase;'>
                        Margin</div><div style='font-weight:600;font-size:14px;'>
                        {pct(float(row['margin_pct']))}</div></div>
                    <div><div style='font-size:10px;color:#94A3B8;text-transform:uppercase;'>
                        Orders</div><div style='font-weight:600;font-size:14px;'>
                        {count(int(row['orders']))}</div></div>
                </div></div>""",
            unsafe_allow_html=True,
        )

st.markdown("")
st.markdown("---")

# ─── Charts ───────────────────────────────────────────────────────────────────
left, right = st.columns(2)

with left:
    section_title("MONTHLY REVENUE — STACKED BY SERVICE LINE")
    if not monthly_pnl.empty:
        fig = px.bar(
            monthly_pnl, x="label", y="revenue_m",
            color="service_line",
            color_discrete_map={"DAASH": COLOR_DAASH, "GoSource": COLOR_GOSOURCE},
            labels={"revenue_m": "₦M", "label": "", "service_line": ""},
            barmode="stack",
        )
        fig.update_layout(**CHART_LAYOUT, height=300)
        st.plotly_chart(fig, use_container_width=True)

with right:
    section_title("MONTH-OVER-MONTH REVENUE GROWTH %")
    if not mom_growth.empty:
        colors = [COLOR_POSITIVE if v >= 0 else COLOR_NEGATIVE
                  for v in mom_growth["growth_pct"]]
        fig2 = go.Figure(go.Bar(
            x=mom_growth["label"],
            y=mom_growth["growth_pct"],
            marker_color=colors,
            text=mom_growth["growth_pct"].apply(lambda x: f"{x:+.1f}%"),
            textposition="outside",
        ))
        fig2.add_hline(y=0, line_dash="solid", line_color="#94A3B8", line_width=1)
        fig2.update_layout(**CHART_LAYOUT, height=300,
                           yaxis_title="Growth %", yaxis_ticksuffix="%")
        st.plotly_chart(fig2, use_container_width=True)

# Margin trend
section_title("GROSS MARGIN % TREND BY SERVICE LINE")
if not monthly_pnl.empty:
    fig3 = px.line(
        monthly_pnl, x="label", y="margin_pct",
        color="service_line",
        color_discrete_map={"DAASH": COLOR_DAASH, "GoSource": COLOR_GOSOURCE},
        labels={"margin_pct": "Gross Margin %", "label": "", "service_line": ""},
        markers=True,
    )
    # Average reference line
    avg_margin = float(monthly_pnl["margin_pct"].mean())
    fig3.add_hline(y=avg_margin, line_dash="dot", line_color="#94A3B8",
                   annotation_text=f"Avg {avg_margin:.1f}%",
                   annotation_position="bottom right")
    fig3.update_layout(**CHART_LAYOUT, height=250,
                       yaxis_ticksuffix="%", yaxis_title="Margin %")
    st.plotly_chart(fig3, use_container_width=True)

# ─── Top customers table ──────────────────────────────────────────────────────
st.markdown("---")
section_title("TOP 20 CUSTOMERS BY REVENUE")
if not top_customers.empty:
    display = top_customers.copy()
    display["share_pct"] = (display["revenue"] / grand_total * 100).round(1)
    display["revenue"]   = display["revenue"].apply(lambda x: naira(float(x)))
    display["gp"]        = display["gp"].apply(lambda x: naira(float(x)))
    display["orders"]    = display["orders"].apply(lambda x: count(int(x)))
    display["margin_pct"] = display["margin_pct"].apply(lambda x: f"{x:.1f}%")
    display["share_pct"]  = display["share_pct"].apply(lambda x: f"{x:.1f}%")
    display.columns = ["Customer", "Service", "Revenue", "GP", "Orders", "Margin", "% of Total"]

    c1, c2 = st.columns([5, 1])
    with c2:
        st.download_button("📥 Download CSV",
                           top_customers.to_csv(index=False),
                           f"top_customers_{start}_{end}.csv", "text/csv")
    st.dataframe(display, use_container_width=True, hide_index=True, height=420)
