import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import datetime as dt
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

from utils.db      import run_query
from utils.fmt     import naira, pct, count
from utils.styles  import (inject_css, page_header, kpi_card, runway_card,
                            section_title, CHART_LAYOUT,
                            COLOR_DAASH, COLOR_GOSOURCE, COLOR_POSITIVE,
                            COLOR_NEGATIVE, COLOR_CASH, COLOR_NEUTRAL)
from utils.periods import sidebar_filters

# ─── Page config ─────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="IPC Finance Dashboard",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css()

start, end, prev_start, prev_end, period_label, _ = sidebar_filters()

page_header(
    "IPC Group — Finance Dashboard",
    f"Executive Summary · {period_label} · as of {dt.date.today().strftime('%d %b %Y')}"
)

# ─── Queries ──────────────────────────────────────────────────────────────────

def _rev(s, e):
    return run_query(f"""
        SELECT
            SUM(revenue_amount)  AS revenue,
            COUNT(*)             AS orders
        FROM gold.fact_revenue
        WHERE revenue_order_date BETWEEN '{s}' AND '{e}'
    """)

def _profit(s, e):
    return run_query(f"""
        SELECT
            SUM(profit_revenue_amount)       AS revenue,
            SUM(profit_gross_profit_amount)  AS gp,
            CASE WHEN SUM(profit_revenue_amount) > 0
                 THEN ROUND(SUM(profit_gross_profit_amount)/SUM(profit_revenue_amount)*100,1)
                 ELSE 0 END                  AS margin
        FROM gold.fact_profitability
        WHERE profit_date BETWEEN '{s}' AND '{e}'
    """)

def _exp(s, e):
    return run_query(f"""
        SELECT SUM(expense_amount) AS expenses
        FROM gold.fact_expenses
        WHERE expense_date BETWEEN '{s}' AND '{e}'
    """)

curr_rev    = _rev(start, end)
prev_rev    = _rev(prev_start, prev_end)
curr_profit = _profit(start, end)
prev_profit = _profit(prev_start, prev_end)
curr_exp    = _exp(start, end)
prev_exp    = _exp(prev_start, prev_end)

cash_now = run_query("""
    SELECT cumulative_net_movement_amount AS cash
    FROM gold.fact_cash_position
    ORDER BY cash_position_date DESC LIMIT 1
""")

ar_now = run_query("""
    SELECT SUM(ar_outstanding_amount) AS ar, COUNT(*) AS cnt
    FROM gold.fact_ar_aging
""")

# Cash runway: avg 3-month burn
burn_rate = run_query("""
    SELECT AVG(monthly_burn) AS avg_burn
    FROM (
        SELECT cash_position_month, SUM(daily_outflow_amount) AS monthly_burn
        FROM gold.fact_cash_position
        WHERE cash_position_date >= CURRENT_DATE - INTERVAL '3 months'
        GROUP BY cash_position_month
        LIMIT 3
    ) t
""")

# P&L bridge numbers (full period)
pnl_bridge = run_query(f"""
    SELECT
        SUM(profit_revenue_amount)       AS revenue,
        SUM(profit_cogs_amount)          AS cogs,
        SUM(profit_gross_profit_amount)  AS gross_profit
    FROM gold.fact_profitability
    WHERE profit_date BETWEEN '{start}' AND '{end}'
""")

# Monthly revenue + expenses trend for combo chart
monthly_combo = run_query(f"""
    SELECT
        m,
        label,
        SUM(CASE WHEN type='revenue'  THEN val ELSE 0 END)/1e6 AS revenue_m,
        SUM(CASE WHEN type='expenses' THEN val ELSE 0 END)/1e6 AS expenses_m
    FROM (
        SELECT date_trunc('month', revenue_order_date)::date AS m,
               TO_CHAR(date_trunc('month', revenue_order_date), 'Mon YY') AS label,
               'revenue' AS type,
               revenue_amount AS val
        FROM gold.fact_revenue
        WHERE revenue_order_date BETWEEN '{start}' AND '{end}'
        UNION ALL
        SELECT date_trunc('month', expense_date)::date AS m,
               TO_CHAR(date_trunc('month', expense_date), 'Mon YY') AS label,
               'expenses' AS type,
               expense_amount AS val
        FROM gold.fact_expenses
        WHERE expense_date BETWEEN '{start}' AND '{end}'
    ) combined
    GROUP BY m, label
    ORDER BY m
""")

# Monthly revenue by service line
monthly_rev_svc = run_query(f"""
    SELECT
        TO_CHAR(revenue_month, 'Mon YY') AS label,
        revenue_month,
        service_line,
        SUM(revenue_amount)/1e6 AS rev_m
    FROM gold.fact_revenue
    WHERE revenue_order_date BETWEEN '{start}' AND '{end}'
    GROUP BY revenue_month, service_line
    ORDER BY revenue_month
""")

# ─── Extract scalar values ────────────────────────────────────────────────────
def _v(df, col, default=0):
    if df.empty or df.iloc[0][col] is None:
        return float(default)
    return float(df.iloc[0][col])

def _delta(curr, prev):
    return ((curr - prev) / prev * 100) if prev and prev > 0 else None

curr_revenue  = _v(curr_rev,    "revenue")
prev_revenue  = _v(prev_rev,    "revenue")
curr_orders   = int(_v(curr_rev, "orders"))
prev_orders   = int(_v(prev_rev, "orders"))
curr_gp       = _v(curr_profit, "gp")
prev_gp       = _v(prev_profit, "gp")
curr_margin   = _v(curr_profit, "margin")
prev_margin   = _v(prev_profit, "margin")
curr_expenses = _v(curr_exp,    "expenses")
prev_expenses = _v(prev_exp,    "expenses")
cash          = _v(cash_now,    "cash")
total_ar      = _v(ar_now,      "ar")
ar_count      = int(_v(ar_now,  "cnt"))
avg_burn      = _v(burn_rate,   "avg_burn")
runway_months = (cash / avg_burn) if avg_burn > 0 else None

aov_curr = curr_revenue / curr_orders if curr_orders > 0 else 0
aov_prev_orders = int(_v(prev_rev, "orders"))
aov_prev_rev    = _v(prev_rev, "revenue")
aov_prev = aov_prev_rev / aov_prev_orders if aov_prev_orders > 0 else 0

# P&L bridge
b_rev  = _v(pnl_bridge, "revenue")
b_cogs = _v(pnl_bridge, "cogs")
b_gp   = _v(pnl_bridge, "gross_profit")
b_opex = curr_expenses
b_net  = b_gp - b_opex

# ─── Row 1: Primary KPIs ──────────────────────────────────────────────────────
section_title("KEY PERFORMANCE INDICATORS")
cols = st.columns(6)
kpis = [
    ("REVENUE",       naira(curr_revenue),  _delta(curr_revenue,  prev_revenue),  "up",   "💰"),
    ("GROSS PROFIT",  naira(curr_gp),       _delta(curr_gp,       prev_gp),       "up",   "📊"),
    ("GROSS MARGIN",  pct(curr_margin),     curr_margin - prev_margin if prev_margin else None, "up", "📈"),
    ("TOTAL ORDERS",  count(curr_orders),   _delta(curr_orders,   prev_orders),   "up",   "🛒"),
    ("AVG ORDER VALUE", naira(aov_curr),    _delta(aov_curr,      aov_prev),      "up",   "🎯"),
    ("TOTAL EXPENSES",  naira(curr_expenses), _delta(curr_expenses, prev_expenses), "down", "💸"),
]
for col, (label, val, delta, direction, icon) in zip(cols, kpis):
    if label == "GROSS MARGIN" and delta is not None:
        # delta is pp not %, display differently
        arrow = "▲" if delta > 0 else ("▼" if delta < 0 else "→")
        color = "#22C55E" if delta > 0 else "#EF4444"
        d_str = f"{arrow} {abs(delta):.1f}pp vs prev"
        col.metric(f"{icon} {label}", val)
        col.markdown(
            f"<div style='margin-top:-16px;font-size:12px;color:{color};'>{d_str}</div>",
            unsafe_allow_html=True
        )
    else:
        col.metric(
            f"{icon} {label}", val,
            delta=(f"{'+' if (delta or 0) >= 0 else ''}{delta:.1f}% vs prev" if delta is not None else None),
            delta_color="normal" if direction == "up" else "inverse",
        )

st.markdown("")

# ─── Row 2: Financial health ───────────────────────────────────────────────────
section_title("FINANCIAL HEALTH")
c1, c2, c3 = st.columns([2, 1, 1])

with c1:
    st.markdown(runway_card(runway_months), unsafe_allow_html=True)

with c2:
    prev_burn = run_query(f"""
        SELECT SUM(daily_outflow_amount) AS prev_expenses
        FROM gold.fact_cash_position
        WHERE cash_position_date BETWEEN '{prev_start}' AND '{prev_end}'
    """)
    prev_exp_val = _v(prev_burn, "prev_expenses")
    st.metric(
        "🏦 Cash Position",
        naira(cash),
        delta=None,
        help="Current Lenco bank running balance"
    )

with c3:
    st.metric(
        "📋 AR Outstanding",
        naira(total_ar),
        delta=f"{ar_count} open invoices",
        delta_color="off",
        help="GoSource credit orders unpaid"
    )

st.markdown("---")

# ─── P&L Bridge + Revenue Trend ───────────────────────────────────────────────
left, right = st.columns([1, 1])

with left:
    section_title("P&L BRIDGE — PERIOD SUMMARY")
    if b_rev > 0:
        wf_x      = ["Revenue", "COGS", "Gross Profit", "OpEx", "Net Position"]
        wf_measure = ["absolute", "relative", "total", "relative", "total"]
        wf_y      = [b_rev, -b_cogs, 0, -b_opex, 0]
        wf_text   = [naira(b_rev), f"-{naira(b_cogs)}", naira(b_gp),
                     f"-{naira(b_opex)}", naira(b_net)]
        fig_wf = go.Figure(go.Waterfall(
            measure=wf_measure,
            x=wf_x,
            y=wf_y,
            text=wf_text,
            textposition="outside",
            decreasing={"marker": {"color": COLOR_NEGATIVE, "line": {"width": 0}}},
            increasing={"marker": {"color": COLOR_POSITIVE, "line": {"width": 0}}},
            totals={"marker":    {"color": "#3B82F6",       "line": {"width": 0}}},
            connector={"line":   {"color": "#CBD5E1",       "width": 1, "dash": "dot"}},
        ))
        fig_wf.update_layout(
            **CHART_LAYOUT,
            height=320,
            showlegend=False,
            yaxis_title="₦",
        )
        st.plotly_chart(fig_wf, use_container_width=True)
    else:
        st.info("No P&L data for the selected period.")

with right:
    section_title("REVENUE vs EXPENSES — MONTHLY TREND")
    if not monthly_combo.empty:
        fig_combo = go.Figure()
        fig_combo.add_trace(go.Bar(
            x=monthly_combo["label"], y=monthly_combo["revenue_m"],
            name="Revenue", marker_color=COLOR_GOSOURCE, opacity=0.85,
        ))
        fig_combo.add_trace(go.Bar(
            x=monthly_combo["label"], y=monthly_combo["expenses_m"],
            name="Expenses", marker_color=COLOR_NEGATIVE, opacity=0.85,
        ))
        # Net line
        net_series = monthly_combo["revenue_m"] - monthly_combo["expenses_m"]
        fig_combo.add_trace(go.Scatter(
            x=monthly_combo["label"], y=net_series,
            name="Net", mode="lines+markers",
            line=dict(color=COLOR_POSITIVE, width=2, dash="dot"),
            marker=dict(size=6),
        ))
        fig_combo.update_layout(
            **CHART_LAYOUT,
            barmode="group",
            height=320,
            yaxis_title="₦M",
        )
        st.plotly_chart(fig_combo, use_container_width=True)

st.markdown("---")

# ─── Revenue by service line ───────────────────────────────────────────────────
section_title("REVENUE BY SERVICE LINE")
if not monthly_rev_svc.empty:
    c1, c2 = st.columns([3, 1])
    with c1:
        fig_svc = px.bar(
            monthly_rev_svc,
            x="label", y="rev_m",
            color="service_line",
            color_discrete_map={"DAASH": COLOR_DAASH, "GoSource": COLOR_GOSOURCE},
            labels={"rev_m": "Revenue (₦M)", "label": "", "service_line": ""},
            barmode="stack",
        )
        fig_svc.update_layout(**CHART_LAYOUT, height=280)
        st.plotly_chart(fig_svc, use_container_width=True)

    with c2:
        totals = monthly_rev_svc.groupby("service_line")["rev_m"].sum().reset_index()
        grand  = totals["rev_m"].sum()
        fig_pie = go.Figure(go.Pie(
            labels=totals["service_line"],
            values=totals["rev_m"],
            hole=0.55,
            marker_colors=[COLOR_DAASH if s == "DAASH" else COLOR_GOSOURCE
                           for s in totals["service_line"]],
            textinfo="percent",
            textfont_size=13,
        ))
        fig_pie.add_annotation(
            text=f"<b>{naira(grand*1e6)}</b>",
            x=0.5, y=0.5, font_size=13, showarrow=False
        )
        fig_pie.update_layout(
            showlegend=True,
            legend=dict(orientation="h", y=-0.1),
            margin=dict(t=0, b=30, l=0, r=0),
            height=280,
        )
        st.plotly_chart(fig_pie, use_container_width=True)
