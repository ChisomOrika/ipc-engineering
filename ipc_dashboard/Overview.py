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
    page_title="Overview · IPC",
    page_icon="🏦",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css()

start, end, prev_start, prev_end, period_label, _ = sidebar_filters()

page_header(
    "IPC Group — Overview",
    f"Executive Summary · {period_label} · as of {dt.date.today().strftime('%d %b %Y')}"
)

# ─── Queries ──────────────────────────────────────────────────────────────────
# One master KPI query replaces 9+ individual round-trips

kpi = run_query(f"""
    WITH rev AS (
        SELECT
            SUM(CASE WHEN revenue_order_date BETWEEN '{start}' AND '{end}'
                     THEN revenue_amount END)                               AS curr_rev,
            COUNT(CASE WHEN revenue_order_date BETWEEN '{start}' AND '{end}'
                       THEN 1 END)                                          AS curr_orders,
            COUNT(CASE WHEN revenue_order_date BETWEEN '{start}' AND '{end}'
                       AND service_line = 'DAASH' THEN 1 END)              AS curr_daash_orders,
            COUNT(CASE WHEN revenue_order_date BETWEEN '{start}' AND '{end}'
                       AND service_line = 'GoSource' THEN 1 END)           AS curr_gs_orders,
            SUM(CASE WHEN revenue_order_date BETWEEN '{prev_start}' AND '{prev_end}'
                     THEN revenue_amount END)                               AS prev_rev,
            COUNT(CASE WHEN revenue_order_date BETWEEN '{prev_start}' AND '{prev_end}'
                       THEN 1 END)                                          AS prev_orders,
            COUNT(CASE WHEN revenue_order_date BETWEEN '{prev_start}' AND '{prev_end}'
                       AND service_line = 'DAASH' THEN 1 END)              AS prev_daash_orders,
            COUNT(CASE WHEN revenue_order_date BETWEEN '{prev_start}' AND '{prev_end}'
                       AND service_line = 'GoSource' THEN 1 END)           AS prev_gs_orders
        FROM gold.fact_revenue
        WHERE revenue_order_date BETWEEN '{prev_start}' AND '{end}'
    ),
    prof AS (
        -- NULLIF guards against PostgreSQL NaN values stored in GP/COGS columns
        SELECT
            COALESCE(SUM(CASE WHEN profit_date BETWEEN '{start}' AND '{end}'
                         THEN NULLIF(profit_gross_profit_amount, 'NaN'::numeric) END), 0) AS curr_gp,
            COALESCE(SUM(CASE WHEN profit_date BETWEEN '{start}' AND '{end}'
                         THEN profit_revenue_amount END), 0)                               AS curr_prof_rev,
            COALESCE(SUM(CASE WHEN profit_date BETWEEN '{start}' AND '{end}'
                         THEN NULLIF(profit_cogs_amount, 'NaN'::numeric) END), 0)         AS curr_cogs,
            COALESCE(SUM(CASE WHEN profit_date BETWEEN '{prev_start}' AND '{prev_end}'
                         THEN NULLIF(profit_gross_profit_amount, 'NaN'::numeric) END), 0) AS prev_gp,
            COALESCE(SUM(CASE WHEN profit_date BETWEEN '{prev_start}' AND '{prev_end}'
                         THEN profit_revenue_amount END), 0)                               AS prev_prof_rev
        FROM gold.fact_profitability
        WHERE profit_date BETWEEN '{prev_start}' AND '{end}'
    ),
    exp AS (
        SELECT
            SUM(CASE WHEN expense_date BETWEEN '{start}' AND '{end}'
                     THEN expense_amount END)                               AS curr_exp,
            SUM(CASE WHEN expense_date BETWEEN '{prev_start}' AND '{prev_end}'
                     THEN expense_amount END)                               AS prev_exp
        FROM gold.fact_expenses
        WHERE expense_date BETWEEN '{prev_start}' AND '{end}'
    ),
    cash AS (
        SELECT cumulative_net_movement_amount AS cash_balance
        FROM gold.fact_cash_position
        ORDER BY cash_position_date DESC LIMIT 1
    ),
    ar AS (
        SELECT
            COALESCE(SUM(ar_outstanding_amount), 0) AS total_ar,
            COUNT(*)                                AS ar_count
        FROM gold.fact_ar_aging
    ),
    burn AS (
        SELECT COALESCE(AVG(monthly_burn), 0) AS avg_burn
        FROM (
            SELECT cash_position_month, SUM(daily_outflow_amount) AS monthly_burn
            FROM gold.fact_cash_position
            WHERE cash_position_date >= CURRENT_DATE - INTERVAL '3 months'
            GROUP BY cash_position_month
            ORDER BY cash_position_month DESC
            LIMIT 3
        ) t
    )
    SELECT
        rev.curr_rev,    rev.curr_orders,
        rev.curr_daash_orders, rev.curr_gs_orders,
        rev.prev_rev,    rev.prev_orders,
        rev.prev_daash_orders, rev.prev_gs_orders,
        prof.curr_gp,    prof.curr_prof_rev, prof.curr_cogs,
        prof.prev_gp,    prof.prev_prof_rev,
        exp.curr_exp,    exp.prev_exp,
        cash.cash_balance,
        ar.total_ar,     ar.ar_count,
        burn.avg_burn
    FROM rev, prof, exp, cash, ar, burn
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

curr_revenue  = _v(kpi, "curr_rev")
prev_revenue  = _v(kpi, "prev_rev")
curr_orders        = int(_v(kpi, "curr_orders"))
prev_orders        = int(_v(kpi, "prev_orders"))
curr_daash_orders  = int(_v(kpi, "curr_daash_orders"))
curr_gs_orders     = int(_v(kpi, "curr_gs_orders"))
prev_daash_orders  = int(_v(kpi, "prev_daash_orders"))
prev_gs_orders     = int(_v(kpi, "prev_gs_orders"))
curr_prof_rev = _v(kpi, "curr_prof_rev")
prev_prof_rev = _v(kpi, "prev_prof_rev")
curr_cogs     = _v(kpi, "curr_cogs")
curr_gp       = _v(kpi, "curr_gp")
prev_gp       = _v(kpi, "prev_gp")
curr_margin   = (curr_gp / curr_prof_rev * 100) if curr_prof_rev > 0 else 0
prev_margin   = (prev_gp / prev_prof_rev * 100) if prev_prof_rev > 0 else 0
curr_expenses = _v(kpi, "curr_exp")
prev_expenses = _v(kpi, "prev_exp")
cash          = _v(kpi, "cash_balance")
total_ar      = _v(kpi, "total_ar")
ar_count      = int(_v(kpi, "ar_count"))
avg_burn      = _v(kpi, "avg_burn")
runway_months = (cash / avg_burn) if avg_burn > 0 else None

aov_curr = curr_revenue / curr_orders if curr_orders > 0 else 0
aov_prev = prev_revenue / prev_orders if prev_orders > 0 else 0

# P&L bridge
b_rev  = curr_prof_rev
b_cogs = curr_cogs
b_gp   = curr_gp
b_opex = curr_expenses
b_net  = b_gp - b_opex

# ─── Row 1: Primary KPIs ──────────────────────────────────────────────────────
section_title("KEY PERFORMANCE INDICATORS")
kpi_help = {
    "REVENUE":         "Total order value (DAASH + GoSource) for delivered orders in the period. Source: gold.fact_revenue → revenue_amount",
    "GROSS PROFIT":    "DAASH: platform fee from revenue ledger per delivered order. GoSource: service charge on credit orders. Source: gold.fact_profitability → profit_gross_profit_amount",
    "GROSS MARGIN":    "Gross Profit ÷ Revenue × 100. pp = percentage points vs previous period.",
    "DAASH ORDERS":    "Delivered DAASH orders in the period. Includes direct (Transfer/Card/Cash) and aggregator (Chowdeck/Glovo) channels. Source: gold.fact_revenue WHERE service_line='DAASH'",
    "GOSOURCE ORDERS": "Delivered + paid GoSource orders in the period. Source: gold.fact_revenue WHERE service_line='GoSource'",
    "AVG ORDER VALUE": "Total Revenue ÷ Total Order Count for the period.",
    "TOTAL EXPENSES":  "Sum of all Lenco bank debits categorised as business expenses. Source: gold.fact_expenses",
}
cols = st.columns(6)
kpis = [
    ("REVENUE",         naira(curr_revenue),      _delta(curr_revenue,       prev_revenue),       "up",   "💰"),
    ("GROSS PROFIT",    naira(curr_gp),            _delta(curr_gp,            prev_gp),            "up",   "📊"),
    ("GROSS MARGIN",    pct(curr_margin),          curr_margin - prev_margin if prev_margin else None, "up", "📈"),
    ("DAASH ORDERS",    count(curr_daash_orders),  _delta(curr_daash_orders,  prev_daash_orders),  "up",   "🍔"),
    ("GOSOURCE ORDERS", count(curr_gs_orders),     _delta(curr_gs_orders,     prev_gs_orders),     "up",   "📦"),
    ("TOTAL EXPENSES",  naira(curr_expenses),      _delta(curr_expenses,      prev_expenses),      "down", "💸"),
]
for col, (label, val, delta, direction, icon) in zip(cols, kpis):
    if label == "GROSS MARGIN" and delta is not None:
        arrow = "▲" if delta > 0 else ("▼" if delta < 0 else "→")
        color = "#22C55E" if delta > 0 else "#EF4444"
        col.metric(f"{icon} {label}", val, help=kpi_help[label])
        col.markdown(
            f"<div style='margin-top:-16px;font-size:12px;color:{color};'>{arrow} {abs(delta):.1f}pp vs prev</div>",
            unsafe_allow_html=True
        )
    else:
        col.metric(
            f"{icon} {label}", val,
            delta=(f"{'+' if (delta or 0) >= 0 else ''}{delta:.1f}% vs prev" if delta is not None else None),
            delta_color="normal" if direction == "up" else "inverse",
            help=kpi_help[label],
        )

st.markdown("")

# ─── Row 2: Financial health ───────────────────────────────────────────────────
section_title("FINANCIAL HEALTH")
c1, c2, c3 = st.columns(3)

with c1:
    st.metric(
        "🔥 AVG MONTHLY BURN",
        naira(avg_burn),
        delta="3-month trailing average",
        delta_color="off",
        help=(
            "Average monthly cash outflows from the Lenco bank account over the last 3 months. "
            "Calculated as: SUM(daily_outflow_amount) per month → 3-month average. "
            "Source: gold.fact_cash_position"
        )
    )

with c2:
    st.metric(
        "🏦 LENCO NET MOVEMENT",
        naira(cash),
        delta="Cumulative since first recorded txn",
        delta_color="off",
        help=(
            "Cumulative net of ALL Lenco transactions = Total Credits − Total Debits. "
            "Verified against raw_lenco.transactions (no duplicates). "
            "This is NET MOVEMENT — not the actual account opening balance. "
            "Source: gold.fact_cash_position (cumulative_net_movement_amount)"
        )
    )

with c3:
    st.metric(
        "📋 AR OUTSTANDING",
        naira(total_ar),
        delta=f"{ar_count} open GoSource invoices",
        delta_color="off",
        help=(
            "Total unpaid GoSource credit orders that have been delivered. "
            "Filter: order_payment_method='Credit' AND order_status='Delivered' AND NOT paid. "
            "Source: gold.fact_ar_aging (ar_outstanding_amount)"
        )
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
