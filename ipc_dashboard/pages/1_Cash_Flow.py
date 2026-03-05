import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

from utils.db      import run_query
from utils.fmt     import naira, pct, count
from utils.styles  import (inject_css, page_header, runway_card, section_title,
                            CHART_LAYOUT, COLOR_POSITIVE, COLOR_NEGATIVE, COLOR_CASH)
from utils.periods import sidebar_filters

st.set_page_config(page_title="Cash Flow · IPC", page_icon="💰", layout="wide")
inject_css()

start, end, prev_start, prev_end, period_label, _ = sidebar_filters()

page_header("Cash Flow & Bank Position",
            f"{period_label} · Lenco Bank Account")

# ─── Queries ──────────────────────────────────────────────────────────────────
cash_now = run_query("""
    SELECT cumulative_net_movement_amount AS cash
    FROM gold.fact_cash_position
    ORDER BY cash_position_date DESC LIMIT 1
""")

period_summary = run_query(f"""
    SELECT
        SUM(daily_inflow_amount)        AS inflow,
        SUM(daily_outflow_amount)       AS outflow,
        SUM(daily_net_movement_amount)  AS net,
        COUNT(*)                        AS txn_days
    FROM gold.fact_cash_position
    WHERE cash_position_date BETWEEN '{start}' AND '{end}'
""")

prev_summary = run_query(f"""
    SELECT
        SUM(daily_inflow_amount)        AS inflow,
        SUM(daily_outflow_amount)       AS outflow
    FROM gold.fact_cash_position
    WHERE cash_position_date BETWEEN '{prev_start}' AND '{prev_end}'
""")

burn_rate = run_query("""
    SELECT AVG(monthly_burn) AS avg_burn
    FROM (
        SELECT cash_position_month, SUM(daily_outflow_amount) AS monthly_burn
        FROM gold.fact_cash_position
        WHERE cash_position_date >= CURRENT_DATE - INTERVAL '3 months'
        GROUP BY cash_position_month
        ORDER BY cash_position_month DESC
        LIMIT 3
    ) t
""")

daily_pos = run_query(f"""
    SELECT
        cash_position_date,
        cumulative_net_movement_amount  AS balance,
        daily_inflow_amount             AS inflow,
        daily_outflow_amount            AS outflow,
        daily_net_movement_amount       AS net
    FROM gold.fact_cash_position
    WHERE cash_position_date BETWEEN '{start}' AND '{end}'
    ORDER BY cash_position_date
""")

monthly_flows = run_query(f"""
    SELECT
        TO_CHAR(cash_position_month, 'Mon YY')  AS label,
        cash_position_month,
        SUM(daily_inflow_amount) /1e6   AS inflow_m,
        SUM(daily_outflow_amount)/1e6   AS outflow_m,
        SUM(daily_net_movement_amount)/1e6 AS net_m
    FROM gold.fact_cash_position
    WHERE cash_position_date BETWEEN '{start}' AND '{end}'
    GROUP BY cash_position_month
    ORDER BY cash_position_month
""")

inflow_sources = run_query(f"""
    SELECT
        COALESCE(inflow_source, 'Other') AS source,
        SUM(cash_inflow_amount) AS amount,
        COUNT(*) AS txn_count
    FROM gold.fact_cash_flow
    WHERE transaction_date BETWEEN '{start}' AND '{end}'
      AND cash_flow_direction = 'Inflow'
    GROUP BY inflow_source
    ORDER BY amount DESC
""")

# ─── Compute scalars ──────────────────────────────────────────────────────────
def _v(df, col, default=0):
    if df.empty or df.iloc[0][col] is None: return float(default)
    return float(df.iloc[0][col])

def _delta(c, p): return ((c - p)/p*100) if p and p > 0 else None

cash      = _v(cash_now, "cash")
inflow    = _v(period_summary, "inflow")
outflow   = _v(period_summary, "outflow")
net       = _v(period_summary, "net")
avg_burn  = _v(burn_rate, "avg_burn")
runway    = (cash / avg_burn) if avg_burn > 0 else None

prev_inflow  = _v(prev_summary, "inflow")
prev_outflow = _v(prev_summary, "outflow")

# ─── KPIs ─────────────────────────────────────────────────────────────────────
section_title("CASH POSITION & FLOWS")

c0, c1, c2, c3, c4 = st.columns([2, 1, 1, 1, 1])

with c0:
    st.markdown(runway_card(runway), unsafe_allow_html=True)

c1.metric("🏦 Current Balance",   naira(cash),
          help="Running Lenco balance — all time")
c2.metric("📈 Period Inflows",    naira(inflow),
          delta=(f"{_delta(inflow, prev_inflow):+.1f}% vs prev"
                 if _delta(inflow, prev_inflow) is not None else None))
c3.metric("📉 Period Outflows",   naira(outflow),
          delta=(f"{_delta(outflow, prev_outflow):+.1f}% vs prev"
                 if _delta(outflow, prev_outflow) is not None else None),
          delta_color="inverse")
c4.metric("📊 Net Movement",      naira(net),
          delta=f"{'▲ Positive' if net >= 0 else '▼ Negative'} cash flow",
          delta_color="normal" if net >= 0 else "inverse")

st.markdown("---")

# ─── Running balance chart ────────────────────────────────────────────────────
section_title("RUNNING BANK BALANCE")
if not daily_pos.empty:
    fig = go.Figure()
    # Area fill
    fig.add_trace(go.Scatter(
        x=daily_pos["cash_position_date"],
        y=daily_pos["balance"] / 1e6,
        mode="lines",
        fill="tozeroy",
        fillcolor="rgba(8,145,178,0.08)",
        line=dict(color=COLOR_CASH, width=2),
        name="Balance",
        hovertemplate="<b>%{x|%d %b %Y}</b><br>Balance: ₦%{y:.2f}M<extra></extra>",
    ))
    # 30-day moving average
    if len(daily_pos) >= 7:
        ma = daily_pos["balance"].rolling(7, min_periods=1).mean() / 1e6
        fig.add_trace(go.Scatter(
            x=daily_pos["cash_position_date"], y=ma,
            mode="lines", line=dict(color="#F59E0B", width=1.5, dash="dot"),
            name="7-Day Avg", opacity=0.8,
        ))
    fig.update_layout(**CHART_LAYOUT, height=280, yaxis_title="Balance (₦M)")
    st.plotly_chart(fig, use_container_width=True)
st.markdown("---")

# ─── Monthly flows + inflow sources ──────────────────────────────────────────
left, right = st.columns([3, 2])

with left:
    section_title("MONTHLY INFLOWS vs OUTFLOWS")
    if not monthly_flows.empty:
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=monthly_flows["label"], y=monthly_flows["inflow_m"],
            name="Inflow", marker_color=COLOR_POSITIVE, opacity=0.85,
        ))
        fig2.add_trace(go.Bar(
            x=monthly_flows["label"], y=monthly_flows["outflow_m"],
            name="Outflow", marker_color=COLOR_NEGATIVE, opacity=0.85,
        ))
        fig2.add_trace(go.Scatter(
            x=monthly_flows["label"], y=monthly_flows["net_m"],
            name="Net", mode="lines+markers",
            line=dict(color=COLOR_CASH, width=2, dash="dot"),
            marker=dict(size=6),
        ))
        fig2.update_layout(**CHART_LAYOUT, barmode="group", height=280, yaxis_title="₦M")
        st.plotly_chart(fig2, use_container_width=True)

with right:
    section_title("INFLOW SOURCES")
    if not inflow_sources.empty:
        fig3 = go.Figure(go.Pie(
            labels=inflow_sources["source"],
            values=inflow_sources["amount"],
            hole=0.52,
            textinfo="percent+label",
            textfont_size=12,
        ))
        fig3.update_layout(
            showlegend=False,
            margin=dict(t=10, b=0, l=0, r=0),
            height=280,
        )
        st.plotly_chart(fig3, use_container_width=True)

        # Summary table
        src_display = inflow_sources.copy()
        src_display["amount"]    = src_display["amount"].apply(lambda x: naira(float(x)))
        src_display["txn_count"] = src_display["txn_count"].apply(lambda x: count(int(x)))
        src_display.columns = ["Source", "Amount", "Txns"]
        st.dataframe(src_display, use_container_width=True, hide_index=True)

# ─── Daily detail table ───────────────────────────────────────────────────────
st.markdown("---")
section_title("DAILY TRANSACTION DETAIL")
if not daily_pos.empty:
    col1, col2 = st.columns([4, 1])
    with col2:
        st.download_button(
            "📥 Download CSV",
            daily_pos.to_csv(index=False),
            file_name=f"cash_flow_{start}_{end}.csv",
            mime="text/csv",
        )
    display = daily_pos.copy()
    for c in ["balance", "inflow", "outflow", "net"]:
        display[c] = display[c].apply(lambda x: f"₦{float(x)/1e6:,.2f}M")
    display.columns = ["Date", "Balance", "Inflow", "Outflow", "Net"]
    st.dataframe(display.sort_values("Date", ascending=False),
                 use_container_width=True, hide_index=True, height=350)
