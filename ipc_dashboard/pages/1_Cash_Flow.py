import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

from utils.db      import run_query
from utils.fmt     import naira, pct, count
from utils.styles  import (inject_css, page_header, section_title,
                            CHART_LAYOUT, COLOR_POSITIVE, COLOR_NEGATIVE,
                            COLOR_CASH, COLOR_DAASH, COLOR_GOSOURCE)
from utils.periods import sidebar_filters

st.set_page_config(page_title="Cash Flow · IPC", page_icon="💰", layout="wide")
inject_css()

start, end, prev_start, prev_end, period_label, _ = sidebar_filters()

page_header("Cash Flow & Bank Position",
            f"{period_label} · Lenco (Providus) Bank Account")

# ─── All queries ──────────────────────────────────────────────────────────────

cash_kpi = run_query(f"""
    WITH real_balance AS (
        SELECT account_current_balance_amount::numeric AS balance
        FROM bv.bv_lenco_accounts LIMIT 1
    ),
    after_period AS (
        SELECT COALESCE(SUM(daily_net_movement_amount), 0) AS net_after
        FROM gold.fact_cash_position
        WHERE cash_position_date > '{end}'
    ),
    flows AS (
        SELECT
            SUM(CASE WHEN cash_position_date BETWEEN '{start}' AND '{end}'
                     THEN daily_inflow_amount END)       AS inflow,
            SUM(CASE WHEN cash_position_date BETWEEN '{start}' AND '{end}'
                     THEN daily_outflow_amount END)      AS outflow,
            SUM(CASE WHEN cash_position_date BETWEEN '{start}' AND '{end}'
                     THEN daily_net_movement_amount END) AS net,
            SUM(CASE WHEN cash_position_date BETWEEN '{prev_start}' AND '{prev_end}'
                     THEN daily_inflow_amount END)       AS prev_inflow,
            SUM(CASE WHEN cash_position_date BETWEEN '{prev_start}' AND '{prev_end}'
                     THEN daily_outflow_amount END)      AS prev_outflow
        FROM gold.fact_cash_position
        WHERE cash_position_date BETWEEN '{prev_start}' AND '{end}'
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
        real_balance.balance - after_period.net_after                           AS closing_balance,
        real_balance.balance - after_period.net_after - COALESCE(flows.net, 0) AS opening_balance,
        real_balance.balance                                                    AS current_balance,
        flows.inflow, flows.outflow, flows.net,
        flows.prev_inflow, flows.prev_outflow,
        burn.avg_burn
    FROM real_balance, after_period, flows, burn
""")

daily_pos = run_query(f"""
    SELECT
        cash_position_date,
        daily_inflow_amount       AS inflow,
        daily_outflow_amount      AS outflow,
        daily_net_movement_amount AS net
    FROM gold.fact_cash_position
    WHERE cash_position_date BETWEEN '{start}' AND '{end}'
    ORDER BY cash_position_date
""")

monthly_flows = run_query(f"""
    SELECT
        TO_CHAR(cash_position_month, 'Mon')     AS label,
        cash_position_month,
        SUM(daily_inflow_amount)  / 1e6         AS inflow_m,
        SUM(daily_outflow_amount) / 1e6         AS outflow_m,
        SUM(daily_net_movement_amount) / 1e6    AS net_m
    FROM gold.fact_cash_position
    WHERE cash_position_date BETWEEN '{start}' AND '{end}'
    GROUP BY cash_position_month
    ORDER BY cash_position_month
""")

inflow_sources = run_query(f"""
    SELECT
        CASE
            WHEN LOWER(transaction_narration) LIKE '%paystack%'
                                                                    THEN 'Paystack (DAASH)'
            WHEN LOWER(transaction_narration) LIKE '%9japay%'
                                                                    THEN '9japay (GoSource)'
            WHEN LOWER(transaction_narration) LIKE '%interest capitalised%'
                                                                    THEN 'Bank Interest'
            WHEN LOWER(transaction_narration) LIKE '%fee cashback%'
              OR LOWER(transaction_narration) LIKE '%sms charges cashback%'
                                                                    THEN 'Fee Rebate'
            WHEN LOWER(transaction_narration) LIKE '%transfer between customers%'
                                                                    THEN 'Internal Transfer'
            WHEN LOWER(transaction_narration) LIKE '%inward%'
                                                                    THEN 'Inward Transfer'
            WHEN LOWER(transaction_narration) LIKE '%uba%'
                                                                    THEN 'UBA Transfer'
            WHEN LOWER(transaction_narration) LIKE '%account transfer%'
              OR LOWER(transaction_narration) LIKE '%mob:%'
                                                                    THEN 'Mobile/Bank Transfer'
            ELSE 'Other'
        END                              AS source,
        SUM(cash_inflow_amount)          AS amount,
        COUNT(*)                         AS txn_count
    FROM gold.fact_cash_flow
    WHERE transaction_date BETWEEN '{start}' AND '{end}'
      AND cash_flow_direction = 'Inflow'
    GROUP BY 1
    ORDER BY amount DESC
""")

outflow_categories = run_query(f"""
    SELECT
        COALESCE(transaction_category, 'Uncategorised') AS category,
        SUM(cash_outflow_amount)                         AS amount,
        COUNT(*)                                         AS txn_count
    FROM gold.fact_cash_flow
    WHERE transaction_date BETWEEN '{start}' AND '{end}'
      AND cash_flow_direction = 'Outflow'
    GROUP BY transaction_category
    ORDER BY amount DESC
""")

top_inflows = run_query(f"""
    SELECT
        LEFT(COALESCE(NULLIF(TRIM(transaction_narration), ''), 'No description'), 60) AS narration,
        SUM(cash_inflow_amount) AS amount,
        COUNT(*)                AS txn_count
    FROM gold.fact_cash_flow
    WHERE transaction_date BETWEEN '{start}' AND '{end}'
      AND cash_flow_direction = 'Inflow'
    GROUP BY transaction_narration
    ORDER BY amount DESC
    LIMIT 10
""")

top_outflows = run_query(f"""
    SELECT
        LEFT(COALESCE(NULLIF(TRIM(transaction_narration), ''), 'No description'), 60) AS narration,
        COALESCE(transaction_category, 'Uncategorised')                               AS category,
        SUM(cash_outflow_amount)                                                      AS amount,
        COUNT(*)                                                                      AS txn_count
    FROM gold.fact_cash_flow
    WHERE transaction_date BETWEEN '{start}' AND '{end}'
      AND cash_flow_direction = 'Outflow'
    GROUP BY transaction_narration, transaction_category
    ORDER BY amount DESC
    LIMIT 15
""")

# Revenue by service line (what was earned per line — best proxy for operational cash)
monthly_sl_rev = run_query(f"""
    SELECT
        TO_CHAR(revenue_month, 'Mon')                                           AS label,
        revenue_month,
        SUM(CASE WHEN service_line = 'DAASH'    THEN revenue_amount ELSE 0 END) / 1e6 AS daash_m,
        SUM(CASE WHEN service_line = 'GoSource' THEN revenue_amount ELSE 0 END) / 1e6 AS gosource_m
    FROM gold.fact_revenue
    WHERE revenue_order_date BETWEEN '{start}' AND '{end}'
    GROUP BY revenue_month
    ORDER BY revenue_month
""")

# ─── Scalars ──────────────────────────────────────────────────────────────────
def _v(df, col, default=0):
    if df.empty or df.iloc[0][col] is None: return float(default)
    return float(df.iloc[0][col])

def _delta(c, p): return ((c - p)/p*100) if p and p > 0 else None

opening_balance = _v(cash_kpi, "opening_balance")
closing_balance = _v(cash_kpi, "closing_balance")
current_balance = _v(cash_kpi, "current_balance")
inflow          = _v(cash_kpi, "inflow")
outflow         = _v(cash_kpi, "outflow")
net             = _v(cash_kpi, "net")
avg_burn        = _v(cash_kpi, "avg_burn")
prev_inflow     = _v(cash_kpi, "prev_inflow")
prev_outflow    = _v(cash_kpi, "prev_outflow")
runway_days     = (current_balance / (avg_burn / 30)) if avg_burn > 0 else None

# ══════════════════════════════════════════════════════════════════════════════
# (a) CASH POSITION
# ══════════════════════════════════════════════════════════════════════════════
section_title("(A) CASH POSITION")
c1, c2, c3, c4 = st.columns(4)
c1.metric("📂 Opening Balance", naira(opening_balance),
          help=f"Estimated Lenco balance at the start of {start}. "
               "= Live balance rewound using all recorded debits/credits from then to now.")
c2.metric("📁 Closing Balance", naira(closing_balance),
          delta=f"{naira(closing_balance - opening_balance)} change in period",
          delta_color="normal" if closing_balance >= opening_balance else "inverse",
          help=f"Estimated Lenco balance at end of {end}. "
               "If the period ends today, this equals the Current Live Balance.")
c3.metric("🏦 Current Live Balance", naira(current_balance),
          help="Live balance from Lenco accounts API (Providus Bank) — the actual account balance right now. "
               "Opening + Credits − Debits over all time = this figure.")
c4.metric("📊 Period Net Movement", naira(net),
          delta="▲ Positive" if (net or 0) >= 0 else "▼ Negative",
          delta_color="normal" if (net or 0) >= 0 else "inverse")

st.markdown("")
c5, c6, c7, c8 = st.columns(4)
c5.metric("📈 Period Inflows", naira(inflow),
          delta=(f"{_delta(inflow, prev_inflow):+.1f}% vs prev"
                 if _delta(inflow, prev_inflow) is not None else None))
c6.metric("📉 Period Outflows", naira(outflow),
          delta=(f"{_delta(outflow, prev_outflow):+.1f}% vs prev"
                 if _delta(outflow, prev_outflow) is not None else None),
          delta_color="inverse")
c7.metric("🔥 Avg Monthly Burn", naira(avg_burn),
          help="Average monthly outflows over the last 3 months (Lenco debits).")
c8.metric("⏳ Cash Runway", f"{runway_days:.0f} days" if runway_days else "N/A",
          help="Current Lenco balance ÷ avg daily burn. Based on Lenco account only.")

st.markdown("")

# Cash by source (inflows) — pie chart
section_title("CASH BY SOURCE — INFLOWS (Period Total)")
if not inflow_sources.empty:
    col_pie, col_tbl = st.columns([1, 1])
    with col_pie:
        fig_src = go.Figure(go.Pie(
            labels=inflow_sources["source"],
            values=inflow_sources["amount"],
            hole=0.52,
            textinfo="percent+label",
            textfont_size=12,
            marker_colors=["#3B82F6", "#10B981", "#F59E0B", "#8B5CF6", "#64748B"],
        ))
        fig_src.update_layout(showlegend=False, margin=dict(t=10, b=0, l=0, r=0), height=240)
        st.plotly_chart(fig_src, use_container_width=True)
    with col_tbl:
        src_d = inflow_sources.copy()
        src_d["amount"]    = src_d["amount"].apply(lambda x: naira(float(x)))
        src_d["txn_count"] = src_d["txn_count"].apply(lambda x: count(int(x)))
        src_d.columns = ["Source", "Amount", "Txns"]
        st.dataframe(src_d, use_container_width=True, hide_index=True, height=240)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# DAILY ACTIVITY
# ══════════════════════════════════════════════════════════════════════════════
section_title("DAILY CREDITS & DEBITS (Daily)")
if not daily_pos.empty:
    fig_d = go.Figure()
    fig_d.add_trace(go.Bar(
        x=daily_pos["cash_position_date"], y=daily_pos["inflow"] / 1e6,
        name="Credits (In)", marker_color=COLOR_POSITIVE, opacity=0.85,
        hovertemplate="<b>%{x|%d %b %Y}</b><br>Credits: ₦%{y:.2f}M<extra></extra>",
    ))
    fig_d.add_trace(go.Bar(
        x=daily_pos["cash_position_date"], y=daily_pos["outflow"] / 1e6,
        name="Debits (Out)", marker_color=COLOR_NEGATIVE, opacity=0.85,
        hovertemplate="<b>%{x|%d %b %Y}</b><br>Debits: ₦%{y:.2f}M<extra></extra>",
    ))
    fig_d.update_layout(**CHART_LAYOUT, barmode="group", height=280, yaxis_title="₦M")
    st.plotly_chart(fig_d, use_container_width=True)

# Daily table
col1, col2 = st.columns([4, 1])
with col2:
    if not daily_pos.empty:
        st.download_button("📥 Download CSV", daily_pos.to_csv(index=False),
                           file_name=f"daily_cash_{start}_{end}.csv", mime="text/csv")
if not daily_pos.empty:
    disp = daily_pos.copy()
    for c in ["inflow", "outflow", "net"]:
        disp[c] = disp[c].apply(lambda x: naira(float(x)))
    disp.columns = ["Date", "Credits (In)", "Debits (Out)", "Net"]
    st.dataframe(disp.sort_values("Date", ascending=False),
                 use_container_width=True, hide_index=True, height=350)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# (b) CASH INFLOWS
# ══════════════════════════════════════════════════════════════════════════════
section_title("(B) CASH INFLOWS (Monthly)")

if not monthly_flows.empty:
    fig_m = go.Figure()
    fig_m.add_trace(go.Bar(
        x=monthly_flows["label"], y=monthly_flows["inflow_m"],
        name="Inflow", marker_color=COLOR_POSITIVE, opacity=0.85,
    ))
    fig_m.add_trace(go.Bar(
        x=monthly_flows["label"], y=monthly_flows["outflow_m"],
        name="Outflow", marker_color=COLOR_NEGATIVE, opacity=0.85,
    ))
    fig_m.add_trace(go.Scatter(
        x=monthly_flows["label"], y=monthly_flows["net_m"],
        name="Net", mode="lines+markers",
        line=dict(color=COLOR_CASH, width=2, dash="dot"), marker=dict(size=6),
    ))
    fig_m.update_layout(**CHART_LAYOUT, barmode="group", height=280, yaxis_title="₦M",
                        title="Monthly Inflows vs Outflows (Lenco)")
    st.plotly_chart(fig_m, use_container_width=True)

# Revenue by service line
section_title("REVENUE BY SERVICE LINE — EARNED (Monthly)")
st.caption("Shows revenue recognised per service line — closest proxy to operational cash inflows.")
if not monthly_sl_rev.empty:
    fig_sl = go.Figure()
    fig_sl.add_trace(go.Bar(
        x=monthly_sl_rev["label"], y=monthly_sl_rev["daash_m"],
        name="DAASH", marker_color=COLOR_DAASH, opacity=0.9,
    ))
    fig_sl.add_trace(go.Bar(
        x=monthly_sl_rev["label"], y=monthly_sl_rev["gosource_m"],
        name="GoSource", marker_color=COLOR_GOSOURCE, opacity=0.9,
    ))
    fig_sl.update_layout(**CHART_LAYOUT, barmode="stack", height=260, yaxis_title="₦M")
    st.plotly_chart(fig_sl, use_container_width=True)

# Top inflow narrations
section_title("TOP INFLOW TRANSACTIONS (Period Total)")
if not top_inflows.empty:
    ti = top_inflows.copy()
    ti["amount"]    = ti["amount"].apply(lambda x: naira(float(x)))
    ti["txn_count"] = ti["txn_count"].apply(lambda x: count(int(x)))
    ti.columns = ["Description", "Amount", "Txns"]
    st.dataframe(ti, use_container_width=True, hide_index=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# (c) CASH OUTFLOWS
# ══════════════════════════════════════════════════════════════════════════════
section_title("(C) CASH OUTFLOWS (Period Total)")
col_l, col_r = st.columns([3, 2])

with col_l:
    if not outflow_categories.empty:
        fig_out = go.Figure(go.Bar(
            x=outflow_categories["amount"] / 1e6,
            y=outflow_categories["category"],
            orientation="h",
            marker_color=COLOR_NEGATIVE,
            text=[naira(float(v) * 1e6) for v in (outflow_categories["amount"] / 1e6)],
            textposition="outside",
        ))
        fig_out.update_layout(
            **CHART_LAYOUT,
            height=max(280, len(outflow_categories) * 38),
            xaxis_title="Amount (₦M)",
            showlegend=False,
        )
        fig_out.update_yaxes(autorange="reversed")
        st.plotly_chart(fig_out, use_container_width=True)

with col_r:
    if not outflow_categories.empty:
        oc = outflow_categories.copy()
        oc["amount"]    = oc["amount"].apply(lambda x: naira(float(x)))
        oc["txn_count"] = oc["txn_count"].apply(lambda x: count(int(x)))
        oc.columns = ["Category", "Amount", "Txns"]
        st.dataframe(oc, use_container_width=True, hide_index=True,
                     height=max(280, len(outflow_categories) * 38))

section_title("TOP OUTFLOW TRANSACTIONS (Period Total)")
if not top_outflows.empty:
    to_ = top_outflows.copy()
    to_["amount"]    = to_["amount"].apply(lambda x: naira(float(x)))
    to_["txn_count"] = to_["txn_count"].apply(lambda x: count(int(x)))
    to_.columns = ["Description", "Category", "Amount", "Txns"]
    st.dataframe(to_, use_container_width=True, hide_index=True)

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# (d) NET CASH FLOW
# ══════════════════════════════════════════════════════════════════════════════
section_title("(D) NET CASH FLOW (Monthly)")
if not monthly_flows.empty:
    net_color = [COLOR_POSITIVE if v >= 0 else COLOR_NEGATIVE
                 for v in monthly_flows["net_m"]]
    fig_net = go.Figure(go.Bar(
        x=monthly_flows["label"],
        y=monthly_flows["net_m"],
        marker_color=net_color,
        text=[f"₦{v:.1f}M" for v in monthly_flows["net_m"]],
        textposition="outside",
        name="Net Cash",
    ))
    fig_net.add_hline(y=0, line_width=1.5, line_color="#94A3B8")
    fig_net.update_layout(**CHART_LAYOUT, height=260, yaxis_title="Net (₦M)",
                          showlegend=False)
    st.plotly_chart(fig_net, use_container_width=True)

col_n1, col_n2, col_n3 = st.columns(3)
col_n1.metric("Net Operating Inflows",  naira(inflow),  help="Total Lenco credits for the period.")
col_n2.metric("Net Operating Outflows", naira(outflow), help="Total Lenco debits for the period.")
col_n3.metric("Overall Net Movement",   naira(net),
              delta="▲ Positive" if (net or 0) >= 0 else "▼ Negative",
              delta_color="normal" if (net or 0) >= 0 else "inverse")

st.markdown("---")

# ══════════════════════════════════════════════════════════════════════════════
# (e) LIQUIDITY & CASH RUNWAY
# ══════════════════════════════════════════════════════════════════════════════
section_title("(E) LIQUIDITY & CASH RUNWAY")
lq1, lq2, lq3 = st.columns(3)
lq1.metric("🏦 Lenco Account Balance", naira(current_balance),
           help="Live balance from Lenco API. This is the Providus bank account balance.")
lq2.metric("🔥 Avg Monthly Burn", naira(avg_burn),
           help="Average monthly Lenco outflows over the last 3 months.")
lq3.metric("⏳ Lenco Runway", f"{runway_days:.0f} days" if runway_days else "N/A",
           help="How long the current Lenco balance lasts at the current burn rate. "
                "Note: this reflects the Lenco account only — not 9japay or other balances.")
