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
from utils.periods import sidebar_filters, bu_filter_sql

st.set_page_config(page_title="Cash Flow · IPC", page_icon="💰", layout="wide")
inject_css()

start, end, prev_start, prev_end, period_label, _, business_unit = sidebar_filters()
bu_clause = bu_filter_sql(business_unit)

page_header("Cash Flow & Bank Position",
            f"{period_label} · Lenco + 9japay")

# ─── All queries ──────────────────────────────────────────────────────────────

cash_kpi = run_query(f"""
    WITH real_balance AS (
        SELECT COALESCE(SUM(account_current_balance_amount::numeric), 0) AS balance
        FROM gold.dim_lenco_accounts
        WHERE 1=1 {bu_clause}
    ),
    after_period AS (
        SELECT COALESCE(SUM(daily_net_movement_amount), 0) AS net_after
        FROM gold.fact_cash_position
        WHERE cash_position_date > '{end}' {bu_clause}
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
        WHERE cash_position_date BETWEEN '{prev_start}' AND '{end}' {bu_clause}
    ),
    burn AS (
        SELECT
            COALESCE(SUM(CASE WHEN cash_position_date >= CURRENT_DATE - 21
                              THEN daily_outflow_amount END)
                     / NULLIF(SUM(CASE WHEN cash_position_date >= CURRENT_DATE - 21
                                       THEN 1 END), 0), 0) AS avg_daily_burn,
            COALESCE(SUM(CASE WHEN cash_position_date >= CURRENT_DATE - 90
                              THEN daily_outflow_amount END)
                     / NULLIF(SUM(CASE WHEN cash_position_date >= CURRENT_DATE - 90
                                       THEN 1 END), 0), 0) AS avg_daily_burn_90d
        FROM gold.fact_cash_position
        WHERE cash_position_date >= CURRENT_DATE - 90 {bu_clause}
    )
    SELECT
        real_balance.balance - after_period.net_after                           AS closing_balance,
        real_balance.balance - after_period.net_after - COALESCE(flows.net, 0) AS opening_balance,
        real_balance.balance                                                    AS current_balance,
        flows.inflow, flows.outflow, flows.net,
        flows.prev_inflow, flows.prev_outflow,
        burn.avg_daily_burn,
        burn.avg_daily_burn * 30 AS avg_monthly_burn,
        burn.avg_daily_burn_90d
    FROM real_balance, after_period, flows, burn
""")

account_balances = run_query(f"""
    SELECT
        account_name,
        account_current_balance_amount::numeric AS balance,
        business_unit,
        account_purpose
    FROM gold.dim_lenco_accounts
    WHERE 1=1 {bu_clause}
    ORDER BY account_current_balance_amount::numeric DESC
""")

daily_pos = run_query(f"""
    SELECT
        cash_position_date,
        SUM(daily_inflow_amount)       AS inflow,
        SUM(daily_outflow_amount)      AS outflow,
        SUM(daily_net_movement_amount) AS net
    FROM gold.fact_cash_position
    WHERE cash_position_date BETWEEN '{start}' AND '{end}' {bu_clause}
    GROUP BY cash_position_date
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
    WHERE cash_position_date BETWEEN '{start}' AND '{end}' {bu_clause}
    GROUP BY cash_position_month
    ORDER BY cash_position_month
""")

inflow_sources = run_query(f"""
    SELECT
        CASE
            WHEN LOWER(transaction_narration) LIKE '%paystack%'
                                                                    THEN 'Paystack (DAASH)'
            WHEN LOWER(transaction_narration) LIKE '%9japay%'
                                                                    THEN '9japay Settlement'
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
      AND cash_flow_direction = 'Inflow' {bu_clause}
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
      AND cash_flow_direction = 'Outflow' {bu_clause}
    GROUP BY 1
    ORDER BY amount DESC
""")

top_inflows = run_query(f"""
    SELECT
        LEFT(COALESCE(NULLIF(TRIM(transaction_narration), ''), 'No description'), 60) AS narration,
        SUM(cash_inflow_amount) AS amount,
        COUNT(*)                AS txn_count
    FROM gold.fact_cash_flow
    WHERE transaction_date BETWEEN '{start}' AND '{end}'
      AND cash_flow_direction = 'Inflow' {bu_clause}
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
      AND cash_flow_direction = 'Outflow' {bu_clause}
    GROUP BY transaction_narration, transaction_category
    ORDER BY amount DESC
    LIMIT 15
""")

# Revenue by service line (what was earned per line — best proxy for operational cash)
monthly_sl_rev = run_query(f"""
    SELECT
        TO_CHAR(revenue_month, 'Mon')                                           AS label,
        revenue_month,
        SUM(CASE WHEN service_line = 'DAASH'    THEN sales_amount ELSE 0 END) / 1e6 AS daash_m,
        SUM(CASE WHEN service_line = 'GoSource' THEN sales_amount ELSE 0 END) / 1e6 AS gosource_m
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
avg_daily_burn   = _v(cash_kpi, "avg_daily_burn")
avg_monthly_burn = _v(cash_kpi, "avg_monthly_burn")
avg_daily_burn_90d = _v(cash_kpi, "avg_daily_burn_90d")
prev_inflow      = _v(cash_kpi, "prev_inflow")
prev_outflow     = _v(cash_kpi, "prev_outflow")
runway_days      = (current_balance / avg_daily_burn) if avg_daily_burn > 0 else None

# ══════════════════════════════════════════════════════════════════════════════
# (a) CASH POSITION
# ══════════════════════════════════════════════════════════════════════════════
import datetime as _dt
_today_label = _dt.date.today().strftime("%d %b %Y")
_start_label = start.strftime("%d %b %Y") if hasattr(start, "strftime") else str(start)
_end_label   = end.strftime("%d %b %Y") if hasattr(end, "strftime") else str(end)

section_title("(A) CASH POSITION")
c1, c2, c3, c4 = st.columns(4)
c1.metric(f"📂 Balance at {_start_label}", naira(opening_balance),
          help=f"Estimated balance on {_start_label}. "
               "Computed by rewinding today's live balance using all recorded Lenco + 9japay transactions.")
c2.metric(f"📁 Balance at {_end_label}", naira(closing_balance),
          delta=f"{naira(closing_balance - opening_balance)} change in period",
          delta_color="normal" if closing_balance >= opening_balance else "inverse",
          help=f"Estimated balance on {_end_label}. "
               "Derived by rewinding from today's live balance.")
c3.metric(f"🏦 Today's Balance ({_today_label})", naira(current_balance),
          help="Live balance across all 5 Lenco sub-accounts (Providus Bank). "
               "Accounts: Purchasing, Admin, Management, Payments, Marketing.")
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
c7.metric("🔥 Avg Daily Burn", naira(avg_daily_burn),
          delta=f"~{naira(avg_monthly_burn)}/mo · 90d: {naira(avg_daily_burn_90d)}/day",
          delta_color="off",
          help="Average daily outflow over the last 21 days (Lenco + 9japay debits). 90d trailing shown for comparison.")
c8.metric("⏳ Cash Runway", f"{runway_days:.0f} days" if runway_days else "N/A",
          help="Total balance across all accounts ÷ avg daily burn (21-day average).")

st.markdown("")

# Cash by Account — live balances as of today
bu_label = f" ({business_unit})" if business_unit != "Combined" else " (All)"
section_title(f"CASH BY ACCOUNT — Live Balances ({_today_label}){bu_label}")
if not account_balances.empty:
    total_bal = float(account_balances["balance"].sum())
    n_cols = min(len(account_balances), 6)
    for chunk_start in range(0, len(account_balances), n_cols):
        chunk = account_balances.iloc[chunk_start:chunk_start + n_cols]
        acct_cols = st.columns(len(chunk))
        for i, (_, row) in enumerate(chunk.iterrows()):
            name = (row["account_name"]
                    .replace("INDEPENDENT PURCHASING COM LTD", "PURCHASING")
                    .replace("INDEPENDENT- ", "")
                    .replace("GO SOURCE SERVICES-LCO", "GS MAIN")
                    .replace("GO SOURCE- ", "GS "))
            bal = float(row["balance"])
            share = (bal / total_bal * 100) if total_bal > 0 else 0
            bu_tag = row.get("business_unit", "")
            acct_cols[i].metric(
                f"🏦 {name}",
                naira(bal),
                delta=f"{share:.0f}% · {bu_tag}",
                delta_color="off",
                help=f"Live balance for {row['account_name']} ({bu_tag}) as of {_today_label}"
            )

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
        st.download_button("📥 Download CSV", inflow_sources.to_csv(index=False),
                           f"inflow_sources_{start}_{end}.csv", "text/csv")
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
                        title="Monthly Inflows vs Outflows (Lenco + 9japay)")
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
    c1, c2 = st.columns([5, 1])
    with c2:
        st.download_button("📥 Download CSV", top_inflows.to_csv(index=False),
                           f"top_inflows_{start}_{end}.csv", "text/csv")
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
        st.download_button("📥 Download CSV", outflow_categories.to_csv(index=False),
                           f"outflow_categories_{start}_{end}.csv", "text/csv")
        oc = outflow_categories.copy()
        oc["amount"]    = oc["amount"].apply(lambda x: naira(float(x)))
        oc["txn_count"] = oc["txn_count"].apply(lambda x: count(int(x)))
        oc.columns = ["Category", "Amount", "Txns"]
        st.dataframe(oc, use_container_width=True, hide_index=True,
                     height=max(280, len(outflow_categories) * 38))

section_title("TOP OUTFLOW TRANSACTIONS (Period Total)")
if not top_outflows.empty:
    c1, c2 = st.columns([5, 1])
    with c2:
        st.download_button("📥 Download CSV", top_outflows.to_csv(index=False),
                           f"top_outflows_{start}_{end}.csv", "text/csv")
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
col_n1.metric("Net Operating Inflows",  naira(inflow),  help="Total Lenco + 9japay credits for the period.")
col_n2.metric("Net Operating Outflows", naira(outflow), help="Total Lenco + 9japay debits for the period.")
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
           help="Live balance from Lenco API (Providus Bank). 9japay balances are swept here daily.")
lq2.metric("🔥 Avg Daily Burn (21d)", naira(avg_daily_burn),
           delta=f"90d trailing: {naira(avg_daily_burn_90d)}/day",
           delta_color="off",
           help="Average daily outflow over the last 21 days (Lenco + 9japay). 90-day trailing shown for comparison.")
lq3.metric("⏳ Cash Runway", f"{runway_days:.0f} days" if runway_days else "N/A",
           help="Current Lenco balance ÷ avg daily burn (Lenco + 9japay, 90-day average).")
