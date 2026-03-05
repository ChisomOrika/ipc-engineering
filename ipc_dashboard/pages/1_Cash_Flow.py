import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datetime as dt
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from utils.db  import run_query
from utils.fmt import naira, pct, count

st.set_page_config(page_title="Cash Flow · IPC", page_icon="💰", layout="wide")

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
    st.markdown(f"**{start.strftime('%d %b %Y')} → {end.strftime('%d %b %Y')}**")
    st.markdown("---")
    st.caption("Data refreshes every hour")

# ─── Queries ──────────────────────────────────────────────────────────────────
cash_pos = run_query("""
    SELECT cumulative_net_movement_amount AS current_position
    FROM gold.fact_cash_position
    ORDER BY cash_position_date DESC
    LIMIT 1
""")

period_flows = run_query(f"""
    SELECT
        SUM(daily_inflow_amount)       AS total_inflow,
        SUM(daily_outflow_amount)      AS total_outflow,
        SUM(daily_net_movement_amount) AS net_movement
    FROM gold.fact_cash_position
    WHERE cash_position_date BETWEEN '{start}' AND '{end}'
""")

daily_pos = run_query(f"""
    SELECT
        cash_position_date,
        cumulative_net_movement_amount / 1e6          AS cash_m,
        daily_inflow_amount / 1e6                     AS inflow_m,
        daily_outflow_amount / 1e6                    AS outflow_m,
        daily_net_movement_amount / 1e6               AS net_m
    FROM gold.fact_cash_position
    WHERE cash_position_date BETWEEN '{start}' AND '{end}'
    ORDER BY cash_position_date
""")

monthly_flows = run_query(f"""
    SELECT
        TO_CHAR(cash_position_month, 'Mon YY')           AS month_label,
        cash_position_month,
        SUM(daily_inflow_amount)  / 1e6                  AS inflow_m,
        SUM(daily_outflow_amount) / 1e6                  AS outflow_m
    FROM gold.fact_cash_position
    WHERE cash_position_date BETWEEN '{start}' AND '{end}'
    GROUP BY cash_position_month
    ORDER BY cash_position_month
""")

inflow_sources = run_query(f"""
    SELECT
        inflow_source,
        SUM(cash_inflow_amount) AS amount
    FROM gold.fact_cash_flow
    WHERE inflow_source IS NOT NULL
      AND transaction_date BETWEEN '{start}' AND '{end}'
    GROUP BY inflow_source
    ORDER BY amount DESC
""")

# ─── KPIs ─────────────────────────────────────────────────────────────────────
st.markdown("## 💰 Cash Flow")

cp = float(cash_pos.iloc[0]["current_position"]) if not cash_pos.empty else 0
pf = period_flows.iloc[0] if not period_flows.empty else None
inflow  = float(pf["total_inflow"]  or 0) if pf is not None else 0
outflow = float(pf["total_outflow"] or 0) if pf is not None else 0
net     = float(pf["net_movement"]  or 0) if pf is not None else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("💳 Current Balance",  naira(cp),      help="Running total as of latest transaction")
col2.metric("📈 Total Inflows",    naira(inflow),  help="All credits in the selected period")
col3.metric("📉 Total Outflows",   naira(outflow), help="All debits in the selected period")
col4.metric("📊 Net Movement",     naira(net),     delta=f"{naira(net)}" if net != 0 else None)

st.markdown("---")

# ─── Charts ───────────────────────────────────────────────────────────────────
st.markdown("#### Running Bank Balance")
if not daily_pos.empty:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily_pos["cash_position_date"],
        y=daily_pos["cash_m"],
        mode="lines",
        fill="tozeroy",
        line=dict(color="#0A9396", width=2),
        fillcolor="rgba(10,147,150,0.12)",
        name="Balance",
    ))
    fig.update_layout(
        yaxis_title="Balance (₦M)",
        xaxis_title="",
        plot_bgcolor="white",
        height=300,
        margin=dict(t=10, b=0, l=0, r=0),
        showlegend=False,
    )
    fig.update_yaxes(gridcolor="#F0F0F0")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("No cash data for selected period.")

left, right = st.columns(2)

with left:
    st.markdown("#### Monthly Inflows vs Outflows")
    if not monthly_flows.empty:
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=monthly_flows["month_label"], y=monthly_flows["inflow_m"],
            name="Inflow", marker_color="#2D9D5D",
        ))
        fig2.add_trace(go.Bar(
            x=monthly_flows["month_label"], y=monthly_flows["outflow_m"],
            name="Outflow", marker_color="#E63946",
        ))
        fig2.update_layout(
            barmode="group",
            yaxis_title="₦M",
            xaxis_title="",
            plot_bgcolor="white",
            height=300,
            margin=dict(t=10, b=0, l=0, r=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        fig2.update_yaxes(gridcolor="#F0F0F0")
        st.plotly_chart(fig2, use_container_width=True)

with right:
    st.markdown("#### Inflow Sources")
    if not inflow_sources.empty:
        fig3 = px.pie(
            inflow_sources, values="amount", names="inflow_source",
            hole=0.5,
            color_discrete_sequence=px.colors.qualitative.Set2,
        )
        fig3.update_traces(textinfo="percent+label")
        fig3.update_layout(
            showlegend=False,
            margin=dict(t=10, b=0, l=0, r=0),
            height=300,
        )
        st.plotly_chart(fig3, use_container_width=True)
    else:
        st.info("No inflow source data available.")

# ─── Daily breakdown table ────────────────────────────────────────────────────
st.markdown("---")
st.markdown("#### Daily Cash Flow Detail")
if not daily_pos.empty:
    display = daily_pos[["cash_position_date", "inflow_m", "outflow_m", "net_m", "cash_m"]].copy()
    display.columns = ["Date", "Inflow (₦M)", "Outflow (₦M)", "Net (₦M)", "Balance (₦M)"]
    for col in ["Inflow (₦M)", "Outflow (₦M)", "Net (₦M)", "Balance (₦M)"]:
        display[col] = display[col].apply(lambda x: f"{x:,.2f}")
    st.dataframe(display.sort_values("Date", ascending=False), use_container_width=True, height=350)
