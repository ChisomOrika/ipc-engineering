import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import datetime as dt
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from utils.db  import run_query
from utils.fmt import naira, pct, count

st.set_page_config(page_title="Expenses · IPC", page_icon="💸", layout="wide")

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
expense_kpi = run_query(f"""
    SELECT
        SUM(expense_amount)                                              AS total,
        SUM(CASE WHEN expense_type = 'Fixed'    THEN expense_amount ELSE 0 END) AS fixed,
        SUM(CASE WHEN expense_type = 'Variable' THEN expense_amount ELSE 0 END) AS variable
    FROM gold.fact_expenses
    WHERE expense_date BETWEEN '{start}' AND '{end}'
""")

by_group = run_query(f"""
    SELECT
        expense_group,
        SUM(expense_amount)  AS amount,
        COUNT(*)             AS txn_count
    FROM gold.fact_expenses
    WHERE expense_date BETWEEN '{start}' AND '{end}'
    GROUP BY expense_group
    ORDER BY amount DESC
""")

by_type = run_query(f"""
    SELECT
        expense_type,
        SUM(expense_amount) AS amount
    FROM gold.fact_expenses
    WHERE expense_date BETWEEN '{start}' AND '{end}'
    GROUP BY expense_type
""")

monthly_by_group = run_query(f"""
    SELECT
        TO_CHAR(expense_month, 'Mon YY')  AS month_label,
        expense_month,
        expense_group,
        SUM(expense_amount) / 1e6         AS amount_m
    FROM gold.fact_expenses
    WHERE expense_date BETWEEN '{start}' AND '{end}'
    GROUP BY expense_month, expense_group
    ORDER BY expense_month
""")

detail = run_query(f"""
    SELECT
        expense_date,
        expense_category,
        expense_group,
        expense_type,
        expense_narration,
        expense_amount
    FROM gold.fact_expenses
    WHERE expense_date BETWEEN '{start}' AND '{end}'
    ORDER BY expense_date DESC
    LIMIT 1000
""")

# ─── KPIs ─────────────────────────────────────────────────────────────────────
st.markdown("## 💸 Expenses")

ek = expense_kpi.iloc[0] if not expense_kpi.empty else None
total_exp  = float(ek["total"]    or 0) if ek is not None else 0
fixed_exp  = float(ek["fixed"]    or 0) if ek is not None else 0
var_exp    = float(ek["variable"] or 0) if ek is not None else 0
fixed_pct  = (fixed_exp / total_exp * 100) if total_exp > 0 else 0
var_pct    = 100 - fixed_pct

top_group = by_group.iloc[0]["expense_group"] if not by_group.empty else "—"

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Expenses",    naira(total_exp))
col2.metric("Fixed Costs",       naira(fixed_exp),  delta=f"{fixed_pct:.1f}%",  delta_color="off")
col3.metric("Variable Costs",    naira(var_exp),    delta=f"{var_pct:.1f}%",    delta_color="off")
col4.metric("Top Expense Group", top_group)

st.markdown("---")

# ─── Charts ───────────────────────────────────────────────────────────────────
left, right = st.columns(2)

with left:
    st.markdown("#### Expense Breakdown by Group")
    if not by_group.empty:
        fig = px.bar(
            by_group,
            x="amount", y="expense_group",
            orientation="h",
            color="expense_group",
            color_discrete_sequence=px.colors.qualitative.Set2,
            labels={"amount": "₦", "expense_group": ""},
        )
        fig.update_layout(
            showlegend=False,
            plot_bgcolor="white",
            height=320,
            margin=dict(t=10, b=0, l=0, r=0),
        )
        fig.update_xaxes(gridcolor="#F0F0F0")
        st.plotly_chart(fig, use_container_width=True)

with right:
    st.markdown("#### Fixed vs Variable")
    if not by_type.empty:
        fig2 = px.pie(
            by_type, values="amount", names="expense_type",
            color="expense_type",
            color_discrete_map={"Fixed": "#023E8A", "Variable": "#48CAE4"},
            hole=0.5,
        )
        fig2.update_traces(textinfo="percent+label")
        fig2.update_layout(
            showlegend=False,
            margin=dict(t=10, b=0, l=0, r=0),
            height=320,
        )
        st.plotly_chart(fig2, use_container_width=True)

st.markdown("#### Monthly Expenses by Group")
if not monthly_by_group.empty:
    fig3 = px.bar(
        monthly_by_group,
        x="month_label", y="amount_m",
        color="expense_group",
        color_discrete_sequence=px.colors.qualitative.Set2,
        labels={"amount_m": "Expenses (₦M)", "month_label": "", "expense_group": ""},
        barmode="stack",
    )
    fig3.update_layout(
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="white",
        height=300,
        margin=dict(t=10, b=0, l=0, r=0),
    )
    fig3.update_yaxes(gridcolor="#F0F0F0")
    st.plotly_chart(fig3, use_container_width=True)

# ─── Detail table ─────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("#### Transaction Detail")
if not detail.empty:
    display = detail.copy()
    display["expense_amount"] = display["expense_amount"].apply(lambda x: naira(float(x)))
    display.columns = ["Date", "Category", "Group", "Type", "Narration", "Amount"]
    st.dataframe(display, use_container_width=True, height=400)
else:
    st.info("No expense data for selected period.")
