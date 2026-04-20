import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

from utils.db      import run_query
from utils.fmt     import naira, pct, count
from utils.styles  import (inject_css, page_header, section_title, CHART_LAYOUT,
                            COLOR_POSITIVE, COLOR_NEGATIVE, COLOR_NEUTRAL)
from utils.periods import sidebar_filters, bu_filter_sql

st.set_page_config(page_title="Expenses · IPC", page_icon="💸", layout="wide")
inject_css()

start, end, prev_start, prev_end, period_label, _, business_unit = sidebar_filters()
bu_clause = bu_filter_sql(business_unit)

page_header("Cost Management & Expenses",
            f"{period_label} · Lenco Bank Debits")

# ─── Queries ──────────────────────────────────────────────────────────────────
# Master KPI query — replaces 3 separate round-trips
exp_kpi = run_query(f"""
    WITH exp AS (
        SELECT
            SUM(CASE WHEN expense_date BETWEEN '{start}' AND '{end}'
                     THEN expense_amount END)                                      AS total,
            SUM(CASE WHEN expense_date BETWEEN '{start}' AND '{end}'
                     AND expense_group = 'Supplies & Procurement'
                     THEN expense_amount END)                                      AS cogs,
            SUM(CASE WHEN expense_date BETWEEN '{start}' AND '{end}'
                     AND expense_group != 'Supplies & Procurement'
                     THEN expense_amount END)                                      AS opex,
            SUM(CASE WHEN expense_date BETWEEN '{start}' AND '{end}'
                     AND expense_type='Fixed' AND expense_group != 'Supplies & Procurement'
                     THEN expense_amount END)                                      AS fixed,
            SUM(CASE WHEN expense_date BETWEEN '{start}' AND '{end}'
                     AND expense_type='Variable' AND expense_group != 'Supplies & Procurement'
                     THEN expense_amount END)                                      AS variable,
            COUNT(CASE WHEN expense_date BETWEEN '{start}' AND '{end}'
                       THEN 1 END)                                                 AS txn_count,
            SUM(CASE WHEN expense_date BETWEEN '{prev_start}' AND '{prev_end}'
                     THEN expense_amount END)                                      AS prev_total,
            SUM(CASE WHEN expense_date BETWEEN '{prev_start}' AND '{prev_end}'
                     AND expense_group != 'Supplies & Procurement'
                     THEN expense_amount END)                                      AS prev_opex
        FROM gold.fact_expenses
        WHERE expense_date BETWEEN '{prev_start}' AND '{end}' {bu_clause}
    ),
    rev AS (
        SELECT SUM(sales_amount) AS revenue
        FROM gold.fact_revenue
        WHERE revenue_order_date BETWEEN '{start}' AND '{end}'
    )
    SELECT exp.total, exp.cogs, exp.opex, exp.fixed, exp.variable, exp.txn_count,
           exp.prev_total, exp.prev_opex, rev.revenue
    FROM exp, rev
""")

by_group = run_query(f"""
    SELECT
        expense_group,
        expense_type,
        SUM(expense_amount) AS amount,
        COUNT(*) AS txns
    FROM gold.fact_expenses
    WHERE expense_date BETWEEN '{start}' AND '{end}' {bu_clause}
    GROUP BY expense_group, expense_type
    ORDER BY amount DESC
""")

by_category = run_query(f"""
    SELECT
        expense_category,
        expense_group,
        SUM(expense_amount) AS amount,
        COUNT(*) AS txns
    FROM gold.fact_expenses
    WHERE expense_date BETWEEN '{start}' AND '{end}' {bu_clause}
    GROUP BY expense_category, expense_group
    ORDER BY amount DESC
""")

monthly_by_group = run_query(f"""
    SELECT
        TO_CHAR(expense_month, 'Mon YY')  AS label,
        expense_month,
        expense_group,
        expense_type,
        SUM(expense_amount)/1e6 AS amount_m
    FROM gold.fact_expenses
    WHERE expense_date BETWEEN '{start}' AND '{end}' {bu_clause}
    GROUP BY expense_month, expense_group, expense_type
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
    WHERE expense_date BETWEEN '{start}' AND '{end}' {bu_clause}
    ORDER BY expense_date DESC
    LIMIT 500
""")

# ─── Compute scalars ──────────────────────────────────────────────────────────
def _v(df, col, default=0):
    if df.empty or df.iloc[0][col] is None: return float(default)
    return float(df.iloc[0][col])
def _d(c, p): return ((c-p)/p*100) if p and p > 0 else None

total_exp  = _v(exp_kpi, "total")
cogs       = _v(exp_kpi, "cogs")
opex       = _v(exp_kpi, "opex")
prev_total = _v(exp_kpi, "prev_total")
prev_opex  = _v(exp_kpi, "prev_opex")
fixed_exp  = _v(exp_kpi, "fixed")
var_exp    = _v(exp_kpi, "variable")
txn_count  = int(_v(exp_kpi, "txn_count"))
revenue    = _v(exp_kpi, "revenue")
opex_ratio = (opex / revenue * 100) if revenue > 0 else 0
gross_margin = ((revenue - cogs) / revenue * 100) if revenue > 0 else 0
fixed_pct  = (fixed_exp / opex * 100) if opex > 0 else 0

payroll_exp = 0
overhead_exp = 0
if not by_group.empty:
    payroll_groups = by_group[by_group["expense_group"] == "People & Payroll"]
    payroll_exp = float(payroll_groups["amount"].sum()) if not payroll_groups.empty else 0
    overhead_groups = by_group[by_group["expense_group"].isin(["Admin & Tech", "Operations & Maintenance", "Bank & Finance"])]
    overhead_exp = float(overhead_groups["amount"].sum()) if not overhead_groups.empty else 0
payroll_pct = (payroll_exp / opex * 100) if opex > 0 else 0
overhead_pct = (overhead_exp / opex * 100) if opex > 0 else 0

# ─── COGS vs OpEx split ──────────────────────────────────────────────────────
section_title("COST OF GOODS vs OPERATING EXPENSES")
st.caption("COGS = what we pay suppliers for goods we resell (GoSource procurement). OpEx = cost of running the business.")

c1, c2, c3, c4 = st.columns(4)
c1.metric("📦 COGS (Procurement)", naira(cogs),
          help="Cost of goods sold — supplier payments for food, packaging, ingredients. This is NOT an operating expense, it's the cost of inventory.")
c2.metric("🏢 OPERATING EXPENSES", naira(opex),
          delta=(f"{_d(opex, prev_opex):+.1f}% vs prev"
                 if _d(opex, prev_opex) is not None else None),
          delta_color="inverse",
          help="Actual cost of running the business — salaries, rent, tech, logistics, admin. Excludes procurement/COGS.")
c3.metric("📊 GROSS MARGIN", f"{gross_margin:.1f}%",
          help="(GMV − COGS) ÷ GMV. How much IPC keeps after paying suppliers. Higher = better.")
c4.metric("📊 OpEx / GMV", f"{opex_ratio:.1f}%",
          help="Operating expenses as % of GMV. Lower = more efficient operations.")

st.markdown("")

# ─── OpEx breakdown KPIs ─────────────────────────────────────────────────────
section_title("OPERATING EXPENSE BREAKDOWN")
cols = st.columns(5)
cols[0].metric("💸 TOTAL OpEx", naira(opex),
               delta=(f"{_d(opex, prev_opex):+.1f}% vs prev"
                      if _d(opex, prev_opex) is not None else None),
               delta_color="inverse")
cols[1].metric("🔒 FIXED COSTS", naira(fixed_exp),
               delta=f"{fixed_pct:.1f}% of OpEx", delta_color="off")
cols[2].metric("⚡ VARIABLE COSTS", naira(var_exp),
               delta=f"{100-fixed_pct:.1f}% of OpEx", delta_color="off")
cols[3].metric("👥 PAYROLL", naira(payroll_exp),
               delta=f"{payroll_pct:.1f}% of OpEx", delta_color="off",
               help="Salaries, staff welfare, rider commissions as % of operating expenses.")
cols[4].metric("🏢 OVERHEAD", naira(overhead_exp),
               delta=f"{overhead_pct:.1f}% of OpEx", delta_color="off",
               help="Admin, tech, ops & bank charges as % of operating expenses.")

st.markdown("")
st.metric("📋 TOTAL TRANSACTIONS", count(txn_count))

st.markdown("---")

# ─── Bar chart + donut ───────────────────────────────────────────────────────
left, right = st.columns([3, 2])

with left:
    section_title("OUTFLOWS BY GROUP (COGS + OpEx)")
    if not by_group.empty:
        group_totals = by_group.groupby("expense_group")["amount"].sum().reset_index()
        group_totals = group_totals.sort_values("amount", ascending=True)
        group_totals["color"] = group_totals["expense_group"].apply(
            lambda x: "#F59E0B" if x == "Supplies & Procurement" else "#3B82F6"
        )
        fig = go.Figure(go.Bar(
            x=group_totals["amount"] / 1e6,
            y=group_totals["expense_group"],
            orientation="h",
            marker_color=group_totals["color"],
            text=(group_totals["amount"]/1e6).apply(lambda x: f"₦{x:.1f}M"),
            textposition="outside",
        ))
        fig.update_layout(**CHART_LAYOUT, height=300,
                          xaxis_title="₦M", showlegend=False)
        fig.update_xaxes(gridcolor="#F1F5F9")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("🟡 Yellow = COGS (procurement) · 🔵 Blue = Operating expenses")

with right:
    section_title("FIXED vs VARIABLE SPLIT")
    type_data = by_group.groupby("expense_type")["amount"].sum().reset_index()
    if not type_data.empty:
        fig2 = go.Figure(go.Pie(
            labels=type_data["expense_type"],
            values=type_data["amount"],
            hole=0.55,
            marker_colors=["#1E40AF", "#93C5FD"],
            textinfo="percent+label",
            textfont_size=12,
        ))
        fig2.update_layout(showlegend=False,
                           margin=dict(t=10, b=0, l=0, r=0), height=300)
        st.plotly_chart(fig2, use_container_width=True)

# Monthly stacked trend
section_title("MONTHLY EXPENSE TREND BY GROUP")
if not monthly_by_group.empty:
    grp_totals_by_month = (
        monthly_by_group.groupby(["label", "expense_month", "expense_group"])["amount_m"]
        .sum().reset_index()
    )
    fig3 = px.bar(
        grp_totals_by_month, x="label", y="amount_m",
        color="expense_group",
        color_discrete_sequence=px.colors.qualitative.Pastel,
        labels={"amount_m": "₦M", "label": "", "expense_group": ""},
        barmode="stack",
    )
    fig3.update_layout(**CHART_LAYOUT, height=280)
    st.plotly_chart(fig3, use_container_width=True)

# ─── Category breakdown table ──────────────────────────────────────────────────
st.markdown("---")

section_title("EXPENSE CATEGORY BREAKDOWN")
if not by_category.empty:
    c1, c2 = st.columns([5, 1])
    with c2:
        st.download_button("📥 Download CSV",
                           by_category.to_csv(index=False),
                           f"expenses_by_category_{start}_{end}.csv", "text/csv")
    display_cat = by_category.copy()
    grand = float(display_cat["amount"].sum() or 1)
    display_cat["share"] = (display_cat["amount"] / grand * 100).round(1).apply(lambda x: f"{x}%")
    display_cat["amount"] = display_cat["amount"].apply(lambda x: naira(float(x)))
    display_cat["txns"]   = display_cat["txns"].apply(lambda x: count(int(x)))
    display_cat.columns = ["Category", "Group", "Amount", "Txns", "% of Total"]
    st.dataframe(display_cat, use_container_width=True, hide_index=True, height=300)

# ─── Transaction detail ────────────────────────────────────────────────────────
with st.expander("📄 Full Transaction Detail"):
    # Filters
    f1, f2 = st.columns(2)
    with f1:
        grp_filter = st.multiselect("Filter by Group",
                                    detail["expense_group"].unique().tolist(),
                                    key="grp_f")
    with f2:
        type_filter = st.multiselect("Filter by Type",
                                     detail["expense_type"].unique().tolist(),
                                     key="typ_f")

    filtered = detail.copy()
    if grp_filter:  filtered = filtered[filtered["expense_group"].isin(grp_filter)]
    if type_filter: filtered = filtered[filtered["expense_type"].isin(type_filter)]

    c1, c2 = st.columns([5, 1])
    with c2:
        st.download_button("📥 Download CSV",
                           filtered.to_csv(index=False),
                           f"expense_detail_{start}_{end}.csv", "text/csv")
    disp = filtered.copy()
    disp["expense_amount"] = disp["expense_amount"].apply(lambda x: naira(float(x)))
    disp.columns = ["Date", "Category", "Group", "Type", "Narration", "Amount"]
    st.dataframe(disp, use_container_width=True, hide_index=True, height=400)
