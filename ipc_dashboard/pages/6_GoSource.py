import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

from utils.db      import run_query
from utils.fmt     import naira, pct, count
from utils.styles  import (inject_css, page_header, section_title,
                            CHART_LAYOUT, COLOR_GOSOURCE, COLOR_POSITIVE,
                            COLOR_NEGATIVE, COLOR_NEUTRAL, COLOR_CASH)
from utils.periods import sidebar_filters

st.set_page_config(page_title="GoSource Analytics", page_icon="📦", layout="wide")
inject_css()

start, end, prev_start, prev_end, period_label, _ = sidebar_filters()

page_header(
    "GoSource — Service Line Analytics",
    f"B2B Procurement · {period_label}"
)

# ─── Master KPI query ─────────────────────────────────────────────────────────
kpi = run_query(f"""
    WITH curr AS (
        SELECT
            COUNT(*)                                                        AS orders,
            SUM(revenue_amount)                                             AS revenue,
            SUM(revenue_service_charge_amount)                              AS svc_charge,
            SUM(revenue_delivery_fee_amount)                                AS delivery_fee,
            -- Upfront = Transfer / Paystack / Wallet
            COUNT(*) FILTER (WHERE LOWER(revenue_payment_method) != 'credit') AS upfront_orders,
            SUM(revenue_amount) FILTER (WHERE LOWER(revenue_payment_method) != 'credit') AS upfront_rev,
            -- Credit (paid)
            COUNT(*) FILTER (WHERE LOWER(revenue_payment_method) = 'credit') AS credit_orders,
            SUM(revenue_amount) FILTER (WHERE LOWER(revenue_payment_method) = 'credit') AS credit_rev
        FROM gold.fact_revenue
        WHERE service_line = 'GoSource'
          AND revenue_order_date BETWEEN '{start}' AND '{end}'
    ),
    prev AS (
        SELECT COUNT(*) AS orders, SUM(revenue_amount) AS revenue
        FROM gold.fact_revenue
        WHERE service_line = 'GoSource'
          AND revenue_order_date BETWEEN '{prev_start}' AND '{prev_end}'
    ),
    gp AS (
        SELECT
            COALESCE(SUM(CASE WHEN profit_date BETWEEN '{start}' AND '{end}'
                         THEN profit_gross_profit_amount END), 0) AS curr_gp,
            COALESCE(SUM(CASE WHEN profit_date BETWEEN '{prev_start}' AND '{prev_end}'
                         THEN profit_gross_profit_amount END), 0) AS prev_gp
        FROM gold.fact_profitability
        WHERE service_line = 'GoSource'
          AND profit_date BETWEEN '{prev_start}' AND '{end}'
    ),
    ar AS (
        SELECT
            COALESCE(SUM(ar_outstanding_amount), 0)  AS ar_total,
            COUNT(*)                                  AS ar_invoices,
            COUNT(DISTINCT ar_customer_id_fk)         AS ar_customers
        FROM gold.fact_ar_aging
    )
    SELECT curr.*, prev.orders AS prev_orders, prev.revenue AS prev_revenue,
           gp.curr_gp, gp.prev_gp,
           ar.ar_total, ar.ar_invoices, ar.ar_customers
    FROM curr, prev, gp, ar
""")

# ─── Monthly trend ────────────────────────────────────────────────────────────
monthly = run_query(f"""
    SELECT
        revenue_month,
        TO_CHAR(revenue_month, 'Mon YY')                                    AS label,
        COUNT(*)                                                             AS orders,
        SUM(revenue_amount) / 1e6                                           AS rev_m,
        SUM(revenue_amount) / NULLIF(COUNT(*), 0)                           AS aov,
        SUM(revenue_amount) FILTER (WHERE LOWER(revenue_payment_method) = 'credit') / 1e6  AS credit_rev_m,
        SUM(revenue_amount) FILTER (WHERE LOWER(revenue_payment_method) != 'credit') / 1e6 AS upfront_rev_m
    FROM gold.fact_revenue
    WHERE service_line = 'GoSource'
      AND revenue_order_date BETWEEN '{start}' AND '{end}'
    GROUP BY revenue_month
    ORDER BY revenue_month
""")

monthly_gp = run_query(f"""
    SELECT
        profit_month                                                         AS month,
        TO_CHAR(profit_month, 'Mon YY')                                     AS label,
        SUM(profit_gross_profit_amount) / 1e6                               AS gp_m
    FROM gold.fact_profitability
    WHERE service_line = 'GoSource'
      AND profit_date BETWEEN '{start}' AND '{end}'
    GROUP BY profit_month
    ORDER BY profit_month
""")

# ─── Top customers ────────────────────────────────────────────────────────────
top_customers = run_query(f"""
    SELECT
        COALESCE(NULLIF(TRIM(revenue_customer_name), ''), 'Unknown')        AS customer,
        COUNT(*)                                                             AS orders,
        SUM(revenue_amount) / 1e6                                           AS rev_m,
        SUM(revenue_amount) / NULLIF(COUNT(*), 0)                           AS aov,
        SUM(revenue_amount) FILTER (WHERE LOWER(revenue_payment_method) = 'credit') / 1e6  AS credit_m,
        SUM(revenue_amount) FILTER (WHERE LOWER(revenue_payment_method) != 'credit') / 1e6 AS upfront_m
    FROM gold.fact_revenue
    WHERE service_line = 'GoSource'
      AND revenue_order_date BETWEEN '{start}' AND '{end}'
    GROUP BY revenue_customer_name
    ORDER BY rev_m DESC
    LIMIT 15
""")

# ─── Payment method breakdown ─────────────────────────────────────────────────
pay_data = run_query(f"""
    SELECT
        CASE
            WHEN LOWER(revenue_payment_method) = 'credit' THEN 'Credit (Paid)'
            WHEN LOWER(revenue_payment_method) IN ('paystack', 'paystack') THEN 'Paystack'
            WHEN LOWER(revenue_payment_method) = 'wallet' THEN 'Wallet'
            ELSE 'Transfer'
        END                                                                  AS method,
        COUNT(*)                                                             AS orders,
        SUM(revenue_amount) / 1e6                                           AS rev_m
    FROM gold.fact_revenue
    WHERE service_line = 'GoSource'
      AND revenue_order_date BETWEEN '{start}' AND '{end}'
    GROUP BY 1
    ORDER BY rev_m DESC
""")

# ─── Scalars ─────────────────────────────────────────────────────────────────
def _v(df, col, default=0):
    if df.empty or df.iloc[0][col] is None: return float(default)
    return float(df.iloc[0][col])

def _delta(curr, prev):
    return ((curr - prev) / prev * 100) if prev and prev > 0 else None

orders         = int(_v(kpi, "orders"))
prev_orders    = int(_v(kpi, "prev_orders"))
revenue        = _v(kpi, "revenue")
prev_revenue   = _v(kpi, "prev_revenue")
svc_charge     = _v(kpi, "svc_charge")
delivery_fee   = _v(kpi, "delivery_fee")
curr_gp        = _v(kpi, "curr_gp")
prev_gp        = _v(kpi, "prev_gp")
upfront_orders = int(_v(kpi, "upfront_orders"))
upfront_rev    = _v(kpi, "upfront_rev")
credit_orders  = int(_v(kpi, "credit_orders"))
credit_rev     = _v(kpi, "credit_rev")
ar_total       = _v(kpi, "ar_total")
ar_invoices    = int(_v(kpi, "ar_invoices"))
ar_customers   = int(_v(kpi, "ar_customers"))
aov            = revenue / orders if orders > 0 else 0
prev_aov       = prev_revenue / prev_orders if prev_orders > 0 else 0
gp_margin      = (curr_gp / revenue * 100) if revenue > 0 else 0
credit_pct     = (credit_orders / orders * 100) if orders > 0 else 0

# ─── KPIs ────────────────────────────────────────────────────────────────────
section_title("KEY METRICS")
c1, c2, c3, c4, c5, c6 = st.columns(6)

c1.metric("📦 TOTAL ORDERS", count(orders),
          delta=f"{'+' if (_delta(orders,prev_orders) or 0) >= 0 else ''}{_delta(orders,prev_orders):.1f}% vs prev" if _delta(orders,prev_orders) else None,
          help="Delivered + paid GoSource orders. Source: gold.fact_revenue WHERE service_line='GoSource'")
c2.metric("💰 REVENUE", naira(revenue),
          delta=f"{'+' if (_delta(revenue,prev_revenue) or 0) >= 0 else ''}{_delta(revenue,prev_revenue):.1f}% vs prev" if _delta(revenue,prev_revenue) else None,
          help="Total order value for delivered + paid GoSource orders. Source: gold.fact_revenue")
c3.metric("📊 SERVICE CHARGE (GP)", naira(curr_gp),
          delta=f"{'+' if (_delta(curr_gp,prev_gp) or 0) >= 0 else ''}{_delta(curr_gp,prev_gp):.1f}% vs prev" if _delta(curr_gp,prev_gp) else None,
          help="Service charge collected. Only applies to credit orders — IPC's explicit margin. COGS not tracked per order, so GP = service charge only.")
c4.metric("🎯 AVG ORDER VALUE", naira(aov),
          delta=f"{'+' if (_delta(aov,prev_aov) or 0) >= 0 else ''}{_delta(aov,prev_aov):.1f}% vs prev" if _delta(aov,prev_aov) else None,
          help="Revenue ÷ Orders for the period.")
c5.metric("💳 CREDIT ORDERS", f"{count(credit_orders)} ({credit_pct:.0f}%)",
          help=f"Orders placed on credit (payment_method='Credit'). {credit_pct:.1f}% of total orders. These generate service charge revenue.")
c6.metric("📋 AR OUTSTANDING", naira(ar_total),
          delta=f"{ar_invoices} invoices · {ar_customers} customers",
          delta_color="off",
          help="Delivered GoSource credit orders not yet paid. Source: gold.fact_ar_aging")

st.markdown("")

# ─── Upfront vs Credit summary cards ─────────────────────────────────────────
section_title("UPFRONT vs CREDIT SALES")
ca, cb, cc = st.columns(3)
with ca:
    st.markdown(f"""
    <div style="background:#EFF6FF;border:1px solid #BFDBFE;border-radius:12px;padding:18px 22px;">
        <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.7px;color:#1D4ED8;">💳 Upfront Payments</div>
        <div style="font-size:24px;font-weight:700;color:#1D4ED8;margin:6px 0 2px;">{count(upfront_orders)} orders</div>
        <div style="font-size:15px;font-weight:600;color:#0F172A;">{naira(upfront_rev)}</div>
        <div style="font-size:11px;color:#64748B;margin-top:3px;">Transfer · Paystack · Wallet</div>
    </div>""", unsafe_allow_html=True)
with cb:
    st.markdown(f"""
    <div style="background:#FFF7ED;border:1px solid #FED7AA;border-radius:12px;padding:18px 22px;">
        <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.7px;color:#C2410C;">📝 Credit (Paid Off)</div>
        <div style="font-size:24px;font-weight:700;color:#C2410C;margin:6px 0 2px;">{count(credit_orders)} orders</div>
        <div style="font-size:15px;font-weight:600;color:#0F172A;">{naira(credit_rev)}</div>
        <div style="font-size:11px;color:#64748B;margin-top:3px;">Delivered + paid · service charge applies</div>
    </div>""", unsafe_allow_html=True)
with cc:
    st.markdown(f"""
    <div style="background:#FEF2F2;border:1px solid #FECACA;border-radius:12px;padding:18px 22px;">
        <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.7px;color:#DC2626;">⏳ AR Outstanding (All Time)</div>
        <div style="font-size:24px;font-weight:700;color:#DC2626;margin:6px 0 2px;">{naira(ar_total)}</div>
        <div style="font-size:12px;color:#0F172A;">{ar_invoices} open invoices · {ar_customers} customers</div>
        <div style="font-size:11px;color:#64748B;margin-top:3px;">Delivered but not yet paid</div>
    </div>""", unsafe_allow_html=True)

st.markdown("")

# ─── Monthly revenue breakdown ────────────────────────────────────────────────
left, right = st.columns([2, 1])

with left:
    section_title("MONTHLY REVENUE — UPFRONT vs CREDIT")
    if not monthly.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=monthly["label"], y=monthly["upfront_rev_m"],
            name="Upfront", marker_color=COLOR_GOSOURCE, opacity=0.9,
        ))
        fig.add_trace(go.Bar(
            x=monthly["label"], y=monthly["credit_rev_m"],
            name="Credit (Paid)", marker_color="#F59E0B", opacity=0.9,
        ))
        if not monthly_gp.empty:
            fig.add_trace(go.Scatter(
                x=monthly_gp["label"], y=monthly_gp["gp_m"],
                name="Service Charge (GP)", mode="lines+markers",
                line=dict(color=COLOR_POSITIVE, width=2),
                marker=dict(size=6), yaxis="y2",
            ))
        fig.update_layout(
            **CHART_LAYOUT,
            barmode="stack", height=320,
            yaxis_title="Revenue (₦M)", yaxis_gridcolor="#F1F5F9",
            yaxis2=dict(title="Service Charge (₦M)", overlaying="y", side="right",
                        showgrid=False, tickfont=dict(size=10)),
        )
        st.plotly_chart(fig, use_container_width=True)

with right:
    section_title("PAYMENT METHOD MIX")
    if not pay_data.empty:
        pay_colors = {
            "Transfer": "#2563EB", "Credit (Paid)": "#F59E0B",
            "Paystack": "#0891B2", "Wallet": "#64748B",
        }
        fig_pie = go.Figure(go.Pie(
            labels=pay_data["method"],
            values=pay_data["rev_m"],
            hole=0.5,
            marker_colors=[pay_colors.get(m, "#CBD5E1") for m in pay_data["method"]],
            textinfo="percent+label",
            textfont_size=11,
        ))
        fig_pie.update_layout(
            showlegend=False,
            margin=dict(t=8, b=8, l=8, r=8),
            height=320,
        )
        st.plotly_chart(fig_pie, use_container_width=True)

st.markdown("---")

# ─── AOV trend ────────────────────────────────────────────────────────────────
section_title("MONTHLY ORDER VOLUME & AVG ORDER VALUE")
if not monthly.empty:
    fig_aov = go.Figure()
    fig_aov.add_trace(go.Bar(
        x=monthly["label"], y=monthly["orders"],
        name="Orders", marker_color=COLOR_GOSOURCE, opacity=0.7,
    ))
    fig_aov.add_trace(go.Scatter(
        x=monthly["label"], y=monthly["aov"],
        name="AOV (₦)", mode="lines+markers",
        line=dict(color=COLOR_CASH, width=2),
        marker=dict(size=6), yaxis="y2",
    ))
    fig_aov.update_layout(
        **CHART_LAYOUT,
        height=280,
        yaxis_title="Orders", yaxis_gridcolor="#F1F5F9",
        yaxis2=dict(title="AOV (₦)", overlaying="y", side="right", showgrid=False),
    )
    st.plotly_chart(fig_aov, use_container_width=True)

st.markdown("---")

# ─── Top customers ────────────────────────────────────────────────────────────
section_title("TOP 15 CUSTOMERS BY REVENUE")
if not top_customers.empty:
    c1, c2 = st.columns([2, 1])
    with c1:
        fig_cust = go.Figure()
        fig_cust.add_trace(go.Bar(
            x=top_customers["upfront_m"], y=top_customers["customer"],
            orientation="h", name="Upfront", marker_color=COLOR_GOSOURCE,
        ))
        fig_cust.add_trace(go.Bar(
            x=top_customers["credit_m"], y=top_customers["customer"],
            orientation="h", name="Credit (Paid)", marker_color="#F59E0B",
        ))
        fig_cust.update_layout(
            **CHART_LAYOUT,
            barmode="stack", height=420,
            xaxis_title="Revenue (₦M)",
            yaxis_autorange="reversed", yaxis_tickfont=dict(size=11),
        )
        st.plotly_chart(fig_cust, use_container_width=True)

    with c2:
        display = top_customers[["customer", "orders", "rev_m", "aov"]].copy()
        display["revenue"] = display["rev_m"].apply(lambda x: naira(x * 1e6))
        display["aov"]     = display["aov"].apply(naira)
        display = display.rename(columns={"customer": "Customer", "orders": "Orders",
                                           "revenue": "Revenue", "aov": "AOV"})
        st.dataframe(display[["Customer", "Orders", "Revenue", "AOV"]],
                     use_container_width=True, hide_index=True)
