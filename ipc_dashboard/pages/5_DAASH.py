import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd

from utils.db      import run_query
from utils.fmt     import naira, pct, count
from utils.styles  import (inject_css, page_header, section_title,
                            CHART_LAYOUT, COLOR_DAASH, COLOR_POSITIVE,
                            COLOR_NEGATIVE, COLOR_NEUTRAL, COLOR_CASH)
from utils.periods import sidebar_filters

st.set_page_config(page_title="DAASH Analytics", page_icon="🍔", layout="wide")
inject_css()

start, end, prev_start, prev_end, period_label, _ = sidebar_filters()

page_header(
    "DAASH — Service Line Analytics",
    f"Food Delivery Marketplace · {period_label}"
)

# ─── Channels ─────────────────────────────────────────────────────────────────
# Direct channels: paystack transfer, card, cash
# Aggregator channels: Chowdeck, Glovo
AGGREGATOR_METHODS = ("'Chowdeck'", "'chowdeck'", "'Glovo'")
AGG_CLAUSE = f"LOWER(revenue_payment_method) IN ('chowdeck', 'glovo')"

# ─── Master KPI query ─────────────────────────────────────────────────────────
kpi = run_query(f"""
    WITH curr AS (
        SELECT
            COUNT(*)                                                        AS orders,
            SUM(revenue_amount)                                             AS revenue,
            SUM(revenue_delivery_fee_amount)                                AS delivery_fee,
            SUM(revenue_service_charge_amount)                              AS svc_charge,
            COUNT(*) FILTER (WHERE {AGG_CLAUSE})                           AS agg_orders,
            SUM(revenue_amount) FILTER (WHERE {AGG_CLAUSE})                AS agg_rev,
            COUNT(*) FILTER (WHERE NOT {AGG_CLAUSE})                       AS direct_orders,
            SUM(revenue_amount) FILTER (WHERE NOT {AGG_CLAUSE})            AS direct_rev
        FROM gold.fact_revenue
        WHERE service_line = 'DAASH'
          AND revenue_order_date BETWEEN '{start}' AND '{end}'
    ),
    prev AS (
        SELECT
            COUNT(*)            AS orders,
            SUM(revenue_amount) AS revenue
        FROM gold.fact_revenue
        WHERE service_line = 'DAASH'
          AND revenue_order_date BETWEEN '{prev_start}' AND '{prev_end}'
    ),
    gp AS (
        SELECT
            COALESCE(SUM(CASE WHEN profit_date BETWEEN '{start}' AND '{end}'
                         THEN profit_gross_profit_amount END), 0) AS curr_gp,
            COALESCE(SUM(CASE WHEN profit_date BETWEEN '{prev_start}' AND '{prev_end}'
                         THEN profit_gross_profit_amount END), 0) AS prev_gp
        FROM gold.fact_profitability
        WHERE service_line = 'DAASH'
          AND profit_date BETWEEN '{prev_start}' AND '{end}'
    )
    SELECT curr.*, prev.orders AS prev_orders, prev.revenue AS prev_revenue,
           gp.curr_gp, gp.prev_gp
    FROM curr, prev, gp
""")

# ─── Monthly trend ────────────────────────────────────────────────────────────
monthly = run_query(f"""
    SELECT
        revenue_month,
        TO_CHAR(revenue_month, 'Mon YY')                                    AS label,
        SUM(revenue_amount) / 1e6                                           AS rev_m,
        COUNT(*)                                                             AS orders,
        SUM(revenue_amount) / NULLIF(COUNT(*), 0)                           AS aov,
        SUM(revenue_amount) FILTER (WHERE {AGG_CLAUSE}) / 1e6              AS agg_rev_m,
        SUM(revenue_amount) FILTER (WHERE NOT {AGG_CLAUSE}) / 1e6          AS direct_rev_m
    FROM gold.fact_revenue
    WHERE service_line = 'DAASH'
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
    WHERE service_line = 'DAASH'
      AND profit_date BETWEEN '{start}' AND '{end}'
    GROUP BY profit_month
    ORDER BY profit_month
""")

# ─── Channel breakdown ────────────────────────────────────────────────────────
channel_data = run_query(f"""
    SELECT
        CASE
            WHEN LOWER(revenue_payment_method) IN ('chowdeck', 'glovo')
                THEN INITCAP(revenue_payment_method)
            WHEN LOWER(revenue_payment_method) IN ('transfer', 'bank transfer')
                THEN 'Transfer'
            WHEN LOWER(revenue_payment_method) IN ('card')
                THEN 'Card'
            WHEN LOWER(revenue_payment_method) IN ('cash')
                THEN 'Cash'
            ELSE 'Other'
        END                                                                  AS channel,
        COUNT(*)                                                             AS orders,
        SUM(revenue_amount) / 1e6                                           AS rev_m
    FROM gold.fact_revenue
    WHERE service_line = 'DAASH'
      AND revenue_order_date BETWEEN '{start}' AND '{end}'
    GROUP BY 1
    ORDER BY rev_m DESC
""")

# ─── Top customers ────────────────────────────────────────────────────────────
top_customers = run_query(f"""
    SELECT
        COALESCE(NULLIF(TRIM(revenue_customer_name), ''), 'Unknown')        AS customer,
        COUNT(*)                                                             AS orders,
        SUM(revenue_amount) / 1e6                                           AS rev_m,
        SUM(revenue_amount) / NULLIF(COUNT(*), 0)                           AS aov
    FROM gold.fact_revenue
    WHERE service_line = 'DAASH'
      AND revenue_order_date BETWEEN '{start}' AND '{end}'
    GROUP BY revenue_customer_name
    ORDER BY rev_m DESC
    LIMIT 15
""")

# ─── Scalars ─────────────────────────────────────────────────────────────────
def _v(df, col, default=0):
    if df.empty or df.iloc[0][col] is None: return float(default)
    return float(df.iloc[0][col])

def _delta(curr, prev):
    return ((curr - prev) / prev * 100) if prev and prev > 0 else None

orders        = int(_v(kpi, "orders"))
prev_orders   = int(_v(kpi, "prev_orders"))
revenue       = _v(kpi, "revenue")
prev_revenue  = _v(kpi, "prev_revenue")
delivery_fee  = _v(kpi, "delivery_fee")
curr_gp       = _v(kpi, "curr_gp")
prev_gp       = _v(kpi, "prev_gp")
agg_orders    = int(_v(kpi, "agg_orders"))
agg_rev       = _v(kpi, "agg_rev")
direct_orders = int(_v(kpi, "direct_orders"))
direct_rev    = _v(kpi, "direct_rev")
aov           = revenue / orders if orders > 0 else 0
prev_aov      = prev_revenue / prev_orders if prev_orders > 0 else 0
gp_margin     = (curr_gp / revenue * 100) if revenue > 0 else 0

# ─── KPIs ────────────────────────────────────────────────────────────────────
section_title("KEY METRICS")
c1, c2, c3, c4, c5, c6 = st.columns(6)

c1.metric("🍔 TOTAL ORDERS", count(orders),
          delta=f"{'+' if _delta(orders,prev_orders) or 0 >= 0 else ''}{_delta(orders,prev_orders):.1f}% vs prev" if _delta(orders,prev_orders) else None,
          help="Delivered DAASH orders in period. Includes direct + aggregator channels.")
c2.metric("💰 REVENUE", naira(revenue),
          delta=f"{'+' if (_delta(revenue,prev_revenue) or 0) >= 0 else ''}{_delta(revenue,prev_revenue):.1f}% vs prev" if _delta(revenue,prev_revenue) else None,
          help="Total order value for delivered DAASH orders. Source: gold.fact_revenue")
c3.metric("📊 PLATFORM FEE (GP)", naira(curr_gp),
          delta=f"{'+' if (_delta(curr_gp,prev_gp) or 0) >= 0 else ''}{_delta(curr_gp,prev_gp):.1f}% vs prev" if _delta(curr_gp,prev_gp) else None,
          help="IPC's platform fee (service charge) per order from bv_dash_revenueledgers credits. This is DAASH's gross profit — restaurant bears food cost.")
c4.metric("🎯 AVG ORDER VALUE", naira(aov),
          delta=f"{'+' if (_delta(aov,prev_aov) or 0) >= 0 else ''}{_delta(aov,prev_aov):.1f}% vs prev" if _delta(aov,prev_aov) else None,
          help="Revenue ÷ Orders for the period.")
c5.metric("🚚 DELIVERY FEE COLLECTED", naira(delivery_fee),
          help="Total delivery fees charged to customers in the period. Source: revenue_delivery_fee_amount")
c6.metric("📈 GP MARGIN", pct(gp_margin),
          help=f"Platform Fee ÷ Revenue × 100. Platform fee only applies to ~{int(curr_gp/revenue*100*10 if revenue>0 else 0)/10}% of orders.")

st.markdown("")

# ─── Channel split row ────────────────────────────────────────────────────────
section_title("CHANNEL BREAKDOWN")
ca, cb, cc = st.columns(3)
with ca:
    st.markdown(f"""
    <div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;padding:18px 22px;box-shadow:0 1px 4px rgba(0,0,0,0.06);">
        <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.7px;color:#64748B;">🍔 Direct Orders</div>
        <div style="font-size:24px;font-weight:700;color:#0F172A;margin:6px 0 2px;">{count(direct_orders)}</div>
        <div style="font-size:12px;color:#64748B;">{naira(direct_rev)} revenue</div>
        <div style="font-size:11px;color:#94A3B8;margin-top:3px;">Transfer · Card · Cash</div>
    </div>""", unsafe_allow_html=True)
with cb:
    st.markdown(f"""
    <div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;padding:18px 22px;box-shadow:0 1px 4px rgba(0,0,0,0.06);">
        <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.7px;color:#64748B;">📱 Aggregator Orders</div>
        <div style="font-size:24px;font-weight:700;color:#0F172A;margin:6px 0 2px;">{count(agg_orders)}</div>
        <div style="font-size:12px;color:#64748B;">{naira(agg_rev)} revenue</div>
        <div style="font-size:11px;color:#94A3B8;margin-top:3px;">Chowdeck · Glovo</div>
    </div>""", unsafe_allow_html=True)
with cc:
    agg_pct = (agg_orders / orders * 100) if orders > 0 else 0
    st.markdown(f"""
    <div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;padding:18px 22px;box-shadow:0 1px 4px rgba(0,0,0,0.06);">
        <div style="font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.7px;color:#64748B;">📊 Aggregator Share</div>
        <div style="font-size:24px;font-weight:700;color:#0F172A;margin:6px 0 2px;">{agg_pct:.1f}%</div>
        <div style="font-size:12px;color:#64748B;">of total orders via 3rd-party</div>
        <div style="font-size:11px;color:#94A3B8;margin-top:3px;">Direct: {100-agg_pct:.1f}%</div>
    </div>""", unsafe_allow_html=True)

st.markdown("")

# ─── Monthly revenue + channel split ─────────────────────────────────────────
left, right = st.columns([2, 1])

with left:
    section_title("MONTHLY REVENUE — DIRECT vs AGGREGATOR")
    if not monthly.empty:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=monthly["label"], y=monthly["direct_rev_m"],
            name="Direct", marker_color=COLOR_DAASH, opacity=0.9,
        ))
        fig.add_trace(go.Bar(
            x=monthly["label"], y=monthly["agg_rev_m"],
            name="Aggregator", marker_color="#8B5CF6", opacity=0.9,
        ))
        if not monthly_gp.empty:
            fig.add_trace(go.Scatter(
                x=monthly_gp["label"], y=monthly_gp["gp_m"],
                name="Platform Fee (GP)", mode="lines+markers",
                line=dict(color=COLOR_POSITIVE, width=2),
                marker=dict(size=6), yaxis="y2",
            ))
        fig.update_layout(
            **CHART_LAYOUT,
            barmode="stack", height=320,
            yaxis_title="Revenue (₦M)", yaxis_gridcolor="#F1F5F9",
            yaxis2=dict(title="Platform Fee (₦M)", overlaying="y", side="right",
                        showgrid=False, tickfont=dict(size=10)),
        )
        st.plotly_chart(fig, use_container_width=True)

with right:
    section_title("CHANNEL MIX")
    if not channel_data.empty:
        channel_colors = {
            "Transfer": "#2563EB", "Card": "#0891B2", "Cash": "#64748B",
            "Chowdeck": "#F59E0B", "Glovo": "#EF4444", "Other": "#CBD5E1",
        }
        fig_pie = go.Figure(go.Pie(
            labels=channel_data["channel"],
            values=channel_data["rev_m"],
            hole=0.5,
            marker_colors=[channel_colors.get(c, "#CBD5E1") for c in channel_data["channel"]],
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
        name="Orders", marker_color=COLOR_DAASH, opacity=0.7,
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
        fig_cust = go.Figure(go.Bar(
            x=top_customers["rev_m"],
            y=top_customers["customer"],
            orientation="h",
            marker_color=COLOR_DAASH,
            text=[naira(v * 1e6) for v in top_customers["rev_m"]],
            textposition="outside",
        ))
        fig_cust.update_layout(
            **CHART_LAYOUT,
            height=420,
            xaxis_title="Revenue (₦M)",
            yaxis_autorange="reversed", yaxis_tickfont=dict(size=11),
            showlegend=False,
        )
        st.plotly_chart(fig_cust, use_container_width=True)

    with c2:
        display = top_customers[["customer", "orders", "rev_m", "aov"]].copy()
        display["revenue"]  = display["rev_m"].apply(lambda x: naira(x * 1e6))
        display["aov"]      = display["aov"].apply(naira)
        display = display.rename(columns={"customer": "Customer", "orders": "Orders", "revenue": "Revenue", "aov": "AOV"})
        st.dataframe(display[["Customer", "Orders", "Revenue", "AOV"]], use_container_width=True, hide_index=True)
