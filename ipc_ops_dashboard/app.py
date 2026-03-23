"""
IPC Operations Dashboard — DAASH & GoSource
Day-to-day operational visibility: orders, revenue, wallet balances, active brands.
"""

import os
import streamlit as st
import plotly.graph_objects as go
import pandas as pd

from utils.db import run_query
from utils.fmt import naira, naira_full, pct, count
from utils.styles import (inject_css, page_header, section_title,
                           CHART_LAYOUT, COLOR_DAASH, COLOR_GOSOURCE)

st.set_page_config(
    page_title="Operations · IPC",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Auth ──
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "ipc2026")

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if not st.session_state.authenticated:
    st.markdown(
        """
        <style>
            [data-testid="stSidebar"] { display: none; }
            header { display: none; }
        </style>
        <div style="max-width:380px;margin:100px auto;text-align:center;">
            <div style="font-size:48px;margin-bottom:12px;">📊</div>
            <div style="font-size:22px;font-weight:700;color:#0F172A;margin-bottom:4px;">
                Operations Dashboard
            </div>
            <div style="font-size:13px;color:#64748B;margin-bottom:32px;">
                IPC Analytics
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    col1, col2, col3 = st.columns([1.2, 1, 1.2])
    with col2:
        pwd = st.text_input("Password", type="password", placeholder="Enter password", label_visibility="collapsed")
        if st.button("Sign in", use_container_width=True, type="primary"):
            if pwd == DASHBOARD_PASSWORD:
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Incorrect password")
    st.stop()

inject_css()


# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## 📊 Operations")
    st.markdown(
        '<div style="font-size:12px;color:#94A3B8;margin-bottom:16px;">'
        'Orders, revenue, wallets & brand activity</div>',
        unsafe_allow_html=True,
    )
    st.markdown("---")
    service = st.selectbox("Service Line", ["DAASH", "GoSource"], index=0)

    if service == "DAASH":
        channel = st.selectbox("Channel", ["All", "Website", "POS"], index=0)
    else:
        channel = "All"

    st.markdown("---")
    st.markdown(
        '<div style="font-size:11px;color:#475569;">Data refreshes every 6 hours</div>',
        unsafe_allow_html=True,
    )

brand_color = COLOR_DAASH if service == "DAASH" else COLOR_GOSOURCE
page_header(
    "Operations Dashboard",
    f"{service} · Orders, revenue & brand activity",
    color=brand_color,
)


# ═══════════════════════════════════════════════════════════════════════════════
# LOAD DATA — DAASH
# ═══════════════════════════════════════════════════════════════════════════════

if service == "DAASH":
    channel_filter = ""
    if channel == "Website":
        channel_filter = "AND o.\"channel\" = 'website'"
    elif channel == "POS":
        channel_filter = "AND o.\"channel\" = 'pos'"

    # Monthly summary
    monthly = run_query(f"""
        SELECT
            DATE_TRUNC('month', o."createdAt"::date)::date AS month,
            COUNT(*) AS order_count,
            SUM(o."totalPrice"::numeric) AS revenue,
            SUM(o."serviceCharge"::numeric) AS service_charge
        FROM raw_dash.orders o
        WHERE LOWER(o."status") = 'delivered'
          {channel_filter}
        GROUP BY DATE_TRUNC('month', o."createdAt"::date)
        ORDER BY month
    """)

    # Current vs last month
    current_month = run_query(f"""
        SELECT
            COUNT(*) AS orders,
            SUM("totalPrice"::numeric) AS revenue,
            SUM("serviceCharge"::numeric) AS service_charge
        FROM raw_dash.orders
        WHERE LOWER(status) = 'delivered'
          AND DATE_TRUNC('month', "createdAt"::date) = DATE_TRUNC('month', CURRENT_DATE)
          {channel_filter}
    """)

    last_month = run_query(f"""
        SELECT
            COUNT(*) AS orders,
            SUM("totalPrice"::numeric) AS revenue,
            SUM("serviceCharge"::numeric) AS service_charge
        FROM raw_dash.orders
        WHERE LOWER(status) = 'delivered'
          AND DATE_TRUNC('month', "createdAt"::date) = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
          {channel_filter}
    """)

    # Payment method breakdown (current year)
    payment_methods = run_query(f"""
        SELECT
            CASE
                WHEN LOWER("paymentMethod") IN ('transfer', 'bank transfer') THEN 'Bank Transfer'
                WHEN LOWER("paymentMethod") IN ('card') THEN 'Card'
                WHEN LOWER("paymentMethod") IN ('cash') THEN 'Cash'
                ELSE INITCAP("paymentMethod")
            END AS method,
            COUNT(*) AS cnt
        FROM raw_dash.orders
        WHERE LOWER(status) = 'delivered'
          {channel_filter}
        GROUP BY 1
        ORDER BY cnt DESC
    """)

    # Wallet balances
    wallets = run_query("""
        SELECT
            c."businessName" AS customer,
            COALESCE(b."name", 'main') AS branch,
            w.balance::numeric AS balance
        FROM raw_dash.wallets w
        JOIN raw_dash.customers c ON c._id = w.customer
        LEFT JOIN raw_dash.branches b ON b._id = w.branch
        WHERE w.balance::numeric > 0
        ORDER BY w.balance::numeric DESC
    """)

    # Active brands
    active_brands = run_query(f"""
        WITH brand_activity AS (
            SELECT
                c."businessName" AS customer,
                MAX(o."createdAt"::date) AS last_order_date,
                COUNT(*) FILTER (
                    WHERE o."createdAt"::date >= CURRENT_DATE - 30
                ) AS orders_last_month,
                COUNT(*) FILTER (
                    WHERE o."createdAt"::date >= CURRENT_DATE - 180
                ) AS orders_last_6_months
            FROM raw_dash.orders o
            JOIN raw_dash.customers c ON c._id = o.customer
            WHERE LOWER(o.status) = 'delivered'
              AND c."businessName" IS NOT NULL
              AND TRIM(c."businessName") != ''
              {channel_filter}
            GROUP BY c."businessName"
        )
        SELECT
            customer,
            CASE WHEN orders_last_month > 0 THEN 'Yes' ELSE 'No' END AS last_month,
            CASE WHEN orders_last_6_months > 0 THEN 'Yes' ELSE 'No' END AS last_6_months,
            last_order_date
        FROM brand_activity
        ORDER BY last_order_date DESC
    """)

    # Conversion rate (website visits vs orders)
    conversion = run_query("""
        WITH visits AS (
            SELECT
                c."businessName" AS customer,
                COUNT(*) AS visit_count
            FROM raw_dash.websitevisits v
            JOIN raw_dash.customers c ON c._id = v."customer"
            WHERE v."createdAt"::date >= CURRENT_DATE - 30
              AND c."businessName" IS NOT NULL
            GROUP BY c."businessName"
        ),
        orders AS (
            SELECT
                c."businessName" AS customer,
                COUNT(*) AS order_count
            FROM raw_dash.orders o
            JOIN raw_dash.customers c ON c._id = o.customer
            WHERE LOWER(o.status) = 'delivered'
              AND o."createdAt"::date >= CURRENT_DATE - 30
              AND o."channel" = 'website'
            GROUP BY c."businessName"
        )
        SELECT
            v.customer,
            v.visit_count,
            COALESCE(ord.order_count, 0) AS order_count,
            CASE WHEN v.visit_count > 0
                 THEN ROUND(COALESCE(ord.order_count, 0)::numeric / v.visit_count * 100, 1)
                 ELSE 0
            END AS conversion_pct
        FROM visits v
        LEFT JOIN orders ord ON ord.customer = v.customer
        WHERE v.visit_count > 10
        ORDER BY v.visit_count DESC
    """)

    # ── KPIs ──
    section_title("KEY METRICS")
    cur_orders = int(current_month.iloc[0]["orders"]) if not current_month.empty else 0
    cur_revenue = float(current_month.iloc[0]["revenue"] or 0) if not current_month.empty else 0
    cur_sc = float(current_month.iloc[0]["service_charge"] or 0) if not current_month.empty else 0
    last_orders = int(last_month.iloc[0]["orders"]) if not last_month.empty else 0
    last_revenue = float(last_month.iloc[0]["revenue"] or 0) if not last_month.empty else 0

    mom_orders = round((cur_orders - last_orders) / last_orders * 100, 1) if last_orders > 0 else 0
    mom_revenue = round((cur_revenue - last_revenue) / last_revenue * 100, 1) if last_revenue > 0 else 0

    active_count = len(active_brands[active_brands["last_month"] == "Yes"]) if not active_brands.empty else 0

    # All-time totals
    total_orders = int(monthly["order_count"].sum()) if not monthly.empty else 0
    total_revenue = float(monthly["revenue"].sum()) if not monthly.empty else 0
    total_sc = float(monthly["service_charge"].sum()) if not monthly.empty else 0

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Orders (This Month)", f"{cur_orders:,}",
              delta=f"{mom_orders:+.1f}% vs last month" if last_orders > 0 else None)
    k2.metric("Revenue (This Month)", naira(cur_revenue),
              delta=f"{mom_revenue:+.1f}% vs last month" if last_revenue > 0 else None)
    k3.metric("Total Orders (All Time)", f"{total_orders:,}")
    k4.metric("Total Service Charge", naira(total_sc))
    k5.metric("Active Brands", f"{active_count}",
              help="Brands with at least 1 delivered order in the last 30 days")

    st.markdown("")

    # ── Order Volume by Month ──
    section_title("ORDER VOLUME BY MONTH")
    if not monthly.empty:
        monthly["month_label"] = pd.to_datetime(monthly["month"]).dt.strftime("%b %Y")
        fig_vol = go.Figure(go.Bar(
            x=monthly["month_label"],
            y=monthly["order_count"],
            marker_color=COLOR_DAASH,
            text=monthly["order_count"].apply(lambda x: f"{x:,}"),
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>%{y:,} orders<extra></extra>",
        ))
        vol_layout = {k: v for k, v in CHART_LAYOUT.items() if k not in ("margin",)}
        fig_vol.update_layout(**vol_layout, height=380,
                              margin=dict(t=30, b=10, l=10, r=10),
                              yaxis_title="Orders")
        st.plotly_chart(fig_vol, use_container_width=True)

    st.markdown("---")

    # ── Payment Method + Service Charge by Month side by side ──
    c1, c2 = st.columns(2)

    with c1:
        section_title("PAYMENT METHOD BREAKDOWN")
        if not payment_methods.empty:
            fig_pay = go.Figure(go.Pie(
                labels=payment_methods["method"],
                values=payment_methods["cnt"],
                hole=0.5,
                textinfo="label+percent",
                textposition="outside",
                marker=dict(colors=["#8B0000", "#DC2626", "#F87171", "#FCA5A5", "#FECACA", "#E5E7EB"]),
                hovertemplate="<b>%{label}</b><br>%{value:,} orders (%{percent})<extra></extra>",
            ))
            pay_layout = {k: v for k, v in CHART_LAYOUT.items() if k not in ("margin", "xaxis", "yaxis")}
            fig_pay.update_layout(**pay_layout, height=350,
                                  margin=dict(t=10, b=10, l=10, r=10),
                                  showlegend=True)
            st.plotly_chart(fig_pay, use_container_width=True)

    with c2:
        section_title("SERVICE CHARGE BY MONTH")
        if not monthly.empty:
            fig_sc = go.Figure(go.Bar(
                x=monthly["month_label"],
                y=monthly["service_charge"],
                marker_color="#DC2626",
                text=monthly["service_charge"].apply(lambda x: naira(x)),
                textposition="outside",
                hovertemplate="<b>%{x}</b><br>%{text}<extra></extra>",
            ))
            sc_layout = {k: v for k, v in CHART_LAYOUT.items() if k not in ("margin",)}
            fig_sc.update_layout(**sc_layout, height=350,
                                 margin=dict(t=30, b=10, l=10, r=10),
                                 yaxis_title="Service Charge (₦)")
            st.plotly_chart(fig_sc, use_container_width=True)

    st.markdown("---")

    # ── Website Conversion Rate ──
    if not conversion.empty:
        section_title("WEBSITE CONVERSION RATE (LAST 30 DAYS)")
        st.markdown(
            '<div style="font-size:12px;color:#64748B;margin-bottom:12px;">'
            'How many website visitors actually place an order. Only brands with 10+ visits shown.</div>',
            unsafe_allow_html=True,
        )
        conv_tbl = conversion.copy()
        conv_tbl.columns = ["Brand", "Visits", "Web Orders", "Conv %"]
        conv_tbl["Visits"] = conv_tbl["Visits"].apply(lambda x: f"{int(x):,}")
        conv_tbl["Web Orders"] = conv_tbl["Web Orders"].apply(lambda x: f"{int(x):,}")
        conv_tbl["Conv %"] = conv_tbl["Conv %"].apply(lambda x: f"{float(x):.1f}%")
        st.dataframe(conv_tbl, use_container_width=True,
                     height=min(400, 40 + len(conv_tbl) * 35))
        st.markdown("---")

    # ── Wallet Balances ──
    section_title("CUSTOMER WALLET BALANCES")
    if not wallets.empty:
        w_tbl = wallets.copy()
        w_tbl.columns = ["Customer", "Branch", "Balance"]
        w_tbl["Balance"] = w_tbl["Balance"].apply(naira_full)
        st.dataframe(w_tbl, use_container_width=True,
                     height=min(500, 40 + len(w_tbl) * 35))
    else:
        st.info("No wallet balances found.")

    st.markdown("---")

    # ── Active Brands ──
    section_title("ACTIVE BRANDS (LAST 1 MONTH & LAST 6 MONTHS)")
    if not active_brands.empty:
        ab_tbl = active_brands.copy()
        ab_tbl.columns = ["Customer", "Last Month", "Last 6 Months", "Last Order Date"]
        ab_tbl["Last Order Date"] = pd.to_datetime(ab_tbl["Last Order Date"]).dt.strftime("%d-%m-%Y")
        st.dataframe(ab_tbl, use_container_width=True,
                     height=min(500, 40 + len(ab_tbl) * 35))
    else:
        st.info("No brand activity data.")


# ═══════════════════════════════════════════════════════════════════════════════
# LOAD DATA — GOSOURCE
# ═══════════════════════════════════════════════════════════════════════════════

else:
    # Monthly summary (deduplicated at order level — gosource has one row per product)
    monthly = run_query("""
        WITH deduped AS (
            SELECT DISTINCT ON ("_id")
                "_id",
                "createdAt"::date AS order_date,
                "totalPrice"::numeric AS total,
                COALESCE("serviceCharge"::numeric, 0) AS service_charge
            FROM raw_gosource.orders
            WHERE LOWER(status) = 'delivered'
            ORDER BY "_id", "createdAt" DESC
        )
        SELECT
            DATE_TRUNC('month', order_date)::date AS month,
            COUNT(*) AS order_count,
            SUM(total) AS revenue,
            SUM(service_charge) AS service_charge
        FROM deduped
        GROUP BY 1
        ORDER BY 1
    """)

    # Current vs last month
    current_month = run_query("""
        WITH deduped AS (
            SELECT DISTINCT ON ("_id")
                "_id",
                "totalPrice"::numeric AS total,
                COALESCE("serviceCharge"::numeric, 0) AS service_charge
            FROM raw_gosource.orders
            WHERE LOWER(status) = 'delivered'
              AND DATE_TRUNC('month', "createdAt"::date) = DATE_TRUNC('month', CURRENT_DATE)
            ORDER BY "_id", "createdAt" DESC
        )
        SELECT COUNT(*) AS orders, SUM(total) AS revenue, SUM(service_charge) AS service_charge
        FROM deduped
    """)

    last_month = run_query("""
        WITH deduped AS (
            SELECT DISTINCT ON ("_id")
                "_id",
                "totalPrice"::numeric AS total,
                COALESCE("serviceCharge"::numeric, 0) AS service_charge
            FROM raw_gosource.orders
            WHERE LOWER(status) = 'delivered'
              AND DATE_TRUNC('month', "createdAt"::date) = DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month'
            ORDER BY "_id", "createdAt" DESC
        )
        SELECT COUNT(*) AS orders, SUM(total) AS revenue, SUM(service_charge) AS service_charge
        FROM deduped
    """)

    # Top products
    top_products = run_query("""
        SELECT
            "product.name" AS product_name,
            COUNT(*) AS line_items,
            SUM(CASE WHEN quantity ~ '^[0-9.]+$' THEN quantity::numeric ELSE 1 END) AS quantity,
            SUM("totalPrice"::numeric) AS total_amount
        FROM raw_gosource.orders
        WHERE LOWER(status) = 'delivered'
          AND "product.name" IS NOT NULL
        GROUP BY "product.name"
        ORDER BY line_items DESC
        LIMIT 20
    """)

    # Customer order history
    customer_history = run_query("""
        WITH deduped AS (
            SELECT DISTINCT ON ("_id")
                "_id",
                "businessName",
                "createdAt"::date AS order_date,
                "totalPrice"::numeric AS total
            FROM raw_gosource.orders
            WHERE LOWER(status) = 'delivered'
              AND "businessName" IS NOT NULL
            ORDER BY "_id", "createdAt" DESC
        )
        SELECT
            "businessName" AS customer,
            COUNT(*) FILTER (WHERE order_date >= CURRENT_DATE - 30) AS orders_1m,
            COUNT(*) FILTER (WHERE order_date >= CURRENT_DATE - 60 AND order_date < CURRENT_DATE - 30) AS orders_2m,
            COUNT(*) FILTER (WHERE order_date >= CURRENT_DATE - 90 AND order_date < CURRENT_DATE - 60) AS orders_3m,
            MAX(order_date) AS last_order_date,
            SUM(total) AS total_revenue
        FROM deduped
        GROUP BY "businessName"
        ORDER BY total_revenue DESC
    """)

    # ── KPIs ──
    section_title("KEY METRICS")
    cur_orders = int(current_month.iloc[0]["orders"]) if not current_month.empty else 0
    cur_revenue = float(current_month.iloc[0]["revenue"] or 0) if not current_month.empty else 0
    cur_sc = float(current_month.iloc[0]["service_charge"] or 0) if not current_month.empty else 0
    last_orders = int(last_month.iloc[0]["orders"]) if not last_month.empty else 0
    last_revenue = float(last_month.iloc[0]["revenue"] or 0) if not last_month.empty else 0

    mom_orders = round((cur_orders - last_orders) / last_orders * 100, 1) if last_orders > 0 else 0
    mom_revenue = round((cur_revenue - last_revenue) / last_revenue * 100, 1) if last_revenue > 0 else 0

    total_orders = int(monthly["order_count"].sum()) if not monthly.empty else 0
    total_revenue = float(monthly["revenue"].sum()) if not monthly.empty else 0
    total_sc = float(monthly["service_charge"].sum()) if not monthly.empty else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Orders (This Month)", f"{cur_orders:,}",
              delta=f"{mom_orders:+.1f}% vs last month" if last_orders > 0 else None)
    k2.metric("Revenue (This Month)", naira(cur_revenue),
              delta=f"{mom_revenue:+.1f}% vs last month" if last_revenue > 0 else None)
    k3.metric("Total Orders (All Time)", f"{total_orders:,}")
    k4.metric("Total Service Charge", naira(total_sc))

    st.markdown("")

    # ── Order Volume by Month ──
    section_title("ORDER VOLUME BY MONTH")
    if not monthly.empty:
        monthly["month_label"] = pd.to_datetime(monthly["month"]).dt.strftime("%b %Y")
        fig_vol = go.Figure(go.Bar(
            x=monthly["month_label"],
            y=monthly["order_count"],
            marker_color=COLOR_GOSOURCE,
            text=monthly["order_count"].apply(lambda x: f"{x:,}"),
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>%{y:,} orders<extra></extra>",
        ))
        vol_layout = {k: v for k, v in CHART_LAYOUT.items() if k not in ("margin",)}
        fig_vol.update_layout(**vol_layout, height=380,
                              margin=dict(t=30, b=10, l=10, r=10),
                              yaxis_title="Orders")
        st.plotly_chart(fig_vol, use_container_width=True)

    st.markdown("---")

    # ── Service Charge by Month ──
    section_title("SERVICE CHARGE BY MONTH")
    if not monthly.empty:
        fig_sc = go.Figure(go.Bar(
            x=monthly["month_label"],
            y=monthly["service_charge"],
            marker_color="#2D5A27",
            text=monthly["service_charge"].apply(lambda x: naira(x)),
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>%{text}<extra></extra>",
        ))
        sc_layout = {k: v for k, v in CHART_LAYOUT.items() if k not in ("margin",)}
        fig_sc.update_layout(**sc_layout, height=350,
                             margin=dict(t=30, b=10, l=10, r=10),
                             yaxis_title="Service Charge (₦)")
        st.plotly_chart(fig_sc, use_container_width=True)

    st.markdown("---")

    # ── Top Products ──
    section_title("TOP PERFORMING PRODUCTS")
    if not top_products.empty:
        tp = top_products[["product_name", "line_items", "total_amount"]].copy()
        tp.columns = ["Product", "Orders", "Total Amount"]
        tp["Orders"] = tp["Orders"].fillna(0).astype(int).apply(lambda x: f"{x:,}")
        tp["Total Amount"] = tp["Total Amount"].fillna(0).apply(lambda x: naira(float(x)))
        st.dataframe(tp, use_container_width=True,
                     height=min(500, 40 + len(tp) * 35))

    st.markdown("---")

    # ── Customer Order History ──
    section_title("CUSTOMER ORDER HISTORY")
    st.markdown(
        '<div style="font-size:12px;color:#64748B;margin-bottom:12px;">'
        'Order counts by month window. Declining counts signal churn risk.</div>',
        unsafe_allow_html=True,
    )
    if not customer_history.empty:
        ch = customer_history.copy()
        ch.columns = ["Customer", "Last 1 Month", "2 Months Ago", "3 Months Ago",
                       "Last Order Date", "Total Revenue"]
        ch["Last Order Date"] = pd.to_datetime(ch["Last Order Date"]).dt.strftime("%d-%m-%Y")
        ch["Total Revenue"] = ch["Total Revenue"].apply(naira)
        st.dataframe(ch, use_container_width=True,
                     height=min(500, 40 + len(ch) * 35))
