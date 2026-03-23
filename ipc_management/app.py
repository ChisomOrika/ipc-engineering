"""
IPC Management Intelligence Dashboard — Restricted
Revenue concentration, credit exposure, AOV trends, subscription lifecycle.
For management only — contains per-client revenue data.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np

from utils.db     import run_query
from utils.fmt    import naira, pct, count
from utils.styles import (inject_css, page_header, section_title, CHART_LAYOUT,
                           COLOR_DAASH, COLOR_GOSOURCE, COLOR_POSITIVE,
                           COLOR_NEGATIVE, COLOR_WARNING, COLOR_NEUTRAL,
                           COLOR_INDIGO)

st.set_page_config(
    page_title="Management Intel · IPC",
    page_icon="🔒",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css()


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def risk_badge(level):
    colors = {
        "High Risk":  (COLOR_NEGATIVE, "#FEF2F2"),
        "Medium Risk": (COLOR_WARNING, "#FFFBEB"),
        "Low Risk":   (COLOR_POSITIVE, "#F0FDF4"),
    }
    fg, bg = colors.get(level, (COLOR_NEUTRAL, "#F3F4F6"))
    return (f'<span style="background:{bg};color:{fg};padding:2px 8px;'
            f'border-radius:12px;font-size:11px;font-weight:700;'
            f'border:1px solid {fg}30;">{level}</span>')


def stat_card(label, value, subtitle="", color="#0F172A"):
    return (
        f'<div style="background:white;border:1px solid #E2E8F0;border-radius:12px;'
        f'padding:18px 22px;box-shadow:0 1px 4px rgba(0,0,0,0.06);">'
        f'<div style="font-size:11px;font-weight:600;text-transform:uppercase;'
        f'letter-spacing:0.7px;color:#64748B;">{label}</div>'
        f'<div style="font-size:24px;font-weight:700;color:{color};margin:6px 0 2px;">{value}</div>'
        f'{f"<div style=font-size:11px;color:#94A3B8;>{subtitle}</div>" if subtitle else ""}'
        f'</div>'
    )


# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## 🔒 Management Intel")
    st.markdown(
        '<div style="font-size:12px;color:#94A3B8;margin-bottom:16px;">'
        'Restricted dashboard — revenue & credit data</div>',
        unsafe_allow_html=True,
    )
    st.markdown("---")
    view = st.radio(
        "Section",
        ["Revenue Concentration", "Credit Exposure", "AOV Trends", "Subscription Lifecycle"],
        index=0,
    )
    st.markdown("---")
    st.markdown(
        '<div style="font-size:11px;color:#475569;margin-top:8px;">'
        '⏱ Data refreshes every 6 hours<br>'
        '🔒 Management access only</div>',
        unsafe_allow_html=True,
    )

page_header(
    "Management Intelligence",
    "Revenue concentration · Credit exposure · AOV trends · Subscriptions"
)


# ═══════════════════════════════════════════════════════════════════════════════
# 1. REVENUE CONCENTRATION
# ═══════════════════════════════════════════════════════════════════════════════
if view == "Revenue Concentration":
    rc = run_query("SELECT * FROM gold.fact_dash_revenue_concentration ORDER BY service_line, revenue_rank")

    if rc.empty:
        st.warning("No revenue concentration data. Run dbt models first.")
        st.stop()

    for svc in rc.service_line.unique():
        svc_data = rc[rc.service_line == svc].copy()
        color = COLOR_DAASH if svc == "DAASH" else COLOR_GOSOURCE

        section_title(f"{svc} REVENUE CONCENTRATION")

        total_rev = svc_data.lifetime_revenue.sum()
        total_rev_30d = svc_data.revenue_last_30d.sum()
        top1 = svc_data.iloc[0] if len(svc_data) > 0 else None
        top5_pct = svc_data.head(5).pct_of_total_revenue.sum() if len(svc_data) >= 5 else svc_data.pct_of_total_revenue.sum()

        # KPIs
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total Customers", count(len(svc_data)))
        k2.metric("Lifetime Revenue", naira(total_rev))
        k3.metric("Top Customer Share",
                   f"{top1.pct_of_total_revenue:.1f}%" if top1 is not None else "—",
                   help=f"{top1.business_name}" if top1 is not None else "")
        k4.metric("Top 5 Share", f"{top5_pct:.1f}%",
                   help="Combined % of total revenue from top 5 customers")

        # Concentration risk indicator
        if top1 is not None and top1.pct_of_total_revenue > 40:
            st.markdown(
                f'<div style="background:#FEF2F2;border:1px solid #FECACA;border-radius:10px;'
                f'padding:12px 20px;margin:8px 0 16px;">'
                f'<span style="font-weight:700;color:#991B1B;">⚠️ High Concentration Risk</span>'
                f'<span style="color:#7F1D1D;font-size:13px;margin-left:8px;">'
                f'{top1.business_name} accounts for {top1.pct_of_total_revenue:.1f}% of all {svc} revenue. '
                f'Losing this customer would be catastrophic.</span></div>',
                unsafe_allow_html=True,
            )

        c1, c2 = st.columns([1, 1])

        with c1:
            # Top 10 bar chart
            top10 = svc_data.head(10)
            fig_bar = go.Figure(go.Bar(
                x=top10.business_name,
                y=top10.pct_of_total_revenue,
                marker_color=[color if i == 0 else f"{color}99" if i < 3 else f"{color}55"
                              for i in range(len(top10))],
                hovertemplate="<b>%{x}</b><br>%{y:.1f}% of total revenue<extra></extra>",
            ))
            fig_bar.update_layout(
                **CHART_LAYOUT,
                title=dict(text="Top 10 Customers by Revenue Share", font=dict(size=14)),
                height=350,
                xaxis=dict(tickangle=-35, tickfont=dict(size=10)),
                yaxis=dict(title="% of Total Revenue"),
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        with c2:
            # Cumulative concentration curve (Lorenz-style)
            svc_data_sorted = svc_data.sort_values("lifetime_revenue", ascending=False)
            svc_data_sorted["customer_pct"] = np.arange(1, len(svc_data_sorted) + 1) / len(svc_data_sorted) * 100

            fig_lorenz = go.Figure()
            fig_lorenz.add_trace(go.Scatter(
                x=svc_data_sorted.customer_pct,
                y=svc_data_sorted.cumulative_revenue_pct,
                mode="lines+markers",
                marker=dict(size=5, color=color),
                line=dict(color=color, width=2),
                hovertemplate="Top %{x:.0f}% of customers<br>= %{y:.1f}% of revenue<extra></extra>",
                name="Actual",
            ))
            # Perfect equality line
            fig_lorenz.add_trace(go.Scatter(
                x=[0, 100], y=[0, 100],
                mode="lines", line=dict(dash="dot", color="#CBD5E1"),
                name="Equal Distribution",
            ))
            fig_lorenz.update_layout(
                **CHART_LAYOUT,
                title=dict(text="Revenue Concentration Curve", font=dict(size=14)),
                height=350,
                xaxis=dict(title="% of Customers (cumulative)", range=[0, 105]),
                yaxis=dict(title="% of Revenue (cumulative)", range=[0, 105]),
            )
            st.plotly_chart(fig_lorenz, use_container_width=True)

        # Full table
        with st.expander(f"📊 Full {svc} Revenue Ranking"):
            tbl = svc_data[["revenue_rank", "business_name", "lifetime_orders",
                            "lifetime_revenue", "orders_last_30d", "revenue_last_30d",
                            "pct_of_total_revenue", "cumulative_revenue_pct"]].copy()
            tbl.columns = ["Rank", "Customer", "Lifetime Orders", "Lifetime Revenue",
                           "Orders 30d", "Revenue 30d", "% of Total", "Cumulative %"]
            tbl["Lifetime Revenue"] = tbl["Lifetime Revenue"].apply(naira)
            tbl["Revenue 30d"] = tbl["Revenue 30d"].apply(naira)
            st.dataframe(tbl, use_container_width=True, height=min(500, 40 + len(tbl) * 35))

            _, dl = st.columns([5, 1])
            with dl:
                st.download_button(f"📥 {svc} CSV", tbl.to_csv(index=False),
                                   f"{svc.lower()}_revenue_concentration.csv", "text/csv",
                                   key=f"dl_rc_{svc}")

        st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. CREDIT EXPOSURE
# ═══════════════════════════════════════════════════════════════════════════════
elif view == "Credit Exposure":
    credit = run_query("SELECT * FROM gold.fact_gosource_credit_exposure ORDER BY total_outstanding DESC")

    if credit.empty:
        st.warning("No credit exposure data. Run dbt models first.")
        st.stop()

    section_title("GOSOURCE CREDIT EXPOSURE")

    total_outstanding = credit.total_outstanding.sum() if "total_outstanding" in credit.columns else 0
    total_overdue_90 = credit.overdue_90d.sum() if "overdue_90d" in credit.columns else 0
    high_risk_count = len(credit[credit.risk_level == "High Risk"]) if "risk_level" in credit.columns else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Customers on Credit", count(len(credit)))
    k2.metric("Total Outstanding", naira(total_outstanding),
              help="All unpaid delivered orders")
    k3.metric("Overdue (90d+)", naira(total_overdue_90),
              help="Unpaid orders older than 90 days — high risk")
    k4.metric("High Risk Customers", count(high_risk_count),
              help="Customers with significant overdue balances")

    # Risk alert
    if total_overdue_90 > 0:
        overdue_pct = total_overdue_90 / total_outstanding * 100 if total_outstanding > 0 else 0
        st.markdown(
            f'<div style="background:#FEF2F2;border:1px solid #FECACA;border-radius:10px;'
            f'padding:16px 20px;margin:8px 0 20px;">'
            f'<div style="font-weight:700;color:#991B1B;margin-bottom:6px;">'
            f'🚨 {naira(total_overdue_90)} is 90+ days overdue ({overdue_pct:.0f}% of outstanding)</div>'
            f'<div style="font-size:13px;color:#7F1D1D;">'
            f'This amount may require provisioning or collection action.</div></div>',
            unsafe_allow_html=True,
        )

    c1, c2 = st.columns([1, 1])

    with c1:
        # AR aging waterfall
        bucket_cols = []
        for col_name, label in [("bucket_0_30", "0-30d"), ("bucket_31_60", "31-60d"),
                                 ("bucket_61_90", "61-90d"), ("overdue_90d", "90d+")]:
            if col_name in credit.columns:
                bucket_cols.append((col_name, label))

        if bucket_cols:
            bucket_totals = [(label, credit[col].sum()) for col, label in bucket_cols]
            colors = [COLOR_POSITIVE, COLOR_WARNING, "#F97316", COLOR_NEGATIVE]

            fig_aging = go.Figure(go.Bar(
                x=[b[0] for b in bucket_totals],
                y=[b[1] for b in bucket_totals],
                marker_color=colors[:len(bucket_totals)],
                hovertemplate="<b>%{x}</b><br>%{customdata}<extra></extra>",
                customdata=[naira(b[1]) for b in bucket_totals],
            ))
            fig_aging.update_layout(
                **CHART_LAYOUT,
                title=dict(text="AR Aging Buckets (All Customers)", font=dict(size=14)),
                height=350,
                yaxis=dict(title="Outstanding Amount"),
            )
            st.plotly_chart(fig_aging, use_container_width=True)

    with c2:
        # Top 10 by total outstanding
        top10 = credit.head(10)
        if "total_outstanding" in top10.columns:
            fig_top = go.Figure(go.Bar(
                y=top10.business_name,
                x=top10.total_outstanding,
                orientation="h",
                marker_color=[COLOR_NEGATIVE if r.get("risk_level") == "High Risk"
                              else COLOR_WARNING if r.get("risk_level") == "Medium Risk"
                              else COLOR_POSITIVE
                              for _, r in top10.iterrows()],
                hovertemplate="<b>%{y}</b><br>%{customdata}<extra></extra>",
                customdata=[naira(v) for v in top10.total_outstanding],
            ))
            layout = {k: v for k, v in CHART_LAYOUT.items() if k not in ("margin", "yaxis", "xaxis")}
            fig_top.update_layout(
                **layout,
                title=dict(text="Top 10 by Outstanding Balance", font=dict(size=14)),
                height=350,
                margin=dict(t=40, b=10, l=140, r=10),
                yaxis=dict(autorange="reversed", gridcolor="#F1F5F9", tickfont=dict(size=11)),
                xaxis=dict(title="Outstanding Amount", showgrid=False, tickfont=dict(size=11)),
            )
            st.plotly_chart(fig_top, use_container_width=True)

    st.markdown("---")

    # Full table
    section_title("ALL CREDIT CUSTOMERS")
    disp_cols = {}
    for col, label in [("business_name", "Customer"), ("risk_level", "Risk Level"),
                        ("total_outstanding", "Total Outstanding"),
                        ("bucket_0_30", "0-30 Days"), ("bucket_31_60", "31-60 Days"),
                        ("bucket_61_90", "61-90 Days"), ("overdue_90d", "90+ Days"),
                        ("total_orders", "Orders"), ("paid_orders", "Paid Orders")]:
        if col in credit.columns:
            disp_cols[col] = label

    tbl = credit[list(disp_cols.keys())].copy()
    tbl.columns = list(disp_cols.values())

    for money_col in ["Total Outstanding", "0-30 Days", "31-60 Days", "61-90 Days", "90+ Days"]:
        if money_col in tbl.columns:
            tbl[money_col] = tbl[money_col].apply(lambda x: naira(x) if pd.notna(x) else "—")

    st.dataframe(
        tbl.style.apply(
            lambda row: [
                "background-color: #FEF2F2" if row.get("Risk Level") == "High Risk"
                else "background-color: #FFFBEB" if row.get("Risk Level") == "Medium Risk"
                else "background-color: #F0FDF4" if row.get("Risk Level") == "Low Risk"
                else ""
            ] * len(row),
            axis=1,
        ) if "Risk Level" in tbl.columns else tbl,
        use_container_width=True,
        height=min(600, 40 + len(tbl) * 35),
    )

    _, dl = st.columns([5, 1])
    with dl:
        st.download_button("📥 Download CSV", tbl.to_csv(index=False),
                           "gosource_credit_exposure.csv", "text/csv")


# ═══════════════════════════════════════════════════════════════════════════════
# 3. AOV TRENDS
# ═══════════════════════════════════════════════════════════════════════════════
elif view == "AOV Trends":
    aov = run_query("SELECT * FROM gold.fact_dash_aov_trend ORDER BY business_name, order_month")

    if aov.empty:
        st.warning("No AOV data. Run dbt models first.")
        st.stop()

    section_title("DAASH AVERAGE ORDER VALUE TRENDS")

    # Overall AOV trend
    monthly_aov = aov.groupby("order_month").agg(
        total_revenue=("monthly_revenue", "sum"),
        total_orders=("monthly_orders", "sum"),
    ).reset_index()
    monthly_aov["aov"] = monthly_aov.total_revenue / monthly_aov.total_orders
    monthly_aov["order_month"] = pd.to_datetime(monthly_aov.order_month)

    latest = monthly_aov.iloc[-1] if len(monthly_aov) > 0 else None
    prev = monthly_aov.iloc[-2] if len(monthly_aov) > 1 else None
    aov_change = None
    if latest is not None and prev is not None and prev.aov > 0:
        aov_change = (latest.aov - prev.aov) / prev.aov * 100

    k1, k2, k3 = st.columns(3)
    k1.metric("Current AOV", naira(latest.aov) if latest is not None else "—",
              help="Average order value this month (all restaurants)")
    if aov_change is not None:
        k2.metric("MoM Change", f"{aov_change:+.1f}%",
                  delta=f"{aov_change:+.1f}%",
                  help="Month-over-month change in AOV")
    else:
        k2.metric("MoM Change", "—")
    k3.metric("Restaurants Tracked", count(aov.business_name.nunique()))

    # Overall AOV line chart
    fig_aov = go.Figure()
    fig_aov.add_trace(go.Scatter(
        x=monthly_aov.order_month,
        y=monthly_aov.aov,
        mode="lines+markers",
        line=dict(color=COLOR_INDIGO, width=3),
        marker=dict(size=7, color=COLOR_INDIGO),
        hovertemplate="<b>%{x|%b %Y}</b><br>AOV: %{customdata}<extra></extra>",
        customdata=[naira(v) for v in monthly_aov.aov],
        name="Overall AOV",
    ))
    fig_aov.update_layout(
        **CHART_LAYOUT,
        title=dict(text="Overall AOV Trend (All Restaurants)", font=dict(size=14)),
        height=350,
        xaxis=dict(title=""),
        yaxis=dict(title="Average Order Value (₦)"),
    )
    st.plotly_chart(fig_aov, use_container_width=True)

    st.markdown("---")

    # Per-customer AOV
    section_title("PER-CUSTOMER AOV")
    customers = sorted(aov.business_name.unique())
    selected = st.multiselect("Select customers to compare", customers,
                              default=customers[:5] if len(customers) >= 5 else customers)

    if selected:
        filtered = aov[aov.business_name.isin(selected)].copy()
        filtered["order_month"] = pd.to_datetime(filtered.order_month)

        fig_multi = go.Figure()
        colors_list = px.colors.qualitative.Set2
        for i, name in enumerate(selected):
            cust_data = filtered[filtered.business_name == name].sort_values("order_month")
            if "aov" in cust_data.columns:
                y_col = "aov"
            elif "monthly_aov" in cust_data.columns:
                y_col = "monthly_aov"
            else:
                continue
            fig_multi.add_trace(go.Scatter(
                x=cust_data.order_month,
                y=cust_data[y_col],
                mode="lines+markers",
                name=name,
                line=dict(color=colors_list[i % len(colors_list)], width=2),
                marker=dict(size=5),
                hovertemplate=f"<b>{name}</b><br>" + "%{x|%b %Y}<br>AOV: %{y:,.0f}<extra></extra>",
            ))

        fig_multi.update_layout(
            **CHART_LAYOUT,
            height=400,
            xaxis=dict(title=""),
            yaxis=dict(title="AOV (₦)"),
        )
        st.plotly_chart(fig_multi, use_container_width=True)

    # AOV table - latest month per customer
    section_title("LATEST MONTH AOV BY CUSTOMER")
    aov["order_month_dt"] = pd.to_datetime(aov.order_month)
    latest_aov = aov.sort_values("order_month_dt").groupby("business_name").last().reset_index()

    disp = {}
    for col, label in [("business_name", "Customer"), ("order_month", "Month"),
                        ("monthly_orders", "Orders"), ("monthly_revenue", "Revenue")]:
        if col in latest_aov.columns:
            disp[col] = label

    aov_col = "aov" if "aov" in latest_aov.columns else "monthly_aov" if "monthly_aov" in latest_aov.columns else None
    if aov_col:
        disp[aov_col] = "AOV"
    mom_col = "aov_change_pct" if "aov_change_pct" in latest_aov.columns else "mom_change_pct" if "mom_change_pct" in latest_aov.columns else None
    if mom_col:
        disp[mom_col] = "MoM Change %"

    tbl = latest_aov[list(disp.keys())].copy()
    tbl.columns = list(disp.values())
    if "Revenue" in tbl.columns:
        tbl["Revenue"] = tbl["Revenue"].apply(naira)
    if "AOV" in tbl.columns:
        tbl = tbl.sort_values("AOV", ascending=False)
        tbl["AOV"] = tbl["AOV"].apply(naira)

    st.dataframe(tbl, use_container_width=True, height=min(500, 40 + len(tbl) * 35))

    _, dl = st.columns([5, 1])
    with dl:
        st.download_button("📥 Download CSV", tbl.to_csv(index=False),
                           "daash_aov_trends.csv", "text/csv")


# ═══════════════════════════════════════════════════════════════════════════════
# 4. SUBSCRIPTION LIFECYCLE
# ═══════════════════════════════════════════════════════════════════════════════
elif view == "Subscription Lifecycle":
    subs = run_query("SELECT * FROM gold.fact_dash_subscription_lifecycle ORDER BY lifetime_orders DESC")

    if subs.empty:
        st.warning("No subscription data. Run dbt models first.")
        st.stop()

    section_title("DAASH SUBSCRIPTION LIFECYCLE")

    # Status breakdown
    status_counts = subs.subscription_status.value_counts()
    total = len(subs)

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Restaurants", count(total))
    k2.metric("Active Paid", count(status_counts.get("Active Paid", 0)),
              help="Active subscription, not on trial")
    k3.metric("Active Trial", count(status_counts.get("Active Trial", 0)),
              help="Currently on trial period")
    k4.metric("Expired", count(status_counts.get("Expired", 0)),
              help="Subscription expired, not renewed")
    k5.metric("Never Subscribed", count(status_counts.get("Never Subscribed", 0)))

    # Risk flags
    churned_count = subs.high_value_churned.sum() if "high_value_churned" in subs.columns else 0
    no_renew_count = subs.active_no_autorenew.sum() if "active_no_autorenew" in subs.columns else 0

    if churned_count > 0 or no_renew_count > 0:
        st.markdown("")
        a1, a2 = st.columns(2)
        with a1:
            if churned_count > 0:
                churned = subs[subs.high_value_churned == True]
                lost_orders = churned.lifetime_orders.sum()
                st.markdown(
                    f'<div style="background:#FEF2F2;border:1px solid #FECACA;border-radius:10px;'
                    f'padding:16px 20px;">'
                    f'<div style="font-weight:700;color:#991B1B;margin-bottom:6px;">'
                    f'💔 {int(churned_count)} High-Value Churns</div>'
                    f'<div style="font-size:13px;color:#7F1D1D;">'
                    f'{count(int(lost_orders))} combined lifetime orders from restaurants '
                    f'with expired/cancelled subscriptions.</div></div>',
                    unsafe_allow_html=True,
                )
        with a2:
            if no_renew_count > 0:
                st.markdown(
                    f'<div style="background:#FFFBEB;border:1px solid #FDE68A;border-radius:10px;'
                    f'padding:16px 20px;">'
                    f'<div style="font-weight:700;color:#92400E;margin-bottom:6px;">'
                    f'🔔 {int(no_renew_count)} Active Without Auto-Renew</div>'
                    f'<div style="font-size:13px;color:#78350F;">'
                    f'Active subscriptions that won\'t renew automatically — churn risk.</div></div>',
                    unsafe_allow_html=True,
                )

    st.markdown("")
    c1, c2 = st.columns([1, 1])

    with c1:
        # Status distribution donut
        labels = status_counts.index.tolist()
        values = status_counts.values.tolist()
        status_colors = {
            "Active Paid": COLOR_POSITIVE,
            "Active Trial": "#3B82F6",
            "Expired": COLOR_NEGATIVE,
            "Cancelled": COLOR_WARNING,
            "Never Subscribed": COLOR_NEUTRAL,
        }
        fig_donut = go.Figure(go.Pie(
            labels=labels,
            values=values,
            marker=dict(colors=[status_colors.get(s, COLOR_NEUTRAL) for s in labels]),
            hole=0.6,
            textinfo="value+percent",
            textfont=dict(size=12, color="white"),
            hovertemplate="<b>%{label}</b><br>%{value} restaurants<br>%{percent}<extra></extra>",
        ))
        donut_layout = {k: v for k, v in CHART_LAYOUT.items() if k != "margin"}
        fig_donut.update_layout(
            **donut_layout,
            showlegend=True,
            height=350,
            margin=dict(t=10, b=10, l=10, r=10),
            annotations=[dict(text=f"<b>{total}</b><br>total",
                              x=0.5, y=0.5, font_size=16, showarrow=False,
                              font_color="#0F172A")],
        )
        st.plotly_chart(fig_donut, use_container_width=True)

    with c2:
        # Lifetime orders by subscription status
        fig_box = go.Figure()
        for status in ["Active Paid", "Active Trial", "Expired", "Cancelled", "Never Subscribed"]:
            subset = subs[subs.subscription_status == status]
            if not subset.empty:
                fig_box.add_trace(go.Box(
                    y=subset.lifetime_orders,
                    name=status,
                    marker_color=status_colors.get(status, COLOR_NEUTRAL),
                    hovertemplate="<b>%{x}</b><br>Orders: %{y:,}<extra></extra>",
                ))
        fig_box.update_layout(
            **CHART_LAYOUT,
            title=dict(text="Order Volume by Subscription Status", font=dict(size=14)),
            height=350,
            showlegend=False,
            yaxis=dict(title="Lifetime Orders"),
        )
        st.plotly_chart(fig_box, use_container_width=True)

    st.markdown("---")

    # Full table
    section_title("ALL RESTAURANTS — SUBSCRIPTION STATUS")

    disp_cols = {}
    for col, label in [("business_name", "Restaurant"), ("subscription_status", "Status"),
                        ("lifetime_orders", "Lifetime Orders"),
                        ("days_since_last_order", "Days Since Order"),
                        ("subscription_amount", "Sub Amount"),
                        ("plan_duration", "Plan Duration"),
                        ("auto_renew", "Auto Renew"),
                        ("high_value_churned", "High-Value Churn"),
                        ("active_no_autorenew", "No Auto-Renew Risk")]:
        if col in subs.columns:
            disp_cols[col] = label

    tbl = subs[list(disp_cols.keys())].copy()
    tbl.columns = list(disp_cols.values())

    if "Sub Amount" in tbl.columns:
        tbl["Sub Amount"] = tbl["Sub Amount"].apply(
            lambda x: naira(x) if pd.notna(x) and x > 0 else "—"
        )

    for bool_col in ["High-Value Churn", "No Auto-Renew Risk", "Auto Renew"]:
        if bool_col in tbl.columns:
            tbl[bool_col] = tbl[bool_col].apply(
                lambda x: "Yes" if x == True or str(x).lower() == "true" else "No"
            )

    status_filter = st.multiselect(
        "Filter by status",
        subs.subscription_status.unique().tolist(),
        default=subs.subscription_status.unique().tolist(),
    )
    tbl_filtered = tbl[tbl["Status"].isin(status_filter)]

    st.dataframe(
        tbl_filtered.style.apply(
            lambda row: [
                "background-color: #F0FDF4" if row["Status"] == "Active Paid"
                else "background-color: #EFF6FF" if row["Status"] == "Active Trial"
                else "background-color: #FEF2F2" if row["Status"] == "Expired"
                else "background-color: #FFFBEB" if row["Status"] == "Cancelled"
                else ""
            ] * len(row),
            axis=1,
        ),
        use_container_width=True,
        height=min(600, 40 + len(tbl_filtered) * 35),
    )

    _, dl = st.columns([5, 1])
    with dl:
        st.download_button("📥 Download CSV", tbl_filtered.to_csv(index=False),
                           "daash_subscription_lifecycle.csv", "text/csv")
