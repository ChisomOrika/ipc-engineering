"""
IPC Growth & Retention — Strategic
Weekly growth trends, retention cohorts, churn detection.
Supports both DAASH and GoSource service lines.
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np

from utils.db     import run_query
from utils.fmt    import naira, count
from utils.styles import (inject_css, page_header, section_title,
                           CHART_LAYOUT, COLOR_HEALTHY, COLOR_AT_RISK,
                           COLOR_CRITICAL, COLOR_NEUTRAL,
                           COLOR_DAASH, COLOR_GOSOURCE)

st.set_page_config(
    page_title="Customer Growth · IPC",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css()

with st.sidebar:
    st.markdown("## 💊 Customer Growth")
    st.markdown(
        '<div style="font-size:12px;color:#94A3B8;margin-bottom:16px;">'
        'Growth, retention & churn tracking</div>',
        unsafe_allow_html=True,
    )
    st.markdown("---")
    service = st.selectbox("Service Line", ["DAASH", "GoSource"], index=0)
    st.markdown("---")
    st.markdown(
        '<div style="font-size:11px;color:#475569;">Data refreshes every 6 hours</div>',
        unsafe_allow_html=True,
    )

brand_color = COLOR_DAASH if service == "DAASH" else COLOR_GOSOURCE
is_daash = service == "DAASH"

page_header(
    "Customer Growth",
    f"{service} · Weekly growth, cohort retention & churn tracking",
    color=brand_color,
)


# ═══════════════════════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════════════════════

if is_daash:
    growth   = run_query("SELECT * FROM gold.fact_dash_growth_summary ORDER BY week_start")
    cohorts  = run_query("SELECT * FROM gold.fact_dash_retention_cohorts ORDER BY cohort_month")
    churn    = run_query("SELECT * FROM gold.fact_dash_churn_weekly ORDER BY revenue_last_week DESC NULLS LAST")
else:
    growth   = run_query("SELECT * FROM gold.fact_gosource_growth_summary ORDER BY week_start")
    cohorts  = run_query("SELECT * FROM gold.fact_gosource_retention_cohorts ORDER BY cohort_month")
    churn    = run_query("SELECT * FROM gold.fact_gosource_churn_weekly ORDER BY revenue_last_week DESC NULLS LAST")

if growth.empty:
    st.warning("No growth data available. Run dbt models first.")
    st.stop()


# ═══════════════════════════════════════════════════════════════════════════════
# 1. THIS WEEK'S SNAPSHOT
# ═══════════════════════════════════════════════════════════════════════════════

latest = growth.iloc[-1]
prev   = growth.iloc[-2] if len(growth) > 1 else latest

# Show date range
week_start_str = pd.to_datetime(latest.week_start).strftime("%b %d")
week_end_str = pd.to_datetime(latest.week_end).strftime("%b %d, %Y") if "week_end" in growth.columns else ""
section_title(f"THIS WEEK · {week_start_str} – {week_end_str} (Sun – Sat)")

churned_this_week = churn[churn.weekly_status == "Churned"]
reactivated_this_week = churn[churn.weekly_status == "Reactivated"]

if is_daash:
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k6.metric("Web Adoption", f"{latest.web_adoption_pct:.0f}%",
              help="% of active brands with website orders this week")
else:
    k1, k2, k3, k4, k5 = st.columns(5)

k1.metric("Active Brands", count(latest.active_brands),
          delta=f"{int(latest.active_brands - prev.active_brands):+d}" if len(growth) > 1 else None)
k2.metric("New This Week", count(latest.new_brands))
k3.metric("Churned", count(len(churned_this_week)),
          delta=None, delta_color="inverse")
k4.metric("Reactivated", count(len(reactivated_this_week)))
k5.metric("Net Growth", f"{int(latest.net_growth):+d}",
          delta_color="normal")

st.markdown("")

# New brands this week — name them
new_this_week = churn[(churn.weekly_status == "Active") & (churn.orders_last_week == 0)]
if not new_this_week.empty:
    st.markdown(
        f'<div style="background:#F0FDF4;border:1px solid #BBF7D0;border-radius:12px;'
        f'padding:16px 22px;margin-bottom:10px;">'
        f'<div style="font-weight:700;color:#166534;font-size:14px;margin-bottom:6px;">'
        f'New brands this week</div>'
        f'<div style="font-size:13px;color:#166534DD;">'
        + '<br>'.join(f"&bull; <b>{r.business_name}</b> — {int(r.orders_this_week)} orders"
                      for _, r in new_this_week.iterrows())
        + '</div></div>',
        unsafe_allow_html=True,
    )

# Churned brands this week — name them
if not churned_this_week.empty:
    st.markdown(
        f'<div style="background:#FEF2F2;border:1px solid #FECACA;border-radius:12px;'
        f'padding:16px 22px;margin-bottom:10px;">'
        f'<div style="font-weight:700;color:#991B1B;font-size:14px;margin-bottom:6px;">'
        f'Churned this week</div>'
        f'<div style="font-size:13px;color:#991B1BDD;">'
        + '<br>'.join(
            f"&bull; <b>{r.business_name}</b> — {int(r.orders_last_week)} orders last week, "
            f"last order {int(r.days_since_last_order)}d ago"
            for _, r in churned_this_week.iterrows())
        + '</div></div>',
        unsafe_allow_html=True,
    )

# Reactivated brands — name them
if not reactivated_this_week.empty:
    st.markdown(
        f'<div style="background:#EFF6FF;border:1px solid #BFDBFE;border-radius:12px;'
        f'padding:16px 22px;margin-bottom:10px;">'
        f'<div style="font-weight:700;color:#1E40AF;font-size:14px;margin-bottom:6px;">'
        f'Reactivated this week</div>'
        f'<div style="font-size:13px;color:#1E40AFDD;">'
        + '<br>'.join(
            f"&bull; <b>{r.business_name}</b> — {int(r.orders_this_week)} orders"
            for _, r in reactivated_this_week.iterrows())
        + '</div></div>',
        unsafe_allow_html=True,
    )

st.markdown("")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. GROWTH TREND
# ═══════════════════════════════════════════════════════════════════════════════

section_title("WEEKLY GROWTH TREND")
st.markdown(
    '<div style="font-size:12px;color:#64748B;margin-bottom:12px;">'
    'New brands joining vs brands churning each week. '
    'Net growth = new - churned + reactivated.</div>',
    unsafe_allow_html=True,
)

# Filter to last 26 weeks for readability
recent_growth = growth.tail(26).copy()
recent_growth["week_label"] = pd.to_datetime(recent_growth.week_start).dt.strftime("%b %d")

fig_growth = go.Figure()
fig_growth.add_trace(go.Bar(
    x=recent_growth.week_label, y=recent_growth.new_brands,
    name="New Brands", marker_color=COLOR_HEALTHY,
    hovertemplate="<b>%{x}</b><br>+%{y} new<extra></extra>",
))
fig_growth.add_trace(go.Bar(
    x=recent_growth.week_label, y=-recent_growth.churned_brands,
    name="Churned", marker_color=COLOR_CRITICAL,
    hovertemplate="<b>%{x}</b><br>-%{customdata} churned<extra></extra>",
    customdata=recent_growth.churned_brands,
))
fig_growth.add_trace(go.Bar(
    x=recent_growth.week_label, y=recent_growth.reactivated_brands,
    name="Reactivated", marker_color="#3B82F6",
    hovertemplate="<b>%{x}</b><br>+%{y} reactivated<extra></extra>",
))
fig_growth.add_trace(go.Scatter(
    x=recent_growth.week_label, y=recent_growth.net_growth,
    name="Net Growth", mode="lines+markers",
    line=dict(color="#0F172A", width=2),
    marker=dict(size=5),
    hovertemplate="<b>%{x}</b><br>Net: %{y:+d}<extra></extra>",
))
growth_layout = {k: v for k, v in CHART_LAYOUT.items() if k not in ("margin",)}
fig_growth.update_layout(
    **growth_layout,
    barmode="relative", height=380,
    margin=dict(t=10, b=10, l=10, r=10),
)
st.plotly_chart(fig_growth, use_container_width=True)

# Active brands trend
section_title("ACTIVE BRANDS OVER TIME")

fig_active = go.Figure()
fig_active.add_trace(go.Scatter(
    x=recent_growth.week_label, y=recent_growth.active_brands,
    mode="lines+markers+text", name="Active Brands",
    line=dict(color=brand_color, width=3),
    marker=dict(size=6),
    text=recent_growth.active_brands,
    textposition="top center",
    textfont=dict(size=9),
    hovertemplate="<b>%{x}</b><br>%{y} active brands<extra></extra>",
))
active_layout = {k: v for k, v in CHART_LAYOUT.items() if k not in ("margin",)}
fig_active.update_layout(
    **active_layout, height=300,
    margin=dict(t=20, b=10, l=10, r=10),
)
st.plotly_chart(fig_active, use_container_width=True)

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════════
# 3. RETENTION COHORTS
# ═══════════════════════════════════════════════════════════════════════════════

section_title("RETENTION COHORTS")
st.markdown(
    '<div style="font-size:12px;color:#64748B;margin-bottom:12px;">'
    'Monthly signup cohorts — what % of brands are still ordering at M1, M3, M6, M12? '
    'Higher = better retention. Grey cells = not enough time has passed yet.</div>',
    unsafe_allow_html=True,
)

if not cohorts.empty:
    cohort_display = cohorts.copy()
    cohort_display["cohort_month"] = pd.to_datetime(cohort_display.cohort_month).dt.strftime("%b %Y")

    # Build heatmap data
    heatmap_cols = ["retention_rate_m1_valid", "retention_rate_m3_valid",
                    "retention_rate_m6_valid", "retention_rate_m12_valid"]
    heatmap_labels = ["M1", "M3", "M6", "M12"]

    z_data = []
    text_data = []
    for _, row in cohort_display.iterrows():
        z_row = []
        t_row = []
        for col in heatmap_cols:
            val = row.get(col)
            if pd.isna(val):
                z_row.append(None)
                t_row.append("—")
            else:
                z_row.append(float(val))
                t_row.append(f"{val:.0f}%")
        z_data.append(z_row)
        text_data.append(t_row)

    fig_cohort = go.Figure(go.Heatmap(
        z=z_data,
        x=heatmap_labels,
        y=[f"{r.cohort_month} ({r.cohort_size})" for _, r in cohort_display.iterrows()],
        text=text_data,
        texttemplate="%{text}",
        textfont=dict(size=12, color="white"),
        colorscale=[[0, COLOR_CRITICAL], [0.5, COLOR_AT_RISK], [1, COLOR_HEALTHY]],
        zmin=0, zmax=100,
        hovertemplate="<b>%{y}</b><br>%{x}: %{text}<extra></extra>",
        showscale=True,
        colorbar=dict(title="Retention %", ticksuffix="%"),
    ))
    cohort_layout = {k: v for k, v in CHART_LAYOUT.items() if k not in ("margin", "xaxis", "yaxis")}
    fig_cohort.update_layout(
        **cohort_layout,
        height=max(300, len(cohort_display) * 32 + 60),
        margin=dict(t=10, b=10, l=160, r=10),
        yaxis=dict(autorange="reversed", tickfont=dict(size=11)),
        xaxis=dict(side="top", tickfont=dict(size=12)),
    )
    st.plotly_chart(fig_cohort, use_container_width=True)

    # Summary stats
    valid_m1 = cohorts.retention_rate_m1_valid.dropna()
    valid_m3 = cohorts.retention_rate_m3_valid.dropna()
    if not valid_m1.empty:
        r1, r2, r3 = st.columns(3)
        r1.metric("Avg M1 Retention", f"{valid_m1.mean():.0f}%",
                  help="Average 1-month retention across all cohorts")
        if not valid_m3.empty:
            r2.metric("Avg M3 Retention", f"{valid_m3.mean():.0f}%")
        valid_m6 = cohorts.retention_rate_m6_valid.dropna()
        if not valid_m6.empty:
            r3.metric("Avg M6 Retention", f"{valid_m6.mean():.0f}%")
else:
    st.info("No cohort data yet.")

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════════
# 4. CHURNED THIS WEEK
# ═══════════════════════════════════════════════════════════════════════════════

section_title("CHURNED THIS WEEK")
st.markdown(
    '<div style="font-size:12px;color:#64748B;margin-bottom:12px;">'
    'Brands that had orders last week (Sun–Sat) but zero orders this week. '
    'Sorted by last week\'s revenue — highest value losses first.</div>',
    unsafe_allow_html=True,
)

if not churned_this_week.empty:
    st.markdown(
        f'<div style="background:#FEF2F2;border:1px solid #FECACA;border-radius:12px;'
        f'padding:18px 22px;margin-bottom:14px;">'
        f'<div style="font-weight:700;color:#991B1B;font-size:14px;">'
        f'{len(churned_this_week)} brands stopped ordering this week</div>'
        f'<div style="font-size:13px;color:#991B1BDD;margin-top:4px;">'
        f'Combined last week revenue: {naira(churned_this_week.revenue_last_week.sum())}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    churn_tbl = churned_this_week[[
        "business_name", "orders_last_week", "revenue_last_week",
        "last_order_date", "days_since_last_order"
    ]].copy()
    churn_tbl.columns = ["Brand", "Orders (Last Week)", "Revenue (Last Week)",
                         "Last Order", "Days Since"]
    churn_tbl["Revenue (Last Week)"] = churn_tbl["Revenue (Last Week)"].apply(
        lambda x: naira(x) if pd.notna(x) else "—"
    )

    st.dataframe(
        churn_tbl.style.apply(
            lambda row: ["background-color: #FEF2F2"] * len(row), axis=1
        ),
        use_container_width=True,
        height=min(400, 40 + len(churn_tbl) * 35),
    )
    dl_prefix = "daash" if is_daash else "gosource"
    st.download_button("Download churn list", churn_tbl.to_csv(index=False),
                       f"{dl_prefix}_churned_this_week.csv", "text/csv", key="dl_churn")
else:
    st.success("No brands churned this week!")

st.markdown("")

# Reactivated
if not reactivated_this_week.empty:
    st.markdown(
        f'<div style="background:#F0FDF4;border:1px solid #BBF7D0;border-radius:12px;'
        f'padding:18px 22px;margin-bottom:14px;">'
        f'<div style="font-weight:700;color:#166534;font-size:14px;">'
        f'{len(reactivated_this_week)} brands reactivated this week</div>'
        f'<div style="font-size:13px;color:#166534DD;margin-top:4px;">'
        f'Previously dormant brands that placed new orders.</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    for _, r in reactivated_this_week.head(10).iterrows():
        st.markdown(
            f"&nbsp;&nbsp;&nbsp;**{r.business_name}** — "
            f"{int(r.orders_this_week)} orders this week · last ordered {int(r.days_since_last_order)}d ago"
        )

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════════
# 5. WEB ADOPTION TREND (DAASH only)
# ═══════════════════════════════════════════════════════════════════════════════

if is_daash:
    section_title("WEB ADOPTION TREND")
    st.markdown(
        '<div style="font-size:12px;color:#64748B;margin-bottom:12px;">'
        'What % of active brands are using the website channel? '
        'Higher = better adoption of the DAASH website product.</div>',
        unsafe_allow_html=True,
    )

    web_data = recent_growth[recent_growth.active_brands > 0].copy()
    if not web_data.empty:
        c1, c2 = st.columns(2)
        with c1:
            fig_web = go.Figure()
            fig_web.add_trace(go.Bar(
                x=web_data.week_label, y=web_data.web_enabled_brands,
                name="Web-Enabled", marker_color="#3B82F6",
            ))
            fig_web.add_trace(go.Bar(
                x=web_data.week_label, y=web_data.pos_only_brands,
                name="POS-Only", marker_color=COLOR_NEUTRAL,
            ))
            web_layout = {k: v for k, v in CHART_LAYOUT.items() if k not in ("margin",)}
            fig_web.update_layout(
                **web_layout, barmode="stack", height=300,
                margin=dict(t=10, b=10, l=10, r=10),
            )
            st.plotly_chart(fig_web, use_container_width=True)

        with c2:
            fig_pct = go.Figure()
            fig_pct.add_trace(go.Scatter(
                x=web_data.week_label, y=web_data.web_adoption_pct,
                mode="lines+markers",
                line=dict(color="#3B82F6", width=2),
                marker=dict(size=5),
                hovertemplate="<b>%{x}</b><br>%{y:.1f}% web adoption<extra></extra>",
            ))
            pct_layout = {k: v for k, v in CHART_LAYOUT.items() if k not in ("margin", "yaxis")}
            fig_pct.update_layout(
                **pct_layout, height=300,
                margin=dict(t=10, b=10, l=10, r=10),
                yaxis=dict(title="Web Adoption %", gridcolor="#F1F5F9",
                          tickfont=dict(size=11), ticksuffix="%"),
            )
            st.plotly_chart(fig_pct, use_container_width=True)
