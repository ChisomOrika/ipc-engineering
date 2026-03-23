"""
IPC Customer Health Dashboard — Operational
For customer service & account managers.
No revenue breakdowns per client.
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

from utils.db     import run_query
from utils.fmt    import naira, pct, count
from utils.styles import (inject_css, page_header, section_title,
                           CHART_LAYOUT, COLOR_HEALTHY, COLOR_AT_RISK,
                           COLOR_CRITICAL, COLOR_NEUTRAL,
                           COLOR_DAASH, COLOR_GOSOURCE)

st.set_page_config(
    page_title="Customer Health · IPC",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded",
)
inject_css()


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def health_badge(status, size="normal"):
    colors = {
        "Healthy":  (COLOR_HEALTHY, "#F0FDF4"),
        "At Risk":  (COLOR_AT_RISK, "#FFFBEB"),
        "Critical": (COLOR_CRITICAL, "#FEF2F2"),
    }
    fg, bg = colors.get(status, ("#6B7280", "#F3F4F6"))
    pad = "4px 14px" if size == "large" else "3px 10px"
    fs = "14px" if size == "large" else "12px"
    return (f'<span style="background:{bg};color:{fg};padding:{pad};'
            f'border-radius:20px;font-size:{fs};font-weight:700;'
            f'border:1px solid {fg}30;">{status}</span>')


def alert_card(bg, border, title_color, title, body, icon=""):
    return (
        f'<div style="background:{bg};border:1px solid {border};border-radius:12px;'
        f'padding:18px 22px;margin-bottom:14px;">'
        f'<div style="font-weight:700;color:{title_color};margin-bottom:6px;font-size:14px;">'
        f'{icon} {title}</div>'
        f'<div style="font-size:13px;color:{title_color}DD;line-height:1.5;">{body}</div></div>'
    )


def kpi_card(label, value, subtitle="", color="#0F172A"):
    return (
        f'<div style="background:white;border:1px solid #E2E8F0;border-radius:12px;'
        f'padding:20px 22px;box-shadow:0 1px 4px rgba(0,0,0,0.06);height:100%;">'
        f'<div style="font-size:11px;font-weight:600;text-transform:uppercase;'
        f'letter-spacing:0.7px;color:#94A3B8;">{label}</div>'
        f'<div style="font-size:28px;font-weight:800;color:{color};margin:6px 0 4px;">{value}</div>'
        f'<div style="font-size:11px;color:#94A3B8;line-height:1.4;">{subtitle}</div>'
        f'</div>'
    )


def activation_color(status):
    return {"Quick Start (≤7d)": COLOR_HEALTHY, "Normal Start (8-30d)": "#3B82F6",
            "Slow Start (31-60d)": COLOR_AT_RISK, "Very Slow (60d+)": "#F97316",
            "Never Activated": COLOR_CRITICAL}.get(status, COLOR_NEUTRAL)


def engagement_color(status):
    return {"Active": COLOR_HEALTHY, "Cooling Off": COLOR_AT_RISK,
            "At Risk": "#F97316", "Dormant": COLOR_CRITICAL,
            "Never Ordered": COLOR_NEUTRAL}.get(status, COLOR_NEUTRAL)


# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown("## 💊 Customer Health")
    st.markdown(
        '<div style="font-size:12px;color:#94A3B8;margin-bottom:16px;">'
        'Operational dashboard for CS & account management</div>',
        unsafe_allow_html=True,
    )
    st.markdown("---")
    service = st.selectbox("Service Line", ["DAASH", "GoSource"], index=0)
    st.markdown("---")
    st.markdown(
        '<div style="font-size:11px;color:#475569;">⏱ Data refreshes every 6 hours</div>',
        unsafe_allow_html=True,
    )

brand_color = COLOR_DAASH if service == "DAASH" else COLOR_GOSOURCE
page_header(
    "Customer Health Dashboard",
    f"{service} · Engagement signals, activation & risk detection",
    color=brand_color,
)


# ═══════════════════════════════════════════════════════════════════════════════
# LOAD DATA
# ═══════════════════════════════════════════════════════════════════════════════

if service == "DAASH":
    health = run_query("SELECT * FROM gold.dim_dash_restaurant_health ORDER BY health_score DESC, orders_last_30d DESC")
    activation = run_query("SELECT * FROM gold.fact_dash_activation ORDER BY signup_date DESC")
    entity = "restaurant"
    score_max = 9  # default; per-brand score_max comes from model column
    SIGNALS = {
        "signal_orders":       ("Order Volume",       "Not declining >50% vs prior 30 days"),
        "signal_visits":       ("Web Traffic",        "Not declining >50% vs prior 30 days"),
        "signal_activity":     ("Platform Activity",  "Activity in the last 14 days"),
        "signal_menu":         ("Menu Freshness",     "Updated within 60 days"),
        "signal_staff":        ("Staff Adoption",     "2+ active team members"),
        "signal_delivery":     ("Delivery Quality",   "80%+ completion rate (90d)"),
        "signal_subscription": ("Subscription",       "Active subscription plan"),
        "signal_channel":      ("Channel Adoption",   "Website orders in last 90d"),
        "signal_quality":      ("Order Quality",      "Rejection/void rate below 5%"),
    }
else:
    health = run_query("SELECT * FROM gold.dim_gosource_customer_health ORDER BY health_score DESC, orders_last_30d DESC")
    activation = run_query("SELECT * FROM gold.fact_gosource_activation ORDER BY signup_date DESC")
    entity = "customer"
    score_max = 6
    SIGNALS = {
        "signal_orders":   ("Order Volume",     "Not declining >50% vs prior 30 days"),
        "signal_recency":  ("Order Recency",    "Last order within 60 days"),
        "signal_staff":    ("Staff Adoption",   "2+ active employees"),
        "signal_branches": ("Branch Presence",  "At least 1 active branch"),
        "signal_payment":  ("Payment Health",   "60%+ of orders paid"),
        "signal_credit":   ("Credit Standing",  "No 90d+ overdue orders"),
    }

if health.empty:
    st.warning("No health data available. Run dbt models first.")
    st.stop()

total    = len(health)
healthy  = len(health[health.health_status == "Healthy"])
at_risk  = len(health[health.health_status == "At Risk"])
critical = len(health[health.health_status == "Critical"])
avg_score = health.health_score.mean()

total_signups = len(activation) if not activation.empty else 0
never_activated = len(activation[activation.activation_status == "Never Activated"]) if not activation.empty else 0
activated = total_signups - never_activated
activation_rate = (activated / total_signups * 100) if total_signups > 0 else 0


# ═══════════════════════════════════════════════════════════════════════════════
# 1. HOW IT WORKS (explainer banner)
# ═══════════════════════════════════════════════════════════════════════════════

thresholds = ("7+ signals", "4-6 signals", "0-3 signals") if service == "DAASH" else ("5-6 signals", "3-4 signals", "0-2 signals")
st.markdown(
    f'<div style="background:#F8FAFC;border:1px solid #E2E8F0;border-radius:12px;'
    f'padding:20px 24px;margin-bottom:24px;">'
    f'<div style="font-size:15px;font-weight:700;color:#0F172A;margin-bottom:10px;">'
    f'How Customer Health Works</div>'
    f'<div style="font-size:13px;color:#475569;line-height:1.7;">'
    f'Every {service} brand is scored across <b>{score_max} engagement signals</b> '
    f'— each signal is either <span style="color:{COLOR_HEALTHY};font-weight:600;">passing</span> '
    f'or <span style="color:{COLOR_CRITICAL};font-weight:600;">failing</span>. '
    f'The total determines health status:<br>'
    f'{health_badge("Healthy")} {thresholds[0]} &nbsp;&nbsp;'
    f'{health_badge("At Risk")} {thresholds[1]} &nbsp;&nbsp;'
    f'{health_badge("Critical")} {thresholds[2]}'
    f'</div></div>',
    unsafe_allow_html=True,
)

# Signal definitions (expandable)
with st.expander(f"📖 What are the {score_max} signals? (click to expand)"):
    sig_html = '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;">'
    for col, (label, desc) in SIGNALS.items():
        sig_html += (
            f'<div style="background:white;border:1px solid #E2E8F0;border-radius:10px;'
            f'padding:14px 16px;">'
            f'<div style="font-size:12px;font-weight:700;color:#0F172A;margin-bottom:4px;">{label}</div>'
            f'<div style="font-size:11px;color:#64748B;line-height:1.4;">{desc}</div>'
            f'</div>'
        )
    sig_html += '</div>'
    st.markdown(sig_html, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# 2. KPI ROW
# ═══════════════════════════════════════════════════════════════════════════════

section_title("OVERVIEW")
k1, k2, k3, k4, k5, k6 = st.columns(6)
with k1:
    st.markdown(kpi_card("Total Signups", count(total_signups),
                         f"{count(activated)} activated"), unsafe_allow_html=True)
with k2:
    st.markdown(kpi_card("Activation Rate", f"{activation_rate:.0f}%",
                         "Brands with user orders",
                         COLOR_HEALTHY if activation_rate > 50 else COLOR_CRITICAL),
                unsafe_allow_html=True)
with k3:
    st.markdown(kpi_card("Healthy", count(healthy),
                         f"{thresholds[0]} passing", COLOR_HEALTHY), unsafe_allow_html=True)
with k4:
    st.markdown(kpi_card("At Risk", count(at_risk),
                         "Needs attention", COLOR_AT_RISK), unsafe_allow_html=True)
with k5:
    st.markdown(kpi_card("Critical", count(critical),
                         "Immediate action", COLOR_CRITICAL), unsafe_allow_html=True)
with k6:
    st.markdown(kpi_card("Avg Score", f"{avg_score:.1f}/{score_max}",
                         f"Across {count(total)} active"), unsafe_allow_html=True)

st.markdown("")


# ═══════════════════════════════════════════════════════════════════════════════
# 3. SIGNAL BREAKDOWN
# ═══════════════════════════════════════════════════════════════════════════════

section_title("SIGNAL BREAKDOWN — WHERE ARE " + entity.upper() + "S FAILING?")
st.markdown(
    '<div style="font-size:12px;color:#64748B;margin-bottom:12px;">'
    'Each bar shows how many brands are passing vs failing each signal. '
    'Longer red bars = widespread issues that need attention.</div>',
    unsafe_allow_html=True,
)

sig_data = []
for col, (label, desc) in SIGNALS.items():
    if col in health.columns:
        passing = int(health[col].sum())
        sig_data.append({"Signal": label, "Passing": passing,
                         "Failing": total - passing,
                         "Pass %": round(passing / total * 100)})

fig_sig = go.Figure()
fig_sig.add_trace(go.Bar(
    y=[s["Signal"] for s in sig_data],
    x=[s["Passing"] for s in sig_data],
    orientation="h", name="Passing",
    marker_color="#60A5FA",
    hovertemplate="<b>%{y}</b><br>%{x} passing (%{customdata}%)<extra></extra>",
    customdata=[s["Pass %"] for s in sig_data],
))
fig_sig.add_trace(go.Bar(
    y=[s["Signal"] for s in sig_data],
    x=[s["Failing"] for s in sig_data],
    orientation="h", name="Failing",
    marker_color="#8B0000",
    hovertemplate="<b>%{y}</b><br>%{x} failing<extra></extra>",
))
sig_layout = {k: v for k, v in CHART_LAYOUT.items() if k not in ("margin", "yaxis")}
fig_sig.update_layout(
    **sig_layout,
    barmode="stack", height=max(250, len(sig_data) * 45),
    margin=dict(t=10, b=10, l=140, r=10),
    yaxis=dict(autorange="reversed", gridcolor="#F1F5F9", tickfont=dict(size=12)),
)
st.plotly_chart(fig_sig, use_container_width=True)

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════════
# 4. ACTIVATION & ONBOARDING
# ═══════════════════════════════════════════════════════════════════════════════

section_title("📈 ACTIVATION & ONBOARDING")
st.markdown(
    '<div style="font-size:12px;color:#64748B;margin-bottom:14px;">'
    'How quickly do new brands receive their first user order after signing up? '
    'A low activation rate means brands are onboarding but users haven\'t started ordering yet.</div>',
    unsafe_allow_html=True,
)

if not activation.empty:
    c1, c2 = st.columns(2)

    with c1:
        # Activation speed
        act_counts = activation.activation_status.value_counts()
        act_order = ["Quick Start (≤7d)", "Normal Start (8-30d)", "Slow Start (31-60d)",
                     "Very Slow (60d+)", "Never Activated"]
        act_vals = [act_counts.get(s, 0) for s in act_order]

        fig_act = go.Figure(go.Bar(
            x=act_order, y=act_vals,
            marker_color=[activation_color(s) for s in act_order],
            hovertemplate="<b>%{x}</b><br>%{y} brands<extra></extra>",
            text=act_vals, textposition="outside",
        ))
        act_layout = {k: v for k, v in CHART_LAYOUT.items() if k not in ("margin", "xaxis", "yaxis")}
        fig_act.update_layout(
            **act_layout, height=320,
            margin=dict(t=10, b=10, l=10, r=10),
            xaxis=dict(tickangle=-20, tickfont=dict(size=10)),
            yaxis=dict(title="# of brands"),
        )
        st.plotly_chart(fig_act, use_container_width=True)

    with c2:
        # Activation funnel
        windows = [
            ("Within 7 days", int(activation.activated_7d.sum()) if "activated_7d" in activation.columns else 0),
            ("Within 14 days", int(activation.activated_14d.sum()) if "activated_14d" in activation.columns else 0),
            ("Within 30 days", int(activation.activated_30d.sum()) if "activated_30d" in activation.columns else 0),
            ("Within 60 days", int(activation.activated_60d.sum()) if "activated_60d" in activation.columns else 0),
            ("Ever activated", activated),
        ]

        fig_funnel = go.Figure(go.Funnel(
            y=[w[0] for w in windows],
            x=[w[1] for w in windows],
            textinfo="value+percent initial",
            marker=dict(color=[COLOR_HEALTHY, "#3B82F6", COLOR_AT_RISK, "#F97316", COLOR_NEUTRAL]),
            hovertemplate="<b>%{y}</b><br>%{x} brands<br>%{percentInitial:.1%} of signups<extra></extra>",
        ))
        funnel_layout = {k: v for k, v in CHART_LAYOUT.items() if k != "margin"}
        fig_funnel.update_layout(**funnel_layout, height=320, margin=dict(t=10, b=10, l=10, r=10))
        st.plotly_chart(fig_funnel, use_container_width=True)

    # Recent never-activated (collapsible)
    recent_inactive = activation[
        (activation.activation_status == "Never Activated")
        & (activation.tenure_days <= 90)
    ]
    if not recent_inactive.empty:
        with st.expander(f"🆕 {len(recent_inactive)} brands signed up in the last 90 days with zero user orders — click to see list"):
            never_tbl = recent_inactive[["business_name", "signup_date", "tenure_days"]].copy()
            never_tbl.columns = ["Brand", "Signup Date", "Days Since Signup"]
            never_tbl = never_tbl.sort_values("Days Since Signup")
            st.dataframe(never_tbl, use_container_width=True,
                         height=min(300, 40 + len(never_tbl) * 35))
            st.download_button("📥 Download list", never_tbl.to_csv(index=False),
                               f"{service.lower()}_never_activated.csv", "text/csv",
                               key="dl_never")

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════════
# 5. ALL CUSTOMERS TABLE
# ═══════════════════════════════════════════════════════════════════════════════

section_title(f"ALL {entity.upper()}S")
st.markdown(
    '<div style="font-size:12px;color:#64748B;margin-bottom:12px;">'
    'Rows are color-coded: '
    '<span style="background:#F0FDF4;padding:1px 6px;border-radius:4px;">green</span> healthy, '
    '<span style="background:#FFFBEB;padding:1px 6px;border-radius:4px;">yellow</span> at risk, '
    '<span style="background:#FEF2F2;padding:1px 6px;border-radius:4px;">red</span> critical.</div>',
    unsafe_allow_html=True,
)

if service == "DAASH":
    display_cols = {
        "business_name": "Restaurant", "health_score": "Score",
        "health_status": "Status", "orders_last_30d": "Orders 30d",
        "days_since_last_order": "Days Inactive",
        "web_order_pct": "Web %", "order_fail_rate_pct": "Fail %",
        "active_members": "Staff", "visits_last_30d": "Visits 30d",
    }
else:
    display_cols = {
        "business_name": "Customer", "health_score": "Score",
        "health_status": "Status", "orders_last_30d": "Orders 30d",
        "days_since_last_order": "Days Inactive",
        "active_employees": "Staff", "active_branches": "Branches",
        "payment_rate": "Pay Rate %", "overdue_90d_amount": "Overdue 90d+",
    }

avail = {k: v for k, v in display_cols.items() if k in health.columns}
tbl = health[list(avail.keys())].copy()
tbl.columns = list(avail.values())

if "Overdue 90d+" in tbl.columns:
    tbl["Overdue 90d+"] = tbl["Overdue 90d+"].apply(
        lambda x: naira(x) if pd.notna(x) and x > 0 else "—"
    )

f1, f2 = st.columns([3, 1])
with f1:
    status_filter = st.multiselect(
        "Filter by status", ["Healthy", "At Risk", "Critical"],
        default=["Healthy", "At Risk", "Critical"], key="tbl_filter",
    )
with f2:
    st.download_button("📥 Download CSV", tbl.to_csv(index=False),
                       f"{service.lower()}_health.csv", "text/csv")

filtered_tbl = tbl[tbl["Status"].isin(status_filter)]
st.dataframe(
    filtered_tbl.style.apply(
        lambda row: [
            "background-color: #F0FDF4" if row["Status"] == "Healthy"
            else "background-color: #FFFBEB" if row["Status"] == "At Risk"
            else "background-color: #FEF2F2"
        ] * len(row),
        axis=1,
    ),
    use_container_width=True,
    height=min(500, 40 + len(filtered_tbl) * 35),
)

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════════
# 6. DEEP DIVE
# ═══════════════════════════════════════════════════════════════════════════════

section_title(f"🔍 {entity.upper()} DEEP DIVE")
st.markdown(
    f'<div style="font-size:12px;color:#64748B;margin-bottom:12px;">'
    f'Select a brand to see their full signal breakdown. '
    f'Hover over signal cards for definitions.</div>',
    unsafe_allow_html=True,
)

names = health.business_name.tolist()
selected = st.selectbox("Select a brand", names, index=0)
row = health[health.business_name == selected].iloc[0]

st.markdown(
    f'<div style="background:white;border:1px solid #E2E8F0;border-radius:14px;'
    f'padding:22px 28px;margin:8px 0 16px;box-shadow:0 2px 8px rgba(0,0,0,0.06);">'
    f'<div style="display:flex;align-items:center;gap:16px;flex-wrap:wrap;">'
    f'<div style="font-size:22px;font-weight:800;color:#0F172A;">{selected}</div>'
    f'{health_badge(row.health_status, "large")}'
    f'<div style="font-size:15px;color:#64748B;margin-left:auto;">'
    f'Score: <b style="font-size:20px;color:#0F172A;">{int(row.health_score)}</b> / {int(row.score_max) if "score_max" in health.columns else score_max}</div>'
    f'</div></div>',
    unsafe_allow_html=True,
)

# Signal cards
brand_pos_only = "is_pos_only" in health.columns and bool(row.get("is_pos_only", False))
web_signals = {"signal_visits", "signal_channel"}
cols = st.columns(3)
card_idx = 0
for col_name, (label, desc) in SIGNALS.items():
    if col_name not in health.columns:
        continue
    # POS-only brands: skip web signals from the grid (they're excluded from score)
    if brand_pos_only and col_name in web_signals:
        continue
    val = int(row[col_name])
    icon = "✅" if val else "❌"
    bg = "#F0FDF4" if val else "#FEF2F2"
    border = "#BBF7D0" if val else "#FECACA"
    status_text = "Passing" if val else "Failing"
    status_color = COLOR_HEALTHY if val else COLOR_CRITICAL
    cols[card_idx % 3].markdown(
        f'<div title="{desc}" style="background:{bg};border:1px solid {border};'
        f'border-radius:10px;padding:14px 16px;margin-bottom:10px;cursor:help;">'
        f'<div style="display:flex;justify-content:space-between;align-items:center;">'
        f'<div style="font-size:13px;font-weight:700;color:#374151;">{icon} {label}</div>'
        f'<div style="font-size:10px;font-weight:600;color:{status_color};">{status_text}</div>'
        f'</div>'
        f'<div style="font-size:11px;color:#6B7280;margin-top:4px;">{desc}</div>'
        f'</div>',
        unsafe_allow_html=True,
    )
    card_idx += 1

# POS-only upsell alert
if brand_pos_only:
    st.info("🖥️ This brand is **POS-only** — no website orders detected. "
            "Web Traffic and Channel Adoption signals are excluded from their score. "
            "Consider onboarding them to their DAASH website to unlock more sales channels.")

# Key metrics
st.markdown("")
if service == "DAASH":
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Orders (30d)", count(row.get("orders_last_30d", 0)),
              help="Delivered orders in the last 30 days")
    m2.metric("Web Order %", pct(row.get("web_order_pct", 0)),
              help="% of orders via website vs POS")
    m3.metric("Fail Rate", pct(row.get("order_fail_rate_pct", 0)),
              help="Rejection + void rate, last 90 days")
    days_val = row.get("days_since_last_order")
    m4.metric("Days Inactive", int(days_val) if pd.notna(days_val) else "—",
              help="Days since last delivered order")
else:
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Orders (30d)", count(row.get("orders_last_30d", 0)))
    m2.metric("Staff", count(row.get("active_employees", 0)))
    pay_rate = row.get("payment_rate")
    m3.metric("Pay Rate", pct(pay_rate) if pd.notna(pay_rate) else "—",
              help="% of delivered orders paid")
    overdue = row.get("overdue_90d_amount", 0)
    m4.metric("Overdue 90d+", naira(overdue) if overdue and overdue > 0 else "—",
              help="Unpaid orders older than 90 days")

# Activation context
if not activation.empty:
    cust_act = activation[activation.business_name == selected]
    if not cust_act.empty:
        a = cust_act.iloc[0]
        st.markdown("")
        a1, a2, a3, a4 = st.columns(4)
        a1.metric("Signup Date", str(a.signup_date) if pd.notna(a.signup_date) else "—")
        a2.metric("Days to First Order",
                  f"{int(a.days_to_first_order)}d" if pd.notna(a.days_to_first_order) else "Never",
                  help="Calendar days from signup to first user order")
        a3.metric("Activation", str(a.activation_status),
                  help="How quickly the brand received its first user order")
        a4.metric("Engagement", str(a.engagement_status),
                  help="Current activity level based on last order date")

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════════
# 7. ALERTS & ACTIONS (last — actionable summary)
# ═══════════════════════════════════════════════════════════════════════════════

section_title("🚨 ALERTS & RECOMMENDED ACTIONS")
st.markdown(
    '<div style="font-size:13px;color:#64748B;margin-bottom:16px;">'
    'Auto-generated from health signals — prioritized items that need attention.</div>',
    unsafe_allow_html=True,
)

alert_count = 0

if service == "DAASH":
    # Active at risk
    high_risk = health[
        (health.health_status.isin(["At Risk", "Critical"]))
        & (health.orders_last_30d > 0)
    ].sort_values("orders_last_30d", ascending=False).head(8)
    if not high_risk.empty:
        alert_count += 1
        st.markdown(alert_card(
            "#FFFBEB", "#FDE68A", "#92400E",
            f"⚠️ {len(high_risk)} Active Restaurants at Risk",
            "Still ordering but health signals are failing — priority outreach."
        ), unsafe_allow_html=True)
        for _, r in high_risk.iterrows():
            r_max = int(r.score_max) if "score_max" in health.columns else score_max
            r_pos = "is_pos_only" in health.columns and bool(r.get("is_pos_only", False))
            failing = [label for col, (label, _) in SIGNALS.items()
                       if col in health.columns and r.get(col) == 0
                       and not (r_pos and col in web_signals)]
            st.markdown(
                f"&nbsp;&nbsp;&nbsp;**{r.business_name}** — {r.health_score}/{r_max} · "
                f"{int(r.orders_last_30d)} orders/30d · Failing: _{', '.join(failing)}_"
            )
        st.markdown("")

    no_web = health[(health.orders_last_30d > 50) & (health["web_order_pct"] == 0)] if "web_order_pct" in health.columns else pd.DataFrame()
    if not no_web.empty:
        alert_count += 1
        st.markdown(alert_card(
            "#EFF6FF", "#BFDBFE", "#1E40AF",
            f"🌐 Website Upsell — {len(no_web)} POS-Only Restaurants",
            "50+ orders/month but zero website usage."
        ), unsafe_allow_html=True)
        for _, r in no_web.iterrows():
            st.markdown(f"&nbsp;&nbsp;&nbsp;**{r.business_name}** — {int(r.orders_last_30d)} orders, 100% POS")
        st.markdown("")

    if "order_fail_rate_pct" in health.columns and "failed_orders_90d" in health.columns:
        bad_quality = health[
            (health.order_fail_rate_pct >= 5)
            & (health.failed_orders_90d >= 3)
        ].sort_values("order_fail_rate_pct", ascending=False)
    else:
        bad_quality = pd.DataFrame()
    if not bad_quality.empty:
        alert_count += 1
        st.markdown(alert_card(
            "#FEF2F2", "#FECACA", "#991B1B",
            f"🚨 High Failure Rate — {len(bad_quality)} Brands",
            "5%+ rejection/void rate with 3+ failed orders — likely menu or operational issues."
        ), unsafe_allow_html=True)
        for _, r in bad_quality.iterrows():
            total_90d = int(r.failed_orders_90d / (r.order_fail_rate_pct / 100)) if r.order_fail_rate_pct > 0 else 0
            st.markdown(
                f"&nbsp;&nbsp;&nbsp;**{r.business_name}** — "
                f"{r.order_fail_rate_pct:.1f}% fail rate ({int(r.failed_orders_90d)} failed / {total_90d} orders in 90d)"
            )
        st.markdown("")

    sub_data = run_query("""
        SELECT business_name, subscription_status, lifetime_orders,
               days_since_last_order, high_value_churned, active_no_autorenew
        FROM gold.fact_dash_subscription_lifecycle ORDER BY lifetime_orders DESC
    """)
    churned = sub_data[sub_data.high_value_churned == True]
    if not churned.empty:
        alert_count += 1
        st.markdown(alert_card(
            "#FDF4FF", "#E9D5FF", "#6B21A8",
            f"💔 {len(churned)} High-Value Subscription Churns",
            "100+ lifetime orders but subscription lapsed — re-engagement opportunity."
        ), unsafe_allow_html=True)
        for _, r in churned.head(5).iterrows():
            days = f" · last order {int(r.days_since_last_order)}d ago" if pd.notna(r.days_since_last_order) else ""
            st.markdown(f"&nbsp;&nbsp;&nbsp;**{r.business_name}** — {int(r.lifetime_orders)} orders{days}")
        st.markdown("")

    no_renew = sub_data[sub_data.active_no_autorenew == True]
    if not no_renew.empty:
        alert_count += 1
        st.markdown(alert_card(
            "#FFF7ED", "#FED7AA", "#9A3412",
            f"🔔 {len(no_renew)} Active Without Auto-Renew",
            "Won't renew automatically — reach out before expiry."
        ), unsafe_allow_html=True)
        for _, r in no_renew.head(5).iterrows():
            st.markdown(f"&nbsp;&nbsp;&nbsp;**{r.business_name}** — {int(r.lifetime_orders)} lifetime orders")
        st.markdown("")

else:  # GoSource
    credit_risk = health[health.overdue_90d_amount > 0].sort_values("overdue_90d_amount", ascending=False)
    if not credit_risk.empty:
        alert_count += 1
        total_overdue = credit_risk.overdue_90d_amount.sum()
        st.markdown(alert_card(
            "#FEF2F2", "#FECACA", "#991B1B",
            f"🚨 Credit Exposure: {naira(total_overdue)} overdue (90+ days)",
            f"{len(credit_risk)} customer(s) with unpaid delivered orders."
        ), unsafe_allow_html=True)
        for _, r in credit_risk.head(5).iterrows():
            st.markdown(
                f"&nbsp;&nbsp;&nbsp;**{r.business_name}** — "
                f"{naira(r.overdue_90d_amount)} overdue · {int(r.overdue_90d_orders)} orders"
            )
        st.markdown("")

    freq = run_query("""
        SELECT business_name, frequency_status, avg_gap_last_90d, avg_gap_prior
        FROM gold.fact_gosource_order_frequency
        WHERE frequency_status IN ('Slowing Down', 'Cooling Off')
        ORDER BY avg_gap_last_90d DESC
    """)
    if not freq.empty:
        alert_count += 1
        st.markdown(alert_card(
            "#FFFBEB", "#FDE68A", "#92400E",
            f"⚠️ {len(freq)} Customers Slowing Down",
            "Order gaps widening — early churn signal."
        ), unsafe_allow_html=True)
        for _, r in freq.iterrows():
            gap = ""
            if pd.notna(r.get("avg_gap_prior")) and pd.notna(r.get("avg_gap_last_90d")):
                gap = f" (gap: {int(r.avg_gap_prior)}d → {int(r.avg_gap_last_90d)}d)"
            st.markdown(f"&nbsp;&nbsp;&nbsp;**{r.business_name}** — {r.frequency_status}{gap}")
        st.markdown("")

    active_risk = health[
        (health.health_status.isin(["At Risk", "Critical"]))
        & (health.orders_last_30d > 0)
    ].sort_values("orders_last_30d", ascending=False).head(8)
    if not active_risk.empty:
        alert_count += 1
        st.markdown(alert_card(
            "#FFF7ED", "#FED7AA", "#9A3412",
            f"🔔 {len(active_risk)} Active Customers at Risk",
            "Still ordering but health signals failing."
        ), unsafe_allow_html=True)
        for _, r in active_risk.iterrows():
            r_max = int(r.score_max) if "score_max" in health.columns else score_max
            failing = [label for col, (label, _) in SIGNALS.items()
                       if col in health.columns and r.get(col) == 0]
            st.markdown(
                f"&nbsp;&nbsp;&nbsp;**{r.business_name}** — {r.health_score}/{r_max} · "
                f"Failing: _{', '.join(failing)}_"
            )
        st.markdown("")

if alert_count == 0:
    st.success("No active alerts — all signals looking good!")
