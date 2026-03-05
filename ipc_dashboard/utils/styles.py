import streamlit as st

# ─── Global CSS injected at the top of every page ────────────────────────────
GLOBAL_CSS = """
<style>
/* ── App shell ── */
.stApp { background-color: #F1F5F9; }
.block-container { padding-top: 1.5rem !important; max-width: 1400px; }

/* ── Metric cards ── */
div[data-testid="metric-container"] {
    background-color: #FFFFFF;
    border: 1px solid #E2E8F0;
    border-radius: 12px;
    padding: 18px 22px !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    min-height: 90px;
}
[data-testid="stMetricValue"] {
    font-size: 22px !important;
    font-weight: 700 !important;
    color: #0F172A !important;
}
[data-testid="stMetricLabel"] {
    font-size: 11px !important;
    font-weight: 600 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.7px !important;
    color: #64748B !important;
}
[data-testid="stMetricDelta"] > div {
    font-size: 12px !important;
    font-weight: 500 !important;
}

/* ── Sidebar dark theme ── */
section[data-testid="stSidebar"] {
    background-color: #0F172A !important;
    border-right: 1px solid #1E293B;
}
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stDateInput label,
section[data-testid="stSidebar"] p,
section[data-testid="stSidebar"] span,
section[data-testid="stSidebar"] div { color: #CBD5E1 !important; }
section[data-testid="stSidebar"] h2 { color: #F1F5F9 !important; }
section[data-testid="stSidebar"] [data-baseweb="select"] {
    background-color: #1E293B !important;
    border-color: #334155 !important;
}
section[data-testid="stSidebar"] [data-baseweb="select"] *,
section[data-testid="stSidebar"] .stDateInput input {
    color: #E2E8F0 !important;
    background-color: #1E293B !important;
}
section[data-testid="stSidebar"] hr { border-color: #1E293B !important; }

/* ── Headings ── */
h1 { color: #0F172A !important; font-weight: 800 !important; font-size: 26px !important; }
h3 { font-size: 13px !important; font-weight: 700 !important; text-transform: uppercase !important;
     letter-spacing: 0.6px !important; color: #64748B !important; margin: 0 0 12px !important; }

/* ── Dividers ── */
hr { border: none !important; border-top: 1px solid #E2E8F0 !important; margin: 24px 0 !important; }

/* ── DataTable ── */
.stDataFrame { border-radius: 10px; overflow: hidden; border: 1px solid #E2E8F0 !important; }
[data-testid="stDataFrameResizable"] { border-radius: 10px; }

/* ── Download button ── */
.stDownloadButton button {
    background-color: #EFF6FF !important;
    color: #2563EB !important;
    border: 1px solid #BFDBFE !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 13px !important;
}

/* ── Expander ── */
details { border: 1px solid #E2E8F0 !important; border-radius: 10px !important;
          background: white !important; padding: 4px; }
</style>
"""


def inject_css():
    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)


def page_header(title: str, subtitle: str = ""):
    st.markdown(
        f"""<div style="background:linear-gradient(135deg,#0F172A 0%,#1E3A5F 100%);
            padding:28px 32px;border-radius:14px;margin-bottom:28px;
            box-shadow:0 4px 16px rgba(0,0,0,0.18);">
            <div style="font-size:22px;font-weight:800;color:#F8FAFC;letter-spacing:-0.3px;">{title}</div>
            {f'<div style="font-size:13px;color:#94A3B8;margin-top:4px;">{subtitle}</div>' if subtitle else ''}
        </div>""",
        unsafe_allow_html=True,
    )


def kpi_card(label: str, value: str, delta_pct: float = None,
             good_direction: str = "up", icon: str = "", subtext: str = "") -> str:
    """
    Returns HTML for a standalone KPI card.
    good_direction: 'up' = positive delta is green; 'down' = positive delta is red
    """
    delta_html = ""
    if delta_pct is not None:
        if delta_pct > 0:
            color = "#22C55E" if good_direction == "up" else "#EF4444"
            arrow = "▲"
        elif delta_pct < 0:
            color = "#EF4444" if good_direction == "up" else "#22C55E"
            arrow = "▼"
        else:
            color = "#94A3B8"; arrow = "→"
        delta_html = (
            f'<div style="font-size:12px;font-weight:500;color:{color};margin-top:2px;">'
            f'{arrow} {abs(delta_pct):.1f}% vs prev period</div>'
        )

    sub_html = (
        f'<div style="font-size:11px;color:#94A3B8;margin-top:3px;">{subtext}</div>'
        if subtext else ""
    )

    return f"""
    <div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;
                padding:18px 22px;box-shadow:0 1px 4px rgba(0,0,0,0.06);height:100%;">
        <div style="font-size:11px;font-weight:600;text-transform:uppercase;
                    letter-spacing:0.7px;color:#64748B;">{icon} {label}</div>
        <div style="font-size:24px;font-weight:700;color:#0F172A;margin:6px 0 2px;">{value}</div>
        {delta_html}
        {sub_html}
    </div>"""


def runway_card(months: float) -> str:
    if months is None or months <= 0:
        color, status, label = "#6B7280", "Unknown", "—"
    elif months < 3:
        color, status = "#EF4444", "CRITICAL"
        label = f"{months:.1f} months"
    elif months < 6:
        color, status = "#F59E0B", "WARNING"
        label = f"{months:.1f} months"
    else:
        color, status = "#22C55E", "HEALTHY"
        label = f"{months:.1f} months"

    return f"""
    <div style="background:{color}12;border:2px solid {color}40;border-radius:12px;
                padding:20px 24px;text-align:center;">
        <div style="font-size:11px;font-weight:700;text-transform:uppercase;
                    letter-spacing:0.8px;color:{color};">🛣️ CASH RUNWAY · {status}</div>
        <div style="font-size:40px;font-weight:800;color:{color};margin:8px 0 4px;">{label}</div>
        <div style="font-size:12px;color:#64748B;">at current burn rate</div>
    </div>"""


def section_title(title: str):
    st.markdown(f"<h3>{title}</h3>", unsafe_allow_html=True)


CHART_LAYOUT = dict(
    plot_bgcolor="white",
    paper_bgcolor="white",
    font=dict(family="Inter, sans-serif", size=12, color="#374151"),
    margin=dict(t=24, b=8, l=8, r=8),
    hoverlabel=dict(bgcolor="white", bordercolor="#E2E8F0", font_size=13),
    xaxis=dict(showgrid=False, tickfont=dict(size=11)),
    yaxis=dict(gridcolor="#F1F5F9", tickfont=dict(size=11)),
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
                font=dict(size=12)),
)

COLOR_DAASH    = "#B91C1C"
COLOR_GOSOURCE = "#22C55E"
COLOR_POSITIVE = "#22C55E"
COLOR_NEGATIVE = "#EF4444"
COLOR_CASH     = "#0891B2"
COLOR_NEUTRAL  = "#64748B"
