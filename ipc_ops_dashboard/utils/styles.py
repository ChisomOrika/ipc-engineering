import streamlit as st

COLOR_DAASH    = "#8B0000"
COLOR_GOSOURCE = "#2D5A27"
COLOR_HEALTHY  = "#22C55E"
COLOR_AT_RISK  = "#F59E0B"
COLOR_CRITICAL = "#EF4444"
COLOR_NEUTRAL  = "#64748B"

GLOBAL_CSS = """
<style>
.stApp { background-color: #FFFFFF; }
.block-container { padding-top: 1.5rem !important; max-width: 1400px; }

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

section[data-testid="stSidebar"] {
    background-color: #0F172A !important;
    border-right: 1px solid #1E293B;
}
section[data-testid="stSidebar"] .stSelectbox label,
section[data-testid="stSidebar"] .stDateInput label,
section[data-testid="stSidebar"] .stMultiSelect label,
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

h1 { color: #0F172A !important; font-weight: 800 !important; font-size: 26px !important; }
h3 { font-size: 13px !important; font-weight: 700 !important; text-transform: uppercase !important;
     letter-spacing: 0.6px !important; color: #64748B !important; margin: 0 0 12px !important; }
hr { border: none !important; border-top: 1px solid #E2E8F0 !important; margin: 24px 0 !important; }

.stDataFrame { border-radius: 10px; overflow: hidden; border: 1px solid #E2E8F0 !important; }
[data-testid="stDataFrameResizable"] { border-radius: 10px; }

.stDownloadButton button {
    background-color: #F8FAFC !important;
    color: #334155 !important;
    border: 1px solid #CBD5E1 !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 13px !important;
}

details { border: 1px solid #E2E8F0 !important; border-radius: 10px !important;
          background: white !important; padding: 4px; }

.stTabs [data-baseweb="tab-list"] { gap: 4px; }
.stTabs [data-baseweb="tab"] {
    background: white; border: 1px solid #E2E8F0; border-radius: 8px 8px 0 0;
    padding: 8px 20px; font-weight: 600; font-size: 13px;
}
.stTabs [aria-selected="true"] {
    background: #0F172A !important; color: white !important;
    border-color: #0F172A !important;
}
</style>
"""


def inject_css():
    st.markdown(GLOBAL_CSS, unsafe_allow_html=True)


def page_header(title: str, subtitle: str = "", color: str = "#0F172A"):
    st.markdown(
        f"""<div style="background:linear-gradient(135deg,{color} 0%,{color}CC 100%);
            padding:28px 32px;border-radius:14px;margin-bottom:28px;
            box-shadow:0 4px 16px rgba(0,0,0,0.18);">
            <div style="font-size:22px;font-weight:800;color:#FFFFFF;letter-spacing:-0.3px;">{title}</div>
            {f'<div style="font-size:13px;color:#FFFFFFBB;margin-top:4px;">{subtitle}</div>' if subtitle else ''}
        </div>""",
        unsafe_allow_html=True,
    )


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
