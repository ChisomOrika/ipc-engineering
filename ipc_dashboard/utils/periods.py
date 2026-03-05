import datetime as dt
import streamlit as st


def sidebar_filters(extra_filters: bool = False):
    """
    Render period selector + optional service line filter in sidebar.
    Returns (start, end, prev_start, prev_end, period_label).
    """
    import datetime as dt

    with st.sidebar:
        st.markdown("## 🏦 IPC Finance")
        st.markdown("---")

        period = st.selectbox(
            "Time Period",
            ["Month to Date", "Last 30 Days", "Last 90 Days",
             "This Year", "Last 12 Months", "All Time", "Custom Range"],
            index=3,
            key="period_select",
        )

        today = dt.date.today()

        if period == "Month to Date":
            start = today.replace(day=1)
            end   = today
        elif period == "Last 30 Days":
            start = today - dt.timedelta(days=30)
            end   = today
        elif period == "Last 90 Days":
            start = today - dt.timedelta(days=90)
            end   = today
        elif period == "This Year":
            start = today.replace(month=1, day=1)
            end   = today
        elif period == "Last 12 Months":
            start = today - dt.timedelta(days=365)
            end   = today
        elif period == "All Time":
            start = dt.date(2020, 1, 1)
            end   = today
        else:
            start = st.date_input("From", today - dt.timedelta(days=365), key="d_start")
            end   = st.date_input("To",   today, key="d_end")

        # Previous period (same length, immediately before start)
        delta_days  = (end - start).days or 1
        prev_end    = start - dt.timedelta(days=1)
        prev_start  = prev_end - dt.timedelta(days=delta_days)

        service_lines = None
        if extra_filters:
            service_lines = st.multiselect(
                "Service Line",
                ["DAASH", "GoSource"],
                default=["DAASH", "GoSource"],
                key="svc_filter",
            )

        st.markdown("---")
        st.markdown(
            f"<div style='font-size:11px;color:#94A3B8;'>"
            f"📅 <b>{start.strftime('%d %b %Y')}</b> → <b>{end.strftime('%d %b %Y')}</b><br>"
            f"⟳ Data refreshes hourly</div>",
            unsafe_allow_html=True,
        )

    return start, end, prev_start, prev_end, period, service_lines


def svc_filter_sql(service_lines) -> str:
    """Return SQL AND clause for service line filter, or empty string."""
    if not service_lines or len(service_lines) == 2:
        return ""
    if len(service_lines) == 1:
        return f"AND service_line = '{service_lines[0]}'"
    return ""
