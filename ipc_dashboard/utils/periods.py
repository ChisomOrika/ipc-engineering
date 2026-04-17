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
            ["Month to Date", "This Month", "Last 30 Days", "Last 90 Days",
             "This Year", "Last 12 Months", "All Time", "Custom Range"],
            index=4,
            key="period_select",
        )

        today = dt.date.today()

        if period == "Month to Date":
            start = today.replace(day=1)
            end   = today
        elif period == "This Month":
            import calendar
            start = today.replace(day=1)
            last_day = calendar.monthrange(today.year, today.month)[1]
            end   = today.replace(day=last_day)
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

        business_unit = st.selectbox(
            "Business Unit",
            ["Combined", "IPC", "GoSource"],
            index=0,
            key="bu_filter",
        )

        service_lines = None
        if extra_filters:
            service_lines = st.multiselect(
                "Service Line",
                ["DAASH", "GoSource"],
                default=["DAASH", "GoSource"],
                key="svc_filter",
            )

        st.markdown("---")
        refresh_time = dt.datetime.now().strftime("%d %b %Y, %H:%M")
        st.markdown(
            f"<div style='font-size:11px;color:#94A3B8;'>"
            f"📅 <b>{start.strftime('%d %b %Y')}</b> → <b>{end.strftime('%d %b %Y')}</b><br><br>"
            f"🕐 <b style='color:#CBD5E1;'>Last refreshed</b><br>"
            f"<span style='color:#E2E8F0;font-weight:600;'>{refresh_time}</span><br>"
            f"<span style='color:#64748B;'>⟳ Refreshes every 10 minutes</span></div>",
            unsafe_allow_html=True,
        )

    return start, end, prev_start, prev_end, period, service_lines, business_unit


def svc_filter_sql(service_lines) -> str:
    """Return SQL AND clause for service line filter, or empty string."""
    if not service_lines or len(service_lines) == 2:
        return ""
    if len(service_lines) == 1:
        return f"AND service_line = '{service_lines[0]}'"
    return ""


def bu_filter_sql(business_unit, col="business_unit") -> str:
    """Return SQL AND clause for business unit filter, or empty string."""
    if not business_unit or business_unit == "Combined":
        return ""
    return f"AND {col} = '{business_unit}'"
