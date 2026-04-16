import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import datetime as dt
import glob
import json
import subprocess
from pathlib import Path

import pandas as pd
import streamlit as st
import altair as alt

from utils.db     import run_query
from utils.fmt    import naira, count
from utils.styles import inject_css, page_header, section_title

st.set_page_config(page_title="Data Health · IPC", page_icon="🩺", layout="wide")
inject_css()

# ─── Paths ────────────────────────────────────────────────────────────────────
REPO_ROOT   = Path(__file__).resolve().parents[2]
RECON_DIR   = REPO_ROOT / "recon" / "reports"

# Primary "freshness" tables per source
SOURCE_TABLES = {
    "paystack": ("bv.bv_paystack_transactions", "transaction_created_at_date_time"),
    "lenco":    ("bv.bv_lenco_transactions",    "transaction_created_at_date_time"),
    "9japay":   ("bv.bv_9japay_transactions",   "transaction_created_at_date_time"),
    "dash":     ("bv.bv_dash_orders",           "order_created_at_date_time"),
    "gosource": ("bv.bv_gosource_orders",       "order_created_at_date"),
}

# ─── Helpers ─────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def load_recon_reports(limit: int = 7) -> list[dict]:
    """Load the most recent N recon JSON reports, newest first."""
    files = sorted(glob.glob(str(RECON_DIR / "recon_*.json")), reverse=True)[:limit]
    reports = []
    for fp in files:
        try:
            with open(fp) as f:
                reports.append(json.load(f))
        except Exception as e:
            reports.append({"day": Path(fp).stem.replace("recon_", ""),
                            "results": [], "_error": str(e)})
    return reports


@st.cache_data(ttl=300, show_spinner="checking warehouse freshness…")
def fetch_source_freshness() -> pd.DataFrame:
    """max(date) per source from warehouse. Safe-handles missing tables."""
    rows = []
    for src, (tbl, col) in SOURCE_TABLES.items():
        try:
            df = run_query(f"select max({col}) as last_seen from {tbl}")
            last = df.iloc[0]["last_seen"] if not df.empty else None
        except Exception as e:
            last = None
        rows.append({"source": src, "table": tbl, "last_seen": last})
    return pd.DataFrame(rows)


def _hours_ago(ts) -> float | None:
    if ts is None or pd.isna(ts):
        return None
    now = dt.datetime.now()
    if isinstance(ts, dt.date) and not isinstance(ts, dt.datetime):
        ts = dt.datetime.combine(ts, dt.time.min)
    if isinstance(ts, pd.Timestamp):
        ts = ts.to_pydatetime()
    # strip tz for naive comparison
    if getattr(ts, "tzinfo", None):
        ts = ts.replace(tzinfo=None)
    return (now - ts).total_seconds() / 3600.0


def _fmt_ago(hours: float | None) -> str:
    if hours is None:
        return "—"
    if hours < 1:
        return f"{int(hours * 60)}m ago"
    if hours < 48:
        return f"{hours:.1f}h ago"
    return f"{hours / 24:.1f}d ago"


def _pipeline_runs(limit: int = 5) -> list[dict]:
    try:
        out = subprocess.run(
            ["gh", "run", "list", "--workflow=run-pipeline.yml",
             "--limit", str(limit), "--json",
             "status,conclusion,displayTitle,createdAt,url,headBranch"],
            cwd=str(REPO_ROOT),
            capture_output=True, text=True, timeout=10,
        )
        if out.returncode != 0:
            return []
        return json.loads(out.stdout or "[]")
    except Exception:
        return []


# ─── Header ──────────────────────────────────────────────────────────────────
now = dt.datetime.now()
page_header(
    "Data Health",
    f"Is the data trustworthy? · last refreshed {now.strftime('%d %b %Y, %H:%M')}"
)

# ─── Load recon data ─────────────────────────────────────────────────────────
reports = load_recon_reports(limit=7)
latest  = reports[0] if reports else None

today     = dt.date.today()
yesterday = today - dt.timedelta(days=1)

latest_day = None
if latest:
    try:
        latest_day = dt.date.fromisoformat(latest["day"])
    except Exception:
        latest_day = None

fresh_report = latest_day in (today, yesterday) if latest_day else False
all_matched  = bool(latest) and all(
    r.get("comparison", {}).get("matched", False) for r in latest.get("results", [])
)
overall_green = fresh_report and all_matched

# ─── Section 1: Overall status ───────────────────────────────────────────────
if overall_green:
    color, icon, headline = "#22C55E", "✅", "All systems matched"
    subtext = (f"Latest recon report for {latest_day.isoformat()} shows every source "
               f"reconciled cleanly.")
elif not latest:
    color, icon, headline = "#EF4444", "⚠️", "No reconciliation reports found"
    subtext = "No JSON reports under recon/reports/. Run the reconcile job to get signal."
elif not fresh_report:
    color, icon, headline = "#EF4444", "⚠️", "Stale reconciliation"
    subtext = (f"Latest report is from {latest_day.isoformat() if latest_day else '?'} "
               f"(>1 day old). Daily recon may be failing.")
else:
    n_mismatch = sum(
        1 for r in latest.get("results", [])
        if not r.get("comparison", {}).get("matched", False)
    )
    color, icon, headline = "#EF4444", "⚠️", f"{n_mismatch} source(s) mismatched"
    subtext = f"Latest recon ({latest_day.isoformat()}) reports mismatches — review below."

st.markdown(
    f"""<div style="background:{color}14;border:2px solid {color}55;border-radius:14px;
                padding:28px 32px;margin-bottom:26px;">
        <div style="font-size:40px;line-height:1;">{icon}</div>
        <div style="font-size:22px;font-weight:800;color:{color};margin-top:8px;">{headline}</div>
        <div style="font-size:13px;color:#475569;margin-top:6px;">{subtext}</div>
    </div>""",
    unsafe_allow_html=True,
)

# ─── Section 2: Latest reconciliation by source ──────────────────────────────
section_title("LATEST RECONCILIATION BY SOURCE")

if not latest or not latest.get("results"):
    st.info("Not yet monitored — no recon results for the latest report.")
else:
    rows = []
    for r in latest["results"]:
        comp = r.get("comparison") or {}
        src_t = r.get("source_totals") or {}
        wh_t  = r.get("warehouse_totals") or {}
        matched = comp.get("matched", False)
        rows.append({
            "Source":          r.get("source", "?"),
            "Mode":            r.get("mode", "—"),
            "Status":          "✅ matched" if matched else "⚠️ mismatch",
            "Source Count":    src_t.get("count"),
            "Warehouse Count": wh_t.get("count"),
            "Count Diff":      comp.get("count_diff"),
            "Amount Diff":     comp.get("amount_diff"),
            "Last Checked":    latest.get("day", "—"),
            "_matched":        matched,
        })
    df = pd.DataFrame(rows)

    display = df.drop(columns=["_matched"]).reset_index(drop=True).copy()
    matched_flags = df["_matched"].reset_index(drop=True).tolist()
    display["Source Count"]    = display["Source Count"].apply(
        lambda v: count(v) if v is not None else "—")
    display["Warehouse Count"] = display["Warehouse Count"].apply(
        lambda v: count(v) if v is not None else "—")
    display["Count Diff"]      = display["Count Diff"].apply(
        lambda v: f"{int(v):+,}" if v is not None else "—")
    display["Amount Diff"]     = display["Amount Diff"].apply(
        lambda v: naira(v) if v is not None else "—")

    def _style_table(d):
        styles = pd.DataFrame("", index=d.index, columns=d.columns)
        for i, matched in enumerate(matched_flags):
            if not matched and i < len(styles):
                styles.iloc[i, :] = "background-color:#FEF2F2;color:#B91C1C;"
        return styles

    styled = display.style.apply(_style_table, axis=None)
    st.dataframe(styled, use_container_width=True, hide_index=True)

    # notes
    notes = [r for r in latest["results"] if r.get("note")]
    if notes:
        with st.expander("📝 reconciler notes"):
            for r in notes:
                st.markdown(f"**{r['source']}** — {r['note']}")

st.markdown("---")

# ─── Section 3: Source freshness ─────────────────────────────────────────────
section_title("SOURCE FRESHNESS (WAREHOUSE)")

fresh_df = fetch_source_freshness()
fresh_df["hours_ago"] = fresh_df["last_seen"].apply(_hours_ago)

cols = st.columns(len(fresh_df))
for col, (_, row) in zip(cols, fresh_df.iterrows()):
    h = row["hours_ago"]
    if h is None:
        color, label = "#94A3B8", "not yet monitored"
    elif h > 24:
        color, label = "#EF4444", _fmt_ago(h)
    elif h > 6:
        color, label = "#F59E0B", _fmt_ago(h)
    else:
        color, label = "#22C55E", _fmt_ago(h)

    last_str = "—" if row["last_seen"] is None or pd.isna(row["last_seen"]) \
               else pd.to_datetime(row["last_seen"]).strftime("%d %b %H:%M")

    col.markdown(
        f"""<div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;
                    padding:16px 18px;box-shadow:0 1px 4px rgba(0,0,0,0.06);">
            <div style="font-size:11px;font-weight:600;text-transform:uppercase;
                        letter-spacing:0.7px;color:#64748B;">{row['source']}</div>
            <div style="font-size:20px;font-weight:700;color:{color};margin:6px 0 2px;">{label}</div>
            <div style="font-size:11px;color:#94A3B8;">last: {last_str}</div>
        </div>""",
        unsafe_allow_html=True,
    )

st.caption("🟢 ≤6h · 🟡 6–24h · 🔴 >24h stale")

st.markdown("---")

# ─── Section 4: Recon history (last 7 days) ──────────────────────────────────
section_title("RECON HISTORY — LAST 7 REPORTS")

if not reports:
    st.info("No recon reports to chart yet.")
else:
    hist_rows = []
    for rep in reports:
        day = rep.get("day")
        results = rep.get("results", [])
        matched   = sum(1 for r in results
                        if r.get("comparison", {}).get("matched", False))
        mismatch  = sum(1 for r in results
                        if not r.get("comparison", {}).get("matched", False))
        hist_rows.append({"day": day, "status": "matched",  "n": matched})
        hist_rows.append({"day": day, "status": "mismatch", "n": mismatch})
    hist = pd.DataFrame(hist_rows)

    chart = (
        alt.Chart(hist)
        .mark_bar()
        .encode(
            x=alt.X("day:O", title="report day", sort=None),
            y=alt.Y("n:Q", title="# sources", stack="zero"),
            color=alt.Color(
                "status:N",
                scale=alt.Scale(
                    domain=["matched", "mismatch"],
                    range=["#22C55E", "#EF4444"],
                ),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=["day", "status", "n"],
        )
        .properties(height=240)
    )
    st.altair_chart(chart, use_container_width=True)

st.markdown("---")

# ─── Section 5: Pipeline runs (optional) ─────────────────────────────────────
section_title("RECENT PIPELINE RUNS")

runs = _pipeline_runs(limit=5)
if not runs:
    st.caption("gh cli not available or no runs found — skipping.")
else:
    run_rows = []
    for r in runs:
        concl = r.get("conclusion") or r.get("status") or "—"
        icon  = {"success": "✅", "failure": "❌", "cancelled": "⚪",
                 "in_progress": "🔄"}.get(concl, "•")
        created = r.get("createdAt", "")
        try:
            created = pd.to_datetime(created).strftime("%d %b %H:%M")
        except Exception:
            pass
        run_rows.append({
            "Status":  f"{icon} {concl}",
            "Title":   r.get("displayTitle", ""),
            "Branch":  r.get("headBranch", ""),
            "Started": created,
            "URL":     r.get("url", ""),
        })
    st.dataframe(pd.DataFrame(run_rows), use_container_width=True, hide_index=True)

st.caption(f"Data Health · cache TTL 5 min · reports from {RECON_DIR}")
