"""
IPC Daily Reconciliation
========================

For each source system, pull yesterday's transaction count + sum directly
from source, query the same window from the warehouse, compare. Flag any
mismatch. Email Chisom + write a JSON/Markdown report to disk.

Sources covered (REST APIs):
    - Paystack
    - Lenco
    - 9japay

MongoDB sources (Dash, GoSource) handled in a separate module — different
query shape.

Environment variables required:
    PG_HOST, PG_PORT, PG_USER, PG_PASSWORD                — warehouse
    PAYSTACK_SECRET_KEY                                   — Paystack
    LENCO_API_TOKEN                                       — Lenco
    9JAPAY_SECRET, 9JAPAY_API_KEY                         — 9japay

Optional (for email — silent skip if absent):
    SMTP_HOST (default smtp.gmail.com)
    SMTP_PORT (default 587)
    SMTP_USER, SMTP_PASSWORD                              — credentials
    SMTP_TO   (default chisomorika@gmail.com)
"""

import os
import json
import logging
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone
from pathlib import Path

import time

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import psycopg2
from dotenv import load_dotenv


def _session() -> requests.Session:
    """Session with retries on transient errors (DNS, timeout, 5xx)."""
    s = requests.Session()
    retry = Retry(
        total=2,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


HTTP = _session()
TIMEOUT = 30

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("recon")

REPORT_DIR = Path(__file__).parent / "reports"
REPORT_DIR.mkdir(exist_ok=True)

# Anything strictly greater than this fraction is flagged as a mismatch.
TOLERANCE = 0.001  # 0.1%


# ---------------------------------------------------------------------------
# Warehouse query helpers
# ---------------------------------------------------------------------------

def warehouse_conn():
    return psycopg2.connect(
        host=os.environ["PG_HOST"],
        port=int(os.environ["PG_PORT"]),
        user=os.environ["PG_USER"],
        password=os.environ["PG_PASSWORD"],
        dbname="PROD_ANALYTICS_DB",
    )


def warehouse_totals(table: str, date_col: str, day: str) -> dict:
    """Count + amount sum for a single UTC day in the warehouse."""
    sql = f"""
        select count(*)::bigint, coalesce(sum(transaction_amount::numeric), 0)::numeric
        from {table}
        where {date_col}::date = %s
    """
    with warehouse_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (day,))
        n, total = cur.fetchone()
    return {"count": int(n), "total_amount": float(total or 0)}


def warehouse_alltime(table: str) -> dict:
    """Count + amount sum across all time — used where source API only exposes summary totals."""
    sql = f"""
        select count(*)::bigint, coalesce(sum(transaction_amount::numeric), 0)::numeric
        from {table}
    """
    with warehouse_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        n, total = cur.fetchone()
    return {"count": int(n), "total_amount": float(total or 0)}


# ---------------------------------------------------------------------------
# Source pullers (one per system)
# ---------------------------------------------------------------------------

def paystack_totals(day: str) -> dict:
    """Pull all Paystack transactions for a single day (all statuses, to match warehouse)."""
    headers = {"Authorization": f"Bearer {os.environ['PAYSTACK_SECRET_KEY']}"}
    start = f"{day}T00:00:00Z"
    end = f"{day}T23:59:59Z"

    count = 0
    total_kobo = 0
    page = 1
    while True:
        r = HTTP.get(
            "https://api.paystack.co/transaction",
            headers=headers,
            params={"from": start, "to": end, "perPage": 100, "page": page},
            timeout=TIMEOUT,
        )
        r.raise_for_status()
        body = r.json()
        rows = body.get("data") or []
        if not rows:
            break
        count += len(rows)
        total_kobo += sum(int(t.get("amount", 0)) for t in rows)
        meta = body.get("meta") or {}
        if page >= int(meta.get("pageCount", 1) or 1):
            break
        page += 1

    return {"count": count, "total_amount": total_kobo / 100.0}  # naira


def lenco_alltime_summary(_day: str) -> dict:
    """Lenco: pull a tiny page, read all-time totals from response meta.

    Lenco's /transactions response includes data.meta.total (count of all txns).
    For amount we'd have to paginate every page, so we accept count-only here
    and rely on warehouse amount as the reference. This is a freshness/completeness
    check ('does warehouse have all transactions source sees').
    """
    r = HTTP.get(
        "https://api.lenco.ng/access/v1/transactions",
        headers={"Authorization": os.environ["LENCO_API_TOKEN"]},
        params={"page": 1, "perPage": 1},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    body = r.json()
    meta = (body.get("data") or {}).get("meta") or {}
    total = int(meta.get("total", 0))
    # Amount not available as a summary — marker None means "skip amount diff"
    return {"count": total, "total_amount": None}


def ninepay_alltime_summary(_day: str) -> dict:
    """9japay: response includes top-level totalCount + totalCreditSum.

    Note: 9japay returns separate credit and debit entries for each transaction;
    totalCount counts both. The warehouse stores both entries too (verified:
    warehouse ~81k rows matches API totalCount ~81k), so no dedup needed.
    """
    r = HTTP.get(
        "https://developer.9japay.com/v1/api/transactions",
        headers={
            "secret":  os.environ["9JAPAY_SECRET"],
            "api-key": os.environ["9JAPAY_API_KEY"],
        },
        params={"page-number": 1, "page-size": 1},
        timeout=TIMEOUT,
        verify=False,
    )
    r.raise_for_status()
    body = r.json()
    # totalCreditSum and totalDebitSum are equal (ledger self-balances).
    # Use credit side to match one row per "transaction amount" — but since
    # warehouse stores BOTH sides, we double it to match scale.
    total_count  = int(body.get("totalCount") or 0)
    credit_sum   = float(body.get("totalCreditSum") or 0)
    debit_sum    = float(body.get("totalDebitSum") or 0)
    return {"count": total_count, "total_amount": credit_sum + debit_sum}


# ---------------------------------------------------------------------------
# Source registry: name → (source puller, warehouse table, warehouse date col)
# ---------------------------------------------------------------------------

# Each source's recon shape:
#   mode 'daily'   — per-day count+sum compared to warehouse date slice
#   mode 'alltime' — count+sum (or count only) compared to warehouse total
#
# Daily catches fresh-data issues the fastest; all-time catches long-run drift
# (missing historical rows). Both are useful signals. Source's native API
# capability determines which we use.
SOURCES = {
    "paystack": {
        "mode":     "daily",
        "puller":   paystack_totals,
        "table":    "bv.bv_paystack_transactions",
        "date_col": "transaction_created_at_date_time",
    },
    "lenco": {
        "mode":     "alltime",
        "puller":   lenco_alltime_summary,
        "table":    "bv.bv_lenco_transactions",
        "note":     "API summary exposes total count only (not amount). Amount diff skipped.",
    },
    "9japay": {
        "mode":     "alltime",
        "puller":   ninepay_alltime_summary,
        "table":    "bv.bv_9japay_transactions",
        "note":     "9japay ledger returns credit+debit rows; summed accordingly.",
    },
}

DISABLED_SOURCES = {
    # Dash + GoSource (MongoDB) planned next.
}


# ---------------------------------------------------------------------------
# Compare + report
# ---------------------------------------------------------------------------

def compare(source: dict, warehouse: dict) -> dict:
    s_count, w_count = source["count"], warehouse["count"]
    s_amt, w_amt     = source["total_amount"], warehouse["total_amount"]

    count_diff = w_count - s_count
    count_pct  = (abs(count_diff) / s_count) if s_count else (1.0 if w_count else 0.0)
    count_ok   = count_pct <= TOLERANCE

    # Amount may be None when the source API doesn't expose it as a summary.
    if s_amt is None:
        amt_diff, amt_pct, amount_ok = None, None, True  # skip amount check
    else:
        amt_diff = w_amt - s_amt
        amt_pct  = (abs(amt_diff) / s_amt) if s_amt else (1.0 if w_amt else 0.0)
        amount_ok = amt_pct <= TOLERANCE

    return {
        "matched":     count_ok and amount_ok,
        "count_diff":  count_diff,
        "count_pct":   count_pct,
        "amount_diff": amt_diff,
        "amount_pct":  amt_pct,
    }


def reconcile_source(name: str, day: str) -> dict:
    cfg = SOURCES[name]
    mode = cfg["mode"]
    log.info(f"[{name}] pulling source ({mode})…")
    try:
        src = cfg["puller"](day)
    except Exception as e:
        log.exception(f"[{name}] source pull failed")
        return {"source": name, "day": day, "mode": mode, "error": f"source pull failed: {e}"}

    log.info(f"[{name}] querying warehouse {cfg['table']} ({mode})…")
    try:
        if mode == "daily":
            wh = warehouse_totals(cfg["table"], cfg["date_col"], day)
        else:
            wh = warehouse_alltime(cfg["table"])
    except Exception as e:
        log.exception(f"[{name}] warehouse query failed")
        return {"source": name, "day": day, "mode": mode,
                "error": f"warehouse query failed: {e}", "source_totals": src}

    return {
        "source":           name,
        "day":              day,
        "mode":             mode,
        "note":             cfg.get("note"),
        "source_totals":    src,
        "warehouse_totals": wh,
        "comparison":       compare(src, wh),
    }


# ---------------------------------------------------------------------------
# Report rendering + email
# ---------------------------------------------------------------------------

def render_markdown(results: list, day: str) -> str:
    lines = [f"# IPC Reconciliation Report — {day}", ""]
    active = [r for r in results if "skipped" not in r]
    any_mismatch = any(
        r.get("error") or not r.get("comparison", {}).get("matched", False)
        for r in active
    )
    lines.append(f"**Status:** {'⚠️ MISMATCH / ERROR' if any_mismatch else '✅ All active sources matched'}")
    lines.append("")

    for r in results:
        name = r["source"]
        lines.append(f"## {name}")
        if "skipped" in r:
            lines.append(f"- ⏸️ SKIPPED: {r['skipped']}")
            lines.append("")
            continue
        if "error" in r:
            lines.append(f"- ❌ ERROR: {r['error']}")
            if "source_totals" in r:
                lines.append(f"- Source: {r['source_totals']}")
            lines.append("")
            continue

        s, w, c = r["source_totals"], r["warehouse_totals"], r["comparison"]
        status = "✅ MATCH" if c["matched"] else "⚠️ MISMATCH"
        mode   = r.get("mode", "daily")
        lines.append(f"- {status}  ({mode})")
        if r.get("note"):
            lines.append(f"- _note: {r['note']}_")

        def _amt(v):
            return f"₦{v:>18,.2f}" if v is not None else " (not compared)    "

        lines.append(f"- Source:    {s['count']:>8,} txns   {_amt(s['total_amount'])}")
        lines.append(f"- Warehouse: {w['count']:>8,} txns   {_amt(w['total_amount'])}")
        amt_diff_str = (f"₦{c['amount_diff']:>+18,.2f}  ({c['amount_pct']*100:.2f}%)"
                        if c["amount_diff"] is not None else "(amount skipped)")
        lines.append(f"- Diff:      {c['count_diff']:>+8,} txns   {amt_diff_str}  "
                     f"[count {c['count_pct']*100:.2f}%]")
        lines.append("")

    return "\n".join(lines)


def send_email(subject: str, body: str) -> bool:
    user = os.environ.get("SMTP_USER")
    pwd  = os.environ.get("SMTP_PASSWORD")
    to   = os.environ.get("SMTP_TO", "chisomorika@gmail.com")
    host = os.environ.get("SMTP_HOST", "smtp.gmail.com")
    port = int(os.environ.get("SMTP_PORT", "587"))

    if not (user and pwd):
        log.info("SMTP_USER/SMTP_PASSWORD not set — skipping email (printed report only).")
        return False

    msg = MIMEText(body, _subtype="plain", _charset="utf-8")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = to
    with smtplib.SMTP(host, port) as s:
        s.starttls()
        s.login(user, pwd)
        s.sendmail(user, [to], msg.as_string())
    log.info(f"Sent reconciliation email to {to}")
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    day = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    log.info(f"Reconciling for {day} (UTC)")

    results = [reconcile_source(name, day) for name in SOURCES]
    for name, reason in DISABLED_SOURCES.items():
        results.append({"source": name, "day": day, "skipped": reason})

    md = render_markdown(results, day)
    print("\n" + md + "\n")

    (REPORT_DIR / f"recon_{day}.md").write_text(md, encoding="utf-8")
    (REPORT_DIR / f"recon_{day}.json").write_text(
        json.dumps({"day": day, "results": results}, default=str, indent=2),
        encoding="utf-8",
    )

    any_bad = any(
        r.get("error") or not r.get("comparison", {}).get("matched", False)
        for r in results if "skipped" not in r
    )
    subject = f"{'⚠️ MISMATCH' if any_bad else '✅ OK'} IPC reconciliation — {day}"
    send_email(subject, md)

    # Exit nonzero on mismatch so GitHub Actions surfaces it as a failed run.
    raise SystemExit(1 if any_bad else 0)


if __name__ == "__main__":
    main()
