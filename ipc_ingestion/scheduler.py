"""
IPC Analytics Ingestion Scheduler
----------------------------------
Runs all incremental ingestion scripts at 07:00, 14:00, and 19:00 (WAT) every day.

Scripts run sequentially in this order:
  1. dash_incremental_load.py      — DAASH MongoDB orders/customers
  2. gosource_incremental_load.py  — GoSource MongoDB orders/receipts
  3. lenco_incremental_load.py     — Lenco bank transactions
  4. paystack_incremental_load.py  — Paystack payment transactions
  5. 9jaypay_incremental_load.py   — 9japay (GoSource) payment transactions

Usage:
  python scheduler.py

To run in the background:
  nohup python scheduler.py > scheduler.log 2>&1 &
"""

import subprocess
import sys
import os
import logging
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scheduler.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Script definitions — order matters (run sequentially)
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SCRIPTS = [
    {"name": "DAASH",    "path": os.path.join(BASE_DIR, "dash_incremental_load.py")},
    {"name": "GoSource", "path": os.path.join(BASE_DIR, "gosource_incremental_load.py")},
    {"name": "Lenco",    "path": os.path.join(BASE_DIR, "lenco_incremental_load.py")},
    {"name": "Paystack", "path": os.path.join(BASE_DIR, "paystack_incremental_load.py")},
    {"name": "9japay",   "path": os.path.join(BASE_DIR, "9japay_incremental_load.py")},
]


# ---------------------------------------------------------------------------
# Run all scripts sequentially
# ---------------------------------------------------------------------------

def run_all_scripts():
    run_start = datetime.now()
    log.info("=" * 60)
    log.info(f"Scheduled run started at {run_start.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    results = {}

    for script in SCRIPTS:
        name = script["name"]
        path = script["path"]

        if not os.path.exists(path):
            log.warning(f"[{name}] Script not found: {path} — skipping")
            results[name] = "skipped"
            continue

        log.info(f"[{name}] Starting...")
        script_start = datetime.now()

        try:
            result = subprocess.run(
                [sys.executable, path],
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour max per script
            )

            elapsed = round((datetime.now() - script_start).total_seconds(), 1)

            if result.returncode == 0:
                log.info(f"[{name}] Completed successfully in {elapsed}s")
                results[name] = "success"
            else:
                log.error(f"[{name}] Failed (exit code {result.returncode}) in {elapsed}s")
                if result.stderr:
                    # Log last 20 lines of stderr to avoid flooding
                    stderr_lines = result.stderr.strip().splitlines()
                    log.error(f"[{name}] stderr (last 20 lines):\n" + "\n".join(stderr_lines[-20:]))
                results[name] = "failed"

        except subprocess.TimeoutExpired:
            log.error(f"[{name}] Timed out after 3600s — killed")
            results[name] = "timeout"

        except Exception as e:
            log.error(f"[{name}] Unexpected error: {e}")
            results[name] = "error"

    # Run dbt transformations after ingestion
    dbt_dir = os.path.join(os.path.dirname(BASE_DIR), "ipc_transform")
    if os.path.isdir(dbt_dir):
        log.info("[dbt] Running transformations...")
        dbt_start = datetime.now()
        try:
            dbt_result = subprocess.run(
                [sys.executable, "-m", "dbt", "run", "--profiles-dir", "."],
                capture_output=True, text=True, timeout=3600, cwd=dbt_dir,
            )
            dbt_elapsed = round((datetime.now() - dbt_start).total_seconds(), 1)
            if dbt_result.returncode == 0:
                log.info(f"[dbt] Completed in {dbt_elapsed}s")
                results["dbt"] = "success"
            else:
                log.error(f"[dbt] Failed (exit code {dbt_result.returncode}) in {dbt_elapsed}s")
                if dbt_result.stderr:
                    log.error(f"[dbt] stderr:\n" + "\n".join(dbt_result.stderr.strip().splitlines()[-20:]))
                results["dbt"] = "failed"
        except subprocess.TimeoutExpired:
            log.error("[dbt] Timed out after 3600s")
            results["dbt"] = "timeout"
        except Exception as e:
            log.error(f"[dbt] Unexpected error: {e}")
            results["dbt"] = "error"

    # Summary
    total = round((datetime.now() - run_start).total_seconds(), 1)
    log.info("-" * 60)
    log.info(f"Run complete in {total}s. Results:")
    for name, status in results.items():
        icon = "OK" if status == "success" else "!!"
        log.info(f"  [{icon}] {name}: {status}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Scheduler — 07:00, 14:00, 19:00 WAT (UTC+1)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    scheduler = BlockingScheduler(timezone="Africa/Lagos")  # WAT = UTC+1

    # 07:00 WAT
    scheduler.add_job(
        run_all_scripts,
        trigger=CronTrigger(hour=7, minute=0),
        id="morning_run",
        name="Morning ingestion (07:00 WAT)",
        misfire_grace_time=600,  # Allow up to 10 min late start
    )

    # 14:00 WAT
    scheduler.add_job(
        run_all_scripts,
        trigger=CronTrigger(hour=14, minute=0),
        id="afternoon_run",
        name="Afternoon ingestion (14:00 WAT)",
        misfire_grace_time=600,
    )

    # 19:00 WAT
    scheduler.add_job(
        run_all_scripts,
        trigger=CronTrigger(hour=19, minute=0),
        id="evening_run",
        name="Evening ingestion (19:00 WAT)",
        misfire_grace_time=600,
    )

    log.info("Scheduler started. Jobs:")
    for job in scheduler.get_jobs():
        log.info(f"  - {job.name} | next run: {job.next_run_time}")
    log.info("Press Ctrl+C to stop.")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped.")
