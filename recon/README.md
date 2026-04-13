# IPC Reconciliation

Daily check that the warehouse matches each source system. Runs against yesterday's data, emails Chisom on completion (success or mismatch).

## What it covers (today)
- **Paystack** ✅ — `bv.bv_paystack_transactions` vs Paystack `/transaction` API. Validated working: 24 txns / ₦390,700 matched exactly on first run.

## What's TODO (skipped in current report)
- **Lenco** — API ignores `from`/`to` params; returned 16K+ all-time txns. Needs recent-page-pull + client-side date filter, OR weekly all-time-volume comparison.
- **9japay** — API uses `page-size`/`page-number` (no date filter). Same pattern needed as Lenco.
- **Dash & GoSource (MongoDB)** — different shape entirely; needs a separate puller using pymongo.

## Run locally
```bash
cd ~/ipc-engineering
pip install -r recon/requirements.txt
python recon/reconcile.py
```

Reports land in `recon/reports/recon_YYYY-MM-DD.{md,json}`.

Without `SMTP_USER`/`SMTP_PASSWORD` set, the script just prints + writes files. Set them to also email.

## Email setup (Gmail)
1. Enable 2FA on the Gmail account
2. Create an app password: https://myaccount.google.com/apppasswords
3. Set in `.env` (local) or GitHub repo secrets (production):
   - `SMTP_USER` = your gmail address
   - `SMTP_PASSWORD` = the 16-char app password (no spaces)
   - `SMTP_TO` (optional, defaults to chisomorika@gmail.com)

## Schedule
Runs daily at 06:00 Lagos time via `.github/workflows/recon-daily.yml`. Exits nonzero on any mismatch so GitHub also flags the run as failed (belt + braces with the email).

## Tolerance
A source is considered matched if both count and amount differ by ≤ 0.1% (handles tiny rounding/timezone edge cases). Tweak `TOLERANCE` in `reconcile.py`.
