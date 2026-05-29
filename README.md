# IPC Analytics

The data backbone for IPC's business — pulls raw data from every operational system (DAASH, GoSource, Lenco, Paystack, 9japay), models it in PostgreSQL with dbt, and serves it through Streamlit dashboards and weekly/monthly PowerPoint reports.

---

## Handoff context — read this first

This repo is being handed off. The person who built it is no longer with the company. There is no one to ask follow-up questions, so everything you need to operate, debug, and extend this system should be in this file or in the files it points you to.

If something here is unclear, the answer is in the code — every business rule is in a SQL query or a Python script, and the queries are short. Read the file, don't guess.

**Start here, in this order:**
1. Read this whole README (10 min).
2. Read [`ipc_transform/README.md`](ipc_transform/README.md) for the dbt project.
3. Get the `.env` values from the IPC engineering team (see "First-time setup" below for the full list).
4. Run a smoke test (also in "First-time setup").

---

## The journey of one number

Say a customer places an order on DAASH. Here's the path that order takes before it shows up in a leadership report:

```
  DAASH MongoDB                        (the source — where the order is born)
        │
        ▼
  ipc_ingestion/dash_incremental_load.py
        │  pulls every new/changed doc and upserts it
        ▼
  Postgres: raw_dash.orders            (raw layer — schema mirrors MongoDB)
        │
        ▼
  ipc_transform/  (dbt)
        │  rv  → renames + types
        │  bv  → business logic, joins, dedup
        │  gold → the tables BI tools actually read
        ▼
  Postgres: gold.fact_revenue          (clean, unified, queryable)
        │
        ├──► Streamlit dashboards (deployed on Streamlit Community Cloud)
        └──► PowerPoint reports (generate_weekly_*.py, generate_*_monthly_*.py)
```

Everything in this repo is one of those four stages: **ingest**, **transform**, **serve a dashboard**, or **generate a report**.

---

## Top-level layout

```
ipc_analytics/
├── ipc_ingestion/          1. Pulls raw data from sources → Postgres raw schemas
├── ipc_transform/          2. dbt project: raw → gold modeling
├── ipc_dashboard/          3a. Streamlit: finance / leadership dashboard
├── ipc_management/         3b. Streamlit: revenue concentration, credit exposure (restricted)
├── ipc_customer_health/    3c. Streamlit: customer health (for CS & account mgrs, no $)
├── ipc_ops_dashboard/      3d. Streamlit: day-to-day operational metrics
├── generate_*_report.py    4. PPTX/PDF report generators (weekly + monthly)
├── recon/                  Cross-source reconciliation (e.g. 9japay vs Lenco sweeps)
├── .do/app.yaml            DigitalOcean App Platform spec for the ingestion worker
├── .github/workflows/      CI: scheduled ingestion + dbt runs
├── requirements.txt        Top-level Python deps (for ingestion + reports)
└── .env                    Secrets — never committed (DB creds, API keys)
```

---

## What each folder actually does

### `ipc_ingestion/` — get the data in

Per-source Python scripts. Two flavors:
- `*_full_load.py` — wipes and reloads everything (use once, or after big schema changes)
- `*_incremental_load.py` — pulls only what's new since the last run (this is what runs on schedule)

| Script | Source | Where it lands |
|---|---|---|
| `dash_*` | DAASH MongoDB | `raw_dash.*` |
| `gosource_*` | GoSource MongoDB | `raw_gosource.*` |
| `lenco_*` | Lenco REST API | `raw_lenco.*` |
| `paystack_*` | Paystack REST API | `raw_paystack.*` |
| `9japay_*` | 9japay REST API | `raw_9japay.*` |

Run them all at once:
```bash
bash ipc_ingestion/run_ingestion.sh
```

`scheduler.py` is the APScheduler entry point used by the DigitalOcean worker (runs ingestion + dbt 3× daily at 07:00, 14:00, 19:00 WAT).

### `ipc_transform/` — model the data (dbt)

Standard dbt project, three layers:

| Layer | Schema | Purpose |
|---|---|---|
| **rv** (raw view) | `rv_dash`, `rv_gosource`, `rv_lenco`, `rv_paystack`, `rv_9japay` | Light renames, type casts. One model per raw table. |
| **bv** (business view) | `bv` | Joins, dedup (esp. GoSource line-items → orders), business filters. |
| **gold** | `gold` | Final fact/dim tables that dashboards and reports query. |

Key gold tables to know:
- `fact_revenue` — unified DAASH + GoSource revenue (filter by `service_line`)
- `fact_cash_flow`, `fact_cash_position` — Lenco bank movements + running balance
- `fact_expenses` — categorized debit transactions
- `fact_ar_aging` — GoSource credit orders that haven't been paid (0–30, 31–60, 61–90, 90+)
- `dim_customers` — unified customer dimension across service lines

Run it:
```bash
cd ipc_transform
dbt run --profiles-dir .          # build everything (~5 min)
dbt run --profiles-dir . -s gold  # build just the gold layer
dbt test --profiles-dir .         # run data tests
```

### Streamlit dashboards (4 apps)

Four separate Streamlit apps, each scoped to a different audience.

| App | For | What's in it |
|---|---|---|
| `ipc_dashboard` | Finance / leadership | Revenue, P&L, cash position |
| `ipc_management` | Management only | Per-client revenue, credit exposure, AOV trends (sensitive) |
| `ipc_customer_health` | CS + account managers | Customer activity, churn signals (no revenue) |
| `ipc_ops_dashboard` | Ops team | Daily orders, wallet balances, active brands |

Each app has its own:
- `requirements.txt` — Python deps for that app only
- `.streamlit/config.toml` — theme + server settings (used by Streamlit Cloud)
- `runtime.txt` (where present) — **pins Python 3.11**. Do not remove. Streamlit Cloud defaults to 3.14, which breaks `psycopg2` builds.

#### Run one locally

```bash
cd ipc_dashboard       # or ipc_management, ipc_customer_health, ipc_ops_dashboard
pip install -r requirements.txt
streamlit run app.py
```

#### How to host these on Streamlit Community Cloud (free)

There is no current hosting — whoever picks this up needs to deploy them. It's free and takes ~5 minutes per app.

1. Go to [share.streamlit.io](https://share.streamlit.io) and sign in with the GitHub account that has access to this repo.
2. Click **New app** → pick this repo and the `master` branch.
3. For each of the four apps, set:
   - **Main file path**: `ipc_dashboard/app.py` (or `ipc_management/app.py`, etc.)
   - **Python version**: 3.11 (Streamlit picks this up automatically if `runtime.txt` is present)
4. In **Advanced settings → Secrets**, paste the database credentials in TOML format:
   ```toml
   PG_HOST = "postgres-db-do-user-...db.ondigitalocean.com"
   PG_PORT = "25060"
   PG_USER = "doadmin"
   PG_PASSWORD = "..."
   PG_DB = "PROD_ANALYTICS_DB"
   ```
   (Secrets are NOT pulled from `.env` — you set them per app in the Streamlit Cloud UI.)
5. Click **Deploy**. You get a public URL like `https://ipc-dashboard-xxxx.streamlit.app`.
6. Repeat for the other 3 apps.

After deployment, every `git push origin master` triggers an automatic redeploy on all four apps.

Alternative hosts if you outgrow Streamlit Cloud's free tier: Render, Fly.io, or self-host on a small VM with `streamlit run` behind nginx.

### Report generators (`generate_*.py`)

These produce the PowerPoint and PDF artifacts you see at the repo root:

| Script | Output | Cadence | Week definition |
|---|---|---|---|
| `generate_weekly_activation_report.py` | `IPC_Weekly_Report_<date>.pptx` | Weekly | Last Fri → This Thu, with 4-week trailing avg |
| `generate_weekly_report.py` | Management-edition weekly | As needed | Last Fri → This Thu |
| `generate_march_monthly_report.py`, `generate_april_monthly_report.py` | Monthly decks | Monthly | Calendar month |
| `generate_gosource_march_report.py` | GoSource-specific monthly | Monthly | Calendar month |

Run example:
```bash
source venv312/bin/activate
export $(grep -v '^#' .env | xargs)
python generate_weekly_activation_report.py    # writes to Desktop
```

The `march_*.py`, `gosource_march_report*.py`, `gosource_fix_queries.py`, and `try.py` files are one-off historical scripts kept for reference. You probably will not need them.

### `recon/` — reconciliation

`reconcile.py` checks that money seen by one source matches what another source recorded — e.g. 9japay credits should match Lenco sweep credits. Outputs land in `recon/reports/`. See [`recon/README.md`](recon/README.md).

### `.do/`, `.github/workflows/` — deploy config

- `.do/app.yaml` → DigitalOcean App Platform spec for the **ingestion + dbt worker**. This is the only thing on DigitalOcean App Platform (the database is on DigitalOcean Managed Postgres, which is separate).
- `.github/workflows/run-pipeline.yml` → GitHub Actions CI workflow.

---

## First-time setup

```bash
# 1. Clone + create venv
git clone <this-repo>
cd ipc_analytics
python3.12 -m venv venv312
source venv312/bin/activate

# 2. Install deps
pip install -r requirements.txt

# 3. Create .env (NEVER commit this — it's in .gitignore)
#    Get all these values from the IPC engineering team.
cat > .env <<EOF
PG_HOST=<Postgres host>
PG_PORT=25060
PG_USER=<Postgres user>
PG_PASSWORD=<Postgres password>
DASH_URL=<DAASH MongoDB connection string>
GOSOURCE_URL=<GoSource MongoDB connection string>
PAYSTACK_SECRET_KEY=<Paystack secret key>
LENCO_API_TOKEN=<Lenco API token>
9JAPAY_API_KEY=<9japay API key>
9JAPAY_SECRET=<9japay secret>
EOF

# 4. Set up dbt profile (uses the same Postgres credentials as above)
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml <<EOF
ipc_transform:
  target: prod
  outputs:
    prod:
      type: postgres
      host: <same as PG_HOST>
      port: 25060
      user: <same as PG_USER>
      password: <same as PG_PASSWORD>
      dbname: PROD_ANALYTICS_DB
      schema: public
      threads: 4
      sslmode: require
EOF

# 5. Smoke test — run the smallest ingestion + the rv dbt layer
python ipc_ingestion/paystack_incremental_load.py
cd ipc_transform && dbt run --profiles-dir . -s rv
```

---

## Where things live (operational reference)

| Thing | Where | How to access |
|---|---|---|
| Production database | DigitalOcean managed Postgres, DB = `PROD_ANALYTICS_DB` | Credentials are with the IPC engineering team |
| Ingestion + dbt worker | DigitalOcean App Platform (see `.do/app.yaml`), runs 3×/day | Same DigitalOcean account as the database |
| Streamlit dashboards (×4) | Not currently hosted — see "How to host these on Streamlit Community Cloud" above | Free to deploy with any GitHub account that has repo access |
| Source: DAASH | MongoDB Atlas | Connection string is with the IPC engineering team |
| Source: GoSource | MongoDB Atlas (separate account from DAASH) | Connection string is with the IPC engineering team |
| Source: Lenco | api.lenco.co (REST) | API token is with the IPC engineering team |
| Source: Paystack | api.paystack.co (REST) | API key is with the IPC engineering team |
| Source: 9japay | 9japay REST API | API key + secret are with the IPC engineering team |
| Weekly report output | `IPC_Weekly_Report_YYYYMMDD.pptx` at repo root | — |
| Monthly report output | `IPC_<Month>_<Year>_Monthly_Report.pptx` at repo root | — |

**Getting credentials:** all secrets (DB password, MongoDB URIs, API tokens) are held by IPC engineering. Ask them for the `.env` values listed in "First-time setup" below. None of these credentials are in this repo, by design.

---

## A few things that will trip you up

- **`.env` has CRLF line endings on macOS.** If `export $(grep -v '^#' .env | xargs)` fails with "not valid in this context", strip carriage returns: `tr -d '\r' < .env > .env.tmp && mv .env.tmp .env`.
- **GoSource orders are line-item rows.** Always use `DISTINCT ON (order_id_pk)` for order-level aggregation. There are ~9k legacy CREDIT orders with `paymentStatus=NULL` — handle those explicitly when computing AR.
- **Paystack amounts are in kobo.** Divide by 100 for naira.
- **9japay credits + Lenco sweep credits = same money counted twice.** 9japay credits are DAASH customer payments coming in; the same money is later swept to Lenco and shows up as a credit there. When aggregating cash inflows, pick one side, not both.
- **The DigitalOcean Postgres cluster is small (~10 GB).** If ingestion suddenly fails with "cannot execute INSERT in a read-only transaction", the cluster has hit its disk limit and DO has forced read-only mode. Check disk usage in the DO console — usually fixed by dropping the unused `DEV_RAW_DASH_DB` (or whatever dev DBs have accumulated) or by upgrading the tier.
- **`websitevisits` is a 2.8M-row collection.** A DAASH incremental load that includes new `websitevisits` data can take 30+ minutes. If you need to skip it, comment out the entry in the `COLLECTIONS` list at the bottom of `ipc_ingestion/dash_incremental_load.py`.
- **Streamlit Cloud builds default to Python 3.14**, which breaks `psycopg2`. Every Streamlit app folder has (or should have) a `runtime.txt` containing `python-3.11`. Do not delete these.
- **Two GoSource Mongo accounts exist.** DAASH and GoSource each have their own MongoDB Atlas cluster, with different connection strings. Do not mix them up.

---

## Common operations

```bash
# Run all ingestion + dbt manually (sequential)
bash ipc_ingestion/run_ingestion.sh

# Run all 5 ingestion scripts in parallel (faster, ~5 min instead of ~15)
source venv312/bin/activate
for s in dash gosource lenco paystack 9japay; do
  python ipc_ingestion/${s}_incremental_load.py > /tmp/${s}.log 2>&1 &
done
wait

# Rebuild gold layer only
cd ipc_transform && dbt run --profiles-dir . -s gold+

# Generate this week's weekly report
python generate_weekly_activation_report.py

# Push a Streamlit dashboard change
# (just git push to master — Streamlit Cloud auto-deploys all 4 apps)
git push origin master
```

---

## What's intentionally NOT here

- **No tests** beyond a few `dbt test` data tests. There is no pytest suite. Adding one would be a good first improvement.
- **No CI for the dashboards.** Streamlit Cloud is the deploy mechanism and it does not validate before deploying. If a dashboard breaks, you find out by visiting the URL.
- **No alerting.** If ingestion fails or dbt fails on the DigitalOcean worker, you only see it in the worker's logs. Adding a Slack webhook in `scheduler.py` would be a good second improvement.
- **No staging environment.** Everything runs against `PROD_ANALYTICS_DB`. Be careful with destructive changes.
