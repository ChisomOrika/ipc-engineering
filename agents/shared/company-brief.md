# IPC Africa — Company Brief (read first, every session)

## The Parent Company
**Independent Purchasing Company (IPC)** — Lagos-based logistics and tech firm building an end-to-end operating system for Nigerian food businesses. HQ: Lekki Phase 1, Lagos. Site: https://www.ipc-africa.com/

## The Three Products

### 1. GoSource — B2B food procurement (https://gosource.app/)
- Centralized marketplace where restaurants and food businesses bulk-order food commodities and kitchen equipment
- Sources direct from farmers/manufacturers
- Features: Bulk Store, instant invoicing, order tracking, dashboard insights, 24h delivery, credit up to ₦500k with 7-day terms
- 100+ business customers (Papa's Grill, Wing Bistro, Spicy Corner, etc.)
- **Key metrics to care about:** GMV, AOV, repeat-order rate, credit utilisation & repayment, fulfilment SLA, take rate, gross margin per order, customer cohort retention.

### 2. Daash — POS + inventory for food businesses (https://daashapp.co/)
- Online store + inventory management + POS in one
- Target: SMB retail/food ops needing unified digital + physical sales
- **Key metrics:** activation (first sale), DAU/MAU, transactions per active merchant, GMV processed, churn, feature adoption (POS vs online vs inventory), payment success rate.

### 3. FindEat — restaurant management + food delivery
- Digital restaurant management + delivery logistics
- **Key metrics:** order volume, delivery time, rider utilisation, restaurant retention, commission revenue.

## The Business Reality
The business is **under pressure right now.** That changes what good data work looks like:
- Vanity metrics are useless. Every analysis should answer: *where is money leaking* or *where can we grow.*
- Speed > polish. A rough answer today beats a perfect answer next week.
- Surface-level dashboards are not enough — leadership needs **decisions**, not charts.

## Chisom's Role
Solo data person covering both **engineering** (ETL pipelines, dbt warehouse) and **analytics** (dashboards, ad-hoc analysis, reporting). Stack:
- **Languages:** SQL, Python
- **Transform:** dbt (`ipc_transform/`)
- **Apps:** Streamlit (`ipc_customer_health/`, `ipc_dashboard/`, `ipc_management/`, `ipc_ops_dashboard/`)
- **Orchestration:** GitHub Actions (`.github/workflows/run-pipeline.yml`, `run-dbt-only.yml`)
- **Sources ingested:** GoSource, Daash, Paystack, 9japay, Lenco

## The Codebase (this repo: `ipc-engineering`)

```
ipc_ingestion/         # Raw extract scripts: gosource, dash, paystack, 9japay, lenco (full + incremental)
ipc_transform/         # dbt project — models split into rv (raw vault) → bv (business vault) → gold
   models/rv/          # Source-aligned raw models (per source)
   models/bv/          # Business vault — conformed entities
   models/gold/        # gosource/, dash/, finance/ — consumption layer
ipc_customer_health/   # Streamlit: customer health + Growth & Retention page
ipc_dashboard/         # Streamlit: general dashboard
ipc_management/        # Streamlit: management view
ipc_ops_dashboard/     # Streamlit: ops view
ipc_gosource/          # GoSource-specific ingestion + transform
generate_weekly_report.py  # Weekly reporting script
```

Architecture is **rv → bv → gold** (data-vault flavour). Finance gold combines all payment processors.

## Stakeholders & Audiences for Chisom's Work
- **Product teams** (GoSource, Daash, FindEat) — need feature/funnel analytics, experiment results
- **Operations** — need fulfilment, delivery, inventory KPIs
- **Finance/Leadership** — need GMV, margin, runway-relevant numbers
- **Chisom's manager** — needs to see Chisom's impact clearly each week

## Working Principles for All Agents
1. **Read this brief first, every session.** Then read `shared/tasks/` for current state.
2. **Reference real files.** When suggesting changes, point to actual paths in this repo.
3. **Respect Chisom's time.** She is one person. Prioritise leverage. Suggest the one thing that matters most before listing ten.
4. **Make her visible.** Many of these agents exist to ensure her work gets credit. Frame outputs so they're shareable to her manager and product partners.
5. **Be Nigerian-context aware.** Naira (₦), local payment processors (Paystack, 9japay, Lenco, Flutterwave), Lagos-time, NDPR for data privacy.
