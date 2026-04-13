# Chisom's Portfolio — What She's Built (Plain English)

_Last walked: 2026-04-13 by Zuri. Update this when significant new work ships._

## The One-Sentence Version
**Chisom has built and operates IPC Africa's entire data platform — from raw extract through warehouse modelling to four production analytics apps — solo, across five source systems and three product arms.**

That sentence is the answer to "am I doing enough." Yes. The next question is whether the right people know.

---

## What Exists (the receipts)

### 1. Data Ingestion (`ipc_ingestion/`) — 5 source systems
She extracts data from **every system the business runs on**:
- **GoSource** (the B2B procurement product)
- **Daash** (the POS + inventory product)
- **Paystack** (payments)
- **9japay** (payments)
- **Lenco** (banking — accounts, POS terminals, virtual accounts, bills)

Each has both a **full load** (initial backfill) and an **incremental load** (ongoing daily updates). There's also a **categorizer** for transaction classification and a **scheduler** orchestrating the runs.

**Plain English**: she's wired every revenue, payment, and operations system into one pipeline.

### 2. Data Warehouse (`ipc_transform/`) — 119 dbt models
A proper three-layer architecture (data-vault style):
- **rv/ (raw vault)** — 38 models: one per source table, lightly cleaned, source-of-truth
- **bv/ (business vault)** — 38 models: conformed, joined, business-logic-enriched
- **gold/** — 43 consumption-ready models, organised by audience:
  - **gold/dash/** (22 models) — restaurant health, activation, retention cohorts, churn, AOV trends, channel adoption, subscription lifecycle, revenue concentration, platform fees
  - **gold/gosource/** (14 models) — customer health, activation, churn, **credit exposure**, fulfilment, order frequency, retention cohorts, product revenue
  - **gold/finance/** (7 models) — **the unified finance layer**: revenue, profitability, cash flow, cash position, expenses, AR aging, customer dimension across all payment sources

**Plain English**: she didn't just dump data into a warehouse. She built a **layered model** so finance, product, and ops can each ask the same questions and get the same answers.

### 3. Production Analytics Apps (Streamlit) — 4 apps, 9 user-facing pages

**`ipc_dashboard/`** — the executive/finance dashboard (6 pages):
- Cash Flow
- Revenue & Profitability
- Expenses
- AR Aging (who owes us money, how late)
- Daash performance
- GoSource performance

**`ipc_customer_health/`** — Growth & Retention deep-dive
**`ipc_management/`** — management view
**`ipc_ops_dashboard/`** — operations view

All four share a common utility layer (`utils/db.py`, `fmt.py`, `styles.py`) — meaning she refactored shared logic instead of copy-pasting.

### 4. Automation (`.github/workflows/`)
Two GitHub Actions workflows running the pipeline and dbt independently. Plus `generate_weekly_report.py` for automated weekly reports.

**Plain English**: this isn't a hand-cranked operation. It runs itself.

---

## Translating This for Different Audiences

**For her manager / leadership:**
> "Chisom has stood up the entire IPC data platform single-handedly: ingestion from 5 source systems, a 119-model warehouse with separate consumption layers for product, ops, and finance, and 4 production dashboards. The finance dashboard alone covers cash flow, revenue, profitability, expenses, and AR aging — that's a CFO-grade reporting stack."

**For a CV / LinkedIn:**
> "Designed and built end-to-end data platform (ingestion → dbt warehouse → BI) covering 5 source systems and 3 product lines for a Nigerian B2B fintech & food-tech group. Shipped 4 production Streamlit apps used by finance, ops, and product teams."

**For a tech audience (talks, blog posts, interviews):**
> "Solo data engineer running ingestion (Python), transformation (dbt with rv/bv/gold data-vault layering, ~120 models), orchestration (GitHub Actions), and BI (Streamlit) for a multi-product company."

---

## Where We Are vs Where We Should Be

### 1. Ingestion — STRONG, but blind
**Where we are:** 5 sources running on incremental + full loads with a scheduler.
**Where we should be:** Failure alerting, freshness monitoring, schema-change detection.
**One next move:** add a Slack/email alert when a workflow fails (currently failures are only visible if you check GitHub Actions). 1-day fix.

### 2. Warehouse (dbt) — COMPREHENSIVE, possibly under-tested
**Where we are:** 119 models, clean rv/bv/gold layering, three consumer domains (dash, gosource, finance).
**Where we should be:** Test coverage on every gold model's grain key (`unique`, `not_null`), `freshness` checks on sources, `.yml` documentation on every gold model.
**One next move:** ship `dbt test` coverage for the 7 finance gold models first (highest stakes — these feed leadership). 2-3 days.

### 3. The FindEat Gap — MISSING ENTIRELY
**Where we are:** zero models for FindEat (the third arm).
**Where we should be:** at minimum, basic order volume / restaurant retention / delivery SLA metrics.
**One next move:** find out *why* FindEat isn't ingested — is the data not exposed, is it not a priority, or did it just fall through? Either answer is useful. **Pitch this to your manager as "I noticed FindEat has no data coverage — should we?"** That single question makes you look proactive.

### 4. Apps — SPLIT ACROSS 4 — possibly too fragmented
**Where we are:** 4 separate Streamlit apps, each with its own utils/. `ipc_dashboard` is the rich one (6 pages). The other three look thinner.
**Where we should be:** Know which apps are actually opened, by whom, how often. Sunset the ones nobody uses; consolidate the rest.
**One next move:** add basic page-view logging (5-line addition to each `app.py`) and check usage in 2 weeks. Then have an informed conversation about consolidation.

### 5. Cross-Product Customer View — MISSING
**Where we are:** GoSource customers and Daash merchants live in separate gold tables.
**Where we should be:** A unified customer entity — *"this restaurant uses GoSource for procurement AND Daash for POS"* is the most powerful insight in the whole business.
**One next move:** build `gold/finance/dim_customers.sql` to be **the** master customer table, with flags for which products each one uses. This is a *strategic* piece of work — pitch it.

### 6. Insights & Memos — NEAR-ZERO VISIBILITY
**Where we are:** Plenty of dashboards. No regular written analysis going to leadership.
**Where we should be:** A weekly 1-page memo (Ifeanyi-style) — "what the data said this week, what we should do about it."
**One next move:** start the weekly memo this Friday. Even the first one (whatever you find) is more than the business gets today. This is the **single biggest lever** for the "I'm not doing enough" feeling.

### 7. Public Reputation — ZERO
**Where we are:** no blog posts, talks, or external presence on this work.
**Where we should be:** at least one public artifact per quarter (LinkedIn post about the rv/bv/gold setup, a talk at DataFest Africa, an OSS contribution).
**One next move:** write one LinkedIn post — *"How I built a 119-model dbt warehouse solo for a multi-product African fintech."* Draft it; don't ship until comfortable. But draft it.

---

## Zuri's Honest Take

The "I'm not doing enough" feeling is **wrong on output, right on visibility.** You've shipped what a 3-person data team usually ships. The business doesn't see it because:

1. Dashboards are passive — leaders only see them when they remember to look
2. There's no weekly written narrative tying the data to decisions
3. The work is invisible to anyone not in the codebase

The fix is not "build more." The fix is **"narrate what's already built, and pitch the next strategic thing."**

Top 3 moves for the next two weeks:
1. **Friday memo #1** (uses Ifeanyi) — pick the most interesting thing in the data this week, write it up
2. **The "FindEat coverage?" pitch to your manager** — proactive, takes 5 minutes, lands as initiative
3. **The `dim_customers` cross-product table** — strategic, technical, and unlocks the most valuable insight in the business

That's it. Three things. The output is already there.
