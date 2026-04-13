# GoSource orders explosion — audit

**Date:** 2026-04-13
**Context:** `bv.bv_gosource_orders` (and its upstream `raw_gosource.orders`) is exploded
one-row-per-product by the ingestion step `transform_orders` (`df.explode("products")`).
71,716 rows for ~3,442 unique orders (~21x fan-out). A naive
`sum(order_total_price_amount)` returns ~₦47.5B vs the true ~₦1.39B.

## TL;DR

Only **one live consumer is affected**: `ipc_ops_dashboard/app.py` — specifically the
"Top Products" card (`total_amount`) double-counts. Everything else (finance dbt models,
main dashboards, AR aging, weekly report, customer health) is already deduping correctly
or reading from non-exploded sources (`raw_gosource.receipts`). Two legacy/unused models
in `ipc_gosource/gosource_transform/` carry the old bug but produce unused outputs.

## Affected files

| File : line | Query summary | Affected? | Risk | Fix |
|---|---|---|---|---|
| `ipc_ops_dashboard/app.py:430-442` | `SELECT "product.name", COUNT(*) line_items, SUM(quantity), SUM("totalPrice") total_amount FROM raw_gosource.orders GROUP BY "product.name"` | **Partial — YES for `total_amount`** | **Medium** (ops dashboard, exec-adjacent) | `total_amount` is summing the *order total* on every product line → heavily inflated. `line_items` and `quantity` are legitimately product-level. Either drop `total_amount`, or compute it as `SUM(actualPrice * quantity)` / `SUM("product.discountPrice" * quantity)` at line level, or stop aggregating revenue on a product-name dimension when the field is order-level. |
| `ipc_gosource/gosource_transform/models/bv/bv_orders.sql` | Legacy bv model, identical explosion semantics | Yes (structural) | **Low** (legacy project — `ipc_gosource/`; confirm whether still deployed) | If legacy project still runs in prod, mirror the fixes made in `ipc_transform` (all downstream consumers should dedupe on `ID_PK`). |
| `ipc_gosource/gosource_transform/models/gold/FACT_PRODUCTS_REVENUE..sql` | `SELECT o.ID_PK AS order_id, ..., o.orders_totalprice AS revenue FROM bv_orders o` (no dedup; materialized `table`) | Yes, if consumed | **Low** (no downstream references found in grep; filename has stray `..` suggesting unused) | Add `distinct on (ID_PK)` dedup like `fact_gosource_products_revenue` does, or delete if obsolete. |
| `ipc_transform/models/gold/gosource/fact_gosource_products_revenue.sql:26` | `SELECT order_id_pk, product_id_fk, ..., order_total_price_amount AS revenue FROM bv_gosource_orders` — materialized incremental with `unique_key='order_id'` | **No** (dbt merge collapses to 1 row per `order_id`, so output has ~3,442 rows) | Low — but **latent risk** | `unique_key='order_id'` keeps a single (arbitrary) product row per order, so `product_id_fk`/`quantity` are essentially meaningless — misleading but not inflated. No downstream consumer uses it (`grep` finds zero refs). Consider deprecating, or rewrite to aggregate per-product correctly. |

## Not affected (verified safe)

- `ipc_transform/models/gold/finance/fact_revenue.sql:32-51` — `select distinct on (order_id_pk) ... from bv_gosource_orders` — correct dedup.
- `ipc_transform/models/gold/finance/fact_profitability.sql:60-80` — same `distinct on (order_id_pk)` pattern.
- `ipc_transform/models/gold/finance/fact_ar_aging.sql:5-37` — CTE then `distinct on (order_id_pk)` dedup CTE.
- `ipc_dashboard/pages/2_Revenue_&_Profitability.py`, `4_AR_Aging.py`, `6_GoSource.py`, `1_Cash_Flow.py` — consume `gold.fact_revenue` / `fact_profitability` / `fact_ar_aging` only, all already deduped upstream.
- `ipc_dashboard/pages/7_Data_Health.py:31` — references table name but only for freshness/count checks; row-count skew tolerable for data-health monitor (though worth noting the displayed row count is the exploded count, not order count).
- `ipc_ops_dashboard/app.py` GoSource `monthly`, `current_month`, `last_month`, `customer_history` blocks — all use `WITH deduped AS (SELECT DISTINCT ON ("_id") ...)` then aggregate. Correct.
- `ipc_ops_dashboard/app.py` `top_products` (line_items + quantity columns) — product-level metrics, explosion is intentional.
- `ipc_customer_health/app.py` and `pages/2_Growth & Retention.py` — consume `gold.fact_gosource_*` / `gold.dim_gosource_*`, which are sourced from `bv_gosource_receipts` (not `bv_gosource_orders`). Receipts are one-row-per-order.
- `ipc_management/app.py` — uses `gold.fact_gosource_credit_exposure`, sourced from `raw_gosource.receipts`. Safe.
- `ipc_transform/models/gold/gosource/fact_gosource_credit_exposure.sql` — uses `raw_gosource.receipts`, not orders.
- `ipc_transform/models/gold/gosource/fact_gosource_{last_order,customer_activity,individual_orders_revenue,orders_timelines}.sql` — all source from `bv_gosource_receipts`, not orders.
- `generate_weekly_report.py` — queries `raw_gosource.receipts`, not orders.
- `recon/reconcile.py:112, 322-330` — explicitly dedupes via `dedup_col=order_id_pk` before counting/summing. Correct (and comment documents the risk).

## Recommended fix pattern

`sum(distinct amount)` is **wrong** — two different orders can have the same total, and
`distinct` collapses them.

The right pattern is dedupe-then-aggregate:

```sql
with deduped as (
    select distinct on (order_id_pk)
        order_id_pk,
        order_total_price_amount,
        order_service_charge_amount,
        order_created_at_date,
        order_status
    from bv.bv_gosource_orders
    order by order_id_pk, order_delivered_at_date
)
select
    count(*)                               as orders,
    sum(order_total_price_amount)          as revenue
from deduped
where lower(order_status) = 'delivered';
```

For the `ipc_ops_dashboard` top-products case, the product-line revenue should come from
per-line values, not the order total:

```sql
sum(
  case when "product.discountPrice" ~ '^[0-9.]+$'
       then "product.discountPrice"::numeric
       else 0 end
  * case when quantity ~ '^[0-9.]+$' then quantity::numeric else 1 end
) as product_revenue
```

## Proposed action plan (quickest wins first)

1. **Fix `ipc_ops_dashboard/app.py` `top_products` query** (~10 min).
   Either drop `total_amount` from the card or replace with a per-line-item computation.
   This is the only *live* wrong number.
2. **Add a data-health note / column comment** on `bv.bv_gosource_orders` warning future
   consumers about the explosion. `ipc_transform/models/bv/gosource/bv_gosource_orders.sql`
   already has no comment; add a `description:` in `_gosource__models.yml`.
3. **Deprecate or fix `fact_gosource_products_revenue`** — it has no consumers and its
   `unique_key='order_id'` behaviour silently drops product rows. Either rewrite to a
   proper per-product aggregate or delete.
4. **Audit & clean up `ipc_gosource/gosource_transform/`** — confirm whether this legacy
   dbt project still runs. If not, archive. If yes, port the dedup pattern (or point it
   at the receipts-based models).
5. **Add a dbt test** (`dbt_utils.expression_is_true` or `unique` on `order_id_pk` for a
   dedup model) on any finance model built from `bv_gosource_orders`, guarding against
   regressions.

## Appendix: why `bv_gosource_receipts` is safe

`raw_gosource.receipts` → `bv_gosource_receipts` is the GoSource receipts collection,
one row per order already at source. Ingestion does not explode it. All customer-health
and gosource analytics models read from receipts, which is why they have not exhibited
the explosion bug.
