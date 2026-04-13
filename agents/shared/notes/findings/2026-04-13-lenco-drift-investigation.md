# Lenco Warehouse Drift Investigation — 2026-04-13

## TL;DR
The 125 missing transactions are all `successful` records with `initiatedAt = NULL` from the Lenco API; the incremental ingestion loader's client-side date filter (`ipc_ingestion/lenco_incremental_load.py`, `fetch_paginated`) drops every record whose `initiatedAt` is null, so any null-initiatedAt rows produced by Lenco after the most recent full load never reach `raw_lenco.transactions` (and therefore never reach `bv.bv_lenco_transactions`).

## Counts
| Source | Rows |
|---|---|
| Lenco API (`meta.total`) | 16,667 |
| `raw_lenco.transactions` | 16,542 |
| `bv.bv_lenco_transactions` | 16,542 |
| Missing in warehouse | **125** |
| Extra in warehouse | 0 |

The drop happens in raw ingestion, not in the bv/dbt layer. `bv.bv_lenco_transactions` matches `raw_lenco.transactions` 1:1 (16,542 = 16,542). The dbt incremental filter on `initiatedAt` is therefore not the proximate cause for this run, although it is a latent risk (see "Notes" below).

## The missing 125: by status, type, date

### By status
| status | count |
|---|---|
| successful | 125 |

### By type
| type | count |
|---|---|
| debit | 99 |
| credit | 26 |

### By date
Every one of the 125 missing records has `initiatedAt = null`. Sampling shows their `completedAt` values fall in **March 2026** (the month right after the last successful backfill of null-initiatedAt rows; raw already contains 23 null-initiatedAt rows with `completedAt` in `2026-03`, presumably from an earlier full load, and 109 in `2026-02`, but none from later in March or April that match the Lenco API tail).

## Sample missing transactions
| id (prefix) | type | status | amount | initiatedAt | completedAt | transactionReference |
|---|---|---|---|---|---|---|
| b17d4732… | debit | successful | 53.75 | null | 2026-03-08T14:37:34Z | 38631258c5 |
| 88f5a9f8… | credit | successful | 230000.00 | null | 2026-03-21T10:16:03Z | 39011864b8 |
| 17a6a0b5… | debit | successful | 50.00 | null | 2026-03-26T08:51:06Z | 39195218b9 |
| 4eb3a6d4… | debit | successful | 50.00 | null | 2026-03-08T12:28:07Z | 38627050de |
| 6e52a7a0… | debit | successful | 53.75 | null | 2026-03-29T11:47:40Z | 39325852b6 |

(Full ID list saved at `/tmp/lenco_missing.json`.)

## Root cause analysis

This is a **systematic filter bug**, not ingestion lag and not a transient API issue.

### Evidence

1. **All 125 missing records have `initiatedAt = null`** in the Lenco API response. None have `initiatedAt` populated.
2. **All 125 are `status=successful`** — they are not pending/failed-and-evicted edge cases; they are real money movements (mostly debits, totalling hundreds of thousands of NGN).
3. The incremental loader at `/Users/sapaleague/ipc-engineering/ipc_ingestion/lenco_incremental_load.py` uses `initiatedAt` as `timestamp_col` for the `transactions` job (line 670) and applies a client-side filter inside `fetch_paginated` (lines 314–328):

   ```python
   for rec in records:
       ts = parse_ts(rec.get(timestamp_col))
       if ts is not None and ts >= from_date:
           new_records.append(rec)
       else:
           old_count += 1
   ```

   `parse_ts(None)` returns `None`, so any record with null `initiatedAt` falls into the `else` branch and is **never appended to `new_records`** — i.e. dropped silently.

4. **Worse**, the same loop drives an early-exit condition (`if old_count == len(records): break`). A page composed entirely of null-initiatedAt records would terminate pagination before reaching older real records, but in practice the API sorts DESC by `initiatedAt` and null-initiatedAt rows appear interleaved, so this just causes per-record drops here.

5. The reason `raw_lenco.transactions` has thousands of older null-initiatedAt rows (8,337 total, 23 in 2026-03 already) is that they were ingested by the **full-load** path (`lenco_full_load.py`, no `from_date`), which has no client-side filter. The incremental run that has been operating since the last full load skips every newly arrived null-initiatedAt record, producing exactly the gap observed.

6. Not ingestion lag: missing records' `completedAt` spans 2026-03-08 through 2026-03-29 — well outside any reasonable lag window for an investigation dated 2026-04-13.

### Secondary risk (not currently triggering)

The `bv_lenco_transactions` dbt model is incremental on `transaction_initiated_at_date_time`:

```sql
WHERE NULLIF("initiatedAt", 'NaN')::timestamp > (SELECT MAX(transaction_initiated_at_date_time) FROM {{ this }})
```

If/when raw ever contains null-initiatedAt rows newer than the bv max, the predicate evaluates to NULL (not TRUE) and those rows would also be silently filtered. Today raw and bv counts match because the ingestion bug already removes them upstream — but fixing ingestion alone will surface this second filter and the rows would still be lost at the bv layer.

## Recommended fix

**Two-part fix.** Both are needed; fixing only ingestion would expose the dbt filter.

### 1. Ingestion (primary) — `ipc_ingestion/lenco_incremental_load.py`

Treat null-timestamp records as "in-window, must keep" rather than "older, drop". In `fetch_paginated` (~line 314), change:

```python
if ts is not None and ts >= from_date:
    new_records.append(rec)
else:
    old_count += 1
```

to keep null-timestamp records and only count records with a *parseable timestamp older than from_date* as "older":

```python
if ts is None or ts >= from_date:
    new_records.append(rec)        # keep null-timestamp rows; let upsert dedupe
else:
    old_count += 1                 # only real-old rows trigger early exit
```

Rationale: the upsert is keyed on `id`, so duplicates are harmless; the early-exit logic still works because only records with a real, older timestamp count toward `old_count`. Also consider falling back to `completedAt` when `initiatedAt` is null, both for filtering and as the timestamp_col seed for the next incremental window.

### 2. dbt (defensive) — `ipc_transform/models/bv/lenco/bv_lenco_transactions.sql`

Make the incremental predicate null-safe so future null-initiatedAt rows in raw still land in bv:

```sql
WHERE COALESCE(
        NULLIF("initiatedAt",  'NaN')::timestamp,
        NULLIF("completedAt", 'NaN')::timestamp
      ) > (SELECT MAX(transaction_initiated_at_date_time) FROM {{ this }})
   OR NULLIF("initiatedAt", 'NaN') IS NULL
```

(Or switch the incremental key to a server-side ingestion timestamp such as `record_load_date`, which is never null.)

### 3. Operational backfill (one-shot)

After deploying the fix, run `lenco_full_load.py` once (or a targeted re-pull of the 125 IDs in `/tmp/lenco_missing.json`) to backfill the gap. Subsequent incremental runs will then stay aligned.
