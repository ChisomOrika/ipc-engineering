# Findings: Cross-Platform Brand View — First Build Validation

**Date:** 2026-04-13
**Model:** `gold.dim_brands_cross_platform`
**Status:** Built, all 7 dbt tests pass, validated against known overlaps

## Top-Line Numbers
- **1,328 unique brands** across IPC's two B2B platforms
- **39 brands on BOTH DAASH and GoSource** — the moat. These are the unfair-advantage relationships nobody else has.
- **681 DAASH-only brands** — potential GoSource cross-sell targets
- **608 GoSource-only brands** — potential DAASH cross-sell targets
- **₦80,236,029 in GoSource credit balance** sits with brands not on DAASH

## The Multi-Million Naira Cross-Sell List (GoSource → DAASH)
These brands are already buying supplies from us through GoSource, with significant credit balances, but are NOT using DAASH:

| Brand | GoSource Credit Balance |
|---|---:|
| Eat N' Go | ₦34,531,671 |
| Food Court | ₦22,763,437 |
| Tiamo Catering Services | ₦10,655,050 |
| Grillshark | ₦5,873,476 |
| masulas | ₦2,579,900 |
| Citysubs Magodo Branch* | ₦2,094,532 |

*Likely a false negative — the parent Citysubs IS on both platforms. See "Match Quality" below.

**Action:** hand this list to whoever owns cross-sell. Each one is an existing customer relationship — the conversation is *"you already trust us for supplies, why not for ordering?"* not a cold pitch.

## Confirmed: The 39 Both-Platform Brands
Spot checks all match expectation: Papa's Grill, Wings Bistro, Citysubs, Spicy Corner, Mr. Krums, Ajebo Chops, Urban Fuxion all show `on_both_platforms = true`. The model works.

## Data Quality Findings (Surfaced, Not Hidden)

### 1. DAASH has duplicate brand registrations
**14 brands have multiple DAASH customer records.** Notable: **Papa's Grill is registered 3 times.** Chopchop, Flavor Grill, Fruta De Lite, Melonypine, Misscravings, Nikkyskitchen, Nouri Kitchen, etc. all have 2 records each.

**Why this matters:** every analysis that counts "active DAASH brands" is over-counting. The "23 brands" figure used in the activation memo may itself be inflated. Worth a separate cleanup investigation.

### 2. Citysubs has 5 GoSource records, 1 DAASH record
Citysubs has multiple GoSource sub-accounts (probably per branch — Magodo, Yaba, etc.) all linked to one DAASH brand. The new model collapses them into one row but preserves `gosource_record_count = 5`, so the structure is visible.

### 3. False Negatives — same brand, different names
Spot-checked names that should match but don't (because normalized names differ):
- "Hot Wings" (DAASH) vs "Hot Wingz" (DAASH) — both DAASH but split
- "Urban Bites" / "URBAN BITE" (DAASH) vs "Urban Eats Cloud Kitchen Ltd" (GoSource) — possibly the same business
- "Captains" / "Captains Cafeteria" (DAASH) vs no GoSource match (but might exist under different name)
- "Citysubs Magodo Branch" / "City Submarine Sandwich Ltd" / "citysubyaba" — all GoSource, all related to the parent Citysubs DAASH record

**Mitigation path:** build a `brand_match_overrides` table — manually-curated mappings for known same-brand-different-name cases. ~30 minutes of manual review against this list would resolve most of them.

## Build Notes
- Initial build had 41 dupes due to within-source duplicate brand registrations
- Fixed by aggregating each side to one row per match key BEFORE the full-outer-join
- `dash_record_count` and `gosource_record_count` columns surface the duplication explicitly
- All 7 dbt tests now pass

## ⚠️ Schema Routing Issue (Tech Debt)
`ipc_transform/macros/generate_schema_name.sql` returns the model's `custom_schema_name` directly when set, ignoring `target.schema`. Result: even running with `--target dev`, this model wrote to **prod's `gold` schema**. No damage (it's a new table, nothing overwritten), but the dev target is effectively a no-op for any model with a custom schema. Should be patched to prefix with `target.name` when target != prod, so dev runs land in `dev_chisom_gold` etc.
