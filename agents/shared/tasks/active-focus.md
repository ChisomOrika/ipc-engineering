# Active Focus

## This Week
- **Build #3: Cross-Platform Brand View** — ✅ SHIPPED & VALIDATED 2026-04-13.
  - `gold/dim_brands_cross_platform` live in prod warehouse, all 7 dbt tests pass
  - 1,328 brands total; 39 on both platforms; 681 DAASH-only; 608 GoSource-only
  - **₦80M GoSource credit balance sits with brands not on DAASH** — top targets: Eat N' Go (₦34.5M), Food Court (₦22.7M), Tiamo (₦10.6M), Grillshark (₦5.9M)
  - Full findings: `agents/shared/notes/findings/2026-04-13-cross-platform-validation.md`
  - Surfaced data quality issue: 14 DAASH brands have duplicate registrations (Papa's Grill = 3 records)
  - **Tech debt found**: `generate_schema_name.sql` ignores target.schema for custom-schema models — dev target is effectively a no-op. Patch later.
  - Next: build `brand_match_overrides` table (~30 min manual review) to catch false negatives like "Hot Wings"/"Hot Wingz", "Urban Bites"/"Urban Eats Cloud Kitchen"
  - Next: Streamlit page on top of this, OR direct CSV to whoever owns cross-sell

## Next Up (after #3 validates)
- **Build #5: Win-Back CSV** — weekly export per brand of customers inactive 14/30/60 days. Practical only if a human owner is identified to send the WhatsApp messages — confirm with whoever runs Brand Success / GoSource account management before building.
- **Build #1: Brand Activation Score** — composite score per DAASH brand. Define "activated" as a measurable outcome FIRST (e.g. ≥X online orders/month for ≥Y consecutive months), then weight inputs against it. Don't ship a hand-wavy weighted average.

## Open Threads
- **FindEat data coverage** — gap flagged in Zuri's portfolio walk. Pitch to manager: *"FindEat has zero models — should it?"* before next 1:1.

## Recently Shipped
- 2026-04-13 — `dim_brands_cross_platform.sql` (gold/finance): cross-platform brand entity, the moat build
- 2026-04-13 — Agents workspace at `agents/` (8 personal agents, shared brief, portfolio doc)
- 2026-04-13 — Untracked `ipc_dash/` venv (17,320 files removed from git)

## Critique Log (decisions stress-tested before building)
- **Why dim_brands_cross_platform first instead of Activation Score?** Score risks rejection as "made-up number" without a defined outcome to weight against. Cross-platform view is pure SQL, low risk, and unlocks every other build.
- **Why business-name match instead of waiting for a shared ID?** No shared ID exists. Match-key approach has known false-match risk; mitigation is a future override table, not blocking the build.
