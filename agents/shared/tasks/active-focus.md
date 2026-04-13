# Active Focus

## This Week
- **Build #3: Cross-Platform Brand View** — `gold/finance/dim_brands_cross_platform.sql` shipped 2026-04-13. Joins DAASH + GoSource brands by normalized business name. Need to:
  - [ ] Run `dbt build --select dim_brands_cross_platform` against warehouse and inspect output
  - [ ] Spot-check known overlaps: Papa's Grill, Wings Bistro, Citysubs, Spicy Corner, Ajebo Chops should all show `on_both_platforms = true`
  - [ ] Note false positives/negatives — flag any need for a `brand_match_overrides` table
  - [ ] Once validated, write the first short Streamlit page on top of it (or add to `ipc_dashboard`)

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
