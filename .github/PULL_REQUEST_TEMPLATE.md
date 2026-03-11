# PR Checklist

**Branch type** (check one):
- [ ] `shiny/` — Shiny UI/server (`app/`)
- [ ] `react/` — React frontend (`frontend-v2/`)
- [ ] `sql/` — Materialized views / SQL functions
- [ ] `etl/` — ETL pipeline
- [ ] `infra/` — CI workflows, scripts, deploy config

---

## Shiny
- [ ] Tested locally (`shiny::runApp('app')`)
- [ ] All 4 tabs load without errors
- [ ] Filters reset correctly

## React
- [ ] `npm run dev` runs without errors
- [ ] Plumber API tested (`frontend-v2/server/run.R`)
- [ ] No TypeScript errors

## SQL
- [ ] MV rebuild order followed (L1 → L2 → L3 → L4)
- [ ] Row counts verified before/after
- [ ] No `DROP ... CASCADE` without explicit intent
- [ ] SQL function signature matches exact param count

## ETL
- [ ] Dry run passed (`etl_full(dry_run=TRUE)`)
- [ ] `etl/logs/last_success.txt` updated after live run

## Infra
- [ ] CI workflow passes on this branch
- [ ] No secrets or `.Renviron` files committed

---

## What does this PR do?
<!-- one or two sentences -->

## Any follow-up needed?
<!-- optional -->
