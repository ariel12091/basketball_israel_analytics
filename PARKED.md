# PARKED: Phase 0 publication layer and `app_api` schema

This branch (`sql/publication-phase0`) is `main` as of 2026-11-06 plus the
unfinished Phase 0 work from `sql/web-foundation`, carried over file by file.
**It is not applied anywhere and must not be merged as it is.**

## What it is

- **Publication** (`sql/publication/001_publication.sql`, `etl/publication.R`,
  `euroleague/src/euroleague_possessions/publication.py`): readers see each ETL
  run whole or not at all. Each write is fenced inside a publication, and
  `publication_close()` marks the run finished or degraded. The Israeli ETL
  (`etl/etl_full.R`, `etl/etl_onoff.R`, the identity and alias loaders) and the
  EuroLeague batch pipeline are rewired to open and fence one.
- **`app_api` schema** (`sql/app_api/*`): web-facing Game Logs readers and a
  version function, with grants in `sql/security/*` and catalogue and parity
  tests (`test-app-api-*`, `helper-app-api-parity.R`).
- Apply scripts: `scripts/apply_publication.R` (first), then
  `scripts/apply_web_foundation.R`, then `scripts/apply_db_security.R`.
- Design: `docs/superpowers/specs/2026-10-20-react-migration-design.md`;
  plan: `docs/superpowers/plans/2026-10-20-react-phase0-foundation.md`;
  last status: `docs/phase0_execution_status.md` (2026-10-22).

## Why it is parked

The live web app reads the existing stored readers as `app_readonly` and
needs none of this. Much of it arrived in one commit labelled "other
session's uncommitted Phase 0 work, unreviewed" (`87c3de2`) and was never
reviewed. On `main`, the ETL changes would call a publication schema that
does not exist, and the nightly run would fail.

## Before reviving it

1. Review it. It has never had a full review.
2. Rebase on the `main` of the day. `etl/etl_full.R` and `etl/etl_onoff.R`
   are hot files.
3. Apply in order on a test schema: publication, then web foundation, then
   the security re-apply. Run the DB tests listed above, then the nightly
   ETL once in dry-run and once for real against the test schema.
4. Only then merge the SQL and the ETL together, in one change.

The frozen source branch is tagged `backup/web-foundation-2026-11-06`.
