# Phase 0 execution status -- HANDOFF (2026-10-20)

Plan: `docs/superpowers/plans/2026-10-20-react-phase0-foundation.md` (read its "Execution
amendment (user)" first). Spec: `docs/superpowers/specs/2026-10-20-react-migration-design.md`.
Branch: `sql/web-foundation` (not pushed, not merged). `main` is at `ae865b0` locally /
`7cd39a2` on origin (D24 fix deployed).

## Authorization and constraints (user, 2026-10-20)

- The web app uses the existing `app_readonly` login. No `app_web` role, no password, role
  default, membership or connection-limit changes. Add only grants the new objects need.
- Every production database change goes through a dry-run-by-default apply script and is
  applied only after the user's explicit go-ahead. Nothing has been applied yet.
- Keep Shiny working unchanged; no production game load to test publication code.
- PostgreSQL is 17.4 (verified live), not the plan's "15" label.

## History

1. A first executor (Claude) committed Task 1 in its original `app_web` form (`a352e98`,
   superseded by the amendment) and a comment fix (`db0f5b8`).
2. A second executor started at 06:58 UTC under the amendment, worked uncommitted across
   Tasks 2-9 and 16, and stopped at its limit without a handoff.
3. Takeover: production verified untouched (no `app_api`, no publication objects, no
   `app_web`). An inventory of the second executor's work is in
   `.superpowers/sdd/2026-10-20-react-phase0-foundation/inventory-of-other-session.md`
   (git-ignored workspace; ledger `progress.md` in the same folder). Its work was saved
   as one unreviewed commit, `87c3de2`.
4. Work unit U1 (database: Tasks 1-amended, 2, 5) fixed and reviewed:
   `0e09866` app_web leftovers removed, identity proof as app_readonly;
   `b4b4cbd` publication liveness scoped to this database;
   `a657ded` Game Logs readers fixed (old version filtered a non-existent column) and proven
   equal to Shiny's own season pulls;
   `7768bf2` safe apply order + deploy-and-call catalogue test in a scratch schema.
   Review verdict: publication semantics, reader parity and grants sound; two Important
   findings sent back (fix round 1, below).

## In flight at handoff

**U1 fix round 1** -- an implementer was mid-way; its edits are UNCOMMITTED in the working
tree (`helper-db-roles.R`, `test-app-api-sql-catalog-db.R`, `test-publication-db.R`,
`test-web-role-db.R`, `apply_publication.R`, `apply_web_foundation.R`, `020_game_logs.sql`,
`001_publication.sql`). Findings it is fixing:
1. (Important) `test-web-role-db.R`: write/DDL probes ran inside `BEGIN READ ONLY`, so
   Postgres answered "read-only transaction" before any privilege check. Probes must run in an
   ordinary rolled-back transaction, assert SQLSTATE 42501, plus `has_table_privilege` /
   `has_schema_privilege(...,'CREATE') = FALSE`.
2. (Important) `apply_web_foundation.R`: smoke calls ran after the security hardening inside
   the same transaction, holding ACCESS EXCLUSIVE on ~68 tables (Shiny's main views) for ~7 s,
   in the dry run too. Move smoke calls before the hardening.
3-7. (Minor) liveness must require a session `ExclusiveLock` held by the owner's recorded
   pid/backend_start; `finish_publication` must require the live lock; poll for `stale`;
   header comment on service_role; note that `game_logs_il`'s FULL JOIN emits one-sided rows
   consumers must drop.
Next step: check `git status`/`git log`; if the round's commits are missing, finish the
fixes above, run the catalogue/parity test, `test-publication-db.R`, `test-etl-publication-db.R`
(scratch `pubtest_*` schemas only) and both dry runs; then a scoped re-review.

## Then, in order

1. **Ask the user for the production apply** (off-peak, not during the nightly ETL window):
   `scripts/apply_publication.R` first, then `scripts/apply_web_foundation.R`, each with its
   CONFIRM flag; then the post-apply DB tests (`test-web-role-db.R`,
   `test-app-api-game-logs-db.R`) and a security re-apply + re-test (review R9).
2. **U2 -- Tasks 3 and 4 (ETL and EuroLeague loader).** Known defects in the WIP:
   - merging them before step 1 breaks the nightly ETL and loader (they call publication
     functions); the merge must follow the apply;
   - `etl_full.R` edit breaks `test-player-identity-dictionary-contracts.R:48` (CI red);
   - the Israeli ETL bumps the version every night even with no relevant writes (plan: it
     must not); last-success marker and Phase 7 run after the publication closes, unfenced;
   - EuroLeague loader: raises on the first failed group (skipping later groups), skips the
     manifest on partial runs, verifies twice -- restore the original behaviour;
   - Task 3 leftovers: write-inventory doc, static test, mutation check, R8 mixed-run test;
   - "Closing open result set" warnings in `test-etl-publication-db.R`.
3. **U3 -- Task 9 (shared Game Logs rules).** 13/15 goldens fail on minutes (Israeli
   Summary off by 0.1; FF/EuroLeague in the last decimals) -- likely summation order /
   rounding vs R. Golden JSON (~280 MB) is gitignored under `web/test/goldens/`. The TS test
   must compare against the stored goldens (it re-ran R), game-type labels must come from
   the catalogue (hardcoded now), and the web code is minified one-liners -- reformat.
4. **U4 -- Tasks 6-8, 10-12** (scaffold leftovers, db client tests, API conventions, routes,
   page, preview). EuroLeague season payload is 12.85 MB from the DB and the reader takes
   ~2.8 s: Task 10 must fit 4.5 MB and the 3 s SQL budget.
5. **U5 -- Part C** (Tasks 13-16). Task 14 must prove crash release through the session
   pooler with a real kill.

`web/.env.example` (no secrets) is untracked because the credential hook blocks `.env*` names.
