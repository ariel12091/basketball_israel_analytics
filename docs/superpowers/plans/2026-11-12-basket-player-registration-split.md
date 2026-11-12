# Basket player registration split Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every basket.co.il registered player-season has one row of player facts (names, team, position, height, date of birth, nationality, identity link) in a new table, `basketball_test.basket_player_registration`. `basket_player_season` keeps only shooting totals.

**Architecture:** Two sources write `basket_player_registration`, each owning whole rows:
- **`source = 'stats'`:** a player on the season-stat pages. Names and team come from the stats row, and position, height, nationality and date of birth from the profile page.
- **`source = 'roster'`:** a player listed only on a team page.

A stats row always wins. The roster writer never touches a stats row, and when a roster-only player appears on the stats pages, the stats writer overwrites the row and its source becomes `'stats'`. The identity matcher, `player_display_names_v` and the data quality name-gap check read only the registration table.

The change ships in two phases:
- **Phase A:** create, fill, repoint and verify. Nothing is dropped.
- **Phase B:** drop the moved columns from `basket_player_season`, and drop the one-day-old `basket_player_roster`. This runs after one scheduled Basket refresh has succeeded on main.

**Tech Stack:** R 4.4.2, DBI/RPostgres, PostgreSQL (Supabase, port 5432 for every write), testthat, GitHub Actions.

**Spec:** this conversation, 2026-11-12 (user: "Why a new table? 95% of the players are already in the pre existing table" → "The shooting stats and the height are in the same table?" → "Yeah" to the split). The measured facts the plan relies on are below.

### Facts measured 2026-11-12

- `basket_player_season`: 1,339 rows, 2022-2027. 2027 has 179 rows, all with height, position, date of birth and an identity link.
- `basket_player_roster` (2027 only): 226 rows. 178 are on both tables and 48 only on the roster. 1 stats row, an outgoing player, has no roster row.
- On the 178 overlaps, height, date of birth, Hebrew name, team name and identity are equal on every row.
- English names differ on 8 rows, all of them NULL on the roster side: the roster lags on new players.
- Positions differ on 3 rows. The roster is finer, for example G vs PG. Because a stats row wins, those 3 keep G and today's view output is unchanged.
- Why the tables were merged on 2026-09-23: `docs/basket_shooting_history_ingestion.md` § Tables records that shooting and profiles were 1:1 on the same key, with a view existing only to rejoin them. That no longer holds. There are now registrations without totals: 48 this season, with 0 shooting rows.

## Global Constraints

- Every database change is a script that dry-runs by default (one transaction, every gate, then ROLLBACK) and commits only with its own `CONFIRM_*=1`, on port 5432. It is applied only after the user has seen the dry run and said yes.
- After creating a table, re-run `CONFIRM_DB_SECURITY_APPLY=1 Rscript scripts/apply_db_security.R`. It gives `app_readonly` SELECT and enables row-level security.
- Move data with `INSERT ... SELECT` and verify it value by value. Never re-run the scrape to fill the table (memory: "Reshape a table in SQL, don't rerun the pipeline").
- View edits start from the live definition (`pg_get_viewdef`), and each edit must match exactly once, as in `scripts/apply_player_display_names_size.R`.
- Writers upsert and never delete, the same as today.
- The `docs/shared_definitions.md` rows "Player display name" and "Gameflow lineup name order" name the owner table. Update them in the same commit as the view.
- Use `--write` / `--commit` flag names consistent with the existing scripts. Don't use purrr; use base apply functions.
- Be polite to basket.co.il: the scraper user agent, at least 5 s between team-page requests, and every page cached. Nothing in this plan adds requests.
- Never commit `web/next-env.d.ts`.

## Review Focus

1. **A roster-only player later appears on the stats pages.** The row becomes `source = 'stats'` with the stats names, and its identity link survives. Pinned in Task 3 by `test_registration_upsert_sql`.
2. **The two writers run in either order in one refresh** (stats then roster, or roster then stats). The final table is the same. Pinned in Task 3 by the gated DB test `test-basket-registration-db.R`, "writer order".
3. **A brand-new season before any stats page exists.** The roster writer can't learn the team's Basket spelling, so `basket_team_name` stays NULL and the run must not fail. The matcher then reports those rows unmatched, and the data quality check lists them as `basket_registration_unlinked`. Pinned in Task 3 by `test_roster_rows_without_stats_team`.
4. **A reader that grep didn't find still uses a column Phase B drops.** `ALTER TABLE ... DROP COLUMN` without CASCADE fails on a dependent view. The Phase B gate also lists dependents in `pg_depend` and searches the repo (`*.R`, `*.sql`, `*.py`, `*.ts`, `*.yml`) for each dropped column. Pinned in Task 6, step 2.
5. **The display names view must not change for any existing row.** The only allowed change is a NULL height or position becoming filled on a roster-only player. Pinned in Task 4 by the view gate (every row's md5 and every existing height and position equal).

---

### Task 1: Create and fill `basket_player_registration` (Phase A, database)

**Files:**
- Create: `sql/migrations/2026-11-12_basket_player_registration.sql`
- Create: `scripts/apply_basket_player_registration.R`
- Delete (uncommitted from PR #31 rework, never applied): `scripts/apply_player_display_names_roster_height.R`, `sql/migrations/2026-11-12_player_display_names_roster_height.sql`

**Interfaces:**
- Produces the table `basketball_test.basket_player_registration`:
  - **Key:** PK `(game_year, basket_player_id)`.
  - **Source:** `source text NOT NULL CHECK (source IN ('stats','roster'))`.
  - **Facts:** `player_name text NOT NULL`, `player_name_en`, `basket_team_name`, `basket_team_id integer` (roster rows only), `jersey`, `position_name`, `position_en`, `height_m` (1.5-2.4 CHECK as on the roster table), `date_of_birth`, `nationality_name`, `nationality_code`.
  - **Provenance:** `source_url`, `source_content_md5`, `fetched_at`, which are the stats page for stats rows and the team page for roster rows. Plus `profile_source_url`, `profile_source_content_md5`, `profile_fetched_at`, `profile_cache_hit`, and `run_id` for stats rows.
  - **Identity link:** `identity_id`, `canonical_player_id`, `identity_match_status`, `identity_matched_on`, `identity_matched_at`, with the same identity-consistency CHECK as `basket_player_season`.
  - **Bookkeeping:** `created_at`, `updated_at`.
  - The verified-identity index on `(game_year, identity_id)`.
  - `basket_player_season` gets `FOREIGN KEY (game_year, basket_player_id) REFERENCES basket_player_registration`. Add it after the fill.
- Copy the CHECKs verbatim from the live `basket_player_season` constraints. Read them with `$CLAUDE_JOB_DIR/tmp/pos11.R`, or `pg_get_constraintdef`.

- [ ] **Step 1: Write the migration SQL.** `CREATE TABLE`, then fill in two inserts:
  - every `basket_player_season` row, as `source = 'stats'`, with its fact, profile and identity columns;
  - the `basket_player_roster` rows with no stats row (48), as `source = 'roster'`, with their identity columns.

  Then add the FK, the table comment and `REVOKE ALL ... FROM PUBLIC`.
- [ ] **Step 2: Write `scripts/apply_basket_player_registration.R`.** It runs the SQL in one transaction (`lock_timeout` 5s, `statement_timeout` 120s). It commits only with `CONFIRM_BASKET_REGISTRATION=1`. Gates, each one `stop()` on failure:
  - the row count equals season rows + roster-only rows (1,339 + 48 = 1,387 today, but computed, not typed);
  - for every stats row, each moved column `IS NOT DISTINCT FROM` its `basket_player_season` value, counted per column, all 0;
  - for every roster-only row, the same check against `basket_player_roster`;
  - no stats key is missing;
  - the FK validates.
- [ ] **Step 3: Dry run.** `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/apply_basket_player_registration.R`. Expected: every gate prints 0 differences and the run ends `ROLLED BACK`.
- [ ] **Step 4: STOP for the user's yes.** Show the dry-run output. Then apply with `CONFIRM_BASKET_REGISTRATION=1`, then `CONFIRM_DB_SECURITY_APPLY=1 Rscript scripts/apply_db_security.R`. Expected: `COMMITTED`, and `has_table_privilege('app_readonly','basketball_test.basket_player_registration','SELECT')` is true.
- [ ] **Step 5: Commit** the SQL, the script and the two deletions: `sql: basket_player_registration holds every Basket player-season's facts`.

### Task 2: The identity matcher reads and writes the registration table

**Files:**
- Modify: `etl/basket_identity_match.R`. Remove `BASKET_IDENTITY_TABLES` and the `--table` flag added on this branch. `fetch_basket_registrations` (around `:99`), `write_basket_identity_links` (`:392`) and `validate_written_links` (`:458`) go back to one table: `basketball_test.basket_player_registration`. Update the header comment at `:4`.
- Modify: `.github/workflows/basket-refresh.yml:82-83`. Delete the step "Link roster rows to identities", and move the single link step after the roster scrape.

**Interfaces:**
- Consumes the Task 1 table.
- Produces `fetch_basket_registrations(con, seasons)` and `write_basket_identity_links(con, matches)`, with the same return shapes as on main.

- [ ] **Step 1: Make the edits.** Diff them against `main`'s version of the file: the only change from main should be the table name.
- [ ] **Step 2: Dry run** `Rscript etl/basket_identity_match.R --seasons=2025,2026,2027`. Expected for 2027: 227 rows, of which 222 are verified (179 stats rows and 43 roster-only), 1 is a proposal and 4 are unmatched, with 2025-2026 unchanged from today (533/533). Any change to an existing link is a failure. Compare with `--seasons` output from main against `basket_player_season`.
- [ ] **Step 3: Commit** `etl: identity matcher links basket_player_registration`.

### Task 3: Both scrapers write the registration table

**Files:**
- Modify: `scripts/backfill_basket_shooting_history.R`.
  - `write_scrape` (`:1037`): the stats upsert (`:1081`) writes only the totals and provenance to `basket_player_season`, after first upserting the registration row (`source = 'stats'`, names, `run_id`, stats provenance).
  - The profile UPDATE (`:1136`) targets `basket_player_registration`.
  - Repoint the read-back checks (`:976`, `:996`, `:1002`) and the column-existence guards (`:892`, `:905-907`) to the column's new table.
  - In Phase A, also keep writing the old columns on `basket_player_season`, so main's readers and this branch both work until Phase B. Mark each such write with `# PHASE B: remove`.
- Modify: `scripts/basket_rosters.R`. `write_basket_rosters` (`:143`) writes `basket_player_registration` rows with `source = 'roster'`. The team-name lookup (`:164`) reads registration rows with `source = 'stats'`.
- Test: `etl/tests/test_basket_rosters.R`, `etl/tests/test_basket_shooting_history_parser.R`, and the new `app/tests/testthat/test-basket-registration-db.R` (gated `identical(Sys.getenv("RUN_DB_TESTS"), "1")`).

**Interfaces:**
- Produces `registration_upsert_sql(source = c("stats", "roster")) -> character`, one SQL string used by both writers, defined once in `scripts/backfill_basket_shooting_history.R`, which `basket_rosters.R` already sources:
  - for `"stats"`: `ON CONFLICT (game_year, basket_player_id) DO UPDATE SET source = 'stats', <fact and provenance columns> = EXCLUDED...`;
  - for `"roster"`: the same `ON CONFLICT` with `... WHERE basket_player_registration.source = 'roster'`.
  - Neither updates the identity columns.

- [ ] **Step 1: Write the failing tests:**
  - in `test_basket_rosters.R`, `test_registration_upsert_sql`: the roster SQL contains `WHERE basket_player_registration.source = 'roster'`; the stats SQL contains `source = 'stats'`; neither contains `identity_id =`.
  - `test_roster_rows_without_stats_team`: `write_basket_rosters`'s row builder leaves `basket_team_name` NA, with no error, when no stats row shares a player with the team.
- [ ] **Step 2: Run them to see them fail.** `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('etl/tests/test_basket_rosters.R')"`. Expected: FAIL, `registration_upsert_sql` not found.
- [ ] **Step 3: Implement** `registration_upsert_sql` and repoint both writers.
- [ ] **Step 4: Run both etl test files.** Expected: PASS, including the existing 19 roster tests and the parser tests.
- [ ] **Step 5: Write the DB test `test-basket-registration-db.R`.** Inside a transaction that it rolls back, it:
  1. runs the roster upsert, then the stats upsert, on a fixed two-row fixture (player A on both sources, player B on the roster only);
  2. snapshots the rows;
  3. resets, runs the two upserts in the other order, and snapshots again.

  The test asserts that the two snapshots are equal. It also asserts that A is `source = 'stats'` with the stats English name, B is `source = 'roster'`, and A's pre-set `identity_id` is unchanged.
- [ ] **Step 6: Run it.** `RUN_DB_TESTS=1` with PG_PORT=5432, nothing else querying. Expected: PASS.
- [ ] **Step 7: Dry-run both scrapers from the cache, with no network.** Run `backfill_basket_shooting_history.R --seasons=2027 --with-profiles` without `--refresh-cache`, and `basket_rosters.R --season=2027`, both without `--write`. Expected: parse counts of 179 and 226, as today.
- [ ] **Step 8: Commit** `etl: Basket scrapers write basket_player_registration`.

### Task 4: `player_display_names_v` reads the registration table

**Files:**
- Create: `scripts/apply_player_display_names_registration.R`, modelled on `scripts/apply_player_display_names_size.R`. Its edit is one: in the live definition's `basket` CTE, `FROM basketball_test.basket_player_season b` becomes `FROM basketball_test.basket_player_registration b`, and it must match exactly once. There is no second CTE and no fallback join.
- Generates: `sql/migrations/2026-11-12_player_display_names_registration.sql`.
- Modify: `docs/shared_definitions.md`. The rows "Player display name" and "Gameflow lineup name order" name `basket_player_registration` as the owner of Basket names, height and position, and drop the roster-fallback wording added on this branch.

- [ ] **Step 1: Write the script.** Its gates, taken from the deleted roster-height script, are:
  - the columns are unchanged;
  - every row's names and identity md5 is unchanged;
  - every non-NULL height and position is unchanged;
  - the per-season height counts are printed;
  - `app_readonly` keeps SELECT;
  - the 2027 read takes under 1 s.

  It commits only with `CONFIRM_DISPLAY_NAMES_REGISTRATION=1`.
- [ ] **Step 2: Dry run.** Expected: 890 rows, 0 changes to names, identities, or existing heights and positions; 2026: 296 → 296; 2027: 179 → about 222 of 234 (the roster-height dry run measured 222).
- [ ] **Step 3: STOP for the user's yes, then apply.**
- [ ] **Step 4: Check the web app's readers on localhost.** `web/src/server/playerNames.ts` reads only the view, so it needs no code change. Run `cd web && npm.cmd run test -- --maxWorkers=1 src/server/playerNames.test.ts src/rules/gameflow.test.ts`. Expected: PASS.
- [ ] **Step 5: Commit** the script, the generated SQL and the contract: `sql: player_display_names_v reads basket_player_registration`.

### Task 5: Data quality name-gap check and docs

**Files:**
- Modify: `etl/player_identity_dictionary.R:962,965`, where `basket_seasons` and `unlinked` read `basket_player_registration`.
- Modify: `etl/run_data_quality_report.R:2884`, where `required_tables` names `basket_player_registration`.
- Modify: `app/tests/testthat/test-dq-player-name-gaps.R:29`, which asserts `basket_player_registration`.
- Modify: `docs/basket_shooting_history_ingestion.md` § Tables. Four relations, with the 2026-09-23 consolidation note kept, and one line on why the split came back: registrations without totals exist.
- Modify: `docs/basket_identity_handoff_2026-09-23.md:159`, adding one line: the link moved to `basket_player_registration` on 2026-11-12.
- Modify: `.github/workflows/basket-refresh.yml`, header comment only.

- [ ] **Step 1: Edit the test assertion first, and run it to see it fail.** `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('app/tests/testthat/test-dq-player-name-gaps.R')"`. Expected: FAIL on the table name.
- [ ] **Step 2: Repoint the check and the report; run again.** Expected: PASS.
- [ ] **Step 3: Run the check live.** Source `etl/player_identity_dictionary.R` and call `player_display_name_gaps(con, schema = "basketball_test")`. Expected for 2027: the 4 unmatched registrations plus the 1 proposal are listed, and the counts for 2025-2026 are unchanged.
- [ ] **Step 4: Update the docs, then commit** `etl: data quality name-gap check reads basket_player_registration`. Push and update PR #31's title and body: "Basket player registration split (phase A)". Merge only on an explicit "deploy".

### Task 6: Phase B: drop the moved columns and the roster table (after one scheduled refresh on main)

**Files:**
- Create: `scripts/apply_basket_registration_phase_b.R` and `sql/migrations/<date>_basket_registration_phase_b.sql`.
- Modify: `scripts/backfill_basket_shooting_history.R`, removing every `# PHASE B: remove` write.

- [ ] **Step 1: Precondition.** The Basket refresh workflow ran green on main after the Phase A merge:
  `gh run list --workflow=basket-refresh.yml --branch=main --limit=1`, then compare the registration table with `basket_player_season` on the shared columns: 0 differences.
- [ ] **Step 2: Write the script.**
  - **Backup:** before dropping anything, export `basket_player_season` and `basket_player_roster` to `exports/basket_backup_<date>/*.parquet` and read them back.
  - **Columns to drop** from `basket_player_season`:
    - names: `player_name`, `player_name_en`;
    - profile facts: `position_name`, `position_en`, `height_m`, `nationality_name`, `nationality_code`;
    - profile provenance: the four `profile_*` columns;
    - team and birth date: `basket_team_name`, `date_of_birth`;
    - identity link: the five `identity_*` and `canonical_player_id` columns.
  - **Gates:**
    - `pg_depend` lists no view depending on those columns;
    - a repo search finds no reference to the dropped columns qualified by `basket_player_season`;
    - after the drop, the display names view and the data quality check give the same output as before.
  - **Roster table:** `DROP TABLE basketball_test.basket_player_roster`.
  - Commits only with `CONFIRM_BASKET_PHASE_B=1`.
- [ ] **Step 3: Dry run, STOP for the user's yes, apply,** then the security re-apply.
- [ ] **Step 4: Remove the Phase A double writes.** Run both etl test files and a cache-only dry run of the backfill. Expected: PASS and 179 rows.
- [ ] **Step 5: Commit, open a PR, and merge on "deploy".** Commit message: `etl: basket_player_season holds only shooting totals`.
