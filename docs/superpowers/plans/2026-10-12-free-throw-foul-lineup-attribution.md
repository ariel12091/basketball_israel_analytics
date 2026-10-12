# Free Throws Credited to the Lineup That Drew the Foul: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Credit every free throw (its points, its possession end and its
true-shooting trip) to the lineup that was on the floor when the foul was
drawn, so a trip is counted once and TS% means the same, correct thing in
every Israeli view.

**Architecture:** The base action table keeps recording who was physically
on the floor for each row; five new `credit_*` columns record which lineup
each row is credited to (the parent action's lineup for a free throw, the
row's own lineup for everything else). Every aggregation of *events* moves
from the physical columns to the credit columns; every computation of
*floor time* stays on the physical columns. Nothing is rebuilt from cold
storage: the credit columns are derived from rows already in the hot table.

**Tech Stack:** PostgreSQL on Supabase (DDL and migrations on port 5432 as
the ETL role), R 4.4.2 scripts with DBI/RPostgres, testthat.

**Spec:** none. The design was agreed in conversation on 2026-10-12; the
"Design" section below is the record. Measurements quoted here were taken
that day and are reproducible with the queries in Task 1.

**A note on code in this plan.** New code (migration, credit function,
gates, tests) is written out in full. Existing SQL that has to be edited is
*pointed at*, with the exact transformation rule and the gate that proves
the edit, and is not retyped here: a retyped copy of 13 verified objects
would be the most likely place for this plan to be wrong. Each task says
which file to read end to end before editing it.

## Why

Measured on the live database, 2026-10-12:

- The Four Factors pipeline counts a free-throw trip once per lineup
  segment. A segment changes whenever *either* team substitutes, so a trip
  with a substitution between its free throws is counted twice.
  `team_metrics_by_game_mv.off_ts_poss_raw` equals "FGA + trips counted per
  segment" in 938 of 938 team-games.
- Share of personal-foul trips split this way: 32.4% (2025), 30.3% (2026),
  33.6% (2027).
- League TS%, 2026: 0.44 estimate 56.60, each trip once 57.02, Four Factors
  as stored 55.24. Four Factors TS% is about 1.8 points low.
- Free throws shot by a later segment than the foul's, offense rows, 2026:
  1,804 of 7,194 personal-foul free throws, carrying 1,317 points (3.5% of
  all points) and 1,309 possession ends (3.8% of all possessions).
- Effect of re-crediting them on offensive rating, 2026: players on court
  with 300+ possessions move by a median 0.15 and at most 0.65 points per
  100 (rank correlation 0.9993); lineups with 30+ possessions by a median
  0.25 and at most 4.99. Team totals do not change.

## Design

**The rule.** A free-throw row is credited to the lineup recorded on its
parent action's row, taken from the same team's same perspective
(`team_id`, `type_lineup`). The parent is a foul of any kind, or the made
shot for an and-one. Every other row is credited to its own lineup. Three
free throws in the table have no parent row; they stay with their own
lineup.

**What "credited" covers.** Points, possession ends, shot and free-throw
counts, rebounds, turnovers and every other event count, and the
true-shooting trip. Not floor time: minutes always follow the physical
columns.

**True-shooting trips are unchanged in definition.** A trip is still a
distinct personal-foul parent. And-one free throws hang off the shot row,
so they were never trips and still are not. Technical and unsportsmanlike
free throws still count in FTR and not in trips. The only change is that a
trip can no longer land in two groups.

**The five credit columns** on `basketball_test.df_pts_poss_lineups_longer_mv`:

| Column | Copied from the parent row's |
|---|---|
| `credit_lineup_hash` | `lineup_hash` |
| `credit_segment_id` | `segment_id` |
| `credit_num_starters` | `num_starters` |
| `credit_own_starters` | `own_starters` |
| `credit_opp_starters` | `opp_starters` |

**The reader rule.** In any aggregation that groups, filters or joins
*event rows* on one of the five physical columns, use the `credit_`
column. In any expression that computes *time* (`segment_seconds`,
`segment_start_elapsed_seconds`, `segment_end_elapsed_seconds`,
`event_elapsed_seconds`, window or stint durations), keep the physical
column. Row-level clutch predicates (`quarter`, `end_game_seconds_remaining`,
`own_team_score`, `opp_team_score`, `team_score`) are untouched: a free
throw is still judged at the clock and score it was shot at.

**Decisions taken, so the executor does not reopen them:**

1. Gameflow (the stint ribbon) keeps using the physical columns. It shows
   who was on the floor when each point was scored, so after this change
   it will disagree with on/off about roughly 3.5% of points, by design.
   This is documented in Task 9, not fixed.
2. EuroLeague is out of scope. It counts a trip with its last free throw,
   which has no double count. Aligning it to this rule is a separate plan.
3. React/Plumber is out of scope. It calls the same stored functions, so
   it inherits the new numbers without a code change.
4. The unwired `app/R/server_tab6_team_stats.R` is not touched.

## Global Constraints

- Schema is `basketball_test`. All SQL is parameterised or static; no
  user value is ever pasted into SQL.
- Every DDL or migration script is dry-run by default and commits only
  with an explicit flag, following `scripts/apply_db_security.R`.
- After any `DROP FUNCTION` or `DROP ... MATERIALIZED VIEW`/`TABLE`, re-run
  `scripts/apply_db_security.R` with `CONFIRM_DB_SECURITY_APPLY=1`. Both
  wipe `app_readonly` grants.
- Never `DROP ... CASCADE` an L1 object. Never rebuild
  `df_pts_poss_lineups_longer_mv` from its definition file during this
  work: its sources (`pws`, `possessions`) are cold and empty between ETL
  runs.
- Edit SQL files with exact string replacements only, never regex. Check
  `git diff --stat` after each edit; several files in this repo have mixed
  line endings and a normalising edit shows as a whole-file diff.
- Run database tests and gates only when nothing else is querying as
  `app_readonly` on port 6543 (no local app, no second test run). See
  CLAUDE.md, "Port 6543 breaks RPostgres when two clients query at once".
  Migration and gate scripts connect on port 5432.
- New heavy functions carry `SET plan_cache_mode = force_custom_plan`.
- The definition file of a table-type object and its
  `refresh_*_for_games` function hold the same SQL twice. Both are edited
  in the same task and the task's gate runs after a full refresh.
- Do not run any of this while the nightly ETL is running (18:15 UTC cron,
  typically starting up to 2.5 hours later). Check
  `gh run list --workflow=etl-full.yml --limit 1` first.
- Branch names: `sql/ft-credit-<step>` for SQL tasks, `shiny/ft-credit-ts`
  for Task 8.

## Review Focus

1. **A nightly ETL game arrives with NULL credit columns.** The refresh of
   the base table deletes and re-inserts a game's rows; if the credit
   function does not run afterwards, every reader that groups on
   `credit_lineup_hash` silently drops that game. Pinned by the NOT NULL
   check in Task 2's test and by wiring the call inside the refresh
   function itself.
2. **A reader that groups stats and time in one CTE.** Switching its keys
   to `credit_*` would move floor time along with the points. Pinned by the
   time gate (G4) in every reader task.
3. **A trip whose free throws sit in two games' worth of ids.** Parent
   lookup must match on `game_id` as well as `id`. Pinned by gate G1.
4. **Starters filters.** A free throw re-credited to the foul's lineup must
   also take that lineup's starters counts, or a "5 starters on" filter
   keeps the points and drops the trip. Pinned by gate G5.
5. **A clutch window that contains the free throws but not the foul.** The
   free throws must still be counted (row-level time and score), under the
   foul's lineup. Pinned by the clutch case in gate G6.

---

## File Structure

| File | Responsibility | Status |
|---|---|---|
| `sql/migrations/2026-10-12_ft_credit_columns.sql` | Add the five columns and their index | Create |
| `sql/functions/refresh_ft_credit_fields_for_games.sql` | Populate the credit columns for given games (NULL = all) | Create |
| `sql/functions/refresh_df_pts_poss_lineups_longer_for_games.sql` | Call the credit function after re-inserting a game | Modify |
| `sql/materialized_views/df_pts_poss_longer.sql` | Add the columns to the full-rebuild definition and populate them | Modify |
| `scripts/apply_ft_credit_migration.R` | Resumable online apply: columns, backfill, per-object refresh and MV swaps | Create |
| `scripts/verify_ft_credit.R` | Baseline snapshot and gates G1-G6 | Create |
| 13 definition files and their refresh functions, 9 reader functions | Event aggregations move to `credit_*` | Modify (Tasks 3-7) |
| `app/R/server_tab3.R`, `app/R/server_tab5_traditional.R`, `app/R/helpers.R`, `app/R/server_tab7_compare.R`, `app/R/ui_tab8_euro.R`, `app/R/ui_tab9_euro_team.R`, `app/R/global.R` | TS% from trips, one usage helper, wording | Modify (Task 8) |
| `app/tests/testthat/test-ft-credit-db.R` | Permanent DB-backed invariants | Create |

---

## The reader switch procedure

Tasks 3 to 7 each apply this to a named set of objects. It is written once
here; each task lists its own files, specifics and gates.

1. Read the object's definition file end to end, and its
   `refresh_*_for_games` function if it has one.
2. List every reference to `lineup_hash`, `segment_id`, `num_starters`,
   `own_starters`, `opp_starters` that reads the base table (directly or
   through a CTE). Classify each as *event* or *time* using the reader
   rule. Write the classification into the task's commit message.
3. Replace the event references with the `credit_` column by exact string
   edits. Where one CTE feeds both an event aggregation and a time
   computation, select both the physical and the credit column from it
   under distinct names; do not split the CTE.
4. `git diff` the file: every changed line must be one of the classified
   event references.
5. Apply it through `scripts/apply_ft_credit_migration.R --object <name>`
   (dry run first). For a table-type object this redeploys the refresh
   function and refreshes all games in batches. For a materialized view it
   stages the new definition under `<name>__ftc_next`, compares, and swaps
   by rename in one short transaction.
6. Run the task's gates with `scripts/verify_ft_credit.R --check <name>`.
7. Re-apply security, run the fast test suite, commit.

---

### Task 1: Baseline snapshot and gate script

**Files:**
- Create: `scripts/verify_ft_credit.R`

**Interfaces:**
- Produces: schema `ft_credit_migration` holding one baseline table per
  object, named `baseline_<object>`; CLI `--snapshot`, `--check <object>`,
  `--check-base`, `--drop`. Later tasks call `--check`.

The script connects as the ETL role on port 5432 (`etl/.Renviron`).

- [ ] **Step 1: Write `--snapshot`.** For each object below, create
  `ft_credit_migration.baseline_<object>` as
  `CREATE TABLE ... AS SELECT * FROM basketball_test.<object>`. Objects:
  `lineup_four_factors_by_game`, `player_four_factors_by_game`,
  `team_metrics_by_game_mv`, `team_four_factors_mv`, `team_metrics_rolling_mv`,
  `onoff_default_mv`, `player_advanced_stats_mv`, `mv_lineup_totals_by_day`,
  `sub_lineups_stats`, `team_ppp_ratings_mv`, `team_ratings_preset_cache`,
  `player_traditional_by_game`, `default_clutch_player_totals_by_game`,
  `player_stats_actions_by_game`, `player_traditional_stats_mv`.
  Refuse to overwrite an existing baseline unless `--force` is passed.

- [ ] **Step 2: Write the object map.** A named list, one entry per
  object, with: `key` (the columns that identify a team-game or a
  team-season in that object), `additive` (count columns that must sum
  identically per `key` before and after), `trips` (the trip-count
  column, if any), `time` (minutes columns that must be identical row for
  row). Fill each entry by reading the object's columns with
  `SELECT * FROM basketball_test.<object> LIMIT 0`. The two entries known
  today:

```r
object_map <- list(
  lineup_four_factors_by_game = list(
    key = c("game_id", "team_id", "type_lineup"),
    additive = c("total_points", "total_poss", "oreb_count", "oreb_opportunities",
                 "tov_count", "total_ft_attempts", "total_fga", "total_fgm", "total_fg3_made"),
    trips = "ts_poss_count",
    time = "minutes"
  ),
  team_metrics_by_game_mv = list(
    key = c("game_id", "team_id"),
    additive = c("off_points_raw", "def_points_raw", "off_poss_raw", "def_poss_raw",
                 "off_fga_raw", "def_fga_raw", "off_fta_raw", "def_fta_raw",
                 "pts", "reb", "ast", "stl", "blk", "dfl", "tov", "fgm", "fga",
                 "3pm", "3pa", "ftm", "fta"),
    trips = c("off_ts_poss_raw", "def_ts_poss_raw"),
    time = c("off_minutes", "def_minutes")
  )
)
```

- [ ] **Step 3: Write the gates.**

  - **G1, credit columns are well formed** (`--check-base`). All four
    queries must return 0:

```sql
-- no NULL credit key where the physical key is set
SELECT COUNT(*) FROM basketball_test.df_pts_poss_lineups_longer_mv
WHERE credit_lineup_hash IS NULL OR credit_segment_id IS NULL
   OR (num_starters IS NOT NULL AND credit_num_starters IS NULL)
   OR (own_starters IS NOT NULL AND credit_own_starters IS NULL)
   OR (opp_starters IS NOT NULL AND credit_opp_starters IS NULL);

-- a row that is not a free throw is credited to its own lineup
SELECT COUNT(*) FROM basketball_test.df_pts_poss_lineups_longer_mv
WHERE type <> 'freeThrow'
  AND (credit_lineup_hash, credit_segment_id) IS DISTINCT FROM (lineup_hash, segment_id);

-- a free throw with a parent row is credited to that row's lineup
SELECT COUNT(*)
FROM basketball_test.df_pts_poss_lineups_longer_mv a
JOIN basketball_test.df_pts_poss_lineups_longer_mv p
  ON p.game_id = a.game_id AND p.id = a.parent_action_id
 AND p.team_id = a.team_id AND p.type_lineup IS NOT DISTINCT FROM a.type_lineup
WHERE a.type = 'freeThrow'
  AND (a.credit_lineup_hash, a.credit_segment_id, a.credit_num_starters,
       a.credit_own_starters, a.credit_opp_starters)
      IS DISTINCT FROM
      (p.lineup_hash, p.segment_id, p.num_starters, p.own_starters, p.opp_starters);

-- every free throw of one trip shares one credit segment
SELECT COUNT(*) FROM (
  SELECT 1 FROM basketball_test.df_pts_poss_lineups_longer_mv
  WHERE type = 'freeThrow'
  GROUP BY game_id, team_id, type_lineup, parent_action_id
  HAVING COUNT(DISTINCT (credit_lineup_hash, credit_segment_id)) > 1) x;
```

  - **G2, team totals did not move.** For the object's `key` and each
    `additive` column, `SUM` in the live object equals `SUM` in the
    baseline for every key. Report the count of keys that differ; must
    be 0.
  - **G3, each trip counted once.** For objects with a `trips` column,
    summed to team-game and offense perspective, it equals:

```sql
SELECT a.game_id, a.team_id,
       COUNT(*) FILTER (WHERE a.type = 'shot')
       + COUNT(DISTINCT a.parent_action_id) FILTER (
           WHERE a.type = 'freeThrow' AND p.id IS NOT NULL) AS expected_ts_poss
FROM basketball_test.df_pts_poss_lineups_longer_mv a
LEFT JOIN basketball_test.df_pts_poss_lineups_longer_mv p
  ON p.game_id = a.game_id AND p.id = a.parent_action_id
 AND p.type = 'foul' AND p.parameters_type = 'personal'
 AND p.team_id = a.team_id AND p.type_lineup = a.type_lineup
WHERE a.type_lineup = 'offense'
GROUP BY a.game_id, a.team_id;
```

    Before this work the live value is the per-segment count and G3
    fails for most team-games; that failure is the "red" for Tasks 3-7.
  - **G4, time did not move.** Each `time` column is identical to the
    baseline for every row, joined on the object's full primary key.
    Must be 0 differing rows.
  - **G5, the move is the size expected.** For lineup- and player-level
    objects, the sum over rows of the absolute change in points is
    positive and no larger than twice the points on re-credited free
    throws, computed from the base table:

```sql
SELECT 2 * SUM(COALESCE(team_score, 0)) AS max_abs_points_moved
FROM basketball_test.df_pts_poss_lineups_longer_mv
WHERE type = 'freeThrow'
  AND (credit_lineup_hash, credit_segment_id) IS DISTINCT FROM (lineup_hash, segment_id);
```

  - **G6, reader functions.** For each stored reader function, call it
    before and after with the same arguments for these cases and compare:
    no filter; `p_home_away := 'home'`; `p_last_n_games := 5`;
    `p_max_margin := 5, p_max_time_remaining := 300`; and, where the
    function takes them, `p_num_starters_off := 5`. Team-level functions
    must return identical rows except TS% and trip-derived columns.
    Lineup- and player-level functions must satisfy G2 when summed to
    team level.

- [ ] **Step 4: Run the snapshot.**

Run: `Rscript scripts/verify_ft_credit.R --snapshot`
Expected: 15 lines `baseline_<object>: <n> rows`, exit 0.

- [ ] **Step 5: Run G3 against today's data to see it fail.**

Run: `Rscript scripts/verify_ft_credit.R --check team_metrics_by_game_mv`
Expected: G2 0 differences, G4 0 differences, **G3 FAIL** with several
hundred team-games (measured 2026-10-12: 371 of 442 in 2026).

- [ ] **Step 6: Commit.**

```bash
git add scripts/verify_ft_credit.R
git commit -m "Add baseline and gate script for free-throw credit migration"
```

### Task 2: Credit columns on the base table

**Files:**
- Create: `sql/migrations/2026-10-12_ft_credit_columns.sql`
- Create: `sql/functions/refresh_ft_credit_fields_for_games.sql`
- Create: `scripts/apply_ft_credit_migration.R`
- Create: `app/tests/testthat/test-ft-credit-db.R`
- Modify: `sql/functions/refresh_df_pts_poss_lineups_longer_for_games.sql` (end of the function body)
- Modify: `sql/materialized_views/df_pts_poss_longer.sql` (column list, and after the timing block)

**Interfaces:**
- Produces: columns `credit_lineup_hash`, `credit_segment_id`,
  `credit_num_starters`, `credit_own_starters`, `credit_opp_starters`,
  never NULL where the physical column is set; function
  `basketball_test.refresh_ft_credit_fields_for_games(game_ids int4[])
  RETURNS bigint` (rows updated; `NULL` argument = every game).

No reader uses the columns yet, so this task changes no number in the app.

- [ ] **Step 1: Write the failing test** in
  `app/tests/testthat/test-ft-credit-db.R`. Use the connection and skip
  preamble of `app/tests/testthat/test-data-shape-db.R` lines 1-22, then:

```r
test_that("every base-table row carries credit columns", {
  bad <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM basketball_test.df_pts_poss_lineups_longer_mv
    WHERE credit_lineup_hash IS NULL OR credit_segment_id IS NULL")
  expect_equal(as.numeric(bad$n), 0)
})

test_that("all free throws of one trip are credited to one segment", {
  split <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM (
      SELECT 1 FROM basketball_test.df_pts_poss_lineups_longer_mv
      WHERE type = 'freeThrow'
      GROUP BY game_id, team_id, type_lineup, parent_action_id
      HAVING COUNT(DISTINCT (credit_lineup_hash, credit_segment_id)) > 1) x")
  expect_equal(as.numeric(split$n), 0)
})
```

- [ ] **Step 2: Run it to see it fail.**

Run (from `app/`, nothing else querying):
`RUN_DB_TESTS=1 Rscript -e "readRenviron('.Renviron'); testthat::test_file('tests/testthat/test-ft-credit-db.R')"`
Expected: both tests ERROR with `column "credit_lineup_hash" does not exist`.

- [ ] **Step 3: Write the migration.** Take the column types from the
  physical columns (`\d basketball_test.df_pts_poss_lineups_longer_mv`)
  and use the same type for each `credit_` twin.

```sql
-- sql/migrations/2026-10-12_ft_credit_columns.sql
-- Which lineup each action is CREDITED to. Equal to the physical columns
-- for every row except a free throw, which takes its parent action's.
-- Populated by basketball_test.refresh_ft_credit_fields_for_games().
ALTER TABLE basketball_test.df_pts_poss_lineups_longer_mv
  ADD COLUMN IF NOT EXISTS credit_lineup_hash  text,
  ADD COLUMN IF NOT EXISTS credit_segment_id   integer,
  ADD COLUMN IF NOT EXISTS credit_num_starters integer,
  ADD COLUMN IF NOT EXISTS credit_own_starters integer,
  ADD COLUMN IF NOT EXISTS credit_opp_starters integer;
```

  If a physical column's type differs from `text`/`integer`, change the
  twin to match before applying. Adding nullable columns without a default
  is metadata-only and takes no table rewrite.

- [ ] **Step 4: Write the credit function.**

```sql
-- sql/functions/refresh_ft_credit_fields_for_games.sql
-- Credit every free throw to the lineup on its parent action's row (the
-- foul, or the made shot of an and-one), from the same team's same
-- perspective. Every other row is credited to its own lineup. Free throws
-- with no parent row keep their own lineup.
CREATE OR REPLACE FUNCTION basketball_test.refresh_ft_credit_fields_for_games(game_ids int4[])
RETURNS bigint
LANGUAGE plpgsql
SET plan_cache_mode = force_custom_plan
AS $$
DECLARE
  v_rows bigint;
BEGIN
  UPDATE basketball_test.df_pts_poss_lineups_longer_mv d
  SET credit_lineup_hash  = src.lineup_hash,
      credit_segment_id   = src.segment_id,
      credit_num_starters = src.num_starters,
      credit_own_starters = src.own_starters,
      credit_opp_starters = src.opp_starters
  FROM (
    SELECT a.game_id, a.id, a.team_id, a.type_lineup,
           COALESCE(p.lineup_hash, a.lineup_hash) AS lineup_hash,
           CASE WHEN p.id IS NULL THEN a.segment_id   ELSE p.segment_id   END AS segment_id,
           CASE WHEN p.id IS NULL THEN a.num_starters ELSE p.num_starters END AS num_starters,
           CASE WHEN p.id IS NULL THEN a.own_starters ELSE p.own_starters END AS own_starters,
           CASE WHEN p.id IS NULL THEN a.opp_starters ELSE p.opp_starters END AS opp_starters
    FROM basketball_test.df_pts_poss_lineups_longer_mv a
    LEFT JOIN basketball_test.df_pts_poss_lineups_longer_mv p
      ON a.type = 'freeThrow'
     AND p.game_id = a.game_id
     AND p.id = a.parent_action_id
     AND p.team_id = a.team_id
     AND p.type_lineup IS NOT DISTINCT FROM a.type_lineup
    WHERE game_ids IS NULL OR a.game_id = ANY(game_ids)
  ) src
  WHERE d.game_id = src.game_id
    AND d.id = src.id
    AND d.team_id = src.team_id
    AND d.type_lineup IS NOT DISTINCT FROM src.type_lineup
    AND (d.credit_lineup_hash, d.credit_segment_id, d.credit_num_starters,
         d.credit_own_starters, d.credit_opp_starters)
        IS DISTINCT FROM
        (src.lineup_hash, src.segment_id, src.num_starters, src.own_starters, src.opp_starters);
  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;
```

  `(game_id, id, team_id, type_lineup)` identifies a row: verified
  2026-10-12, 0 duplicate keys in 558,362 rows. The refresh functions must
  not be granted to `app_readonly`; `scripts/apply_db_security.R` only
  grants the allow-listed reader functions, so no change is needed there.

- [ ] **Step 5: Wire the nightly path.** In
  `sql/functions/refresh_df_pts_poss_lineups_longer_for_games.sql`, as the
  last statement before the function returns, add:

```sql
  PERFORM basketball_test.refresh_ft_credit_fields_for_games(game_ids);
```

  Read the function first to find its parameter name; if it is not
  `game_ids`, use the actual name. This keeps the ETL R code unchanged:
  the nightly run already calls this function for each new game.

- [ ] **Step 6: Keep the full-rebuild definition in step.** In
  `sql/materialized_views/df_pts_poss_longer.sql`, add the five columns
  to the `CREATE TABLE ... AS SELECT` list as typed NULLs, next to the
  existing `NULL::numeric AS event_elapsed_seconds` lines, and after the
  existing "Populate canonical timing" block add:

```sql
-- Populate credit attribution. See refresh_ft_credit_fields_for_games.
SELECT basketball_test.refresh_ft_credit_fields_for_games(NULL::int4[]);
```

  Do not run this file. It only matters for a future rebuild from restored
  cold storage.

- [ ] **Step 7: Write `scripts/apply_ft_credit_migration.R`.** Model it on
  `scripts/apply_canonical_segment_minutes_online.R`: reuse its
  `run_transaction()`, `set_session_limits()`, `split_batches()` and
  `call_game_batches()` helpers by sourcing them from
  `scripts/minutes_migration_helpers.R` if they live there, otherwise copy
  those four functions. Stages for this task, each idempotent:
  1. `--apply` absent: print the plan and exit 0 (dry run).
  2. Take a session advisory lock on
     `hashtext('basketball_test.ft_credit.2026_10_12')`.
  3. Run the migration file (`immediate = TRUE`).
  4. Deploy `refresh_ft_credit_fields_for_games.sql` and the modified
     `refresh_df_pts_poss_lineups_longer_for_games.sql`.
  5. `call_game_batches("refresh_ft_credit_fields_for_games", <all game
     ids>, 25, "credit backfill")`, one transaction per batch.
  6. Print total rows updated.
  Later tasks add an `--object <name>` stage to this script.

- [ ] **Step 8: Apply.**

Run: `Rscript scripts/apply_ft_credit_migration.R` then
`Rscript scripts/apply_ft_credit_migration.R --apply`
Expected: about 558,000 rows updated across 469 games on the first run;
0 rows on an immediate second run.

- [ ] **Step 9: Run the base gate and the tests.**

Run: `Rscript scripts/verify_ft_credit.R --check-base`
Expected: G1, four queries, all 0.

Run: the Step 2 command.
Expected: 2 tests PASS.

- [ ] **Step 10: Confirm nothing in the app moved.** Every baseline object
  is still identical to its live object:

Run: `Rscript scripts/verify_ft_credit.R --check lineup_four_factors_by_game`
Expected: G2 0, G4 0 (G3 still fails; no reader has switched yet).

- [ ] **Step 11: Re-apply security and commit.**

```bash
CONFIRM_DB_SECURITY_APPLY=1 Rscript scripts/apply_db_security.R
git add sql/migrations/2026-10-12_ft_credit_columns.sql \
        sql/functions/refresh_ft_credit_fields_for_games.sql \
        sql/functions/refresh_df_pts_poss_lineups_longer_for_games.sql \
        sql/materialized_views/df_pts_poss_longer.sql \
        scripts/apply_ft_credit_migration.R app/tests/testthat/test-ft-credit-db.R
git commit -m "Add credit attribution columns to the base action table"
```

### Task 3: Lineup Four Factors and the team objects built on it

This is the task that corrects team and lineup TS%.

**Files:**
- Modify: `sql/materialized_views/lineup_four_factors_by_game.sql`
- Modify: `sql/materialized_views/team_metrics_by_game_mv.sql`, `sql/functions/refresh_team_metrics_by_game_for_games.sql` (only if either reads the base table's physical lineup columns directly; both read `lineup_four_factors_by_game` for trips)
- Modify: `scripts/apply_ft_credit_migration.R` (add the `--object` stage)
- Modify: `scripts/verify_ft_credit.R` (object map entries if missing)
- Test: `app/tests/testthat/test-ft-credit-db.R`

**Interfaces:**
- Consumes: the five `credit_*` columns (Task 2).
- Produces: `lineup_four_factors_by_game.ts_poss_count` summing, per
  team-game, to FGA + distinct personal-foul trips; `team_metrics_by_game_mv`
  `off_ts_poss_raw`/`def_ts_poss_raw` and `team_four_factors_mv` TS% derived
  from it.

- [ ] **Step 1: Write the failing test.** Append to
  `app/tests/testthat/test-ft-credit-db.R`:

```r
test_that("team Four Factors counts each free-throw trip once", {
  bad <- DBI::dbGetQuery(con, "
    WITH expected AS (
      SELECT a.game_id, a.team_id,
             COUNT(*) FILTER (WHERE a.type = 'shot')
             + COUNT(DISTINCT a.parent_action_id) FILTER (
                 WHERE a.type = 'freeThrow' AND p.id IS NOT NULL) AS ts_poss
      FROM basketball_test.df_pts_poss_lineups_longer_mv a
      LEFT JOIN basketball_test.df_pts_poss_lineups_longer_mv p
        ON p.game_id = a.game_id AND p.id = a.parent_action_id
       AND p.type = 'foul' AND p.parameters_type = 'personal'
       AND p.team_id = a.team_id AND p.type_lineup = a.type_lineup
      WHERE a.type_lineup = 'offense'
      GROUP BY a.game_id, a.team_id
    )
    SELECT COUNT(*) AS n
    FROM basketball_test.team_metrics_by_game_mv tm
    JOIN expected e ON e.game_id = tm.game_id AND e.team_id = tm.team_id
    WHERE tm.off_ts_poss_raw <> e.ts_poss")
  expect_equal(as.numeric(bad$n), 0)
})
```

- [ ] **Step 2: Run it to see it fail.** Expected: FAIL, `n` in the
  hundreds (808 across all seasons on 2026-10-12).

- [ ] **Step 3: Switch `lineup_four_factors_by_game`** using the reader
  switch procedure. Specifics for this file:
  - `segment_stats` (the CTE that sums `team_score`, `final_end_flag` and
    counts shots, rebounds, turnovers and `ts_poss_count`) is the event
    aggregation: its `lineup_hash`, `num_starters`, `opp_starters` and
    `segment_id` keys become the `credit_` columns.
  - `window_times`, `normalized_window_starts`, `window_minutes` and
    everything that derives `opp_island` are time: they stay physical.
  - The base CTE feeds both. Select the credit columns from it under
    their own names alongside the physical ones.
  - The final join between stats and minutes is on lineup, team, game and
    `opp_starters`. After the switch the stats side carries credit values
    in those columns and the minutes side physical ones; the join stays
    as written, because both describe "lineup X against N opposing
    starters in game G". A lineup-game that now has minutes but no stats
    row, or stats but no minutes, must still appear: check whether the
    join is `FULL`/`LEFT` and, if a lineup-game can now exist on only one
    side, make it a `FULL JOIN` with `COALESCE` on the keys.

- [ ] **Step 4: Add the `--object` stage** to
  `scripts/apply_ft_credit_migration.R`: for a materialized view, create
  `<name>__ftc_next` from the edited definition (rewriting the object
  name and index names by exact substitution, as `transform_mv_sql()` in
  the canonical-minutes script does), print old and new row counts, then
  in one transaction with `lock_timeout = '5s'` rename the live view to
  `<name>__ftc_old`, the staged view to `<name>`, and the indexes to
  match; retry the rename up to 12 times on lock timeout. For a
  table-type object, deploy its refresh function and call it for all
  games in batches of 25. Drop `__ftc_old` objects only in Task 9.

- [ ] **Step 5: Apply in dependency order.**

Run, each as dry run then `--apply`:
```
Rscript scripts/apply_ft_credit_migration.R --object lineup_four_factors_by_game
Rscript scripts/apply_ft_credit_migration.R --object team_metrics_by_game_mv
Rscript scripts/apply_ft_credit_migration.R --object team_four_factors_mv
Rscript scripts/apply_ft_credit_migration.R --object team_metrics_rolling_mv
```
  `team_four_factors_mv` and `team_metrics_rolling_mv` need no definition
  edit if they only read the objects above; they are refreshed
  (`REFRESH MATERIALIZED VIEW`) so they pick up the new counts.

- [ ] **Step 6: Run the gates.**

Run: `Rscript scripts/verify_ft_credit.R --check <object>` for each of the
four.
Expected: G2 0 differences, G4 0 differences, **G3 0 differences** (now
passing). For `lineup_four_factors_by_game` G5 passes.

- [ ] **Step 7: Run the Step 1 test.** Expected: PASS.

- [ ] **Step 8: Spot-check the headline number.** League TS% for 2026 from
  `team_metrics_by_game_mv`
  (`100 * SUM(off_points_raw) / (2 * SUM(off_ts_poss_raw))`) should read
  57.0 (it read 55.2 before).

- [ ] **Step 9: Advance the cache marker.** The app caches season pulls
  keyed on `app_meta.etl_full_last_success`. After the objects change,
  advance it so running workers drop stale results:

```sql
UPDATE basketball_test.app_meta SET value = to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')
WHERE key = 'etl_full_last_success';
```

  Read the row first and keep its existing value format.

- [ ] **Step 10: Security, tests, commit.**

```bash
CONFIRM_DB_SECURITY_APPLY=1 Rscript scripts/apply_db_security.R
git add sql/materialized_views/lineup_four_factors_by_game.sql scripts/ app/tests/testthat/test-ft-credit-db.R
git commit -m "Lineup and team Four Factors read credit attribution"
```

### Task 4: Player on/off objects

**Files:**
- Modify: `sql/materialized_views/player_four_factors_by_game.sql`, `sql/functions/refresh_player_four_factors_by_game_for_games.sql`
- Modify: `sql/materialized_views/onoff_mv.sql`, `sql/functions/refresh_onoff_default_for_games.sql`
- Modify: `sql/materialized_views/player_advanced_stats_mv.sql`, `sql/functions/refresh_player_advanced_stats_for_games.sql`
- Modify: `sql/functions/onoff_compute.sql` (only its direct reads of the base table)
- Test: `app/tests/testthat/test-ft-credit-db.R`

**Interfaces:**
- Consumes: `credit_*` columns.
- Produces: `player_four_factors_by_game` on/off rows whose points,
  possessions and trips follow credit attribution; unchanged columns and
  keys.

- [ ] **Step 1: Write the failing test.** Append:

```r
test_that("a player's ON and OFF Four Factors rows add up to the team's", {
  bad <- DBI::dbGetQuery(con, "
    WITH split AS (
      SELECT game_id, team_id, player_id, type_lineup,
             SUM(ts_poss_count) AS ts_poss
      FROM basketball_test.player_four_factors_by_game
      GROUP BY game_id, team_id, player_id, type_lineup
    ),
    team AS (
      SELECT game_id, team_id, type_lineup, SUM(ts_poss_count) AS ts_poss
      FROM basketball_test.lineup_four_factors_by_game
      GROUP BY game_id, team_id, type_lineup
    )
    SELECT COUNT(*) AS n FROM split s JOIN team t USING (game_id, team_id, type_lineup)
    WHERE s.ts_poss <> t.ts_poss")
  expect_equal(as.numeric(bad$n), 0)
})
```

  Read `player_four_factors_by_game`'s columns first; if its trip column
  has a different name, use that name here.

- [ ] **Step 2: Run it to see it fail** (after Task 3 the team side is
  corrected and the player side is not).

- [ ] **Step 3: Switch the three definition files and their refresh
  functions** with the reader switch procedure. Specifics:
  - Player ON/OFF is decided by joining a row's lineup to
    `lineups_lookup`. That join is an event reference: it uses
    `credit_lineup_hash`.
  - `onoff_minutes` and any `segment_seconds` aggregate stay physical,
    including their own join to `lineups_lookup`.
  - `own_starters`/`opp_starters` predicates and keys on event rows use
    the credit columns.

- [ ] **Step 4: Apply in order** (`player_four_factors_by_game`,
  `onoff_default_mv`, `player_advanced_stats_mv`), dry run then `--apply`.

- [ ] **Step 5: Gates.** `--check` each: G2 0, G4 0, G3 0, G5 passes.

- [ ] **Step 6: Switch `onoff_compute`** if it reads the base table
  directly (it is listed among the base table's readers; read it to find
  where). Deploy with `scripts/deploy_sql_functions.R`, then run G6 for
  `onoff_compute`, `four_factors_compute` and
  `four_factors_dashboard_compute` (the latter two read
  `player_four_factors_by_game` and need no edit).

- [ ] **Step 7: Step 1 test passes. Advance the cache marker (Task 3
  Step 9). Security, fast suite, commit.**

```bash
git commit -m "Player on/off objects read credit attribution"
```

### Task 5: Lineup totals and lineup readers

**Files:**
- Modify: `sql/materialized_views/sub_lineups_by_day.sql` (`mv_lineup_totals_by_day`)
- Modify: `sql/functions/refresh_sub_lineups.sql`, `sql/functions/refresh_sub_lineups_incremental.sql` (`sub_lineups_stats`)
- Modify: `sql/functions/fetch_lineups_all.sql`, `sql/functions/fetch_lineups_four_factors.sql` (raw-action paths only)

**Interfaces:**
- Consumes: `credit_*` columns; `lineup_four_factors_by_game` from Task 3.
- Produces: unchanged signatures and columns.

- [ ] **Step 1: Write the failing test.** Append:

```r
test_that("lineup day totals add up to the team's points and possessions", {
  bad <- DBI::dbGetQuery(con, "
    WITH lineups AS (
      SELECT game_id, team_id, type_lineup,
             SUM(total_pts) AS pts, SUM(total_poss) AS poss
      FROM basketball_test.mv_lineup_totals_by_day
      GROUP BY game_id, team_id, type_lineup
    ),
    base AS (
      SELECT game_id, team_id, type_lineup,
             SUM(COALESCE(team_score, 0)) AS pts,
             COUNT(*) FILTER (WHERE final_end_poss) AS poss
      FROM basketball_test.df_pts_poss_lineups_longer_mv
      GROUP BY game_id, team_id, type_lineup
    )
    SELECT COUNT(*) AS n FROM lineups l JOIN base b USING (game_id, team_id, type_lineup)
    WHERE l.pts <> b.pts OR l.poss <> b.poss")
  expect_equal(as.numeric(bad$n), 0)
})
```

  Read `mv_lineup_totals_by_day`'s columns first and correct the column
  names if they differ. This test should already pass before the switch
  (totals are conserved); it is here to keep passing after it.

- [ ] **Step 2: Run it.** Expected: PASS before and after. If it fails
  before, stop: the object is not what this plan assumes.

- [ ] **Step 3: Switch the definition and the two refresh functions**
  with the reader switch procedure.

- [ ] **Step 4: Apply** `mv_lineup_totals_by_day`, then refresh
  `sub_lineups_stats` with `SELECT basketball_test.refresh_sub_lineups_stats();`.

- [ ] **Step 5: Gates** for both: G2 0, G4 0, G5 passes.

- [ ] **Step 6: Switch the raw-action branches** of `fetch_lineups_all`
  and `fetch_lineups_four_factors`. CLAUDE.md requires these two files'
  clutch structures to stay in sync: make the same edits in the same CTEs
  of both and diff the two clutch sections against each other afterwards.
  Deploy both in one `scripts/deploy_sql_functions.R` call.

- [ ] **Step 7: G6** for `fetch_lineups_csv_v2` and
  `fetch_lineups_four_factors_csv` (the wrappers the app calls), including
  the clutch case and the `p_num_starters_off := 5` case.

- [ ] **Step 8: Advance the cache marker, security, fast suite, commit.**

```bash
git commit -m "Lineup totals and lineup readers use credit attribution"
```

### Task 6: Team reader functions

**Files:**
- Modify: `sql/functions/get_team_ratings_dynamic.sql`
- Modify: `sql/functions/get_team_four_factors_dynamic.sql`
- Modify: `sql/functions/get_team_four_factors_dashboard_dynamic.sql`
- Modify: `sql/functions/get_team_traditional_dynamic.sql`
- Modify: `sql/materialized_views/team_ppp_ratings_mv.sql` (only if it keys on starters)
- Refresh: `team_ratings_preset_cache` via `refresh_team_ratings_preset_cache_for_games`

**Interfaces:**
- Consumes: `credit_*` columns.
- Produces: unchanged signatures. Team totals are unchanged by design;
  what changes is trips (counted once) and results under a starters
  filter.

- [ ] **Step 1: Write the failing test.** Append:

```r
test_that("clutch team Four Factors counts each trip once", {
  ff <- DBI::dbGetQuery(con, "
    SELECT * FROM basketball_test.get_team_four_factors_dynamic(
      2026, p_max_margin := 5, p_max_time_remaining := 300)")
  expected <- DBI::dbGetQuery(con, "
    SELECT a.team_id,
           100.0 * SUM(COALESCE(a.team_score, 0))
           / NULLIF(2.0 * (COUNT(*) FILTER (WHERE a.type = 'shot')
               + COUNT(DISTINCT (a.game_id, a.parent_action_id)) FILTER (
                   WHERE a.type = 'freeThrow' AND p.id IS NOT NULL)), 0) AS ts
    FROM basketball_test.df_pts_poss_lineups_longer_mv a
    JOIN basketball_test.final_schedule_mv fs
      ON fs.game_id = a.game_id AND fs.team_id = a.team_id AND fs.game_year = 2026
    LEFT JOIN basketball_test.df_pts_poss_lineups_longer_mv p
      ON p.game_id = a.game_id AND p.id = a.parent_action_id
     AND p.type = 'foul' AND p.parameters_type = 'personal'
     AND p.team_id = a.team_id AND p.type_lineup = a.type_lineup
    WHERE a.type_lineup = 'offense'
      AND (ABS((a.own_team_score - COALESCE(a.team_score, 0)) - a.opp_team_score) <= 5 OR a.quarter > 4)
      AND (a.end_game_seconds_remaining <= 300 OR a.quarter > 4)
    GROUP BY a.team_id")
  both <- merge(ff[, c("team_id", "off_ts")], expected, by = "team_id")
  expect_equal(nrow(both), 14)
  expect_true(all(abs(as.numeric(both$off_ts) - as.numeric(both$ts)) < 0.06))
})
```

  Read the function's return columns first; if the offensive TS% column
  is not `off_ts`, use its actual name.

- [ ] **Step 2: Run it to see it fail.**

- [ ] **Step 3: Switch each function** with the reader switch procedure.
  Specifics:
  - Starters predicates (`own_starters`, `opp_starters` against
    `p_num_starters_*`) on event rows use the credit columns.
  - Wherever one of these functions counts trips as
    `COUNT(DISTINCT parent_action_id)` inside a group that includes
    `segment_id` or `lineup_hash`, the group key becomes the credit
    column, which makes the count once-per-trip.
  - `get_team_traditional_dynamic`'s `seg_times` CTE is time: unchanged.

- [ ] **Step 4: Deploy all four** in one `scripts/deploy_sql_functions.R`
  call, then re-apply security (each file drops its function first).

- [ ] **Step 5: G6** for the four functions: team rows identical to the
  pre-change call except TS% and trip columns in every case; the
  `p_num_starters_off := 5` case may differ in all columns and must
  satisfy "sum of the 0..5 starters results equals the unfiltered result"
  for points and possessions.

- [ ] **Step 6: Refresh the preset cache** for all games in batches
  through `refresh_team_ratings_preset_cache_for_games`, then `--check
  team_ratings_preset_cache`: G2 0.

- [ ] **Step 7: Step 1 test passes. Advance the cache marker, fast suite,
  commit.**

```bash
git commit -m "Team reader functions use credit attribution"
```

### Task 7: Player traditional objects, and TS% from trips

**Files:**
- Modify: `sql/functions/compute_player_traditional_by_game.sql`
- Modify: `sql/functions/compute_player_stats_actions_by_game.sql`
- Modify: `sql/functions/get_player_traditional_from_games.sql` (TS% expression, lines 201-202)
- Modify: `sql/functions/get_player_traditional_custom_clutch.sql` (TS% expression, lines 240-241)
- Modify: `sql/functions/get_player_traditional_dynamic.sql` (TS% expression, line 452; and credit keys)
- Modify: `sql/materialized_views/player_traditional_stats_mv.sql` (TS% expression, line 240; and credit keys)
- Modify: `sql/functions/get_team_traditional_dynamic.sql` (TS% expression)
- Modify: `sql/materialized_views/team_metrics_by_game_mv.sql` line 244 and `sql/functions/refresh_team_metrics_by_game_for_games.sql` line 264 (`ts` column)

**Interfaces:**
- Consumes: `credit_*` columns; per-player trip counts that already exist
  in every one of these sources (`player_ts_poss_count` /
  `ts_poss_count`, today used only for usage%).
- Produces: `ts` in every Israeli traditional reader =
  `100 * pts / (2 * trips-based ts_poss)`, replacing the 0.44 estimate.
  Signatures and column lists unchanged.

A player's own box score does not depend on which lineup is credited.
What does depend on it here is `poss_on_floor` and the team-while-on-floor
denominators used for usage%.

- [ ] **Step 1: Write the failing test.** Append:

```r
test_that("no Israeli traditional reader still uses the 0.44 TS% estimate", {
  files <- c(
    "sql/functions/get_player_traditional_from_games.sql",
    "sql/functions/get_player_traditional_custom_clutch.sql",
    "sql/functions/get_player_traditional_dynamic.sql",
    "sql/functions/get_team_traditional_dynamic.sql",
    "sql/functions/refresh_team_metrics_by_game_for_games.sql",
    "sql/materialized_views/player_traditional_stats_mv.sql",
    "sql/materialized_views/team_metrics_by_game_mv.sql")
  for (f in files) {
    expect_false(grepl("0.44", read_repo_txt("..", f), fixed = TRUE), info = f)
  }
})

test_that("team Traditional TS% equals team Four Factors TS%", {
  trad <- DBI::dbGetQuery(con, "
    SELECT team_id, ts FROM basketball_test.get_team_traditional_dynamic(2026)")
  ff <- DBI::dbGetQuery(con, "
    SELECT team_id, 100.0 * SUM(off_points_raw) / NULLIF(2.0 * SUM(off_ts_poss_raw), 0) AS ff_ts
    FROM basketball_test.team_metrics_by_game_mv WHERE game_year = 2026 GROUP BY team_id")
  both <- merge(trad, ff, by = "team_id")
  expect_equal(nrow(both), 14)
  expect_true(all(abs(as.numeric(both$ts) - as.numeric(both$ff_ts)) < 0.06))
})
```

  `read_repo_txt()` is the helper the existing contract tests use; check
  its base directory in `app/tests/testthat/` and adjust the relative
  path so it resolves to the repository's `sql/` directory.

- [ ] **Step 2: Run them to see them fail.**

- [ ] **Step 3: Replace each TS% expression.** In each file, the
  expression of the form
  `pts / (2 * (fga + 0.44 * fta))` becomes
  `pts / (2 * <that source's trips-based ts_poss>)`, keeping the file's
  own rounding, casts and NULL guard. The trips column per file:
  `p.player_ts_poss_count` (from_games), `s.ts_poss_count` (custom_clutch
  and dynamic), `bt.ts_poss_count` (the MV). Guard on the trips count
  being greater than 0.
  For `get_team_traditional_dynamic`, add a trips count to `team_stats`:
  FGA plus `COUNT(DISTINCT (game_id, parent_action_id))` over free-throw
  rows whose parent is a personal foul, joining the parent from the base
  table (not from the clutch-filtered `acts`, since the foul can fall
  outside the window), for the side selected by `p_side`.
  For `team_metrics_by_game_mv.ts`, use the row's own `off_ts_poss_raw`.

- [ ] **Step 4: Switch credit keys** in `compute_player_traditional_by_game`,
  `compute_player_stats_actions_by_game`, `get_player_traditional_dynamic`
  and `player_traditional_stats_mv` with the reader switch procedure. The
  `segment_times`/`seg_times` CTEs are time and stay physical.

- [ ] **Step 5: Apply.** Deploy the functions; refresh
  `player_traditional_by_game`, `default_clutch_player_totals_by_game`,
  `player_stats_actions_by_game` and `team_metrics_by_game_mv` in game
  batches; stage and swap `player_traditional_stats_mv`. Re-apply
  security.

- [ ] **Step 6: Gates.** `--check` the four tables and the MV: G2 0, G4 0.
  G6 for the three player reader functions: every column identical to the
  pre-change call except `ts`, `usg_pct` and `poss_on_floor`.

- [ ] **Step 7: Both Step 1 tests pass. Advance the cache marker, fast
  suite, commit.**

```bash
git commit -m "Israeli traditional TS% uses real free-throw trips"
```

### Task 8: The app

**Files:**
- Modify: `app/R/server_tab3.R` (the rank-arrow query near line 1085 and its `q$ts` line near 1179)
- Modify: `app/R/server_tab5_traditional.R:262-276` (`build_ts_total_row`)
- Modify: `app/R/helpers.R:2040-2060`, `app/R/server_tab7_compare.R:932-948` (duplicate usage helper)
- Modify: `app/R/ui_tab8_euro.R:86`, `app/R/ui_tab9_euro_team.R:77`
- Modify: `app/R/global.R` (`COLUMN_TOOLTIPS`, the TS% entry)
- Test: `app/tests/testthat/test-ts-trips.R`

**Interfaces:**
- Consumes: readers from Tasks 3-7.
- Produces: `ts_pct_from_trips(pts, ts_poss)` in `app/R/helpers.R`,
  returning `round(100 * pts / (2 * ts_poss), 1)` and `NA` where
  `ts_poss` is not positive.

Use branch `shiny/ft-credit-ts`. `helpers.R`, `global.R` and
`server_tab3.R` have mixed line endings: after each edit compare
`tr -cd '\r' < file | wc -c` with the committed blob and restore per-line
endings if the count moved by more than the lines you changed.

- [ ] **Step 1: Write the failing tests** in
  `app/tests/testthat/test-ts-trips.R`:

```r
test_that("ts_pct_from_trips divides points by twice the true-shooting attempts", {
  expect_equal(ts_pct_from_trips(20, 10), 100)
  expect_equal(ts_pct_from_trips(c(11, 0), c(10, 5)), c(55, 0))
  expect_true(is.na(ts_pct_from_trips(4, 0)))
  expect_true(is.na(ts_pct_from_trips(4, NA)))
})

test_that("the app's R code no longer estimates trips as 0.44 x FTA", {
  for (f in c("helpers.R", "server_tab3.R", "server_tab5_traditional.R", "server_tab7_compare.R")) {
    expect_false(grepl("0.44", read_repo_txt("R", f), fixed = TRUE), info = f)
  }
})
```

- [ ] **Step 2: Run them to see them fail.**

Run (from `app/`): `Rscript -e "testthat::test_file('tests/testthat/test-ts-trips.R')"`
Expected: first test errors (`could not find function`), second fails on
four files.

- [ ] **Step 3: Add the helper** to `app/R/helpers.R`, next to the other
  shooting helpers:

```r
# True shooting from real attempts: FGA plus one per free-throw trip.
ts_pct_from_trips <- function(pts, ts_poss) {
  pts <- suppressWarnings(as.numeric(pts))
  ts_poss <- suppressWarnings(as.numeric(ts_poss))
  out <- rep(NA_real_, length(ts_poss))
  ok <- is.finite(ts_poss) & ts_poss > 0 & is.finite(pts)
  out[ok] <- round(100 * pts[ok] / (2 * ts_poss[ok]), 1)
  out
}
```

- [ ] **Step 4: Tab 3 rank arrows.** In the previous-ranks query, add
  `COALESCE(SUM(s.off_ts_poss_raw), 0)::numeric AS ts_poss` to the select
  list beside the other sums, and replace the `q$ts <- ifelse(...)` line
  with `q$ts <- ts_pct_from_trips(q$pts, q$ts_poss)`.

- [ ] **Step 5: One usage helper.** `helpers.R:2040-2060` and
  `server_tab7_compare.R:932-948` hold the same body. Keep the one in
  `helpers.R`, delete the copy in `server_tab7_compare.R` and call the
  helper from there. In the kept body, the true-shooting attempts for a
  row are implied from its own TS% (`pts / (2 * ts / 100)`), which is now
  exact for both leagues; where `pts` is 0 the attempts cannot be implied,
  so use `fga` alone and delete the `0.44 * fta` term. Read the callers to
  confirm the function's name before editing.

- [ ] **Step 6: The multi-team TOTAL row.** In `build_ts_total_row`, the
  TOTAL row's TS% becomes `ts_pct_from_trips(sum(pts), sum(attempts))`,
  where each team row's attempts are implied as in Step 5. Add this case
  to `test-ts-trips.R`:

```r
test_that("a TOTAL row's TS% is the attempts-weighted combination of its rows", {
  rows <- data.frame(pts = c(100, 50), ts = c(50, 62.5))  # 100 and 40 attempts
  attempts <- rows$pts / (2 * rows$ts / 100)
  expect_equal(ts_pct_from_trips(sum(rows$pts), sum(attempts)), round(100 * 150 / 280, 1))
})
```

- [ ] **Step 7: Wording.**
  - `ui_tab8_euro.R:86` and `ui_tab9_euro_team.R:77` say TS% uses "the
    same denominator as the Israeli tabs (FGA plus the last free throw of
    a shooting-foul trip)". Replace the parenthesis in both with
    "(FGA plus one per shooting-foul trip)", which is true of both
    leagues.
  - In `COLUMN_TOOLTIPS` (`global.R`), set the TS% entry to:
    "True shooting: points per two true-shooting attempts, where an
    attempt is a field-goal attempt or a trip to the line for a personal
    foul. A trip is credited to the lineup that drew the foul."
  - In `ui_tab1_onoff.R`, where the Four Factors help text lists the
    metrics, add one sentence: "Free throws, their points and their
    possession are credited to the lineup on the floor when the foul was
    drawn, including free throws shot after a substitution."

- [ ] **Step 8: Run the tests and the full fast suite.**

Run (from `app/`): `Rscript -e "testthat::test_dir('tests/testthat')"`
Expected: 0 failures, 0 errors. The mock in
`app/tests/testthat/helper-server-mocks.R` returns a `ts` column for the
traditional readers; no mock change is expected.

- [ ] **Step 9: Drive it locally.** Launch with the `run-shiny-local`
  skill (`IBPL_CACHE_UI=false`). In Team Ratings, compare TS% for three
  teams between the Four Factors view and the Traditional view: they must
  match to one decimal. In Player Stats, open a player with a multi-team
  TOTAL row in each league and check the TOTAL's TS% lies between the
  team rows' values.

- [ ] **Step 10: Commit.**

```bash
git add app/R app/tests/testthat/test-ts-trips.R
git commit -m "App reads TS% from real trips; one usage helper"
```

### Task 9: Close out

**Files:**
- Modify: `CLAUDE.md` (Four Factors table; Pitfalls; Key MV designs)
- Modify: `etl/run_data_quality_report.R` (one new check)
- Modify: `scripts/apply_ft_credit_migration.R` (`--cleanup` stage)

- [ ] **Step 1: Run every gate once more**, all objects, plus the whole
  suite with `RUN_DB_TESTS=1`, with nothing else querying.
  Expected: every G2 and G4 0; every G3 0; 0 test failures.

- [ ] **Step 2: Confirm the nightly path.** Pick the most recent game id,
  run `SELECT basketball_test.refresh_ft_credit_fields_for_games(ARRAY[<id>])`
  and expect 0 rows updated (already populated). After the next nightly
  ETL, run `scripts/verify_ft_credit.R --check-base` again: G1 all 0.

- [ ] **Step 3: Add the data-quality check.** In
  `etl/run_data_quality_report.R`, add a finding that fails when the base
  table has any row with `credit_lineup_hash IS NULL`, following the shape
  of the existing checks in that file. This file is modified on
  `origin/main` relative to the local checkout: make the edit on a fresh
  branch from `origin/main`.

- [ ] **Step 4: Document.** In `CLAUDE.md`:
  - Four Factors table, TS% row: "pts / (2 x (FGA + personal-foul FT
    trips)), each trip counted once and credited to the lineup that drew
    the foul; and-one free throws hang off the shot and are not a trip".
  - Key MV designs: add the five `credit_*` columns and the reader rule
    (events on credit, time on physical).
  - Pitfalls: "A new aggregation over the base table must group events on
    `credit_lineup_hash`/`credit_segment_id`, not the physical columns, or
    it double counts free-throw trips split by a substitution (about a
    third of them)."
  - Gameflow section: the ribbon uses physical columns, so its stint
    plus-minus differs from on/off on free throws shot after a
    substitution (about 3.5% of points), by design.
  - Backlogs: EuroLeague still credits a trip with its last free throw;
    aligning it is an open decision.

- [ ] **Step 5: Clean up.** Add `--cleanup` to the apply script: drop
  every `__ftc_old` object and the `ft_credit_migration` schema. Run it
  only after Step 2's post-ETL check has passed.

- [ ] **Step 6: Verify in production.** After the push deploys, open the
  live site in a browser (not curl) and repeat Task 8 Step 9's two
  comparisons.

- [ ] **Step 7: Commit.**

```bash
git add CLAUDE.md etl/run_data_quality_report.R scripts/apply_ft_credit_migration.R
git commit -m "Docs and data-quality check for free-throw credit attribution"
```

---

## Rollback

- Tasks 1-2 add columns and a function and change no reader: they can stay
  in place indefinitely, or be removed with
  `ALTER TABLE ... DROP COLUMN` for the five columns.
- Tasks 3-7: for a materialized view, rename `<name>__ftc_old` back. For a
  table-type object, redeploy the previous refresh function from git and
  refresh all games in batches. Reader functions: redeploy the previous
  file from git and re-apply security. The baseline schema holds the
  pre-change contents of every object until Task 9 Step 5.
- Task 8 is an app commit and reverts with git.

## Order and stopping points

Tasks must run in order. The work can stop cleanly after any task: after
Task 2 nothing visible has changed; after Task 3 team and lineup Four
Factors TS% is corrected while player on/off still uses the old
attribution; after Task 7 the database is complete and only the app's own
arithmetic and wording remain. Between Task 3 and Task 7 different tabs
are on different attributions, so plan to run Tasks 3-7 in one sitting.
