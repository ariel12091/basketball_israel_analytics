# Stint Ribbon Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Per-game player lanes over the game clock with the score margin behind them, opened from a game-log row in either league, where hovering a lane re-clips the margin curve to that player's floor time.

**Architecture:** One SQL query per ribbon open returns a single row with `lanes` and `margin` as `jsonb`. Two thin league readers normalise into one canonical frame; pure transforms in `helpers.R` merge adjacent stints and compute geometry; a pure builder emits an inline SVG tag tree. Hover is one `clip-path` attribute swap in `app.js`. No plotting library, no new R package.

**Tech Stack:** R 4.4.2, Shiny/bslib, DBI/RPostgres against PostgreSQL (Supabase, schema `basketball_test` and `euroleague`), htmltools SVG, testthat, vanilla JS.

**Spec:** `docs/superpowers/specs/2026-09-05-stint-ribbon-design.md`

**Branch:** `shiny/stint-ribbon` (already created; the spec's three commits are on it)

## Global Constraints

- **One query per ribbon open.** The pooler round-trip floor is 238 ms and the budget is 500 ms; a second trip costs more than the entire rest of the feature. Never split lanes and margin into two queries.
- **Never put a `type_lineup` predicate in the Israeli query.** Filtering to `offense` loses 1.46% of floor time. No predicate is needed at all.
- **X-axis extent comes from nominal period structure** (600 s quarters, 300 s OT), never from `max(elapsed)`.
- **Drop zero-length segments** (`segment_seconds > 0`) before laying out lanes.
- **Merge adjacent runs per player, not per lineup hash.**
- **No new R package dependencies.** `app/R/global.R` loads shiny, DBI, dplyr, pool, RPostgres, DT, bslib, htmltools — that is the whole toolbox.
- `helpers.R` has **no side effects at source time**: no `library()`, no DB pool, no cache objects, no env reads. Impure infrastructure lives in `global.R`.
- **Never copy a helper implementation into `helper-server-mocks.R`** — the mocks `source()` the real `helpers.R` and stub only impure pieces.
- Parameterized SQL only (`$1`, `$2`); never `sprintf()`/`paste0()` for user values.
- 2-space indent, snake_case. Base apply (`lapply`/`vapply`/`Filter`), **not** purrr.
- **Set `IBPL_CACHE_UI=false`** in the environment while editing `app/www/app.css` or `app/www/app.js`, or an edit needs an app restart rather than a browser reload.
- **Launch with Run App / `runApp('app')`, never select-all + Ctrl+Enter** — the latter builds a broken BS3-style navbar and caches it for the life of the process. Health check: the served page carries 11 `class="nav-link"` occurrences.
- After any `DROP` touching the new views, **re-run `scripts/apply_db_security.R` with `CONFIRM_DB_SECURITY_APPLY=1`** — DROP wipes `app_readonly` grants.
- The EuroLeague read layer is enumerated in **two files that must stay in sync**: `sql/security/enable_readonly_rls.sql:91` and `sql/security/audit_app_access.sql:101`.

## Review corrections — ALL APPLIED 2026-09-05

The eight corrections below were raised in review, verified against the code and
the database, and folded into the tasks. They are recorded here as acceptance
criteria: a reviewer should be able to check each one against the task text.

Verification notes on the three that were checkable claims: `disp` really does
drop the identifiers (`server_tab4.R:554` Summary, `:699` Four Factors), so the
old guard was always false; `euroleague.schedule.home_team_id` exists and is
already granted to `app_readonly`; and `sendShinyEvent` really is private to its
IIFE (`app/www/app.js:270`), so the first draft dropped early clicks.

1. **Build ribbon links before dropping the row identifiers.** Both game-log
   renderers remove `game_id` and `team_id` when they construct `disp`, so a
   later `all(c("game_id", "team_id") %in% names(disp))` guard is always false.
   Build `disp$game_date` from the row-aligned `df$game_id` / `df$team_id`, or
   retain the identifiers until after the link is built. Apply this to both
   Summary and Four Factors in Tab 4, and to both modes in Tab 11. Tasks 7 and
   8 must test that real rendered cells contain the link, not merely that the
   helper exists.

2. **Make the stepped margin a complete, deterministic game-clock series.**
   Prepend `(elapsed = 0, margin = 0)` when it is absent and extend the final
   score horizontally to the nominal game end. Preserve an action ordering key
   in both readers/views, and define how multiple score states at the same
   elapsed second are ordered or collapsed; `DISTINCT` plus `ORDER BY elapsed`
   is not deterministic. Clamp invalid elapsed values to the nominal frame.
   Add tests for the opening interval, the final tail, and multiple scoring
   records at the same clock.

3. **Render visible identification and scale context.** A rectangle with a
   native `<title>` is not enough to read a 20-player rotation chart. Reserve a
   left gutter and render one visible player label per lane, identify the two
   teams, label period boundaries, and draw and label the zero-margin baseline.
   Give each focusable lane an explicit `aria-label`; do not rely on descendants
   of an SVG with `role="img"` being exposed consistently to assistive tools.
   Update the geometry (Task 2), builder (Task 4), CSS (Task 6) and their tests together.

4. **Use the existing queue-and-replay path for ribbon clicks.** The proposed
   `handleRibbonLinkClick()` returns while Shiny is disconnected, contradicting
   the design contract. Expose/reuse `sendShinyEvent()` (or route the delegated
   link through the same mechanism) so an early click is queued and replayed.
   Test the queued path as well as the connected path.

5. **Deploy the EuroLeague views, grants, and audit atomically.** The proposed
   `ribbon_views.sql` contains multiple SQL statements, so its RPostgres call
   must use `immediate = TRUE` (or execute parsed statements safely); otherwise
   it fails with `cannot insert multiple commands into a prepared statement`.
   Prefer one transaction that creates/replaces both views, restores grants,
   runs the access audit, and rolls back on any failure. The EuroLeague views task must also use
   `euroleague.schedule.home_team_id` as the authoritative home-team mapping
   instead of aggregating `actions.is_home_team`.

6. **Highlight every stint for the hovered player.** Multiple `<g>` elements
   can share one `data-clip`. `setFocus()` must add `is-active` to every element
   with that clip id, while keeping the single clipped margin path. Add a test
   or browser assertion using a player with two separated stints.

7. **Fix the reader-test regex literals before running them.** R strings such
   as `"unnest\s*\("` in the readers task contain invalid escapes. Use escaped patterns
   such as `"unnest\\s*\\("`, and parse the new test files as part of the
   focused test command.

8. **Do not use `git stash && ... && git stash pop` for the baseline.** This
   repository commonly has unrelated user work in progress. Record the test
   baseline before edits or run it from an isolated worktree, preserving the
   existing working tree.

**Test command** (run from the repo root):

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

---

## File Structure

| Task | File | Responsibility |
|---|---|---|
| 1 | `sql/euroleague/ribbon_views.sql` (create) | The two EuroLeague read-layer views. |
| 1 | `sql/security/enable_readonly_rls.sql` (modify) | Register the two views in the curated grant list. |
| 1 | `sql/security/audit_app_access.sql` (modify) | Same two names, kept in sync. |
| 2-4 | `app/R/helpers.R` (modify) | Pure transforms + the SVG builder. Everything testable lives here. |
| 2-4, 7-8 | `app/tests/testthat/test-stint-ribbon.R` (create) | Unit tests for the transforms, the builder, and the link cell. |
| 5 | `app/R/global.R` (modify) | `fetch_stint_ribbon()` — impure: DB + cache. |
| 5 | `app/tests/testthat/test-stint-ribbon-readers.R` (create) | Reader tests against a stubbed `db_get_query`. |
| 6 | `app/www/app.css` (modify) | Ribbon styling and theme tokens. |
| 6-7 | `app/www/app.js` (modify) | Exported queue helper, hover/tap handler, ribbon link handler. |
| 7 | `app/R/server_tab4.R` (modify) | Israeli link column (both view modes), click observer, modal. |
| 8 | `app/R/server_tab11_euro_gamelogs.R` (modify) | EuroLeague wiring, same helpers. |

**Execution order rationale.** Task 1 goes first because it is the only task
with external side effects — DDL, grants, and a security-surface run — and it
depends on nothing else; a blocker there should surface before three tasks of R
code exist. Tasks 2 and 3 are pure and independent of each other. Task 6 (CSS
and JS) cannot be *seen* until Task 7 wires a tab, so its own gate is a presence
assertion and the browser check lands in Task 7.

---

### Task 0: Record the test baseline

**Files:**
- Create: `.superpowers/sdd/2026-09-05-stint-ribbon/test-baseline.txt`

**Interfaces:**
- Consumes: nothing.
- Produces: the pre-change test result, which every later task compares against.

This repository routinely carries unrelated work in progress — 96 modified or
untracked entries when this plan was written — so the baseline must be recorded
**before** the first edit. Do not try to reconstruct it later with
`git stash` / `git stash pop`: that cycle puts unrelated user work at risk for
no benefit.

- [ ] **Step 1: Run the full suite and save the result**

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', reporter='summary')"   > ../.superpowers/sdd/2026-09-05-stint-ribbon/test-baseline.txt 2>&1
```

- [ ] **Step 2: Record the headline numbers**

Read the last lines of that file and note the FAIL/WARN/SKIP/PASS counts. Some
tests in this suite require a database or a deployed app and may already fail or
skip; that is the point of a baseline. "No new failures" is the bar for every
later task, not "zero failures".

Nothing is committed by this task — the baseline lives in the git-ignored SDD
workspace.

---


### Task 1: EuroLeague read-layer views and grants

**Files:**
- Create: `sql/euroleague/ribbon_views.sql`
- Modify: `sql/security/enable_readonly_rls.sql:91` (the `euro_app_relations` array)
- Modify: `sql/security/audit_app_access.sql:101` (the `euro_app_relations` VALUES list)
- Test: `app/tests/testthat/test-stint-ribbon.R` (append)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `euroleague.ribbon_segments_v` with columns `game_id, team_id, segment_id, start_elapsed_seconds, end_elapsed_seconds, player_ids` (ids, resolved via `lineup_totals_by_game` — the view exposes no player names and no `opp_lineup`); and `euroleague.ribbon_margin_v` with `game_id, period, source_event_order, elapsed_seconds, points_a, points_b, home_team_id`. Task 5's EuroLeague reader depends on exactly these names — `source_event_order` is the `order_key` that makes the margin series deterministic when several scoring records share one elapsed second.

**Why views rather than table grants:** `app_readonly` is denied on `euroleague.actions` (211 MB of raw play-by-play, 40 columns including provider ids and parser traces) and on `matchup_segments_actions`, by design — the euro schema uses a curated read layer, unlike the Israeli blanket grant. Exposing two narrow views keeps that boundary and puts the clock derivation next to its data.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
test_that("both euro read-layer enumerations list the ribbon views", {
  # The curated grant list is duplicated in two files. They must agree, or
  # apply_db_security.R grants a relation the audit then reports as unexpected.
  grant_sql <- paste(readLines(
    testthat::test_path("..", "..", "..", "sql", "security", "enable_readonly_rls.sql"),
    warn = FALSE), collapse = "\n")
  audit_sql <- paste(readLines(
    testthat::test_path("..", "..", "..", "sql", "security", "audit_app_access.sql"),
    warn = FALSE), collapse = "\n")

  for (view in c("ribbon_segments_v", "ribbon_margin_v")) {
    expect_match(grant_sql, view, fixed = TRUE)
    expect_match(audit_sql, view, fixed = TRUE)
  }
})

test_that("the two euro read-layer lists contain exactly the same relations", {
  extract <- function(path, pattern) {
    txt <- paste(readLines(testthat::test_path("..", "..", "..", "sql", "security", path),
                           warn = FALSE), collapse = "\n")
    block <- regmatches(txt, regexpr(pattern, txt, perl = TRUE))
    sort(unique(gsub("'", "", regmatches(block, gregexpr("'[a-z_]+'", block))[[1]])))
  }
  grants <- extract("enable_readonly_rls.sql",
                    "euro_app_relations constant text\\[\\] := ARRAY\\[.*?\\];")
  audit <- extract("audit_app_access.sql",
                   "euro_app_relations\\(relation_name\\) AS \\(.*?\\),")
  expect_identical(grants, audit)
})
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: FAIL, no match for "ribbon_segments_v".

- [ ] **Step 3: Create the views**

Create `sql/euroleague/ribbon_views.sql`:

```sql
-- Narrow read layer for the stint ribbon.
--
-- app_readonly is denied on euroleague.actions and
-- euroleague.matchup_segments_actions on purpose: the euro schema keeps raw
-- provider evidence closed and grants only an enumerated read layer. These two
-- views expose exactly what the ribbon needs and nothing else.
--
-- Editing either view means DROP + CREATE, which wipes the app_readonly grant.
-- Re-run scripts/apply_db_security.R with CONFIRM_DB_SECURITY_APPLY=1 after.

-- Lanes are keyed on player_id, never on player names.
--
-- matchup_segments_actions stores lineups as text[] of NAMES and carries no
-- ids. Resolving those names one by one against full_rosters would be a
-- name-keyed join, the exact shape that manufactured 258 false findings in the
-- 2026-08-19 data-quality report. Instead this reuses the lineup -> player_id
-- link the on/off system is already built on: lineup_totals_by_game holds
-- own_lineup and player_ids for the same lineup. Measured 2026-09-05: all
-- 22,597 distinct segment lineups match on (game_id, team_id, own_lineup),
-- with zero fan-out.
--
-- Do NOT pair own_lineup and player_ids positionally. The two arrays are
-- sorted independently (names alphabetically, ids ascending), so names[i] is
-- unrelated to ids[i] -- measured 31,907 mismatches in 40,000 pairs. Only the
-- id SET is trustworthy; labels come from full_rosters by id.
--
-- opp_lineup is deliberately not exposed: both teams have a row for every
-- segment, so the opponent's lanes are the other team's own rows.
CREATE OR REPLACE VIEW euroleague.ribbon_segments_v AS
SELECT
  m.game_id,
  m.team_id,
  m.segment_id,
  m.start_elapsed_seconds,
  m.end_elapsed_seconds,
  l.player_ids
FROM euroleague.matchup_segments_actions m
JOIN (
  SELECT DISTINCT game_id, team_id, own_lineup, player_ids
  FROM euroleague.lineup_totals_by_game
) l
  ON l.game_id    = m.game_id
 AND l.team_id    = m.team_id
 AND l.own_lineup = m.own_lineup
-- 45% of rows are zero-length (two substitutions at the same clock). Dropping
-- them here is load-bearing: they would occupy lane slots while rendering as
-- invisible slivers.
WHERE m.segment_seconds > 0;

-- The provider records a per-period countdown (marker_time) plus a period
-- number; the ribbon needs elapsed seconds. Periods 1-4 run 10:00 and period
-- 5+ runs 05:00, verified against observed game lengths of exactly
-- 2400 / 2700 / 3000 / 3300 seconds.
-- home_team_id comes from euroleague.schedule, the authoritative mapping, not
-- from aggregating actions.is_home_team over a 211 MB table.
CREATE OR REPLACE VIEW euroleague.ribbon_margin_v AS
SELECT
  a.game_id,
  a.period,
  (
    (CASE WHEN a.period <= 4 THEN (a.period - 1) * 600
          ELSE 2400 + (a.period - 5) * 300 END)
    + ((CASE WHEN a.period <= 4 THEN 600 ELSE 300 END)
       - (split_part(a.marker_time, ':', 1)::int * 60
          + split_part(a.marker_time, ':', 2)::int))
  )::numeric AS elapsed_seconds,
  a.source_event_order,
  a.points_a,
  a.points_b,
  sc.home_team_id
FROM euroleague.actions a
JOIN euroleague.schedule sc ON sc.game_id = a.game_id
WHERE a.marker_time IS NOT NULL
  AND (a.points_a IS NOT NULL OR a.points_b IS NOT NULL);
```

- [ ] **Step 4: Apply the views, grants and audit atomically**

`ribbon_views.sql` holds multiple statements, so it cannot go through a prepared
statement — `dbExecute()` would fail with *"cannot insert multiple commands into
a prepared statement"*. Use `immediate = TRUE`, and wrap the whole deploy in one
transaction so a failure anywhere leaves no half-applied state.

Write it to a file (long `Rscript -e` segfaults on this box):

```bash
cat > /tmp/ribbon_deploy.R <<'EOF'
readRenviron("etl/.Renviron")
library(DBI); library(RPostgres)
con <- dbConnect(RPostgres::Postgres(), host = Sys.getenv("PG_HOST"),
  port = 5432L, dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
  connect_timeout = 15L)
on.exit(dbDisconnect(con), add = TRUE)

sql <- paste(readLines("sql/euroleague/ribbon_views.sql", warn = FALSE), collapse = "
")

dbBegin(con)
ok <- tryCatch({
  # immediate = TRUE: multi-statement DDL cannot be a prepared statement.
  dbExecute(con, sql, immediate = TRUE)
  dbExecute(con, "GRANT SELECT ON euroleague.ribbon_segments_v TO app_readonly", immediate = TRUE)
  dbExecute(con, "GRANT SELECT ON euroleague.ribbon_margin_v  TO app_readonly", immediate = TRUE)
  TRUE
}, error = function(e) { message("deploy failed: ", conditionMessage(e)); FALSE })

if (!ok) { dbRollback(con); quit(status = 1) }
dbCommit(con)

# Verify from the app's own role before declaring success.
ro <- dbConnect(RPostgres::Postgres(), host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT")), dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE"), connect_timeout = 15L)
on.exit(dbDisconnect(ro), add = TRUE)
for (v in c("ribbon_segments_v", "ribbon_margin_v")) {
  stopifnot(dbGetQuery(ro, sprintf(
    "select has_table_privilege('app_readonly','euroleague.%s','SELECT') p", v))$p[1])
}
cat("views deployed and readable by app_readonly
")
EOF
"$RSCRIPT" /tmp/ribbon_deploy.R
```

Note port **5432** (direct) for the DDL connection, not 6543 — per `CLAUDE.md`.
The grants here are the immediate fix; Step 5 registers the views in the
permanent enumerations so `apply_db_security.R` keeps them.

- [ ] **Step 5: Register both views in the two enumerations**

In `sql/security/enable_readonly_rls.sql`, add to the `euro_app_relations` array after `'sub_lineups_stats_mv'`:

```sql
    'sub_lineups_stats_mv',
    'ribbon_segments_v',
    'ribbon_margin_v'
  ];
```

In `sql/security/audit_app_access.sql`, add to the `euro_app_relations` VALUES list after `('sub_lineups_stats_mv')`:

```sql
    ('sub_lineups_stats_mv'),
    ('ribbon_segments_v'),
    ('ribbon_margin_v')
),
```

- [ ] **Step 6: Apply the grants**

```bash
CONFIRM_DB_SECURITY_APPLY=1 "$RSCRIPT" scripts/apply_db_security.R
```

- [ ] **Step 7: Verify the grant landed and the query stays in budget**

The `home` CTE aggregates over `euroleague.actions`; confirm the `game_id`
predicate still pushes down rather than scanning the whole table. Write to a
temp file (long `Rscript -e` segfaults on this box):

```bash
cat > /tmp/ribbon_verify.R <<'EOF'
readRenviron("app/.Renviron")
library(DBI); library(RPostgres)
con <- dbConnect(RPostgres::Postgres(), host=Sys.getenv("PG_HOST"),
  port=as.integer(Sys.getenv("PG_PORT")), dbname=Sys.getenv("PG_DB"),
  user=Sys.getenv("PG_USER"), password=Sys.getenv("PG_PASS"),
  sslmode=Sys.getenv("PG_SSLMODE"), connect_timeout=15L, bigint="numeric")
g <- dbGetQuery(con, "select game_id from euroleague.ribbon_segments_v limit 1")$game_id[1]
ms <- replicate(7, {
  t0 <- Sys.time()
  dbGetQuery(con, "select * from euroleague.ribbon_margin_v where game_id = $1",
             params = list(g))
  as.numeric(difftime(Sys.time(), t0, units = "secs")) * 1000
})
cat("app_readonly can read both views; margin median ms:", median(ms), "\n")
stopifnot(median(ms) < 500)
dbDisconnect(con)
EOF
"$RSCRIPT" /tmp/ribbon_verify.R
```

Expected: prints a median well under 500 ms and exits 0. If it exceeds budget, the `home` CTE is not pushing the predicate down — replace it with a lateral subquery keyed on `a.game_id` and re-measure.

- [ ] **Step 8: Run the tests**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: PASS, no failures.

- [ ] **Step 9: Commit**

```bash
git add sql/euroleague/ribbon_views.sql sql/security/enable_readonly_rls.sql sql/security/audit_app_access.sql app/tests/testthat/test-stint-ribbon.R
git commit -m "feat: euroleague ribbon read-layer views and grants"
```

---

### Task 2: Lane transforms — merging, starters, ordering, geometry

**Files:**
- Modify: `app/R/helpers.R` (append at end)
- Test: `app/tests/testthat/test-stint-ribbon.R` (create)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `merge_adjacent_stints(lanes)` → data.frame with the same columns, contiguous runs collapsed. Input/output columns: `side` (chr, "own"/"opp"), `player_key` (chr), `player_label` (chr), `is_starter` (lgl), `start_elapsed` (num), `end_elapsed` (num).
  - `ribbon_mark_starters(lanes)` → input plus logical `is_starter`, TRUE for every player on the floor in that side's earliest segment.
  - `ribbon_lane_index(lanes)` → input plus integer `lane_index`, restarting at 1 per `side`.
  - `ribbon_geometry(lanes, total_seconds, width = 1000, lane_height = 14, lane_gap = 3)` → input plus numeric `x`, `w`, `y`, `h`.

- [ ] **Step 1: Write the failing tests**

Create `app/tests/testthat/test-stint-ribbon.R`:

```r
# Pure transforms behind the stint ribbon. These run without a database:
# the readers hand them plain data frames, so every geometry rule and every
# merge rule is checked here rather than through a query.

lane_row <- function(side, player_key, start_elapsed, end_elapsed,
                     is_starter = FALSE, player_label = NULL) {
  data.frame(
    side = side,
    player_key = player_key,
    player_label = player_label %||% paste("Player", player_key),
    is_starter = is_starter,
    start_elapsed = start_elapsed,
    end_elapsed = end_elapsed,
    stringsAsFactors = FALSE
  )
}

test_that("merge_adjacent_stints collapses a contiguous run into one bar", {
  lanes <- rbind(
    lane_row("own", "7", 0, 242),
    lane_row("own", "7", 242, 337),
    lane_row("own", "7", 337, 410)
  )
  out <- merge_adjacent_stints(lanes)
  expect_identical(nrow(out), 1L)
  expect_identical(out$start_elapsed, 0)
  expect_identical(out$end_elapsed, 410)
})

test_that("merge_adjacent_stints splits on a real gap", {
  lanes <- rbind(
    lane_row("own", "7", 0, 242),
    lane_row("own", "7", 600, 700)
  )
  out <- merge_adjacent_stints(lanes)
  expect_identical(nrow(out), 2L)
  expect_identical(out$end_elapsed, c(242, 700))
})

test_that("merge_adjacent_stints never merges across players or sides", {
  lanes <- rbind(
    lane_row("own", "7", 0, 242),
    lane_row("own", "8", 242, 337),
    lane_row("opp", "7", 337, 410)
  )
  out <- merge_adjacent_stints(lanes)
  expect_identical(nrow(out), 3L)
})

test_that("merge_adjacent_stints tolerates unsorted input", {
  lanes <- rbind(
    lane_row("own", "7", 242, 337),
    lane_row("own", "7", 0, 242)
  )
  out <- merge_adjacent_stints(lanes)
  expect_identical(nrow(out), 1L)
  expect_identical(out$start_elapsed, 0)
  expect_identical(out$end_elapsed, 337)
})

test_that("merge_adjacent_stints returns empty input unchanged", {
  empty <- lane_row("own", "7", 0, 1)[0, , drop = FALSE]
  expect_identical(nrow(merge_adjacent_stints(empty)), 0L)
})



test_that("ribbon_mark_starters flags whoever is on the floor in the first segment", {
  lanes <- rbind(
    lane_row("own", "starter_a", 0, 300),
    lane_row("own", "starter_b", 0, 300),
    lane_row("own", "bench", 300, 600)
  )
  out <- ribbon_mark_starters(lanes)
  flag <- setNames(out$is_starter, out$player_key)
  expect_true(flag[["starter_a"]])
  expect_true(flag[["starter_b"]])
  expect_false(flag[["bench"]])
})

test_that("ribbon_mark_starters resolves each side independently", {
  # The two sides always share a segment timeline, but a side whose first
  # segment starts later must still get its own starters.
  lanes <- rbind(
    lane_row("own", "a", 0, 300),
    lane_row("opp", "b", 0, 300),
    lane_row("opp", "c", 300, 600)
  )
  out <- ribbon_mark_starters(lanes)
  flag <- setNames(out$is_starter, paste(out$side, out$player_key))
  expect_true(flag[["own a"]])
  expect_true(flag[["opp b"]])
  expect_false(flag[["opp c"]])
})

test_that("ribbon_mark_starters keeps a player who returns later flagged once", {
  lanes <- rbind(
    lane_row("own", "a", 0, 300),
    lane_row("own", "a", 900, 1200)
  )
  out <- ribbon_mark_starters(lanes)
  expect_true(all(out$is_starter))
})






test_that("ribbon_lane_index orders starters first, then by floor time", {
  lanes <- rbind(
    lane_row("own", "sub", 0, 100, is_starter = FALSE),
    lane_row("own", "big_sub", 0, 900, is_starter = FALSE),
    lane_row("own", "starter", 0, 200, is_starter = TRUE)
  )
  out <- ribbon_lane_index(lanes)
  idx <- setNames(out$lane_index, out$player_key)
  expect_identical(unname(idx[["starter"]]), 1L)
  expect_identical(unname(idx[["big_sub"]]), 2L)
  expect_identical(unname(idx[["sub"]]), 3L)
})

test_that("ribbon_lane_index restarts numbering per side", {
  lanes <- rbind(
    lane_row("own", "a", 0, 100),
    lane_row("opp", "b", 0, 100)
  )
  out <- ribbon_lane_index(lanes)
  expect_identical(sort(out$lane_index), c(1L, 1L))
})

test_that("ribbon_lane_index sums floor time across a player's stints", {
  lanes <- rbind(
    lane_row("own", "split", 0, 100),
    lane_row("own", "split", 500, 700),   # 300s total
    lane_row("own", "solid", 0, 250)      # 250s total
  )
  out <- ribbon_lane_index(lanes)
  idx <- setNames(out$lane_index, out$player_key)
  expect_identical(unname(idx[["split"]][1]), 1L)
})

test_that("ribbon_geometry scales into the plot area, after the label gutter", {
  lanes <- lane_row("own", "7", 0, 1200)
  lanes$lane_index <- 1L
  out <- ribbon_geometry(lanes, total_seconds = 2400, width = 1000, gutter = 150)
  expect_identical(out$x, 150)              # t=0 sits at the gutter edge
  expect_identical(out$w, (1000 - 150) / 2) # half the game = half the plot area
})

test_that("ribbon_geometry gives a short stint a visible minimum width", {
  lanes <- lane_row("own", "7", 100, 101)
  lanes$lane_index <- 1L
  out <- ribbon_geometry(lanes, total_seconds = 2400, width = 1000)
  expect_gte(out$w, 0.75)
})

test_that("ribbon_geometry stacks lanes by index", {
  lanes <- rbind(lane_row("own", "a", 0, 10), lane_row("own", "b", 0, 10))
  lanes$lane_index <- c(1L, 2L)
  out <- ribbon_geometry(lanes, total_seconds = 2400,
                         lane_height = 14, lane_gap = 3)
  expect_identical(out$y, c(0, 17))
  expect_identical(out$h, c(14, 14))
})
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: FAIL, `could not find function "merge_adjacent_stints"`.

- [ ] **Step 3: Implement the transforms**

Append to `app/R/helpers.R`:

```r
# ---------------- Stint ribbon: pure transforms ----------------
# The ribbon draws one bar per continuous stretch a player spent on the floor.
# The readers hand these functions a row per (segment, player); everything
# below is geometry and has no idea which league or table it came from.

# Collapse consecutive segments in which the same player stayed on the floor.
# Merging is per PLAYER, not per lineup hash: a player who survives a
# substitution around them keeps one continuous bar rather than abutting
# rectangles with a visible seam.
merge_adjacent_stints <- function(lanes) {
  if (is.null(lanes) || !nrow(lanes)) return(lanes)

  lanes <- lanes[order(lanes$side, lanes$player_key, lanes$start_elapsed), , drop = FALSE]
  key <- paste(lanes$side, lanes$player_key, sep = "\r")
  prev_key <- c("", key[-length(key)])
  prev_end <- c(NA_real_, lanes$end_elapsed[-nrow(lanes)])

  # A new bar starts when the player changes, or when this interval does not
  # begin exactly where the previous one ended.
  new_run <- key != prev_key | is.na(prev_end) | lanes$start_elapsed > prev_end
  run_id <- cumsum(new_run)

  merged <- lapply(split(seq_len(nrow(lanes)), run_id), function(i) {
    row <- lanes[i[1], , drop = FALSE]
    row$end_elapsed <- max(lanes$end_elapsed[i])
    row
  })

  out <- do.call(rbind, merged)
  rownames(out) <- NULL
  out
}


# Whoever is on the floor in a side's earliest segment started the game.
# Derived rather than read from a boxscore flag so both leagues use one
# definition: measured exactly 5 per team-game across 1,178 EuroLeague and 878
# Israeli team-games, while the EuroLeague boxscore carries 40 stray flags.
ribbon_mark_starters <- function(lanes) {
  if (is.null(lanes) || !nrow(lanes)) {
    lanes$is_starter <- logical(0)
    return(lanes)
  }
  first_start <- tapply(lanes$start_elapsed, lanes$side, min)
  on_first <- lanes$start_elapsed == first_start[lanes$side]
  starters <- unique(paste(lanes$side, lanes$player_key, sep = "\r")[on_first])
  lanes$is_starter <- paste(lanes$side, lanes$player_key, sep = "\r") %in% starters
  lanes
}


# Lane order within each side: starters first, then most floor time.
ribbon_lane_index <- function(lanes) {
  if (is.null(lanes) || !nrow(lanes)) return(lanes)

  order_df <- lanes %>%
    mutate(.dur = end_elapsed - start_elapsed) %>%
    group_by(side, player_key) %>%
    summarise(floor_time = sum(.dur), is_starter = any(is_starter), .groups = "drop") %>%
    arrange(side, desc(is_starter), desc(floor_time), player_key) %>%
    group_by(side) %>%
    mutate(lane_index = as.integer(row_number())) %>%
    ungroup() %>%
    select(side, player_key, lane_index)

  lanes %>% left_join(order_df, by = c("side", "player_key"))
}

# Map elapsed seconds to the 1000-unit viewBox and lane index to a y offset.
ribbon_geometry <- function(lanes, total_seconds, width = 1000,
                            lane_height = 14, lane_gap = 3,
                            gutter = RIBBON_GUTTER) {
  if (is.null(lanes) || !nrow(lanes)) return(lanes)
  stopifnot(is.numeric(total_seconds), length(total_seconds) == 1, total_seconds > 0)

  # The plot area starts after the gutter, which holds one visible name per
  # lane. A 20-lane rotation chart cannot be read through hover tooltips alone.
  scale <- (width - gutter) / total_seconds
  lanes$x <- gutter + lanes$start_elapsed * scale
  # A one-second stint would otherwise be a sub-pixel sliver that still
  # occupies a lane slot; give it a hairline so it is visible and hoverable.
  lanes$w <- pmax((lanes$end_elapsed - lanes$start_elapsed) * scale, 0.75)
  lanes$y <- (lanes$lane_index - 1L) * (lane_height + lane_gap)
  lanes$h <- lane_height
  lanes
}

# Width reserved on the left for lane labels.
RIBBON_GUTTER <- 150
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: PASS, no failures.

- [ ] **Step 5: Commit**

```bash
git add app/R/helpers.R app/tests/testthat/test-stint-ribbon.R
git commit -m "feat: stint ribbon geometry and per-player stint merging"
```

---

### Task 3: Game-clock frame and the margin series

**Files:**
- Modify: `app/R/helpers.R` (append after Task 2's block)
- Test: `app/tests/testthat/test-stint-ribbon.R` (append)

**Interfaces:**
- Consumes: nothing from earlier tasks. These two helpers are pure and depend
  only on their arguments, which is why they are separable from the lane
  geometry in Task 2.
- Produces:
  - `ribbon_period_bounds(n_periods, regulation = 4L, regulation_seconds = 600, ot_seconds = 300)` → numeric vector of cumulative period end times; the last element is the nominal game length.
  - `ribbon_complete_margin(margin, total_seconds)` → the margin series made complete and deterministic: tied elapsed values collapsed to their last state by `order_key`, a leading `(0, 0)` prepended when absent, the final score extended to `total_seconds`, and elapsed clamped to `[0, total_seconds]`. Input columns: `elapsed`, `margin`, `order_key`.

Both helpers exist because the raw data does not describe a full game. Segment
boundaries stop at the last recorded action, and scoring records start at the
first basket and stop before the buzzer — so the chart's horizontal frame and
its curve both have to be completed deliberately rather than inferred from
whatever the data happens to contain.

- [ ] **Step 1: Write the failing tests**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
test_that("ribbon_period_bounds uses nominal lengths, not observed data", {
  # Regulation: four 10-minute quarters.
  expect_identical(ribbon_period_bounds(4), c(600, 1200, 1800, 2400))
  # One overtime adds 5 minutes, matching the measured EuroLeague max of 2700.
  expect_identical(ribbon_period_bounds(5), c(600, 1200, 1800, 2400, 2700))
  expect_identical(ribbon_period_bounds(6), c(600, 1200, 1800, 2400, 2700, 3000))
})

test_that("ribbon_period_bounds floors at regulation for truncated games", {
  # A game whose actions stop early (one Israeli game ends at 961s) must still
  # be drawn on a full regulation axis, or its lanes read at the wrong scale.
  expect_identical(ribbon_period_bounds(2), c(600, 1200, 1800, 2400))
  expect_identical(ribbon_period_bounds(NA), c(600, 1200, 1800, 2400))
})

test_that("ribbon_complete_margin opens the game at zero", {
  # The first scoring event can be a minute in. Without a leading (0, 0) the
  # curve starts mid-air and the first stint has no baseline behind it.
  m <- data.frame(elapsed = c(60, 120), margin = c(2, 5), order_key = c(1, 2))
  out <- ribbon_complete_margin(m, total_seconds = 2400)
  expect_identical(out$elapsed[1], 0)
  expect_identical(out$margin[1], 0)
})

test_that("ribbon_complete_margin extends the final score to the game end", {
  m <- data.frame(elapsed = c(0, 1200), margin = c(0, 7), order_key = c(1, 2))
  out <- ribbon_complete_margin(m, total_seconds = 2400)
  expect_identical(out$elapsed[nrow(out)], 2400)
  expect_identical(out$margin[nrow(out)], 7)
})

test_that("ribbon_complete_margin collapses ties by order_key, keeping the last", {
  # Several scoring records share one elapsed second (an and-1, or a made shot
  # and the ensuing free throw). DISTINCT + ORDER BY elapsed is not
  # deterministic; the last state at that second is the true one.
  m <- data.frame(elapsed = c(0, 600, 600, 600), margin = c(0, 3, 5, 4),
                  order_key = c(1, 10, 11, 12))
  out <- ribbon_complete_margin(m, total_seconds = 2400)
  expect_identical(sum(out$elapsed == 600), 1L)
  expect_identical(out$margin[out$elapsed == 600], 4)
})

test_that("ribbon_complete_margin clamps elapsed into the nominal frame", {
  m <- data.frame(elapsed = c(0, 2500, -10), margin = c(0, 9, 1),
                  order_key = c(1, 2, 3))
  out <- ribbon_complete_margin(m, total_seconds = 2400)
  expect_true(all(out$elapsed >= 0 & out$elapsed <= 2400))
})

test_that("ribbon_complete_margin returns a usable series from no data", {
  m <- data.frame(elapsed = numeric(0), margin = numeric(0), order_key = numeric(0))
  out <- ribbon_complete_margin(m, total_seconds = 2400)
  expect_identical(out$elapsed, c(0, 2400))
  expect_identical(out$margin, c(0, 0))
})
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: FAIL, `could not find function "ribbon_period_bounds"`.

- [ ] **Step 3: Implement both helpers**

Append to `app/R/helpers.R`:

```r
# Cumulative period end times from the NOMINAL clock, never from observed
# data. Israeli games end ragged (2344, 2351, one at 961), so sizing the axis
# from max(elapsed) would draw different games at different scales and drift
# the period gridlines.
ribbon_period_bounds <- function(n_periods, regulation = 4L,
                                 regulation_seconds = 600, ot_seconds = 300) {
  n <- suppressWarnings(as.integer(n_periods))
  if (length(n) != 1 || is.na(n) || n < regulation) n <- as.integer(regulation)
  lengths <- c(rep(regulation_seconds, regulation),
               rep(ot_seconds, n - regulation))
  cumsum(lengths)
}

# Turn raw scoring records into a complete, deterministic step series.
#
# Three problems in the raw data, all of which show as a wrong curve rather than
# an error: the first scoring event may be a minute into the game, the last one
# is well before the final buzzer, and several records can share one elapsed
# second (an and-1, or a shot plus its free throw). DISTINCT + ORDER BY elapsed
# does not decide the last of those -- order_key does.
ribbon_complete_margin <- function(margin, total_seconds) {
  stopifnot(is.numeric(total_seconds), length(total_seconds) == 1, total_seconds > 0)

  if (is.null(margin) || !nrow(margin)) {
    return(data.frame(elapsed = c(0, total_seconds), margin = c(0, 0)))
  }

  m <- data.frame(
    elapsed = pmin(pmax(as.numeric(margin$elapsed), 0), total_seconds),
    margin = as.numeric(margin$margin),
    order_key = as.numeric(margin$order_key %||% seq_len(nrow(margin)))
  )
  m <- m[order(m$elapsed, m$order_key), , drop = FALSE]

  # One state per elapsed second: the last one recorded there.
  keep <- !duplicated(m$elapsed, fromLast = TRUE)
  m <- m[keep, c("elapsed", "margin"), drop = FALSE]

  if (m$elapsed[1] > 0) {
    m <- rbind(data.frame(elapsed = 0, margin = 0), m)
  }
  if (m$elapsed[nrow(m)] < total_seconds) {
    m <- rbind(m, data.frame(elapsed = total_seconds, margin = m$margin[nrow(m)]))
  }

  rownames(m) <- NULL
  m
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: PASS, no failures.

- [ ] **Step 5: Commit**

```bash
git add app/R/helpers.R app/tests/testthat/test-stint-ribbon.R
git commit -m "feat: nominal game-clock frame and completed margin series"
```

---


### Task 4: SVG builder

**Files:**
- Modify: `app/R/helpers.R` (append after Task 2's block)
- Test: `app/tests/testthat/test-stint-ribbon.R` (append)

**Interfaces:**
- Consumes: `ribbon_lane_index()`, `ribbon_geometry()` from Task 2 and `ribbon_period_bounds()` from Task 3.
- Produces:
  - `ribbon_margin_path(margin, total_seconds, width, top, height)` → SVG path `d` string, stepped.
  - `ribbon_clip_id(id_prefix, side, player_key)` → chr, the DOM id shared by the builder and `app.js`.
  - `build_stint_ribbon_svg(lanes, margin, meta, id_prefix = "ribbon")` → an `htmltools` tag, or `NULL` when `lanes` is empty. `meta` is a list with `game_label` (chr, the SVG `aria-label`), `n_periods` (int), and `own_team` / `opp_team` (chr, rendered above and below the curve; defaulted when absent).

- [ ] **Step 1: Write the failing tests**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
ribbon_fixture <- function() {
  lanes <- rbind(
    lane_row("own", "1", 0, 1200, is_starter = TRUE, player_label = "A Cohen"),
    lane_row("own", "2", 1200, 2400, player_label = "B Levy"),
    lane_row("opp", "9", 0, 2400, is_starter = TRUE, player_label = "C Katz")
  )
  margin <- data.frame(elapsed = c(0, 600, 1200), margin = c(0, 5, -3))
  meta <- list(game_label = "Team A vs Team B", n_periods = 4L,
               own_team = "Team A", opp_team = "Team B")
  list(lanes = lanes, margin = margin, meta = meta)
}

test_that("ribbon_margin_path emits a stepped path, not a diagonal one", {
  margin <- data.frame(elapsed = c(0, 1200), margin = c(0, 10))
  d <- ribbon_margin_path(margin, total_seconds = 2400, width = 1000,
                          top = 0, height = 100)
  # H before V is what makes it a step: hold the old value, then jump.
  expect_match(d, "^M ")
  expect_match(d, "H .* V ")
})

test_that("ribbon_margin_path returns an empty string for no data", {
  expect_identical(
    ribbon_margin_path(data.frame(elapsed = numeric(0), margin = numeric(0)),
                       2400, 1000, 0, 100),
    ""
  )
})

test_that("build_stint_ribbon_svg draws one rect per merged stint", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_identical(lengths(regmatches(html, gregexpr("ibpl-ribbon-lane", html))), 3L)
})

test_that("build_stint_ribbon_svg emits one clipPath per player", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_identical(lengths(regmatches(html, gregexpr("<clipPath", html))), 3L)
})

test_that("every lane's data-clip matches a clipPath id that exists", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  clips <- regmatches(html, gregexpr('data-clip="[^"]+"', html))[[1]]
  clips <- sub('data-clip="', "", sub('"$', "", clips))
  ids <- regmatches(html, gregexpr('id="[^"]+"', html))[[1]]
  ids <- sub('id="', "", sub('"$', "", ids))
  expect_true(all(clips %in% ids))
})

test_that("the viewBox width comes from nominal period length", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_match(html, 'viewBox="0 0 1000 ')
})

test_that("an overtime game gets more period gridlines than regulation", {
  f <- ribbon_fixture()
  reg <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  f$meta$n_periods <- 5L
  ot <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  n_reg <- lengths(regmatches(reg, gregexpr("ibpl-ribbon-period", reg)))
  n_ot <- lengths(regmatches(ot, gregexpr("ibpl-ribbon-period", ot)))
  expect_gt(n_ot, n_reg)
})

test_that("the margin curve is drawn twice: a base copy and a focus copy", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_match(html, "ibpl-ribbon-margin-base")
  expect_match(html, "ibpl-ribbon-margin-focus")
})

test_that("every lane renders one visible name in the gutter", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_identical(lengths(regmatches(html, gregexpr("ibpl-ribbon-name", html))), 3L)
  expect_match(html, "A Cohen", fixed = TRUE)
})

test_that("the two teams are identified on the chart", {
  f <- ribbon_fixture()
  f$meta$own_team <- "Hapoel TA"; f$meta$opp_team <- "Maccabi"
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_match(html, "Hapoel TA", fixed = TRUE)
  expect_match(html, "Maccabi", fixed = TRUE)
})

test_that("the zero-margin baseline is drawn and labelled", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_match(html, "ibpl-ribbon-zero", fixed = TRUE)
  expect_match(html, ">tied<", fixed = TRUE)
})

test_that("period boundaries are labelled, not merely drawn", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_match(html, ">Q1<", fixed = TRUE)
  expect_match(html, ">Q4<", fixed = TRUE)
  f$meta$n_periods <- 5L
  ot <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_match(ot, ">OT1<", fixed = TRUE)
})

test_that("each lane carries an explicit aria-label, not just a title", {
  # Descendants of an SVG with role="img" are not reliably exposed to assistive
  # tools, so <title> alone does not give the lane an accessible name.
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_identical(lengths(regmatches(html, gregexpr('aria-label="A Cohen', html))), 1L)
})

test_that("each lane carries a title for tooltip and screen readers", {
  f <- ribbon_fixture()
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  expect_match(html, "<title>A Cohen")
})

test_that("build_stint_ribbon_svg returns NULL when there are no lanes", {
  f <- ribbon_fixture()
  expect_null(build_stint_ribbon_svg(f$lanes[0, , drop = FALSE], f$margin, f$meta))
})

test_that("clip ids are namespaced so two ribbons on a page cannot collide", {
  f <- ribbon_fixture()
  a <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta, id_prefix = "r1"))
  b <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta, id_prefix = "r2"))
  expect_match(a, 'id="r1-on-own-1"')
  expect_match(b, 'id="r2-on-own-1"')
})

test_that("clip ids stay valid when the key is a EuroLeague player name", {
  # Unmatched euro players key on the name, e.g. "BIRCH, KHEM". A space or
  # comma in a DOM id makes url(#...) fail silently and hover dies for exactly
  # those players.
  id <- ribbon_clip_id("r1", "own", "BIRCH, KHEM")
  expect_false(grepl("[ ,]", id))
  expect_identical(id, "r1-on-own-BIRCH-KHEM")
})

test_that("a named-key lane still resolves to a clipPath that exists", {
  f <- ribbon_fixture()
  f$lanes$player_key <- c("BIRCH, KHEM", "HALL, DEVON", "SLEVA, DUSTIN")
  html <- as.character(build_stint_ribbon_svg(f$lanes, f$margin, f$meta))
  clips <- regmatches(html, gregexpr('data-clip="[^"]+"', html))[[1]]
  clips <- sub('data-clip="', "", sub('"$', "", clips))
  ids <- regmatches(html, gregexpr('id="[^"]+"', html))[[1]]
  ids <- sub('id="', "", sub('"$', "", ids))
  expect_true(all(clips %in% ids))
})
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: FAIL, `could not find function "ribbon_margin_path"`.

- [ ] **Step 3: Implement the builder**

Append to `app/R/helpers.R`:

```r
# ---------------- Stint ribbon: SVG builder ----------------
# The app has no plotting library. This emits an htmltools tag tree directly,
# which keeps the output testable as structure rather than as pixels.

RIBBON_WIDTH <- 1000
RIBBON_LANE_HEIGHT <- 14
RIBBON_LANE_GAP <- 3
RIBBON_MARGIN_HEIGHT <- 90

# EuroLeague player keys fall back to the player NAME when the roster join
# misses ("BIRCH, KHEM"), and that key becomes a DOM id. Spaces and commas make
# url(#...) fail silently, so hover would die for exactly those players.
ribbon_clip_id <- function(id_prefix, side, player_key) {
  slug <- gsub("[^A-Za-z0-9_-]+", "-", as.character(player_key))
  slug <- gsub("(^-+)|(-+$)", "", slug)
  paste0(id_prefix, "-on-", side, "-", slug)
}

# A stepped path: the score holds its value until the next scoring event, then
# jumps. Drawing straight segments between events would imply the margin drifted
# continuously, which it did not.
ribbon_margin_path <- function(margin, total_seconds, width, top, height,
                              gutter = RIBBON_GUTTER) {
  if (is.null(margin) || !nrow(margin)) return("")

  margin <- margin[order(margin$elapsed), , drop = FALSE]
  max_abs <- suppressWarnings(max(abs(margin$margin), na.rm = TRUE))
  if (!is.finite(max_abs) || max_abs <= 0) max_abs <- 1

  x <- gutter + margin$elapsed * ((width - gutter) / total_seconds)
  y <- top + height / 2 - (margin$margin / max_abs) * (height / 2)

  parts <- sprintf("M %.2f %.2f", x[1], y[1])
  if (length(x) > 1) {
    parts <- c(parts, sprintf("H %.2f V %.2f", x[-1], y[-1]))
  }
  paste(parts, collapse = " ")
}

build_stint_ribbon_svg <- function(lanes, margin, meta, id_prefix = "ribbon") {
  if (is.null(lanes) || !nrow(lanes)) return(NULL)

  bounds <- ribbon_period_bounds(meta$n_periods)
  total_seconds <- bounds[length(bounds)]

  lanes <- merge_adjacent_stints(lanes)
  lanes <- ribbon_lane_index(lanes)
  lanes <- ribbon_geometry(lanes, total_seconds, width = RIBBON_WIDTH,
                           lane_height = RIBBON_LANE_HEIGHT,
                           lane_gap = RIBBON_LANE_GAP)

  own <- lanes[lanes$side == "own", , drop = FALSE]
  opp <- lanes[lanes$side == "opp", , drop = FALSE]
  own_h <- max(own$y + own$h, 0)
  margin_top <- own_h + RIBBON_LANE_GAP * 2
  opp_top <- margin_top + RIBBON_MARGIN_HEIGHT + RIBBON_LANE_GAP * 2
  total_h <- opp_top + max(opp$y + opp$h, 0)

  # Own lanes sit above the curve, opponent lanes mirrored below it.
  lanes$abs_y <- ifelse(lanes$side == "own", lanes$y, opp_top + lanes$y)
  lanes$clip <- ribbon_clip_id(id_prefix, lanes$side, lanes$player_key)

  path_d <- ribbon_margin_path(margin, total_seconds, RIBBON_WIDTH,
                               margin_top, RIBBON_MARGIN_HEIGHT)

  # One clipPath per player: hovering swaps a single clip-path attribute on the
  # focus curve, so no geometry is computed in the browser.
  clip_keys <- unique(lanes$clip)
  clip_paths <- lapply(clip_keys, function(cid) {
    rows <- lanes[lanes$clip == cid, , drop = FALSE]
    tags$clipPath(
      id = cid,
      lapply(seq_len(nrow(rows)), function(i) {
        tags$rect(x = rows$x[i], y = margin_top,
                  width = rows$w[i], height = RIBBON_MARGIN_HEIGHT)
      })
    )
  })

  period_lines <- lapply(bounds[-length(bounds)], function(b) {
    bx <- RIBBON_GUTTER + b * ((RIBBON_WIDTH - RIBBON_GUTTER) / total_seconds)
    tags$line(class = "ibpl-ribbon-period",
              x1 = bx, x2 = bx, y1 = 0, y2 = total_h)
  })

  lane_rects <- lapply(seq_len(nrow(lanes)), function(i) {
    secs <- lanes$end_elapsed[i] - lanes$start_elapsed[i]
    label <- sprintf("%s, %.0f:%02.0f on the floor",
                     lanes$player_label[i], secs %/% 60, secs %% 60)
    tags$g(
      class = paste("ibpl-ribbon-lane", paste0("is-", lanes$side[i])),
      `data-clip` = lanes$clip[i],
      tabindex = "0",
      role = "listitem",
      # An explicit aria-label per lane: descendants of an SVG with role="img"
      # are not reliably exposed, so <title> alone is not an accessible name.
      `aria-label` = label,
      tags$title(label),
      tags$rect(x = lanes$x[i], y = lanes$abs_y[i],
                width = lanes$w[i], height = lanes$h[i], rx = 2)
    )
  })

  # One visible name per lane, in the gutter.
  first_row <- !duplicated(paste(lanes$side, lanes$player_key))
  lane_labels <- lapply(which(first_row), function(i) {
    tags$text(class = "ibpl-ribbon-name", x = RIBBON_GUTTER - 8,
              y = lanes$abs_y[i] + lanes$h[i] - 3, `text-anchor` = "end",
              lanes$player_label[i])
  })

  # Which team is above the curve and which is below.
  team_labels <- list(
    tags$text(class = "ibpl-ribbon-team", x = 0, y = -6, meta$own_team %||% "Own"),
    tags$text(class = "ibpl-ribbon-team", x = 0, y = opp_top - 6,
              meta$opp_team %||% "Opponent")
  )

  # Zero-margin baseline, labelled, so the curve has a readable scale.
  zero_y <- margin_top + RIBBON_MARGIN_HEIGHT / 2
  baseline <- list(
    tags$line(class = "ibpl-ribbon-zero", x1 = RIBBON_GUTTER, x2 = RIBBON_WIDTH,
              y1 = zero_y, y2 = zero_y),
    tags$text(class = "ibpl-ribbon-zero-label", x = RIBBON_GUTTER - 8, y = zero_y + 3,
              `text-anchor` = "end", "tied")
  )

  # Period boundaries named, not just drawn.
  period_labels <- lapply(seq_along(bounds), function(k) {
    bx <- RIBBON_GUTTER + bounds[k] * ((RIBBON_WIDTH - RIBBON_GUTTER) / total_seconds)
    tags$text(class = "ibpl-ribbon-period-label", x = bx - 4, y = total_h + 12,
              `text-anchor` = "end",
              if (k <= 4) paste0("Q", k) else paste0("OT", k - 4))
  })

  tags$svg(
    xmlns = "http://www.w3.org/2000/svg",
    viewBox = sprintf("0 0 %d %.0f", RIBBON_WIDTH, total_h),
    class = "ibpl-ribbon",
    role = "img",
    `aria-label` = meta$game_label,
    tags$defs(clip_paths),
    period_lines,
    baseline,
    tags$path(class = "ibpl-ribbon-margin-base", d = path_d),
    tags$path(class = "ibpl-ribbon-margin-focus", d = path_d),
    team_labels,
    lane_labels,
    period_labels,
    lane_rects
  )
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: PASS, no failures.

- [ ] **Step 5: Commit**

```bash
git add app/R/helpers.R app/tests/testthat/test-stint-ribbon.R
git commit -m "feat: stint ribbon SVG builder with per-player clip paths"
```

---

### Task 5: The two readers behind one canonical frame

**Files:**
- Modify: `app/R/global.R` (append after `cached_season_df`, around line 265)
- Test: `app/tests/testthat/test-stint-ribbon-readers.R` (create)

**Interfaces:**
- Consumes: `db_get_query()` and `cached_season_df()` from `global.R`; `ribbon_normalise_lanes()` and `ribbon_mark_starters()` from Task 2; `ribbon_period_bounds()` and `ribbon_complete_margin()` from Task 3; the two views from Task 1.
- Produces: `fetch_stint_ribbon(pool, league, game_id, team_id, data_version = NULL)` → `list(lanes = <data.frame>, margin = <data.frame>, meta = <list>, health = <chr or NULL>)`.
  - `lanes` columns: `side`, `player_key` (always the player_id), `player_label` (display only), `is_starter` (added by `ribbon_mark_starters`, not by SQL), `start_elapsed`, `end_elapsed`.
  - `margin` columns: `elapsed`, `margin` (signed so positive means the clicked team leads).
  - `meta`: `n_periods` (int) only. The calling observer adds `game_label` before passing `meta` to the builder, because the reader has no access to team names without a second query.
  - Also produces `ribbon_normalise_lanes(raw, own_team_id)` (pure, in `helpers.R`) — maps a reader's raw rows onto the canonical frame.

- [ ] **Step 1: Write the failing tests**

Create `app/tests/testthat/test-stint-ribbon-readers.R`:

```r
# The readers are the only league-aware code in the feature. These tests stub
# the database and assert the canonical frame, so a schema difference between
# the two leagues can never reach the renderer.

test_that("ribbon_normalise_lanes labels the clicked team own and the other opp", {
  raw <- data.frame(
    team_id = c(7L, 7L, 10L),
    player_id = c(1L, 2L, 3L),
    player_label = c("A", "B", "C"),
    start_elapsed = c(0, 0, 0),
    end_elapsed = c(100, 100, 100),
    stringsAsFactors = FALSE
  )
  out <- ribbon_normalise_lanes(raw, own_team_id = 7L)
  expect_identical(out$side, c("own", "own", "opp"))
  expect_identical(out$player_key, c("1", "2", "3"))
})

test_that("ribbon_normalise_lanes returns the canonical columns and nothing else", {
  raw <- data.frame(team_id = 7L, player_id = 1L, player_label = "A",
                    start_elapsed = 0, end_elapsed = 10,
                    extra_junk = "drop me", stringsAsFactors = FALSE)
  out <- ribbon_normalise_lanes(raw, own_team_id = 7L)
  expect_identical(
    sort(names(out)),
    sort(c("side", "player_key", "player_label",
           "start_elapsed", "end_elapsed"))
  )
})

test_that("ribbon_normalise_lanes keys on player_id, never on the label", {
  # Both leagues carry same-name/different-id players on one team (Israeli has
  # a same-name pair inside a single game+team, and team 4 has "NEW NEW" across
  # three ids). Keying on the label would merge two people into one lane.
  raw <- data.frame(
    team_id = c(7L, 7L),
    player_id = c(101L, 202L),
    player_label = c("NEW NEW", "NEW NEW"),
    start_elapsed = c(0, 0), end_elapsed = c(100, 100),
    stringsAsFactors = FALSE
  )
  out <- ribbon_normalise_lanes(raw, own_team_id = 7L)
  expect_identical(out$player_key, c("101", "202"))
  expect_identical(length(unique(out$player_key)), 2L)
})

test_that("ribbon_health_message speaks only when segments were excluded", {
  expect_null(ribbon_health_message(0))
  expect_null(ribbon_health_message(NULL))
  expect_match(ribbon_health_message(3), "3")
  expect_match(ribbon_health_message(3), "not drawn")
})

test_that("ribbon_sign_margin flips the sign when the clicked team is away", {
  m <- data.frame(elapsed = c(0, 60), points_a = c(0, 10), points_b = c(0, 4))
  home <- ribbon_sign_margin(m, own_team_id = 5L, home_team_id = 5L)
  away <- ribbon_sign_margin(m, own_team_id = 9L, home_team_id = 5L)
  expect_identical(home$margin, c(0, 6))
  expect_identical(away$margin, c(0, -6))
})

test_that("euro segment lineups resolve to player_ids with no fan-out", {
  # matchup_segments_actions stores lineups as text[] of NAMES with no ids.
  # Rather than resolving names one by one -- the join shape that manufactured
  # 258 false findings in the 2026-08-19 DQ report -- the view reuses the
  # lineup -> player_id link the on/off system already has. This guards that
  # link: every segment lineup must resolve, and to exactly one id set.
  skip_on_cran()
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")

  con <- DBI::dbConnect(RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
    connect_timeout = 15L, bigint = "numeric")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  unresolved <- DBI::dbGetQuery(con, "
    WITH seg AS (
      SELECT DISTINCT game_id, team_id, own_lineup
      FROM euroleague.matchup_segments_actions
      WHERE segment_seconds > 0)
    SELECT COUNT(*) AS n
    FROM seg s
    LEFT JOIN (SELECT DISTINCT game_id, team_id, own_lineup, player_ids
               FROM euroleague.lineup_totals_by_game) l
      ON l.game_id = s.game_id AND l.team_id = s.team_id
     AND l.own_lineup = s.own_lineup
    WHERE l.player_ids IS NULL")
  expect_identical(as.numeric(unresolved$n[1]), 0)

  fanout <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM (
      SELECT game_id, team_id, own_lineup
      FROM euroleague.lineup_totals_by_game
      GROUP BY 1, 2, 3 HAVING COUNT(DISTINCT player_ids::text) > 1) t")
  expect_identical(as.numeric(fanout$n[1]), 0)
})

test_that("the euro reader never pairs lineup names with ids positionally", {
  # own_lineup and player_ids are each sorted independently, so names[i] is
  # unrelated to ids[i] -- measured 31,907 mismatches in 40,000 pairs. Pairing
  # positionally would mislabel four lanes in five, plausibly.
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "
")
  sql <- regmatches(src, regexpr('RIBBON_SQL_EURO <- "(.|
)*?"
', src, perl = TRUE))
  expect_true(nzchar(sql))
  expect_false(grepl("unnest\s*\([^)]*own_lineup[^)]*,", sql))
  expect_match(sql, "unnest\(s\.player_ids\)", fixed = FALSE)
})

test_that("the Israeli lane label comes from the per-game roster, not a season map", {
  # The provider reuses a player id for a DIFFERENT person in specific games
  # (id 2060 is Josh Hagins season-wide but J'Von McCormick in ~7 games), so a
  # season-level name lookup would label those games with the wrong player.
  # full_rosters is per game, which is what makes the label safe -- keep the
  # game_id predicate on that join.
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  sql <- regmatches(src, regexpr('RIBBON_SQL_ISRAEL <- "(.|\n)*?"\n', src, perl = TRUE))
  expect_true(nzchar(sql))
  expect_match(sql, "full_rosters r")
  expect_match(sql, "r\\.game_id\\s*=\\s*\\$1")
})

test_that("the Israeli ribbon SQL carries no type_lineup predicate", {
  # Filtering to offense loses 1.46% of floor time -- the same defect as the
  # unattributed floor time fixed 2026-09-05. This is a source assertion so it
  # runs without a database and fails loudly if someone "optimises" the query.
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  sql <- regmatches(src, regexpr('RIBBON_SQL_ISRAEL <- "(.|\n)*?"\n', src, perl = TRUE))
  expect_true(nzchar(sql))
  expect_false(grepl("type_lineup\\s*=", sql))
  expect_false(grepl("type_lineup\\s+IS\\s+NOT\\s+NULL", sql, ignore.case = TRUE))
  expect_false(grepl("GROUP BY[^)]*type_lineup", sql, ignore.case = TRUE))
})

test_that("the ribbon segment count matches the type_lineup-absent grouping", {
  skip_on_cran()
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")

  con <- DBI::dbConnect(RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
    connect_timeout = 15L, bigint = "numeric")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  counts <- DBI::dbGetQuery(con, "
    SELECT
      (SELECT COUNT(*) FROM (
         SELECT team_id, segment_id, lineup_hash
         FROM basketball_test.df_pts_poss_lineups_longer_mv
         WHERE game_id = 115
         GROUP BY team_id, segment_id, lineup_hash
         HAVING MAX(segment_seconds) > 0) a)          AS all_perspectives,
      (SELECT COUNT(*) FROM (
         SELECT team_id, segment_id, lineup_hash
         FROM basketball_test.df_pts_poss_lineups_longer_mv
         WHERE game_id = 115 AND type_lineup = 'offense'
         GROUP BY team_id, segment_id, lineup_hash
         HAVING MAX(segment_seconds) > 0) b)          AS offense_only")

  # Offense-only must be strictly fewer. If these ever match, the NULL and
  # defense perspectives have stopped carrying segments of their own and this
  # guard has lost its meaning -- investigate before relaxing it.
  expect_gt(counts$all_perspectives[1], counts$offense_only[1])
})
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: FAIL, `could not find function "ribbon_normalise_lanes"`.

- [ ] **Step 3: Add the pure normalisers to `helpers.R`**

Append to `app/R/helpers.R`:

```r
# ---------------- Stint ribbon: reader normalisers ----------------
# Pure so both league readers share one definition of the canonical frame.

RIBBON_LANE_COLS <- c("side", "player_key", "player_label",
                      "start_elapsed", "end_elapsed")

# player_key is always the player_id. Names are display labels only -- both
# leagues carry same-name/different-id players on one team, so a name key would
# silently merge two people's floor time into one lane.
ribbon_normalise_lanes <- function(raw, own_team_id) {
  if (is.null(raw) || !nrow(raw)) {
    return(data.frame(side = character(0), player_key = character(0),
                      player_label = character(0),
                      start_elapsed = numeric(0), end_elapsed = numeric(0),
                      stringsAsFactors = FALSE))
  }
  data.frame(
    side = ifelse(as.integer(raw$team_id) == as.integer(own_team_id), "own", "opp"),
    player_key = as.character(raw$player_id),
    player_label = as.character(raw$player_label),
    start_elapsed = as.numeric(raw$start_elapsed),
    end_elapsed = as.numeric(raw$end_elapsed),
    stringsAsFactors = FALSE
  )
}

# Positive margin always means the clicked team is ahead.
ribbon_sign_margin <- function(m, own_team_id, home_team_id) {
  if (is.null(m) || !nrow(m)) {
    return(data.frame(elapsed = numeric(0), margin = numeric(0),
                      order_key = numeric(0)))
  }
  diff <- as.numeric(m$points_a) - as.numeric(m$points_b)
  own_is_home <- !is.na(home_team_id) &&
    as.integer(own_team_id) == as.integer(home_team_id)
  data.frame(
    elapsed = as.numeric(m$elapsed),
    margin = if (own_is_home) diff else -diff,
    order_key = as.numeric(m$order_key %||% seq_len(nrow(m)))
  )
}

# The SQL drops segments whose lineup fails the cardinality = 5 guard, matching
# what fetch_lineups_all.sql already does. This only reports that it happened,
# so a game with incomplete lineups says so instead of showing a silent gap.
# Affects roughly 4-6 games in 221.
ribbon_health_message <- function(excluded_segments) {
  n <- suppressWarnings(as.integer(excluded_segments %||% 0))
  if (length(n) != 1 || is.na(n) || n <= 0) return(NULL)
  sprintf(paste("Lineup data is incomplete for this game: %d segment(s) had no",
                "five-player lineup on record and are not drawn."), n)
}

```

- [ ] **Step 4: Add the readers to `global.R`**

Append to `app/R/global.R` after `cached_season_df()`:

```r
# ---------------- Stint ribbon readers ----------------
# ONE query per ribbon open. The pooler round-trip floor is 238ms against a
# 500ms budget, and server-side execution is 0.5-21.6ms, so a second query
# costs more than everything else in the feature combined. Lanes and margin
# therefore come back as two jsonb columns of a single row.

RIBBON_SQL_ISRAEL <- "
WITH gy AS (
  SELECT game_year FROM basketball_test.final_schedule_mv
  WHERE game_id = $1 LIMIT 1
),
segs AS (
  -- Same collapse the app already performs in server_tab3.R:1327-1345: group
  -- WITHOUT type_lineup and take MAX(segment_seconds). Filtering to 'offense'
  -- loses 1.46% of floor time; the NULL rows are substitutions and timeouts and
  -- cost 0.14%, nearly all of it zero-length.
  --
  -- The ribbon's ONLY departure from the existing readers: it keeps the
  -- interval (start/end elapsed) rather than collapsing it to a duration.
  SELECT team_id, segment_id, lineup_hash,
         MIN(segment_start_elapsed_seconds) AS start_elapsed,
         MAX(segment_end_elapsed_seconds)   AS end_elapsed
  FROM basketball_test.df_pts_poss_lineups_longer_mv
  WHERE game_id = $1
  GROUP BY team_id, segment_id, lineup_hash
  HAVING MAX(segment_seconds) > 0
),
lineup_players AS (
  -- The canonical hash -> players expansion, copied from
  -- fetch_lineups_all.sql:205-214 including its cardinality = 5 guard. The app
  -- already excludes odd-sized lineups everywhere; the ribbon follows that
  -- convention instead of inventing its own handling.
  SELECT l.team_id, l.lineup_hash,
         ARRAY_AGG(DISTINCT l.player_id ORDER BY l.player_id)::int4[] AS player_ids
  FROM basketball_test.lineups_lookup_on l
  WHERE l.game_year = (SELECT game_year FROM gy)
  GROUP BY l.team_id, l.lineup_hash
  HAVING cardinality(ARRAY_AGG(DISTINCT l.player_id)) = 5
),
lanes AS (
  SELECT s.team_id,
         s.start_elapsed,
         s.end_elapsed,
         p.player_id,
         -- Label only, and fetched BY id. full_rosters is per GAME, which is
         -- what makes it safe: the provider reuses an id for a different person
         -- in specific games (id 2060 is Josh Hagins season-wide but J'Von
         -- McCormick in ~7), so a season-level name map would mislabel those.
         COALESCE(NULLIF(TRIM(COALESCE(r.firstname, '') || ' ' ||
                              COALESCE(r.lastname, '')), ''),
                  'Player ' || p.player_id) AS player_label
  FROM segs s
  JOIN lineup_players lp
    ON lp.lineup_hash = s.lineup_hash AND lp.team_id = s.team_id
  CROSS JOIN LATERAL unnest(lp.player_ids) AS p(player_id)
  LEFT JOIN basketball_test.full_rosters r
    ON r.game_id   = $1
   AND r.team_id   = s.team_id
   AND r.player_id = p.player_id
),
marg AS (
  -- order_key breaks ties when several scoring records share one elapsed
  -- second (an and-1, or a made shot and its free throw). Without it,
  -- DISTINCT + ORDER BY elapsed picks a state arbitrarily.
  SELECT DISTINCT event_elapsed_seconds AS elapsed,
         (own_team_score - opp_team_score) AS margin,
         id AS order_key
  FROM basketball_test.df_pts_poss_lineups_longer_mv
  WHERE game_id = $1 AND team_id = $2
)
SELECT
  (SELECT jsonb_agg(to_jsonb(lanes)) FROM lanes)                AS lanes,
  (SELECT jsonb_agg(to_jsonb(marg) ORDER BY elapsed, order_key) FROM marg) AS margin,
  (SELECT MAX(quarter) FROM basketball_test.df_pts_poss_lineups_longer_mv
    WHERE game_id = $1)                                         AS n_periods,
  -- Segments the cardinality = 5 guard dropped. One counter, not a bespoke
  -- health subsystem: it exists only so the modal can say the lanes are
  -- incomplete rather than silently showing a gap.
  (SELECT COUNT(*) FROM segs s
    WHERE NOT EXISTS (SELECT 1 FROM lineup_players lp
                       WHERE lp.lineup_hash = s.lineup_hash
                         AND lp.team_id = s.team_id))           AS excluded_segments
"

RIBBON_SQL_EURO <- "
WITH segs AS (
  SELECT team_id, segment_id, player_ids,
         start_elapsed_seconds AS start_elapsed,
         end_elapsed_seconds   AS end_elapsed
  FROM euroleague.ribbon_segments_v
  WHERE game_id = $1
),
lanes AS (
  -- Keyed on player_id throughout. The name is a display label fetched BY id,
  -- never a join key: euro rosters contain same-name/different-id players, and
  -- the lineup name array is not positionally aligned with the id array.
  SELECT s.team_id,
         s.start_elapsed,
         s.end_elapsed,
         p.player_id,
         COALESCE(r.source_player_name, 'Player ' || p.player_id) AS player_label
  FROM segs s
  CROSS JOIN LATERAL unnest(s.player_ids) AS p(player_id)
  LEFT JOIN euroleague.full_rosters r
    ON r.game_id   = $1
   AND r.team_id   = s.team_id
   AND r.player_id = p.player_id
),
marg AS (
  SELECT DISTINCT elapsed_seconds AS elapsed, points_a, points_b, home_team_id,
         source_event_order AS order_key
  FROM euroleague.ribbon_margin_v
  WHERE game_id = $1
)
SELECT
  (SELECT jsonb_agg(to_jsonb(lanes)) FROM lanes)                AS lanes,
  (SELECT jsonb_agg(to_jsonb(marg) ORDER BY elapsed, order_key) FROM marg) AS margin,
  (SELECT MAX(period) FROM euroleague.ribbon_margin_v WHERE game_id = $1) AS n_periods,
  0 AS excluded_segments
"

fetch_stint_ribbon <- function(pool, league, game_id, team_id, data_version = NULL) {
  league <- match.arg(league, c("israel", "euroleague"))
  game_id <- as.integer(game_id)
  team_id <- as.integer(team_id)

  cached_season_df(list("stint_ribbon", league, game_id, team_id, data_version), function() {
    sql <- if (identical(league, "israel")) RIBBON_SQL_ISRAEL else RIBBON_SQL_EURO
    params <- if (identical(league, "israel")) list(game_id, team_id) else list(game_id)

    row <- db_get_query(pool, sql, params = params)
    if (is.null(row) || !nrow(row)) return(NULL)

    lanes_raw <- if (is.na(row$lanes[1])) NULL else
      jsonlite::fromJSON(row$lanes[1], simplifyDataFrame = TRUE)
    marg_raw <- if (is.na(row$margin[1])) NULL else
      jsonlite::fromJSON(row$margin[1], simplifyDataFrame = TRUE)

    if (is.null(lanes_raw) || !NROW(lanes_raw)) return(NULL)

    if (identical(league, "euroleague")) {
      margin <- ribbon_sign_margin(marg_raw, team_id,
                                   if (NROW(marg_raw)) marg_raw$home_team_id[1] else NA)
    } else {
      margin <- data.frame(
        elapsed = as.numeric(marg_raw$elapsed %||% numeric(0)),
        margin = as.numeric(marg_raw$margin %||% numeric(0)),
        order_key = as.numeric(marg_raw$order_key %||% numeric(0))
      )
    }

    # Complete the series before it reaches the renderer: open at 0-0, hold the
    # final score to the buzzer, one state per second.
    n_periods <- as.integer(row$n_periods[1] %||% 4L)
    bounds <- ribbon_period_bounds(n_periods)
    margin <- ribbon_complete_margin(margin, bounds[length(bounds)])

    # Starters are derived from the first segment, identically in both leagues:
    # measured exactly 5 per team-game in 1,178 EuroLeague and 878 Israeli
    # team-games, and cleaner than the EuroLeague boxscore flag (40 stray flags).
    lanes <- ribbon_normalise_lanes(lanes_raw, team_id)
    lanes <- ribbon_mark_starters(lanes)

    list(
      lanes = lanes,
      margin = margin,
      meta = list(n_periods = n_periods),
      health = ribbon_health_message(row$excluded_segments[1])
    )
  })
}
```

Note: `jsonlite` is already a transitive dependency of shiny and is loaded in the session; it is referenced with `::` so no new `library()` call is added.

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: PASS, no failures.

- [ ] **Step 6: Verify both readers against the live database and the budget**

```bash
cat > /tmp/ribbon_readers.R <<'EOF'
setwd("app")
readRenviron(".Renviron")
library(DBI); library(RPostgres); library(dplyr); library(htmltools)
source("R/helpers.R")
pool <- dbConnect(RPostgres::Postgres(), host=Sys.getenv("PG_HOST"),
  port=as.integer(Sys.getenv("PG_PORT")), dbname=Sys.getenv("PG_DB"),
  user=Sys.getenv("PG_USER"), password=Sys.getenv("PG_PASS"),
  sslmode=Sys.getenv("PG_SSLMODE"), connect_timeout=15L, bigint="numeric")
src <- readLines("R/global.R")
eval(parse(text = paste(src[grep("^RIBBON_SQL_ISRAEL", src)[1]:length(src)], collapse="\n")))
db_get_query <- function(conn, statement, params = NULL)
  if (is.null(params)) DBI::dbGetQuery(conn, statement) else DBI::dbGetQuery(conn, statement, params = params)
cached_season_df <- function(key_parts, fn) fn()

for (arm in list(list("israel", 115L, 7L), list("euroleague", 25L, 25L))) {
  ms <- replicate(7, {
    t0 <- Sys.time()
    r <- fetch_stint_ribbon(pool, arm[[1]], arm[[2]], arm[[3]])
    as.numeric(difftime(Sys.time(), t0, units="secs")) * 1000
  })
  r <- fetch_stint_ribbon(pool, arm[[1]], arm[[2]], arm[[3]])
  cat(arm[[1]], "lanes:", nrow(r$lanes), "margin:", nrow(r$margin),
      "sides:", paste(sort(unique(r$lanes$side)), collapse="/"),
      "median ms:", round(median(ms), 1), "\n")
  stopifnot(median(ms) < 500, nrow(r$lanes) > 0,
            identical(sort(unique(r$lanes$side)), c("opp", "own")))
}
dbDisconnect(pool)
EOF
"$RSCRIPT" /tmp/ribbon_readers.R
```

Expected: both leagues print lanes > 0, `sides: opp/own`, and a median under 500 ms.

- [ ] **Step 7: Commit**

```bash
git add app/R/helpers.R app/R/global.R app/tests/testthat/test-stint-ribbon-readers.R
git commit -m "feat: single-round-trip stint ribbon readers for both leagues"
```

---

### Task 6: Ribbon styling and the hover handler

**Files:**
- Modify: `app/www/app.css` (append)
- Modify: `app/www/app.js` (append a new IIFE at end of file)
- Test: manual, plus the CSS/JS presence assertions below

**Interfaces:**
- Consumes: the class names and `data-clip` attribute emitted by `build_stint_ribbon_svg()` in Task 4 — `.ibpl-ribbon`, `.ibpl-ribbon-lane`, `.ibpl-ribbon-margin-base`, `.ibpl-ribbon-margin-focus`, `.ibpl-ribbon-period`, `.ibpl-ribbon-name`, `.ibpl-ribbon-team`, `.ibpl-ribbon-zero`, `.ibpl-ribbon-period-label`.
- Produces: no R interface. The contract is the DOM one above.

**Before starting:** export `IBPL_CACHE_UI=false` in the shell that runs the app, or edits to these two files will not appear on a browser reload.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
test_that("app.css styles every class the SVG builder emits", {
  css <- paste(readLines(testthat::test_path("..", "..", "www", "app.css"),
                         warn = FALSE), collapse = "\n")
  for (cls in c("ibpl-ribbon", "ibpl-ribbon-lane", "ibpl-ribbon-margin-base",
                "ibpl-ribbon-margin-focus", "ibpl-ribbon-period",
                "ibpl-ribbon-name", "ibpl-ribbon-team", "ibpl-ribbon-zero",
                "ibpl-ribbon-period-label")) {
    expect_match(css, cls, fixed = TRUE,
                 info = paste("missing ribbon style for", cls))
  }
})

test_that("app.js swaps clip-path rather than recomputing geometry", {
  js <- paste(readLines(testthat::test_path("..", "..", "www", "app.js"),
                        warn = FALSE), collapse = "\n")
  expect_match(js, "ibpl-ribbon-margin-focus", fixed = TRUE)
  expect_match(js, "clip-path", fixed = TRUE)
  # The hover target is the lane group, keyed by the builder's data-clip.
  expect_match(js, "ibpl-ribbon-lane", fixed = TRUE)
})
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: FAIL, "missing ribbon style for ibpl-ribbon".

- [ ] **Step 3: Add the styles**

Append to `app/www/app.css`:

```css
/* ---------------- Stint ribbon ---------------- */
.ibpl-ribbon {
  width: 100%;
  height: auto;
  display: block;
}

.ibpl-ribbon-period {
  stroke: var(--ibpl-border, #3a3a3a);
  stroke-width: 1;
  opacity: 0.45;
}

.ibpl-ribbon-lane rect {
  fill: var(--ibpl-accent, #e8a435);
  opacity: 0.85;
  transition: opacity 120ms ease;
}

.ibpl-ribbon-lane.is-opp rect {
  fill: var(--ibpl-text-muted, #8a8a8a);
}

/* At rest the chart is a rotation map: every lane reads equally. */
.ibpl-ribbon.is-focused .ibpl-ribbon-lane rect { opacity: 0.3; }
.ibpl-ribbon.is-focused .ibpl-ribbon-lane.is-active rect { opacity: 1; }

.ibpl-ribbon-lane:focus { outline: none; }
.ibpl-ribbon-lane:focus rect {
  stroke: var(--ibpl-accent, #e8a435);
  stroke-width: 1.5;
}

.ibpl-ribbon-name,
.ibpl-ribbon-team,
.ibpl-ribbon-period-label,
.ibpl-ribbon-zero-label {
  fill: var(--ibpl-text-muted, #8a8a8a);
  font-family: var(--ibpl-font-sans, system-ui, sans-serif);
  font-size: 11px;
}

.ibpl-ribbon-name { fill: var(--ibpl-text, #e6e6e6); }

.ibpl-ribbon-team {
  fill: var(--ibpl-text, #e6e6e6);
  font-size: 12px;
  font-weight: 600;
  letter-spacing: 0.02em;
}

.ibpl-ribbon-zero {
  stroke: var(--ibpl-border, #3a3a3a);
  stroke-width: 1;
  stroke-dasharray: 3 3;
}

.ibpl-ribbon-margin-base,
.ibpl-ribbon-margin-focus {
  fill: none;
  stroke-width: 1.75;
}

.ibpl-ribbon-margin-base {
  stroke: var(--ibpl-text-muted, #8a8a8a);
  opacity: 0.5;
}

/* Hidden until a lane is hovered, then revealed only where that player was on. */
.ibpl-ribbon-margin-focus {
  stroke: var(--ibpl-accent, #e8a435);
  opacity: 0;
}

.ibpl-ribbon.is-focused .ibpl-ribbon-margin-focus { opacity: 1; }
```

- [ ] **Step 4: Add the hover handler**

Append to `app/www/app.js`:

```js
// Expose the queue-and-replay sender so delegated handlers defined in other
// IIFEs (the ribbon link) can use it instead of calling Shiny.setInputValue
// directly and dropping clicks that land before shiny:connected.
window.ibplSendShinyEvent = function(inputId, value) { sendShinyEvent(inputId, value); };

// ---------------- Stint ribbon hover ----------------
// Hovering a lane reveals the margin curve only where that player was on the
// floor. The clip rectangles are rendered server-side, one clipPath per
// player, so this is a single attribute write -- no geometry in the browser.
(function() {
  function setFocus(svg, lane) {
    var focus = svg.querySelector(".ibpl-ribbon-margin-focus");
    if (!focus) return;

    var active = svg.querySelectorAll(".ibpl-ribbon-lane.is-active");
    for (var i = 0; i < active.length; i++) active[i].classList.remove("is-active");

    if (lane && lane.dataset.clip) {
      focus.setAttribute("clip-path", "url(#" + lane.dataset.clip + ")");
      // A player with two separated stints has two <g> elements sharing one
      // data-clip. Light all of them, or the curve shows both stints while only
      // one bar highlights.
      var mates = svg.querySelectorAll(
        '.ibpl-ribbon-lane[data-clip="' + lane.dataset.clip + '"]');
      for (var m = 0; m < mates.length; m++) mates[m].classList.add("is-active");
      svg.classList.add("is-focused");
    } else {
      focus.removeAttribute("clip-path");
      svg.classList.remove("is-focused");
    }
  }

  function laneFrom(target) {
    return target && target.closest ? target.closest(".ibpl-ribbon-lane") : null;
  }

  document.addEventListener("mouseover", function(e) {
    var lane = laneFrom(e.target);
    if (!lane) return;
    var svg = lane.closest(".ibpl-ribbon");
    if (svg) setFocus(svg, lane);
  });

  document.addEventListener("mouseout", function(e) {
    var lane = laneFrom(e.target);
    if (!lane) return;
    var svg = lane.closest(".ibpl-ribbon");
    // Ignore moves between the <g> and its own <rect>/<title> children.
    if (svg && !laneFrom(e.relatedTarget)) setFocus(svg, null);
  });

  // Keyboard parity: lanes are focusable, so focus drives the same split.
  document.addEventListener("focusin", function(e) {
    var lane = laneFrom(e.target);
    if (!lane) return;
    var svg = lane.closest(".ibpl-ribbon");
    if (svg) setFocus(svg, lane);
  });

  // Touch has no hover: tap toggles the same state.
  document.addEventListener("click", function(e) {
    var lane = laneFrom(e.target);
    if (!lane) return;
    var svg = lane.closest(".ibpl-ribbon");
    if (!svg) return;
    setFocus(svg, lane.classList.contains("is-active") ? null : lane);
  });
})();
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: PASS, no failures.

- [ ] **Step 6: Commit**

```bash
git add app/www/app.css app/www/app.js app/tests/testthat/test-stint-ribbon.R
git commit -m "feat: ribbon styling and clip-path hover handler"
```

---

### Task 7: Wire the ribbon into Tab 4 (Israeli game logs)

**Files:**
- Modify: `app/R/server_tab4.R` (the Summary `DT::datatable` block around line 640-690, and a new observer near the other `observeEvent` calls around line 300)
- Modify: `app/www/app.js` (add `window.handleRibbonLinkClick`, next to `window.handleLineupLinkClick` around line 104)
- Test: `app/tests/testthat/test-stint-ribbon.R` (append)

**Interfaces:**
- Consumes: `fetch_stint_ribbon()` from Task 5, `build_stint_ribbon_svg()` from Task 4, the CSS/JS contract from Task 6.
- Produces: `input$gl_ribbon_click` carrying `list(game_id, team_id, ts)`; `ribbon_link_cell(game_id, team_id, label)` in `helpers.R`.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
test_that("ribbon_link_cell carries the ids and team names the modal needs", {
  html <- ribbon_link_cell(115L, 7L, "12 Mar", own_team = "Hapoel TA",
                           opp_team = "Maccabi")
  expect_match(html, 'data-game-id="115"')
  expect_match(html, 'data-team-id="7"')
  expect_match(html, 'data-own-team="Hapoel TA"')
  expect_match(html, 'data-opp-team="Maccabi"')
  expect_match(html, "12 Mar", fixed = TRUE)
  expect_match(html, "ribbon-link", fixed = TRUE)
})

test_that("ribbon_link_cell escapes its label", {
  html <- ribbon_link_cell(1L, 1L, "<script>alert(1)</script>")
  expect_false(grepl("<script>", html, fixed = TRUE))
})

test_that("add_ribbon_link_column builds links from the source frame", {
  df <- data.frame(game_id = c(115L, 116L), team_id = c(7L, 10L),
                   game_date = c("12 Mar", "14 Mar"), stringsAsFactors = FALSE)
  out <- add_ribbon_link_column(df)
  expect_match(out$game_date[1], 'data-game-id="115"')
  expect_match(out$game_date[2], 'data-team-id="10"')
  expect_match(out$game_date[1], "12 Mar", fixed = TRUE)
})

test_that("add_ribbon_link_column fails loudly on a frame missing the ids", {
  # disp drops game_id/team_id. Called on the wrong frame this must error, not
  # quietly return an unlinked column -- that failure mode would leave the
  # feature dead while every other test passed.
  disp <- data.frame(gn = 1L, game_date = "12 Mar", stringsAsFactors = FALSE)
  expect_error(add_ribbon_link_column(disp), "game_id")
})

test_that("both Tab 4 view modes build the link before select() drops the ids", {
  src <- readLines(testthat::test_path("..", "..", "R", "server_tab4.R"), warn = FALSE)
  add_lines <- grep("add_ribbon_link_column", src)
  sel_lines <- grep("disp <- df %>% select", src)
  expect_length(sel_lines, 2)          # Summary and Four Factors
  expect_length(add_lines, 2)
  # Each select must be preceded by an add_ribbon_link_column call.
  for (sl in sel_lines) expect_true(any(add_lines < sl & add_lines > sl - 12))
})

test_that("app.js exposes the ribbon click handler", {
  js <- paste(readLines(testthat::test_path("..", "..", "www", "app.js"),
                        warn = FALSE), collapse = "\n")
  expect_match(js, "handleRibbonLinkClick", fixed = TRUE)
  expect_match(js, "gl_ribbon_click", fixed = TRUE)
})
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: FAIL, `could not find function "ribbon_link_cell"`.

- [ ] **Step 3: Add the link cell helper**

Append to `app/R/helpers.R`:

```r
# A game-log cell that opens the stint ribbon. Both ids travel on the anchor so
# the click handler needs no table lookup.
ribbon_link_cell <- function(game_id, team_id, label, input_id = "gl_ribbon_click",
                             own_team = "", opp_team = "") {
  sprintf(
    paste0('<a href="#" class="ribbon-link" data-game-id="%d" data-team-id="%d" ',
           'data-input-id="%s" data-own-team="%s" data-opp-team="%s" ',
           'onclick="window.handleRibbonLinkClick(this); return false;">%s</a>'),
    as.integer(game_id), as.integer(team_id),
    htmltools::htmlEscape(input_id),
    htmltools::htmlEscape(own_team), htmltools::htmlEscape(opp_team),
    htmltools::htmlEscape(label)
  )
}
```

- [ ] **Step 4: Add the JS handler**

In `app/www/app.js`, immediately after the `window.handleLineupLinkClick` definition (around line 112), add:

```js
  window.handleRibbonLinkClick = function(linkEl) {
    if (!linkEl) return;
    var gameId = parseInt(linkEl.dataset.gameId, 10);
    var teamId = parseInt(linkEl.dataset.teamId, 10);
    if (Number.isNaN(gameId) || Number.isNaN(teamId)) return;
    // Routed through the queue-and-replay helper, not Shiny.setInputValue: a
    // click landing before shiny:connected must be queued and replayed, not
    // dropped. See the dead-window fix in 0189b4e.
    window.ibplSendShinyEvent(linkEl.dataset.inputId || "gl_ribbon_click", {
      game_id: gameId,
      team_id: teamId,
      own_team: linkEl.dataset.ownTeam || "",
      opp_team: linkEl.dataset.oppTeam || "",
      ts: Date.now()
    });
  };
```

- [ ] **Step 5: Render the link in BOTH Tab 4 view modes**

**This is the step that decides whether the feature works at all.** Both
renderers build `disp` with an explicit `select()` that does **not** carry
`game_id` or `team_id` (`server_tab4.R:554` for Summary, `:699` for Four
Factors). Any guard of the form
`all(c("game_id", "team_id") %in% names(disp))` is therefore always FALSE, the
link never renders, and every unit test on the helper still passes while the
feature is dead. Build the link from `df`, which does carry the identifiers,
*before* `select()` drops them.

Four call sites need identical treatment (Tab 4 ×2 modes, Tab 11 ×2), so it is
a helper, not four copies. Add to `app/R/helpers.R`:

```r
# Replace the date cell with a ribbon link, keyed on the row identifiers that
# `select()` is about to drop. Call this on the SOURCE frame (df), never on the
# display frame (disp) -- disp has no game_id/team_id.
add_ribbon_link_column <- function(df, input_id = "gl_ribbon_click",
                                   date_col = "game_date") {
  if (is.null(df) || !nrow(df)) return(df)
  needed <- c("game_id", "team_id", date_col)
  if (!all(needed %in% names(df))) {
    stop("add_ribbon_link_column() needs ", paste(needed, collapse = ", "),
         "; got: ", paste(names(df), collapse = ", "))
  }
  # Team names ride along on the anchor so the modal can title itself without a
  # second query. They are already present on df.
  own <- if ("team_name" %in% names(df)) as.character(df$team_name) else ""
  opp <- if ("opp_team_name" %in% names(df)) as.character(df$opp_team_name) else ""

  df[[date_col]] <- mapply(
    ribbon_link_cell,
    df$game_id, df$team_id, as.character(df[[date_col]]),
    own_team = own, opp_team = opp,
    MoreArgs = list(input_id = input_id), USE.NAMES = FALSE
  )
  df
}
```

It fails loudly rather than silently doing nothing — the failure mode that made
this correction necessary.

Then in `server_tab4.R`, immediately **before** each `disp <- df %>% select(...)`
(both the Summary branch at ~line 554 and the Four Factors branch at ~line 699):

```r
      df <- add_ribbon_link_column(df)
```

and set each of those two `DT::datatable(...)` calls to
`escape = dt_escape_except(disp, "game_date")`.

- [ ] **Step 6: Add the click observer and modal**

In `app/R/server_tab4.R`, alongside the other observers (after the `observeEvent(input$gl_view_mode, ...)` block around line 327), add:

```r
  observeEvent(input$gl_ribbon_click, {
    click <- input$gl_ribbon_click
    req(click$game_id, click$team_id)

    ribbon <- fetch_stint_ribbon(
      pg_pool, "israel", click$game_id, click$team_id,
      data_version = shared_data_version(shared)
    )

    if (is.null(ribbon) || !nrow(ribbon$lanes)) {
      showModal(modalDialog(title = "No lineup data",
                            "This game has no segment data to draw.",
                            easyClose = TRUE))
      return()
    }

    meta <- ribbon$meta
    meta$game_label <- sprintf("Game %s", click$game_id)
    meta$own_team <- if (nzchar(click$own_team %||% "")) click$own_team else "Own"
    meta$opp_team <- if (nzchar(click$opp_team %||% "")) click$opp_team else "Opponent"
    if (nzchar(click$own_team %||% "")) {
      meta$game_label <- sprintf("%s vs %s", click$own_team, click$opp_team)
    }

    output$gl_ribbon_svg <- renderUI({
      tagList(
        if (!is.null(ribbon$health)) {
          div(class = "alert alert-warning py-2 px-3 mb-2", ribbon$health)
        },
        build_stint_ribbon_svg(ribbon$lanes, ribbon$margin, meta,
                               id_prefix = paste0("gl", click$game_id))
      )
    })

    showModal(modalDialog(
      title = meta$game_label,
      uiOutput("gl_ribbon_svg"),
      size = "xl",
      easyClose = TRUE
    ))
  })
```

- [ ] **Step 7: Run the tests**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: PASS, no failures.

- [ ] **Step 8: Verify in the running app**

```bash
IBPL_CACHE_UI=false "$RSCRIPT" -e "shiny::runApp('app')"
```

Launch with `runApp()`, never select-all + Ctrl+Enter. In the browser: open **Game Logs**, click a date cell, confirm the modal shows lanes above and below a margin curve, and that hovering a lane dims the other lanes while the amber curve appears only over that player's stints. Check the console is free of errors.

- [ ] **Step 9: Commit**

```bash
git add app/R/helpers.R app/R/server_tab4.R app/www/app.js app/tests/testthat/test-stint-ribbon.R
git commit -m "feat: open the stint ribbon from Israeli game logs"
```

---

### Task 8: Wire the ribbon into Tab 11 (EuroLeague game logs)

**Files:**
- Modify: `app/R/server_tab11_euro_gamelogs.R`
- Modify: `app/www/app.js` (generalise the handler's input id)
- Test: `app/tests/testthat/test-stint-ribbon.R` (append)

**Interfaces:**
- Consumes: everything from Tasks 1-7.
- Produces: `input$euro_gl_ribbon_click`, same payload shape as `gl_ribbon_click`.

**Note:** per `CLAUDE.md`, do not write a parallel `euro_` implementation. The only difference is the input id, the league argument, and the tab file — every helper is reused unchanged.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-stint-ribbon.R`:

```r
test_that("one link cell serves both leagues via its input_id argument", {
  # The league lives in the argument, not in the function name -- per CLAUDE.md,
  # no parallel euro_ implementation of logic that already exists.
  israeli <- ribbon_link_cell(1L, 2L, "x")
  euro <- ribbon_link_cell(1L, 2L, "x", input_id = "euro_gl_ribbon_click")
  expect_match(israeli, 'data-input-id="gl_ribbon_click"', fixed = TRUE)
  expect_match(euro, 'data-input-id="euro_gl_ribbon_click"', fixed = TRUE)
})

test_that("both EuroLeague view modes build the ribbon link", {
  src <- readLines(testthat::test_path("..", "..", "R", "server_tab11_euro_gamelogs.R"),
                   warn = FALSE)
  expect_gte(length(grep("add_ribbon_link_column", src)), 2)
  expect_true(any(grepl("euro_gl_ribbon_click", src, fixed = TRUE)))
})

test_that("both game-log tabs wire a ribbon click observer", {
  il <- paste(readLines(testthat::test_path("..", "..", "R", "server_tab4.R"),
                        warn = FALSE), collapse = "\n")
  eu <- paste(readLines(testthat::test_path("..", "..", "R", "server_tab11_euro_gamelogs.R"),
                        warn = FALSE), collapse = "\n")
  expect_match(il, "fetch_stint_ribbon", fixed = TRUE)
  expect_match(eu, "fetch_stint_ribbon", fixed = TRUE)
  # The EuroLeague tab must call the shared builder, not a euro_ clone.
  expect_match(eu, "build_stint_ribbon_svg", fixed = TRUE)
  expect_false(grepl("euro_build_stint_ribbon", eu, fixed = TRUE))
})
```

- [ ] **Step 2: Run to verify it fails**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter='stint-ribbon')"
```

Expected: FAIL, `unused argument (input_id = ...)`.

- [ ] **Step 3: Route the JS handler by input id**

`ribbon_link_cell()` already takes `input_id` (Task 7 defined it that way), and
already emits `data-input-id`. The handler must honour it rather than hardcoding
the Israeli input. In `app/www/app.js`, confirm the ribbon handler reads it:

```js
    window.ibplSendShinyEvent(linkEl.dataset.inputId || "gl_ribbon_click", {
```

If Task 7 left a hardcoded `"gl_ribbon_click"` there, change it to the line
above. No change to `helpers.R` is needed in this task.

- [ ] **Step 4: Wire Tab 11**

In `app/R/server_tab11_euro_gamelogs.R`, apply the same helper immediately
before **each** `select()` that builds a display frame — both view modes, same
reason as Tab 4 (the display frame has no `game_id`/`team_id`):

```r
      df <- add_ribbon_link_column(df, input_id = "euro_gl_ribbon_click")
```

Set each of those `DT::datatable(...)` calls to
`escape = dt_escape_except(disp, "game_date")`, then add the observer:

```r
  observeEvent(input$euro_gl_ribbon_click, {
    click <- input$euro_gl_ribbon_click
    req(click$game_id, click$team_id)

    ribbon <- fetch_stint_ribbon(
      pg_pool, "euroleague", click$game_id, click$team_id,
      data_version = shared_data_version(shared)
    )

    if (is.null(ribbon) || !nrow(ribbon$lanes)) {
      showModal(modalDialog(title = "No lineup data",
                            "This game has no segment data to draw.",
                            easyClose = TRUE))
      return()
    }

    meta <- ribbon$meta
    meta$game_label <- sprintf("Game %s", click$game_id)
    meta$own_team <- if (nzchar(click$own_team %||% "")) click$own_team else "Own"
    meta$opp_team <- if (nzchar(click$opp_team %||% "")) click$opp_team else "Opponent"
    if (nzchar(click$own_team %||% "")) {
      meta$game_label <- sprintf("%s vs %s", click$own_team, click$opp_team)
    }

    output$euro_gl_ribbon_svg <- renderUI({
      tagList(
        if (!is.null(ribbon$health)) {
          div(class = "alert alert-warning py-2 px-3 mb-2", ribbon$health)
        },
        build_stint_ribbon_svg(ribbon$lanes, ribbon$margin, meta,
                               id_prefix = paste0("eugl", click$game_id))
      )
    })

    showModal(modalDialog(
      title = meta$game_label,
      uiOutput("euro_gl_ribbon_svg"),
      size = "xl",
      easyClose = TRUE
    ))
  })
```

- [ ] **Step 5: Confirm Tab 4 still passes**

`server_tab4.R` relies on the `input_id` default, so nothing there changes.
Re-run the focused suite to confirm Task 7's tests still pass alongside Task 8's.

- [ ] **Step 6: Run the full suite**

```bash
cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', reporter = 'summary')"
```

Expected: no new failures versus the pre-change baseline.

**Do not use `git stash` to get that baseline.** This repository routinely holds
unrelated work in progress (96 modified/untracked entries at the time of
writing), and a stash/pop cycle around a test run risks it. Record the baseline
*before* starting Task 1 and save it to the SDD workspace, or run it from a
separate worktree:

```bash
"$RSCRIPT" -e "testthat::test_dir('tests/testthat', reporter='summary')"   > ../.superpowers/sdd/2026-09-05-stint-ribbon/test-baseline.txt 2>&1
```

- [ ] **Step 7: Verify both tabs in the running app**

```bash
IBPL_CACHE_UI=false "$RSCRIPT" -e "shiny::runApp('app')"
```

Open **Game Logs** and the **EuroLeague** game-log tab, click a date in each, and confirm both ribbons render and hover behaves identically. Navbar health check: the served page carries 11 `class="nav-link"` occurrences.

- [ ] **Step 8: Commit**

```bash
git add app/R/helpers.R app/R/server_tab11_euro_gamelogs.R app/www/app.js app/tests/testthat/test-stint-ribbon.R
git commit -m "feat: open the stint ribbon from EuroLeague game logs"
```

---

## Verification Checklist

Before considering the feature done:

- [ ] `cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', reporter='summary')"` shows no new failures against the Task 0 baseline (`.superpowers/sdd/2026-09-05-stint-ribbon/test-baseline.txt`).
- [ ] Both readers return `sides: opp/own` and a median under 500 ms (Task 5, Step 6).
- [ ] `CONFIRM_DB_SECURITY_APPLY=1 "$RSCRIPT" scripts/apply_db_security.R` has been run after the views were created, and the audit query returns zero rows.
- [ ] Neither game-log query contains a `type_lineup` predicate.
- [ ] An overtime game draws a wider axis than a regulation game (pick one from the EuroLeague set with max elapsed 2700).
- [ ] A game with incomplete Israeli lineup data shows the health warning rather than silently wrong lanes.
- [ ] The date cell in **all four** game-log renderers (Tab 4 Summary and Four Factors, Tab 11 both modes) renders an actual `ribbon-link` anchor — check the served table, not just the helper's unit test.
- [ ] The margin curve starts at 0-0 and runs to the nominal game end, with no diagonal segments.
- [ ] Every lane shows a visible player name; both teams and the period boundaries are labelled; the zero baseline is drawn.
- [ ] Hovering a player with two separated stints highlights both bars.
- [ ] The app was launched with `runApp()`, and the served page carries 11 `class="nav-link"` occurrences.
