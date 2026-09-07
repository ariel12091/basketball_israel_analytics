# Stint Ribbon Lineup Decomposition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Click a player's stint bar and it splits in place into the five-man lineups that composed it, with those fives listed below the chart and markable on the lane and the margin curve.

**Architecture:** The decomposition already exists in the reader's output and is thrown away by `merge_adjacent_stints()` — the reader returns one row per `(segment, player)` and a segment has a constant lineup by construction. This plan carries a lineup key on those rows, keeps the pre-merge frame beside the merged bars, and emits it as one compact attribute per bar for the JS to draw on click. It also deletes the teammate-overlap machinery, whose question the decomposition now answers.

**Tech Stack:** R 4.4.2, Shiny, `htmltools` inline SVG, plain ES5 JavaScript (no framework, no build step), PostgreSQL on Supabase, testthat.

**Spec:** `docs/superpowers/specs/2026-09-07-stint-ribbon-lineup-decomposition-design.md`

## Global Constraints

- **Branch:** `shiny/stint-ribbon`, base `9c9808b`. Merge-base with `main` is `c2ec291`.
- **Baseline test state:** full suite 2210 pass / 1 fail / 10 skip. The one failure is `test-companion-query-counts.R:293` (`object 'minutes' not found`), pre-existing at the merge base. **Any second failure is yours.** Run the *whole* suite before claiming a task is clean — a previous plan on this branch had ten task reviews miss a regression because each ran only two ribbon test files.
- **Mixed line endings.** `app/R/helpers.R`, `app/R/global.R` and `app/R/server_tab4.R` contain CRLF. Their CR byte counts are exactly **helpers.R 2670, global.R 1070, server_tab4.R 719** and must not change. Verify with `tr -cd '\r' < FILE | wc -c` before and after every edit to those files. Neither the Edit tool nor `sed -i` preserves them reliably — if a count drifts, restore and edit on bytes.
- **No `euro_`-prefixed clones.** Every helper here is league-blind and lives in `app/R/helpers.R`; the league enters as an argument. This is enforced by an existing test.
- **Base apply family only** (`lapply`/`vapply`/`Filter`). No purrr, despite purrr being loaded.
- **Parameterized SQL only** (`$1`, `$2`). The app connects as `app_readonly`.
- **Every numeric that reaches emitted SVG must be finite.** This branch's history includes a `NULL → NaN` that made an invalid path `d` and blanked the entire curve.
- **Every new test states what makes it non-vacuous, and is proved by mutation** — break the behaviour, watch the test fail, restore. The predecessor plan produced *four* tests that passed against a stub, including one in the task written to prevent exactly that.
- **L5 (mark all windows of a five) is provisional.** The user chose it unsure and will decide after seeing it live. Keep the scope in **one predicate** so narrowing it is a one-line change.

---

## File Structure

| File | Responsibility | Change |
|---|---|---|
| `app/R/global.R` | `RIBBON_SQL_ISRAEL`, `RIBBON_SQL_EURO`, `fetch_stint_ribbon()` | Carry `lineup_key`; score-availability flag |
| `app/R/helpers.R` | All league-blind ribbon logic | Add dictionary + segment helpers + side perspective; delete overlap helpers; emit segment data |
| `app/R/mod_ribbon_modal.R` | **New.** The shared game-log→ribbon modal observer | Extracted from two tabs |
| `app/R/server_tab4.R` | Israeli game logs | Call the shared observer |
| `app/R/server_tab11_euro_gamelogs.R` | EuroLeague game logs | Call the shared observer |
| `app/www/app.js` | Ribbon interaction | Click-to-split, strip list, row hover marking; delete `setOverlaps` |
| `app/www/app.css` | Ribbon styling | Segment divisions, strip list rows, echo treatment; delete `.is-overlap` |
| `app/tests/testthat/test-stint-ribbon.R` | Pure-function + built-SVG tests | Extend |
| `app/tests/testthat/test-stint-ribbon-readers.R` | Live-DB reconciliation | Extend |

Tasks 1–3 are independent of the redesign and independently mergeable. They come first so the worst live bug does not wait behind a UI change.

---

### Task 1: Do not offer the ribbon for a game with no score data

Spec §8. Israeli 2026 games 139, 140, 141 and 143 have `own_team_score`/`opp_team_score` NULL on **every** row for **both** teams. The margin frame empties, `ribbon_complete_margin()` returns its `data.frame(elapsed = c(0, 2400), margin = c(0, 0))` fallback, and the chart confidently asserts the game was level throughout with blank numbers and no health message. The fix is to not make the date cell a link at all.

**Files:**
- Modify: `app/R/helpers.R` (`add_ribbon_link_column()`, ~line 3585)
- Test: `app/tests/testthat/test-stint-ribbon.R`, `app/tests/testthat/test-stint-ribbon-readers.R`

**Interfaces:**
- Consumes: nothing.
- Produces: `add_ribbon_link_column(df, input_id, date_col, has_scores_col = "has_scores")` — when the named column is present, rows where it is `FALSE` keep their plain date text and get no `<a>`.

- [ ] **Step 1: Measure the gate's cost before choosing how to compute it**

The spec leaves this open deliberately. The game logs read `mv_lineup_totals_by_day` + `final_schedule_mv`; the NULL lives in `df_pts_poss_lineups_longer_mv`. Write a scratch script (temp `.R` file, **not** `Rscript -e` — that segfaults on long input) that `EXPLAIN (ANALYZE, BUFFERS)`s the game-log query with and without a correlated `EXISTS`:

```sql
EXISTS (SELECT 1 FROM basketball_test.df_pts_poss_lineups_longer_mv d
         WHERE d.game_id = x.game_id AND d.team_id = x.team_id
           AND d.own_team_score IS NOT NULL) AS has_scores
```

Compare **BUFFERS pages, not wall-clock** — timings on this pooler swing 40% run to run. Record both numbers in your report. If the `EXISTS` adds more than ~20% of shared-hit pages, stop and report; a precomputed flag is then the answer and needs its own task. Do **not** add a second round trip to the game-log load either way.

- [ ] **Step 2: Write the failing test**

```r
test_that("a game with no score data gets no ribbon link", {
  df <- data.frame(
    game_id = c(139L, 200L), team_id = c(6L, 6L),
    game_date = c("2026-01-10", "2026-01-17"),
    team_name = c("A", "A"), opp_team_name = c("B", "C"),
    has_scores = c(FALSE, TRUE), stringsAsFactors = FALSE
  )
  out <- add_ribbon_link_column(df)
  expect_false(grepl("<a", out$game_date[1], fixed = TRUE))
  expect_identical(out$game_date[1], "2026-01-10")
  expect_true(grepl('class="ribbon-link"', out$game_date[2], fixed = TRUE))
})

test_that("the link column is unchanged when no has_scores column is supplied", {
  df <- data.frame(game_id = 200L, team_id = 6L, game_date = "2026-01-17",
                   team_name = "A", opp_team_name = "C", stringsAsFactors = FALSE)
  out <- add_ribbon_link_column(df)
  expect_true(grepl('class="ribbon-link"', out$game_date[1], fixed = TRUE))
})
```

Non-vacuous because: the first asserts an exact string equal to the raw date, so a row that silently kept its `<a>` fails on `expect_false` *and* on `expect_identical`. The second pins that absence of the column is not treated as "gate everything off" — the failure mode that would silently kill every link in both tabs.

- [ ] **Step 3: Run it and watch it fail**

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
"$RSCRIPT" -e "testthat::test_file('app/tests/testthat/test-stint-ribbon.R')"
```
Expected: FAIL — the first test's row 1 still contains `<a`.

- [ ] **Step 4: Implement**

In `add_ribbon_link_column()`, after the existing `needed` check:

```r
  gate <- if (!is.null(has_scores_col) && has_scores_col %in% names(df)) {
    !isFALSE(df[[has_scores_col]]) & !is.na(df[[has_scores_col]])
  } else {
    rep(TRUE, nrow(df))
  }
```

then build the links as now but write them back only where `gate` holds:

```r
  linked <- mapply(
    ribbon_link_cell,
    df$game_id, df$team_id, as.character(df[[date_col]]),
    own_team = own, opp_team = opp,
    MoreArgs = list(input_id = input_id), USE.NAMES = FALSE
  )
  df[[date_col]] <- ifelse(gate, linked, as.character(df[[date_col]]))
```

Note `!isFALSE(...)`: the gate must fail **open**, so an unexpected value leaves the link in place rather than silently removing every ribbon in the app.

- [ ] **Step 5: Add the live-DB guard, dynamic not hardcoded**

In `test-stint-ribbon-readers.R`, following the existing `RUN_DB_TESTS` idiom copied byte-for-byte from lines 120-124:

```r
test_that("games with no score data are identified dynamically", {
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")
  con <- ribbon_test_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)

  bad <- DBI::dbGetQuery(con, "
    SELECT game_id, team_id
    FROM basketball_test.df_pts_poss_lineups_longer_mv
    GROUP BY game_id, team_id
    HAVING COUNT(own_team_score) = 0")

  # Not an equality against a fixed list: these four are what the condition
  # catches today, not the definition of it. The ETL can produce more.
  expect_gt(nrow(bad), 0)
  expect_true(all(c(139L, 140L, 141L, 143L) %in% as.integer(bad$game_id)))
})
```

Non-vacuous because `expect_gt(nrow(bad), 0)` fails if the query returns nothing, which is the way this test would otherwise pass having checked nothing.

- [ ] **Step 6: Wire the flag into both game-log queries**

Using whichever mechanism Step 1's measurement chose. Both tabs, same shape — the column is named `has_scores` in both.

- [ ] **Step 7: Run the full suite**

```bash
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat')"
```
Expected: 1 failure only (`test-companion-query-counts.R:293`). Then with the DB: `RUN_DB_TESTS=1` on the two ribbon files — expect **0 skips**; a skip here means the guard did not run.

- [ ] **Step 8: Verify line endings and commit**

```bash
tr -cd '\r' < app/R/helpers.R | wc -c   # must print 2670
git add app/R/helpers.R app/R/global.R app/R/server_tab4.R app/R/server_tab11_euro_gamelogs.R app/tests/testthat/test-stint-ribbon.R app/tests/testthat/test-stint-ribbon-readers.R
git commit -m "shiny: withhold the ribbon link for games with no score data"
```

---

### Task 2: Extract the duplicated ribbon modal observer

Final-review issue 4. `server_tab4.R:331-376` and `server_tab11_euro_gamelogs.R:32-77` are 46 lines each and differ in exactly five values. This is the one remaining league clone and the exact shape `CLAUDE.md` forbids.

**Files:**
- Create: `app/R/mod_ribbon_modal.R`
- Modify: `app/R/app.R` (source the new file), `app/R/server_tab4.R:331-376`, `app/R/server_tab11_euro_gamelogs.R:32-77`
- Test: `app/tests/testthat/test-stint-ribbon.R`

**Interfaces:**
- Consumes: `fetch_stint_ribbon()`, `build_stint_ribbon_svg()`, `ribbon_detail_strip()`.
- Produces: `ribbon_modal_server(input, output, session, prefix, league, data_version_fn)` where `prefix` is `"gl"` or `"eugl"`, `league` is `"israel"` or `"euroleague"`, and `data_version_fn` is a zero-arg function returning the cache-busting version.

- [ ] **Step 1: Verify the two blocks really differ only in those five values**

```bash
sed -n '331,376p' app/R/server_tab4.R > /tmp/a.txt
sed -n '32,77p' app/R/server_tab11_euro_gamelogs.R > /tmp/b.txt
sed -e 's/eurogl_/gl_/g; s/eugl/gl/g; s/"euroleague"/"israel"/g; s/euro_data_version()/shared_data_version(shared)/g' /tmp/b.txt | diff - /tmp/a.txt
```
Expected: empty, or differences only in the five parameters. **If anything else differs, stop and report it** — an unnoticed behavioural difference between the two is a finding, not an obstacle, and merging them would silently pick one league's behaviour for both.

- [ ] **Step 2: Move the block byte-identically**

Create `app/R/mod_ribbon_modal.R` containing the Israeli block verbatim, with only those five values replaced by the parameters. **This is a move, not a rewrite** — do not reformat, rename locals, or "improve" anything while moving. Verify by reversing the transform:

```bash
sed -e 's/prefix/gl/g' app/R/mod_ribbon_modal.R | diff - /tmp/a.txt
```

- [ ] **Step 3: Replace both call sites**

`server_tab4.R`: `ribbon_modal_server(input, output, session, "gl", "israel", function() shared_data_version(shared))`
`server_tab11_euro_gamelogs.R`: `ribbon_modal_server(input, output, session, "eugl", "euroleague", euro_data_version)`

Add `source("R/mod_ribbon_modal.R", local = TRUE)` to `app.R` alongside the other module sources.

- [ ] **Step 4: Add a test that catches a re-clone of the wiring**

The existing test only asserts no `euro_build_stint_ribbon` exists — it does not catch a copy-paste of the observer.

```r
test_that("neither game-log tab defines its own ribbon modal observer", {
  for (f in c("app/R/server_tab4.R", "app/R/server_tab11_euro_gamelogs.R")) {
    src <- paste(readLines(f, warn = FALSE), collapse = "\n")
    expect_true(grepl("ribbon_modal_server(", src, fixed = TRUE), info = f)
    expect_false(grepl("showModal(", src, fixed = TRUE), info = f)
  }
})
```

Non-vacuous because reverting either call site to the inline block restores `showModal(` in that file and fails the second assertion; the first fails if the call is dropped entirely.

- [ ] **Step 5: Full suite, then commit**

```bash
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat')"
tr -cd '\r' < app/R/server_tab4.R | wc -c   # must print 719
git commit -am "shiny: extract the shared ribbon modal observer"
```

---

### Task 3: Rate-limit the ribbon open

Final-review issue 5. Every other heavy path guards (`server_tab1.R:174`, `tab2.R:188`, `tab3.R:484`, `tab5.R:857`, `tab8.R:178`, `tab9.R:207`, `tab10.R:245`, `mod_team_hub.R:709`). The ribbon does not, and its click target is a whole column of date cells on a single-worker deployment.

**Files:**
- Modify: `app/R/mod_ribbon_modal.R` (created in Task 2)
- Test: `app/tests/testthat/test-stint-ribbon.R`

- [ ] **Step 1: Read one existing call site and copy its convention**

```bash
sed -n '170,180p' app/R/server_tab1.R
```
Use that exact idiom. Do not invent a variant.

- [ ] **Step 2: Add the guard as the first statement inside the observer**, before `fetch_stint_ribbon()`.

- [ ] **Step 3: Test that the guard is present**

```r
test_that("the ribbon modal observer is rate limited", {
  src <- paste(readLines("app/R/mod_ribbon_modal.R", warn = FALSE), collapse = "\n")
  expect_true(grepl("guard_heavy_request(", src, fixed = TRUE))
  expect_lt(regexpr("guard_heavy_request(", src, fixed = TRUE)[1],
            regexpr("fetch_stint_ribbon(", src, fixed = TRUE)[1])
})
```

Non-vacuous because the ordering assertion fails if the guard is added *after* the fetch, which would defeat its purpose. Beware: `regexpr` returns FIRST matches — confirm neither token appears in a comment above the code, or the ordering compare is meaningless. This exact trap bit the predecessor plan twice.

- [ ] **Step 4: Full suite, commit**

```bash
git commit -am "shiny: rate-limit the ribbon open"
```

---

### Task 4: Carry a lineup key on every lane row

Spec §5. Israeli already has `s.lineup_hash` in `segs` and drops it in the `unnest`. EuroLeague has no hash: `euroleague.ribbon_segments_v` carries `player_ids`, which is already sorted and verified 5/5 with zero fan-out — **that array is the identity, so use its text form. Do not mint a second identifier.**

**Files:**
- Modify: `app/R/global.R` (`RIBBON_SQL_ISRAEL` `lanes` CTE ~line 285, `RIBBON_SQL_EURO` `lanes` CTE), `app/R/helpers.R` (`ribbon_normalise_lanes()`)
- Test: both ribbon test files

**Interfaces:**
- Produces: `lanes$lineup_key`, a non-empty character column on every row returned by `fetch_stint_ribbon()$lanes`.

- [ ] **Step 1: Write the failing tests**

```r
test_that("normalised lanes carry a lineup key", {
  raw <- data.frame(team_id = c(6L, 6L), player_id = c(1L, 2L),
                    player_label = c("A", "B"),
                    start_elapsed = c(0, 0), end_elapsed = c(60, 60),
                    lineup_key = c("h1", "h1"), stringsAsFactors = FALSE)
  out <- ribbon_normalise_lanes(raw, 6L)
  expect_identical(out$lineup_key, c("h1", "h1"))
})

test_that("both readers select a lineup key into the lanes CTE", {
  expect_match(RIBBON_SQL_ISRAEL, "s.lineup_hash AS lineup_key", fixed = TRUE)
  expect_match(RIBBON_SQL_EURO, "s.player_ids::text AS lineup_key", fixed = TRUE)
})
```

- [ ] **Step 2: Run, watch both fail.**

- [ ] **Step 3: Implement**

Israeli `lanes` CTE — add to the select list:
```sql
         s.lineup_hash AS lineup_key,
```
EuroLeague `lanes` CTE — add:
```sql
         s.player_ids::text AS lineup_key,
```
`ribbon_normalise_lanes()` — add to the `data.frame(...)`:
```r
    lineup_key = as.character(raw$lineup_key),
```
and to the zero-row early return:
```r
                      lineup_key = character(0),
```

- [ ] **Step 4: Live-DB guard — the key must actually partition into fives**

```r
test_that("a lineup key identifies exactly five players, both leagues", {
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")
  con <- ribbon_test_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)

  for (lg in c("israel", "euroleague")) {
    games <- ribbon_sample_games(con, lg, n = 3)
    expect_gt(nrow(games), 0)                       # non-vacuous guard
    for (g in seq_len(nrow(games))) {
      fx <- fetch_stint_ribbon(con, lg, games$game_id[g], games$team_id[g])
      expect_gt(nrow(fx$lanes), 0)                  # non-vacuous guard
      expect_false(any(is.na(fx$lanes$lineup_key)))
      per <- tapply(fx$lanes$player_key,
                    paste(fx$lanes$side, fx$lanes$lineup_key, fx$lanes$start_elapsed),
                    function(x) length(unique(x)))
      expect_true(all(per == 5))
    }
  }
})
```

**Both `expect_gt` guards are mandatory.** Without them an empty `games` or empty `lanes` makes the loop body never run and the test passes having verified nothing — this exact defect shipped four times in the predecessor plan.

- [ ] **Step 5: Full suite + `RUN_DB_TESTS=1`, expecting 0 skips. Commit.**

```bash
tr -cd '\r' < app/R/global.R | wc -c   # must print 1070
git commit -am "shiny: carry a lineup key on every ribbon lane row"
```

---

### Task 5: Build the per-game lineup dictionary

Spec §5 — names are stored once per lineup, not repeated on every segment row.

**Files:**
- Modify: `app/R/helpers.R` (new function beside `ribbon_normalise_lanes()`)
- Test: `app/tests/testthat/test-stint-ribbon.R`

**Interfaces:**
- Consumes: `lanes` with `side`, `lineup_key`, `player_key`, `player_label` (Task 4).
- Produces: `ribbon_lineup_dictionary(lanes)` → data.frame with `side`, `lineup_key`, `members` (the five labels joined by `" · "`, sorted).

- [ ] **Step 1: Write the failing test**

```r
test_that("the lineup dictionary lists each five once, sorted", {
  lanes <- data.frame(
    side = rep("own", 6),
    lineup_key = c("h1","h1","h1","h1","h1","h2"),
    player_key = c("3","1","2","5","4","9"),
    player_label = c("Cohen","Ash","Bar","Eyal","Dan","Zed"),
    stringsAsFactors = FALSE)
  d <- ribbon_lineup_dictionary(lanes)
  expect_identical(nrow(d), 2L)
  expect_identical(d$members[d$lineup_key == "h1"],
                   "Ash · Bar · Cohen · Dan · Eyal")
})

test_that("the dictionary keys on player_key, never on label", {
  # Both leagues carry same-name/different-id players on one team. A label
  # key would collapse these two people into one and yield four members.
  lanes <- data.frame(
    side = rep("own", 5), lineup_key = rep("h1", 5),
    player_key = c("1","2","3","4","5"),
    player_label = c("NEW NEW","NEW NEW","Bar","Dan","Eyal"),
    stringsAsFactors = FALSE)
  d <- ribbon_lineup_dictionary(lanes)
  expect_identical(lengths(strsplit(d$members, " · ", fixed = TRUE))[[1]], 5L)
})

test_that("an empty frame yields a typed empty dictionary", {
  d <- ribbon_lineup_dictionary(data.frame(
    side = character(0), lineup_key = character(0),
    player_key = character(0), player_label = character(0)))
  expect_identical(nrow(d), 0L)
  expect_true(is.data.frame(d))
  expect_identical(names(d), c("side", "lineup_key", "members"))
})
```

The second test is the load-bearing one: de-duplicating on `player_label` instead of `player_key` passes the first test and fails this one.

- [ ] **Step 2: Run, watch all three fail.**

- [ ] **Step 3: Implement**

```r
# One row per (side, lineup_key) with the five member labels. Keyed on
# player_key, NEVER on player_label: both leagues carry same-name /
# different-id players on one team, and a label key would collapse two
# people into one and report a four-man five.
ribbon_lineup_dictionary <- function(lanes) {
  empty <- data.frame(side = character(0), lineup_key = character(0),
                      members = character(0), stringsAsFactors = FALSE)
  if (is.null(lanes) || !nrow(lanes)) return(empty)

  grp <- paste(lanes$side, lanes$lineup_key, sep = "\r")
  keep <- !duplicated(paste(grp, lanes$player_key, sep = "\r"))
  u <- lanes[keep, , drop = FALSE]

  rows <- lapply(split(seq_len(nrow(u)), paste(u$side, u$lineup_key, sep = "\r")),
    function(i) data.frame(
      side = u$side[i[1]], lineup_key = u$lineup_key[i[1]],
      members = paste(sort(u$player_label[i]), collapse = " · "),
      stringsAsFactors = FALSE))

  out <- do.call(rbind, rows)
  rownames(out) <- NULL
  out
}
```

- [ ] **Step 4: Run the two ribbon files, then the full suite. Commit.**

```bash
git commit -am "shiny: build the per-game ribbon lineup dictionary"
```

---

### Task 6: Decompose a merged bar into its lineup segments

Spec §5. The pre-merge rows for one player inside one bar are contiguous and non-overlapping by construction; this returns them in time order, clipped to the bar's window.

**Files:**
- Modify: `app/R/helpers.R`
- Test: `app/tests/testthat/test-stint-ribbon.R`, `app/tests/testthat/test-stint-ribbon-readers.R`

**Interfaces:**
- Consumes: pre-merge `lanes` (Task 4), `ribbon_stint_points()`.
- Produces: `ribbon_stint_segments(lanes, side, player_key, start_elapsed, end_elapsed)` → the rows of `lanes` overlapping that window, clipped, ordered by `start_elapsed`.

- [ ] **Step 1: Write the failing tests, including the invariant**

```r
test_that("a bar decomposes into its segments in time order", {
  lanes <- data.frame(
    side = rep("own", 4), player_key = rep("1", 4),
    player_label = rep("Ash", 4),
    lineup_key = c("h1","h2","h3","h1"),
    start_elapsed = c(0, 60, 120, 600),
    end_elapsed   = c(60, 120, 180, 660), stringsAsFactors = FALSE)
  seg <- ribbon_stint_segments(lanes, "own", "1", 0, 180)
  expect_identical(seg$lineup_key, c("h1", "h2", "h3"))
  expect_identical(seg$start_elapsed, c(0, 60, 120))
})

test_that("segment plus-minus sums to the bar's plus-minus", {
  lanes <- data.frame(
    side = rep("own", 3), player_key = rep("1", 3), player_label = rep("Ash", 3),
    lineup_key = c("h1","h2","h3"),
    start_elapsed = c(0, 60, 120), end_elapsed = c(60, 120, 180),
    stringsAsFactors = FALSE)
  steps <- data.frame(elapsed = c(0, 30, 90, 150, 180),
                      order_key = 1:5,
                      margin = c(0, 4, 1, 6, 2))
  bar <- ribbon_stint_points(
    data.frame(start_elapsed = 0, end_elapsed = 180), steps)
  seg <- ribbon_stint_points(ribbon_stint_segments(lanes, "own", "1", 0, 180), steps)
  expect_equal(sum(seg$pm), bar$pm)
  expect_gt(nrow(seg), 1)      # non-vacuous: a single segment would be trivial
})

test_that("a window matching no segment returns a typed empty frame", {
  lanes <- data.frame(
    side = "own", player_key = "1", player_label = "Ash", lineup_key = "h1",
    start_elapsed = 0, end_elapsed = 60, stringsAsFactors = FALSE)
  seg <- ribbon_stint_segments(lanes, "own", "9", 0, 60)
  expect_identical(nrow(seg), 0L)
  expect_true(is.data.frame(seg))
})
```

The invariant test is the point of this task. Its `expect_gt(nrow(seg), 1)` stops it passing on a degenerate single-segment decomposition where the sum is trivially the whole.

- [ ] **Step 2: Run, watch them fail.**

- [ ] **Step 3: Implement**

```r
# The segments of one merged bar. merge_adjacent_stints() built the bar by
# collapsing contiguous per-(segment, player) rows; this is the same set,
# un-collapsed. Clipping is defensive -- within one bar the rows abut
# exactly -- but it is what makes sum(segment pm) == bar pm hold by
# construction rather than by luck, since each pm is a NET difference and
# abutting windows telescope.
ribbon_stint_segments <- function(lanes, side, player_key,
                                  start_elapsed, end_elapsed) {
  if (is.null(lanes) || !nrow(lanes)) return(lanes)
  sel <- lanes$side == side & lanes$player_key == player_key &
    lanes$end_elapsed > start_elapsed & lanes$start_elapsed < end_elapsed
  out <- lanes[sel, , drop = FALSE]
  if (!nrow(out)) return(out)
  out$start_elapsed <- pmax(out$start_elapsed, start_elapsed)
  out$end_elapsed <- pmin(out$end_elapsed, end_elapsed)
  out <- out[out$end_elapsed > out$start_elapsed, , drop = FALSE]
  out <- out[order(out$start_elapsed), , drop = FALSE]
  rownames(out) <- NULL
  out
}
```

- [ ] **Step 4: Live-DB reconciliation, both leagues**

```r
test_that("segments reconcile with their bar on live data, both leagues", {
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")
  con <- ribbon_test_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)

  checked <- 0L
  for (lg in c("israel", "euroleague")) {
    games <- ribbon_sample_games(con, lg, n = 3)
    expect_gt(nrow(games), 0)
    for (g in seq_len(nrow(games))) {
      fx <- fetch_stint_ribbon(con, lg, games$game_id[g], games$team_id[g])
      expect_gt(nrow(fx$lanes), 0)
      bars <- ribbon_stint_points(merge_adjacent_stints(fx$lanes), fx$steps)
      for (i in seq_len(nrow(bars))) {
        seg <- ribbon_stint_points(
          ribbon_stint_segments(fx$lanes, bars$side[i], bars$player_key[i],
                                bars$start_elapsed[i], bars$end_elapsed[i]),
          fx$steps)
        expect_equal(sum(seg$pm), bars$pm[i], tolerance = 1e-9)
        checked <- checked + 1L
      }
    }
  }
  expect_gt(checked, 100L)   # non-vacuous: proves bars were actually compared
})
```

- [ ] **Step 5: Mutation proof — mandatory**

Force `fx$lanes` empty in the DB test and confirm the `expect_gt` guards fail rather than the test passing green. Then change `pmax` to `pmin` in the implementation and confirm the invariant test fails. Restore with `git checkout` and report both outputs.

- [ ] **Step 6: Full suite + `RUN_DB_TESTS=1` (0 skips). Commit.**

```bash
git commit -am "shiny: decompose a stint bar into its lineup segments"
```

---

### Task 7: Each side's numbers in its own perspective

Spec L7, resolving final-review issue 1. `ribbon_stint_points()` has no `side` awareness: `pm` comes from the clicked team's margin and `pf` from its running score, so an opponent bar reads the clicked team's result. An opponent five that outscored you 12-4 must read **+8, 12 for, 4 against**.

**Files:**
- Modify: `app/R/helpers.R` (`build_stint_ribbon_svg()` ~line 3350)
- Test: `app/tests/testthat/test-stint-ribbon.R`, `app/tests/testthat/test-stint-ribbon-readers.R`

**Interfaces:**
- Produces: `ribbon_side_perspective(stints)` — negates `pm` and swaps `pf`/`pa` on `side == "opp"` rows only.

- [ ] **Step 1: Write the failing test**

```r
test_that("opponent rows carry their own perspective", {
  s <- data.frame(side = c("own", "opp"), pm = c(-8, -8),
                  pf = c(4, 4), pa = c(12, 12))
  out <- ribbon_side_perspective(s)
  expect_identical(out$pm, c(-8, 8))
  expect_identical(out$pf, c(4, 12))
  expect_identical(out$pa, c(12, 4))
})

test_that("NA survives the flip as NA, not as a sign-flipped zero", {
  s <- data.frame(side = "opp", pm = NA_real_, pf = NA_real_, pa = NA_real_)
  out <- ribbon_side_perspective(s)
  expect_true(is.na(out$pm) && is.na(out$pf) && is.na(out$pa))
})
```

The second matters because four games reach production with no score series at all (Task 1) and `-NA` must stay `NA`, never become a printed number.

- [ ] **Step 2: Run, watch both fail.**

- [ ] **Step 3: Implement, and call it**

```r
# A lineup's plus-minus is its own. An opponent bar reads +8 when that
# opponent five won those minutes by 8, matching what the same five looks
# up to in Tab 2. This makes the "bar +/- equals the curve's rise"
# property true of the OWN block only, by design -- the curve is signed to
# the clicked team and the opponent block is drawn mirrored beneath it.
ribbon_side_perspective <- function(stints) {
  if (is.null(stints) || !nrow(stints)) return(stints)
  opp <- !is.na(stints$side) & stints$side == "opp"
  if (!any(opp)) return(stints)
  stints$pm[opp] <- -stints$pm[opp]
  pf <- stints$pf[opp]
  stints$pf[opp] <- stints$pa[opp]
  stints$pa[opp] <- pf
  stints
}
```

In `build_stint_ribbon_svg()`, immediately after the existing `lanes <- ribbon_stint_points(lanes, steps)`:

```r
  lanes <- ribbon_side_perspective(lanes)
```

- [ ] **Step 4: Narrow the existing reconciliation test**

In `test-stint-ribbon-readers.R`, the test *"each bar's +/- equals the margin curve's rise across that bar"* is now true of the own block only. Add `side == "own"` to its row selection and **say so in a comment** referencing L7 — a future reader must not read the narrowing as a weakening. Then add its opposite:

```r
  # The same property, mirrored: an opponent bar's +/- is the NEGATIVE of
  # the clicked team's curve rise across it.
  expect_equal(sum(opp_bars$pm), -(curve_rise_over(opp_bars)), tolerance = 1e-9)
```

- [ ] **Step 5: Mutation proof.** Remove the `ribbon_side_perspective()` call and confirm both the unit test and the new opponent reconciliation fail. Restore.

- [ ] **Step 6: Full suite + DB run. Commit.**

```bash
git commit -am "shiny: give each side's ribbon numbers its own perspective"
```

---

### Task 8: Emit segment data per bar; delete the overlap machinery

Spec L8, L9. The teammate-overlap computation is replaced, not adapted — its question is answered by the decomposition. It is also the dominant cost in the current 250-350 ms render (final-review issue 3), because `ribbon_stint_overlaps()` builds a data.frame per bar and `ribbon_score_as_of()` re-sorts the whole step series on each of four calls per bar.

**Files:**
- Modify: `app/R/helpers.R` (`build_stint_ribbon_svg()`; delete `ribbon_stint_overlaps()`, `ribbon_overlap_label()`), `app/www/app.css`
- Test: `app/tests/testthat/test-stint-ribbon.R`

**Interfaces:**
- Produces: each lane `<g>` carries `data-segments` = `"start,end,pm,dictIndex"` joined by `";"`; the `<svg>` root carries `data-lineups` = a JSON array of member strings indexed by `dictIndex`.

- [ ] **Step 1: Write the failing tests**

```r
test_that("each bar carries its segment decomposition", {
  svg <- as.character(build_stint_ribbon_svg(fixture_lanes(), fixture_margin(),
                                             fixture_meta(), steps = fixture_steps()))
  expect_match(svg, 'data-segments="[0-9]', perl = TRUE)
  expect_match(svg, 'data-lineups="\\[', perl = TRUE)
  expect_false(grepl("data-with=", svg, fixed = TRUE))
})

test_that("the overlap helpers are gone, not merely unused", {
  expect_false(exists("ribbon_stint_overlaps"))
  expect_false(exists("ribbon_overlap_label"))
})
```

- [ ] **Step 2: Run, watch them fail.**

- [ ] **Step 3: Sort the step series once, before the bar loop**

This is the performance fix that pays for the new work. `ribbon_score_as_of()` currently calls `order()` on its own input every time. Hoist it: sort `steps` once in `build_stint_ribbon_svg()` before any per-bar call, and add a comment saying the series arrives pre-sorted so `ribbon_score_as_of()`'s own `order()` is idempotent rather than removing that safety.

- [ ] **Step 4: Replace the per-bar overlap block**

Inside `lane_rects`, delete the `ov`/`with_txt` lines and build instead:

```r
    seg <- ribbon_side_perspective(
      ribbon_stint_points(
        ribbon_stint_segments(premerge, lanes$side[i], lanes$player_key[i],
                              lanes$start_elapsed[i], lanes$end_elapsed[i]),
        steps))
    seg_txt <- if (!nrow(seg)) "" else paste(sprintf(
      "%.0f,%.0f,%s,%d", seg$start_elapsed, seg$end_elapsed,
      ribbon_pm_label(seg$pm), match(paste(seg$side, seg$lineup_key, sep = "\r"),
                                     dict_key) - 1L), collapse = ";")
```

`premerge` is the un-merged `lanes` captured before `merge_adjacent_stints()`; `dict_key` is `paste(dict$side, dict$lineup_key, sep = "\r")` from `ribbon_lineup_dictionary()`. Replace `data-with = with_txt` with `data-segments = seg_txt`, and put `data-lineups = jsonlite::toJSON(dict$members)` on the `<svg>` root.

The accessible name (`label`) replaces its `on with ...` clause with the decomposition: `", made up of N lineups"` plus the members of each, since the strip is `aria-hidden` and this stays the only screen-reader path.

- [ ] **Step 5: Delete `ribbon_stint_overlaps()`, `ribbon_overlap_label()`, and `.is-overlap` from `app.css`.**

- [ ] **Step 6: Confirm the render actually got faster, with n >= 5**

Do not claim a speedup from one run — a previous session asserted and then retracted two conclusions taken from single A/B runs, and a full `C:` drive silently produced 2x timing swings for a whole session. Check disk space first, time 5 repetitions of `build_stint_ribbon_svg()` on a real 98-bar game before and after, and report both distributions.

- [ ] **Step 7: Full suite. Commit.**

```bash
tr -cd '\r' < app/R/helpers.R | wc -c   # must print 2670
git commit -am "shiny: emit per-bar lineup segments and retire the overlap machinery"
```

---

### Task 9: Click a stint to split the bar and list its fives

Spec L2, L3, §4.

**Files:**
- Modify: `app/www/app.js` (the ribbon IIFE, ~lines 440-565), `app/www/app.css`
- Test: `app/tests/testthat/test-stint-ribbon.R` (source-level assertions on the JS, the convention this file already uses)

**Run the app with `IBPL_CACHE_UI=false` while editing `www/app.js` or `www/app.css`** — they are read by `includeScript()`/`includeCSS()` at build time, so otherwise an edit needs an app restart, not a browser reload. **Launch with Run App / `runApp()`, never select-all + Ctrl+Enter** — the latter builds a BS3-style navbar with no `nav-link`, the hover menus never build, and that broken build is cached for the life of the process. Health check: the served page contains 11 `nav-link` occurrences.

- [ ] **Step 1: Implement `setSelection(svg, lane)`**

Clears any previous selection first (the stale-state discipline `setOverlaps` already followed), then for a stint lane: parses `data-segments`, draws one divider line per internal boundary inside the bar's rect, writes a per-segment `+/-` where `ribbon_number_fits`-equivalent width allows, and renders the strip list — one row per segment carrying window, members (from `data-lineups[dictIndex]`), and `+/-`. Cap at 12 rows with a `+N more` row; the measured tail is 31.

- [ ] **Step 2: Bind click, and clear on `Esc` and on click outside**

Keep the existing `(hover: none)` tap-to-focus branch working — it is a separate concern from selection. Exactly one stint selected at a time.

- [ ] **Step 3: Keyboard parity (L10).** `Enter`/`Space` on a focused stint does what click does; strip rows are focusable.

- [ ] **Step 4: Assert the wiring at source level**

```r
test_that("the ribbon JS selects on click and clears on Escape", {
  js <- paste(readLines("app/www/app.js", warn = FALSE), collapse = "\n")
  expect_true(grepl("setSelection", js, fixed = TRUE))
  expect_true(grepl("data-segments", js, fixed = TRUE) ||
              grepl("dataset.segments", js, fixed = TRUE))
  expect_true(grepl("Escape", js, fixed = TRUE))
  expect_false(grepl("setOverlaps", js, fixed = TRUE))
})
```

- [ ] **Step 5: Commit.** `git commit -am "shiny: split a stint into its lineups on click"`

---

### Task 10: Hover a strip row to mark the five on the lane and the curve

Spec L4, L5. **L5 is provisional** — build the broad behaviour behind ONE predicate.

**Files:**
- Modify: `app/www/app.js`, `app/www/app.css`

- [ ] **Step 1: Implement the scope predicate, isolated**

```js
  // L5 is PROVISIONAL: the user chose "all windows that five played" while
  // unsure, and will decide once they have seen it live. Narrowing to the
  // clicked stint alone must stay a one-line change -- return false here.
  // Do not inline this test anywhere else.
  function marksOtherWindows() { return true; }
```

- [ ] **Step 2: On row hover, mark the clicked player's lane only** (L4 — *not* the other four members' lanes), and emphasise the same window on the margin curve using the existing `clip-path` mechanism the lane hover already uses.

- [ ] **Step 3: Echo treatment.** When `marksOtherWindows()`, other windows of the same `lineup_key` in that lane are marked at a visibly weaker strength; the clicked stint's own occurrence stays at full strength, so the original is unmistakable.

- [ ] **Step 4: Compute the specificity of the new CSS rules before assuming they win.** The predecessor plan found `.is-active` and `.is-overlap` tied at (0,4,1), decided only by source order. State the computed specificity of each new rule against `.ibpl-ribbon.is-focused .ibpl-ribbon-lane rect` (0,3,1) and `.is-active` (0,4,1) in your report. Verify contrast against the threshold, not against what it replaced.

- [ ] **Step 5: Do NOT put a raw hex anywhere in `app.css` — including in a comment.** The design-token guard greps the whole file outside `:root`; a comment documenting measured contrast ratios tripped it once already and the guard now strips comments, but the rule stands: use `var(--ibpl-*)`.

- [ ] **Step 6: Full suite. Commit.** `git commit -am "shiny: mark a lineup on the lane and the curve from the strip"`

---

### Task 11: Whole-feature verification

- [ ] **Step 1: Full suite.** Expect 2210+ pass, exactly 1 failure (`test-companion-query-counts.R:293`).
- [ ] **Step 2: `RUN_DB_TESTS=1`** on both ribbon files. Expect **0 skips** — a skip means a reconciliation silently did not run.
- [ ] **Step 3: Line endings.** helpers.R 2670, global.R 1070, server_tab4.R 719.
- [ ] **Step 4: Browser pass** — the step no subagent can do; hand this list to the user:
  - Israeli game 139, 140, 141 or 143: the date cell must be **plain text, not a link** (Task 1).
  - Click an own-team stint: bar splits, strip lists its fives, segment numbers sum to the bar's number.
  - Click an **opponent** stint: same behaviour, and its `+/-` reads from the opponent's perspective (Task 7).
  - Hover a strip row: the clicked player's lane marks, the curve marks, other windows of that five show as weaker echoes with the original unmistakable.
  - `Esc` clears; clicking another stint replaces rather than accumulates.
  - Tab to a stint and press Enter: same as click.
  - Reopen the same game: confirm whether the second open is faster (only the query is cached, not the render).
  - Both leagues — Tab 4 and Tab 11.

---

## Self-Review

**Spec coverage.** L1 unchanged-at-rest (no task needed — Task 8 preserves the resting emission); L2 Task 9; L3 Task 9; L4 Task 10; L5 Task 10 (behind one predicate); L6 preserved by Task 8 keeping the bar's own `data-pm`; L7 Task 7; L8 Task 8; L9 Task 8; L10 Task 9 Step 3. §5 data contract Tasks 4-6. §8 review findings Tasks 1, 2, 3, 7. §9 assumptions carried (gutter and lane order untouched; `+N more` cap in Task 9 Step 1; single round trip enforced in Task 1 Step 1).

**Known gap, deliberate.** Task 1 Step 1 may find the `EXISTS` too expensive, in which case a precomputed flag needs its own task. That branch is called out rather than guessed at because the cost is unmeasured and this project's rule is to measure rather than estimate.

**Type consistency.** `lineup_key` (character) is introduced in Task 4 and consumed in 5, 6, 8, 10 under that name. `ribbon_lineup_dictionary()` returns `side`/`lineup_key`/`members` and Task 8 indexes it by `paste(side, lineup_key, sep = "\r")`, the same composite Task 5 groups on. `ribbon_stint_segments()` returns rows of `lanes`, so it carries `lineup_key` into `ribbon_stint_points()` unchanged. `ribbon_side_perspective()` is applied to both bars (Task 7) and segments (Task 8).
