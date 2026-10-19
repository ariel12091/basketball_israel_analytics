# On/Off Player-Stat Filter Chips Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the Israeli On/Off Impact tab's `+ Filter` chip narrow rows by eleven traditional Player Stats (GP, MIN/G, PTS/G, REB/G, AST/G, STL/G, BLK/G, 3P%, eFG%, TS%, USG%) computed over the same selected games, composed with AND alongside the existing on/off chips.

**Architecture:** Player Stats chips are ordinary stat-filter chips whose internal column starts with `ps_`. When at least one is active, Tab 1 fetches a Player Stats frame through readers extracted from Tab 5 (the default-season MV via the shared season cache, or `get_player_traditional_from_games()` for a filtered context), projects it to `ps_*` per-game columns with Tab 5's own formulas, joins it onto the already-ranked On/Off population by `game_year + team_id + player_id`, and filters there, upstream of the renderers. The renderers receive only the remaining on/off chips, so the grouped-header column contract and percentile ranks are untouched.

**Tech Stack:** R 4.4.2, Shiny (bslib/BS5), DBI/RPostgres, DT, testthat (`shiny::testServer` with `tests/testthat/helper-server-mocks.R`).

**Spec:** `docs/superpowers/specs/2026-09-15-onoff-player-stat-filters-design.md`

## Global Constraints

- Scope: Israeli-league Shiny On/Off Impact tab (Tab 1) only. EuroLeague parity is a later rollout step.
- Player-stat columns are used only for filtering; they are never added to the visible On/Off table.
- Operators remain `>=` and `<=`; multiple chips compose with AND; two chips on one field form a range.
- Percentage inputs use the displayed 0-100 scale; per-game inputs use normal displayed values.
- Rows with `NA` for an active field are excluded. Division by zero produces `NA`, not zero.
- Join Player Stats onto On/Off data using `game_year + team_id + player_id`. Never join by display name. Never use a multi-team `TOTAL` row.
- Player Stats must use the same definitions as the Player Stats tab. Do not create a second set of basketball formulas in the On/Off server.
- Classification must use the internal column namespace (`ps_`), not label text.
- Zero Player Stats queries when no `ps_*` chip is active.
- Player Stats filtering does not change On/Off percentile ranks, heat colors, or auto-minimum possessions.
- Query failure must never silently produce an unfiltered result.
- Starter-count scope is disclosed and never misrepresented.
- No SQL or materialized-view change.
- R source must stay pure ASCII (`tests/testthat/test-source-encoding.R` fails on any byte > 127). Use `0-100`, not an en dash.
- Parameterized SQL only; no `sprintf()`/`paste0()` of user values.
- Thresholds compare against the value the Player Stats table displays: `round_display_1()` of Tab 5's per-game/percentage double, identical to `DT::formatRound(..., 1)` (JavaScript `toFixed(1)`). See remark 1.
- Shared helpers take the database pool as an argument; none of the helpers added here references `pg_pool`. See remark 4.
- Every test run goes through `<SCRATCH>/run_tests.R`, unpiped, with its exit status checked against the Task 1 baseline. See remark 2.

### Line endings and staging (read before touching any file)

`app/R/helpers.R`, `app/R/server_tab1.R` and `app/R/server_tab5_traditional.R` mix CRLF and LF lines in their committed blobs (2648 / 537 / 1255 CRs at `f58422d`). Two traps, both measured on this machine:

1. The Edit tool rewrites a whole file's line-ending convention. **Never use Edit/Write on those three files.** Change them only with the `edit_tools.R` helpers below (Task 1 Step 1). Test files under `app/tests/testthat/` are LF-only; Edit/Write is fine there.
2. `core.autocrlf=true` is set system-wide. A plain `git add` of `helpers.R` would store **0** CRs (a whole-file diff). Stage those three files with `git -c core.autocrlf=false add <file>`; stage everything else with plain `git add`.

Before every commit run `git diff --cached --stat` and check the counts against the step's expected numbers. The working tree also carries unrelated user changes (`AGENTS.md`, `CLAUDE.md`, `PROJECT.md`, `app/rsconnect/...dcf`, `app/tests/testthat/test-team-hub-ui.R`, many untracked files). **Stage only the files each task names.**

Write every helper R script with the Write tool, never a bash heredoc: Git Bash collapses backslashes inside heredocs. `<SCRATCH>` below means your session scratchpad directory.

Commands assume Git Bash in `app/`:

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
cd /c/Users/ariel/documents/on_off_israel_pbp/app
```

## Spec decisions taken in this plan

Each one is a place where the spec offered a choice or left a detail open:

- **Menu organisation:** native `<optgroup>`s ("Impact and on/off results", "Sample size", "Player Stats") instead of label prefixes. A one-element group still renders as an optgroup when passed as a list (verified with `shiny::selectInput`). The stored chip label is therefore already the short label (`PTS/G >= 10`).
- **Teams are not part of the Player Stats query context.** `get_player_traditional_from_games()` applies the team list as `fs.team_id = ANY(v_team_ids)` on team-game rows (`sql/functions/get_player_traditional_from_games.sql:103`), so a team-player row's values do not depend on it. Leaving teams out lets a team change reuse the fetched frame. Task 6 verifies the equality against real data. This departs from the spec's server-test bullet "Changing ... teams ... invalidates the frame". The test asserts the opposite, and the reason is written into the code comment.
- **Explanatory copy:** a persistent popover note (the spec allows this), plus a small chips-row note whenever a starter-count restriction and a Player Stats chip are both active, so the disclosure stays visible with the popover closed.
- **Empty result:** the DataTable's normal empty state. The renderers are not changed.
- **Caching:** the default season goes through `cached_season_df()` under the same key Tab 5 uses after Task 2. Filtered contexts use a per-session memo of the last context's frame, so threshold edits never re-query.
- **League changes:** Tab 1 is Israeli-only and keeps its own chip state; Tab 8 does not get the Player Stats menu in this version, so no cross-league chip can exist.

## Review remarks: resolution log

All four remarks below were accepted and are folded into the tasks. Each
resolution was verified by measurement; see the "Resolved" line under each remark.

1. **Define whether thresholds use raw or displayed values.**
   `apply_ts_mode(..., "Per Game")` can produce values with more precision than
   the Player Stats table displays, while that table formats numeric columns to
   one decimal. As currently written, a player displayed at `10.1 PTS/G` could
   fail `PTS/G >= 10.1` when the underlying value is `10.06`. Choose one rule and
   apply it consistently in the projection, filter tests, cross-tab validation,
   and manual checks. The preferred user-facing rule is to filter the same
   one-decimal value the user sees. Add boundary tests around the rounding point.

   **Resolved:** filter on the displayed value. Tab 5 displays these columns with
   `DT::formatRound(..., 1)` (`server_tab5_traditional.R`), which is JavaScript
   `toFixed(1)`: it rounds the double's *exact* decimal expansion and takes the
   larger candidate on a tie. Neither R `round(x, 1)` nor `floor(x * 10 + 0.5)`
   matches it. `round(41/4, 1)` is 10.2 where the table shows 10.3, and
   `round(1.95, 1)` is 2.0 where it shows 1.9. Task 3 adds
   `round_display_1()`, which reads the hundredths digit of `sprintf("%.60f", x)`.
   Checked against node `toFixed(1)` on 3,135,580 realistic values (every
   `(j/10)/gp` for j <= 25000 and gp <= 45, every thousandth to 2500, and every
   one-decimal percentage), it had **0 mismatches**; base `round()` had 5,232. It
   costs about 6s per million values, which is under 2ms for a season. The
   projection applies it to every `ps_*` column except `ps_gp`. Boundary tests use
   node-verified fixtures (10.25 -> 10.3, 1.95 -> 1.9, 0.05 -> 0.1,
   100.6/10 -> 10.1, -10.25 -> -10.3). Tab 5's own stat filters still compare raw
   values; that is existing Tab 5 behaviour and out of scope.

2. **Reconcile targeted-test expectations with the recorded baseline.**
   This plan records pre-existing failures in `test-source-encoding.R`,
   `test-companion-query-counts.R`, and `test-idle-restore-bookmarking.R`, but
   several later steps say every listed test must finish with no failures.
   Either omit known baseline failures from those targeted loops or compare
   their results with the captured baseline. Test commands must also preserve
   the `Rscript` exit status (for example, enable `pipefail` or inspect the
   unpiped result) so the reporting `grep` cannot hide a failed test process.

   **Resolved:** every test step now runs `<SCRATCH>/run_tests.R` unpiped and
   checks its exit status. Task 1 Step 1 records a per-test baseline of the
   whole suite (`--record`). Every later run uses `--baseline`, which exits 1 on
   any failing test that did not fail at baseline, or on a test file that cannot
   run, and lists baseline failures separately as ignored. Checked during
   planning: a baseline-only failure exits 0, and a new failure or missing file
   exits 1. Because `test-source-encoding.R` already fails at baseline and so
   cannot guard this work, each task that edits R source also runs a direct
   non-ASCII byte check on the files it changed.

3. **Complete game-context invalidation and parameter-forwarding coverage.**
   Server tests currently exercise dates, home/away, and last N. Add coverage
   for game type, opponent, outcome, opponent-rank side/count/metric, and GN
   minimum/maximum. Tests should assert both memo invalidation and the exact
   arguments forwarded to the traditional-stat reader. The opponent test needs
   a mutable `shared$selected_opp_ids_on` mock; setting only
   `input$on_opponents` does not exercise the production reactive used by
   `build_onoff_db_args()`.

   **Resolved:** the mock DB now records the last parameter list for each read
   (`mock_db_last_params("player_traditional_reader")`). Task 4 replaces the
   ad-hoc invalidation test with one table-driven test covering dates, game type,
   opponents, home/away, outcome, opponent-rank side/count/metric, GN min/max and
   last N. Each case starts from the default context, applies one change, and
   asserts three things: exactly one new reader call (memo invalidated), the
   exact forwarded parameter, and the parameters that must never vary (season,
   no team list, no clutch window). A threshold edit afterwards must add no call.
   The test app swaps `shared$selected_opp_ids_on` for a `reactiveVal` and the
   opponents case sets both it and `input$on_opponents`, mirroring `app.R`, where
   the reactive derives from that input.

   Running this test during planning **found a real defect in the planned Tab 1
   code**. The GN min, GN max and last-N cases each made two reader calls. The
   context took its path decision from `onoff_fallback_needed(..., input, ...)`,
   which also reads the *raw* GN inputs (correct for the On/Off table), while the
   GN arguments come from the 150ms-debounced `gn_params()`. Inside that window
   the context was `fast = FALSE` with no GN filter, a full-season reader query
   that is thrown away. Traced by instrumenting the fetcher on the scratch copy.
   Fix: pass `list()` in place of `input`, so the path is decided from the same
   resolved values the reader gets. With the fix all 33 tests in the file pass;
   without it this test fails, so it stays as the regression guard.

4. **Do not hardcode `pg_pool` inside the shared frame fetcher.**
   Change the proposed interface to
   `fetch_player_stat_filter_frame(pool, ctx, session)` and pass `pg_pool` from
   Tab 1. Forward `pool` to `run_player_traditional_israel()`. This keeps the
   shared helper explicit, reusable, and independently testable.

   **Resolved:** the interface is `fetch_player_stat_filter_frame(pool, ctx, session)`.
   The same reasoning applies one level down, so the season reader also takes
   the pool: `fetch_player_traditional_season_israel(pool, game_year, data_version)`.
   Tab 5 passes `pg_pool` to it too. No shared helper added by this plan
   references `pg_pool`.

## Data verified before planning (2026-09-15, production DB)

- `onoff_default_mv` LEFT JOIN `player_traditional_stats_mv` on `(game_year, team_id, player_id)`: 2025 255/295 matched, 2026 313/362, 2027 96/144. **Every unmatched row has fewer than 100 ON possessions.** `(team_id, player_id)` is unique per season in the MV.
- `player_traditional_stats_mv` columns include `game_year, team_id, player_id, team_name, player_name, gp, poss_on_floor, minutes, pts, reb, ast, stl, blk, tov, fga, fta, tp_pct, efg, ts, usg_pct`.
- `get_player_traditional_from_games(2026, 2025-12-01, 2026-03-01, ...)` returned 207 rows in 0.46s.

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `app/R/helpers.R` | modify | Receives Tab 5's pure Player Stats helpers (moved), the two Israeli Player Stats readers (extracted), the `ps_*` filter constants, and pure classification/projection/join/filter helpers; `stat_filter_chips_ui()` gains optional optgroups |
| `app/R/server_tab5_traditional.R` | modify | Loses the moved helpers; calls the shared readers. Behaviour unchanged |
| `app/R/server_tab1.R` | modify | Player Stats menu entries, lazy frame, upstream filtering, error state, view-switch retention, disclosure |
| `app/tests/testthat/helper-server-mocks.R` | modify | Mock `player_traditional_stats_mv` read, query counters, a failure switch |
| `app/tests/testthat/test-player-traditional-readers.R` | create | Shared reader tests |
| `app/tests/testthat/test-onoff-player-stat-filters.R` | create | Pure helper tests and Tab 1 server tests |

---

### Task 1: Move Tab 5's pure Player Stats helpers into helpers.R

A pure move. Nothing else changes. `normalize_ts_result_cols()` and `ts_player_key()` are top-level in `server_tab5_traditional.R`. `clean_ts_rows()`, `add_ts_usage_pct()` and `apply_ts_mode()` are closures inside `server_tab5_traditional()` that read no `input`/`session`/reactive state; they move de-indented by exactly two spaces.

**Files:**
- Modify: `app/R/server_tab5_traditional.R` (remove 5 functions)
- Modify: `app/R/helpers.R` (insert them above `# ---- Stat-filter column menus for the on/off tabs ----`)

**Interfaces:**
- Consumes: nothing.
- Produces (unchanged signatures, now defined in `helpers.R`): `normalize_ts_result_cols(df)`, `ts_player_key(team_id, player_id)`, `clean_ts_rows(df)`, `add_ts_usage_pct(df)`, `apply_ts_mode(df, mode, x_poss = NA_real_, x_min = NA_real_)`.

- [ ] **Step 1: Create the branch, the edit tools, and a full-suite baseline**

```bash
git checkout -b shiny/onoff-player-stat-filters
```

Write `<SCRATCH>/edit_tools.R` with the Write tool. This exact file was dry-run on copies of both files during planning: the move below produced `137 deletions` / `143 insertions`, and the moved lines were identical under `diff -w`.

```r
# Byte-safe editing for app/R/helpers.R, server_tab1.R, server_tab5_traditional.R.
# Those files mix CRLF and LF line endings; the Edit tool rewrites the whole
# file's convention. These helpers never touch bytes outside the edited span.

# splice(file, old, new): replace exactly one occurrence of `old`.
# Matching ignores carriage returns; inserted text is written as given (LF).
splice <- function(file, old, new) {
  bytes <- readBin(file, "raw", file.size(file))
  keep <- which(bytes != as.raw(13L))
  stripped <- rawToChar(bytes[keep])
  old <- gsub(rawToChar(as.raw(13L)), "", old, fixed = TRUE)
  hits <- gregexpr(old, stripped, fixed = TRUE, useBytes = TRUE)[[1]]
  n <- if (hits[1] == -1L) 0L else length(hits)
  if (n != 1L) stop(sprintf("splice: expected 1 match in %s, found %d", basename(file), n))
  first <- keep[hits[1]]
  last <- keep[hits[1] + attr(hits, "match.length")[1] - 1L]
  out <- c(
    if (first > 1L) bytes[seq_len(first - 1L)],
    charToRaw(enc2utf8(new)),
    if (last < length(bytes)) bytes[(last + 1L):length(bytes)]
  )
  writeBin(out, file)
  invisible(TRUE)
}

# Lines keep their own trailing "\r" (if any); the final newline is tracked.
read_lines_raw <- function(file) {
  txt <- rawToChar(readBin(file, "raw", file.size(file)))
  list(lines = strsplit(txt, "\n", fixed = TRUE)[[1]],
       final_nl = endsWith(txt, "\n"))
}

write_lines_raw <- function(file, x) {
  txt <- paste(x$lines, collapse = "\n")
  if (isTRUE(x$final_nl)) txt <- paste0(txt, "\n")
  writeBin(charToRaw(txt), file)
  invisible(TRUE)
}

strip_cr <- function(s) sub("\r$", "", s)

# Index range of a block: from the one line equal to `start` (CR-stripped) to
# the first following line equal to `end`. Errors unless `start` is unique.
find_block <- function(lines, start, end) {
  s <- which(strip_cr(lines) == start)
  if (length(s) != 1L) stop(sprintf("find_block: start line found %d times: %s", length(s), start))
  e <- which(strip_cr(lines) == end)
  e <- e[e > s]
  if (!length(e)) stop(sprintf("find_block: no end line after: %s", start))
  s:e[1]
}
```

Write `<SCRATCH>/run_tests.R` with the Write tool. Every test step in this plan uses it, never a piped `test_file()` loop, so a failing test process always shows up in the exit status. It was exercised during planning: a baseline-only failure exits 0, and a new failure or a missing test file exits 1.

```r
# Test runner whose exit status cannot be hidden by a pipe.
# Run from app/.
#   Rscript run_tests.R --record   <tsv> ALL|<file>...   record every test's result; exit 0
#   Rscript run_tests.R --baseline <tsv> ALL|<file>...   exit 1 if any test fails that did
#                                                          not fail in <tsv>, or a file crashes
# <file> is a test file name without ".R" (e.g. test-onoff-player-stat-filters).
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 3L || !args[[1]] %in% c("--record", "--baseline")) {
  stop("usage: run_tests.R --record|--baseline <tsv> ALL|<file>...")
}
mode <- args[[1]]
tsv <- args[[2]]
files <- args[-(1:2)]
if (identical(files, "ALL")) {
  files <- sub("[.]R$", "", list.files("tests/testthat", "^test-.*[.]R$"))
}

run_one <- function(f) {
  path <- file.path("tests/testthat", paste0(f, ".R"))
  res <- tryCatch(
    as.data.frame(testthat::test_file(path, reporter = "silent", stop_on_failure = FALSE)),
    error = function(e) e
  )
  if (inherits(res, "error") || !nrow(res)) {
    msg <- if (inherits(res, "error")) conditionMessage(res) else "no tests ran"
    return(data.frame(file = f, test = paste("<file did not run>", gsub("[\t\n\r]", " ", msg)),
                      failed = 1L, error = TRUE, stringsAsFactors = FALSE))
  }
  data.frame(file = f, test = gsub("[\t\n\r]", " ", res$test),
             failed = as.integer(res$failed), error = as.logical(res$error),
             stringsAsFactors = FALSE)
}

results <- do.call(rbind, lapply(files, run_one))
results$bad <- results$failed > 0L | results$error
summary <- aggregate(cbind(tests = 1L, bad = as.integer(bad)) ~ file, data = results, FUN = sum)
print(summary, row.names = FALSE)

if (identical(mode, "--record")) {
  utils::write.table(results, tsv, sep = "\t", row.names = FALSE, quote = FALSE)
  cat(sprintf("recorded %d tests (%d failing) to %s\n", nrow(results), sum(results$bad), tsv))
  quit(status = 0L)
}

baseline <- utils::read.delim(tsv, stringsAsFactors = FALSE, quote = "")
known <- paste(baseline$file, baseline$test)[baseline$bad]
failing <- results[results$bad, , drop = FALSE]
new_fail <- failing[!paste(failing$file, failing$test) %in% known, , drop = FALSE]
old_fail <- failing[paste(failing$file, failing$test) %in% known, , drop = FALSE]
if (nrow(old_fail)) cat("\nfailing at baseline too (ignored):\n", paste0("  ", old_fail$file, ": ", old_fail$test, "\n"), sep = "")
if (nrow(new_fail)) {
  cat("\nNEW FAILURES:\n", paste0("  ", new_fail$file, ": ", new_fail$test, "\n"), sep = "")
  quit(status = 1L)
}
cat("\nno new failures\n")
quit(status = 0L)
```

Record the whole suite's per-test baseline before any change:

```bash
"$RSCRIPT" <SCRATCH>/run_tests.R --record <SCRATCH>/baseline.tsv ALL; echo "exit=$?"
```

Expected: `exit=0`. Measured at `f58422d` during planning: `recorded 550 tests (7 failing)` in 84s. The seven are `test-source-encoding` (native-encoding test), `test-companion-query-counts` (EuroLeague Game Logs `transmute`), `test-idle-restore-bookmarking` (`update_restore_aware_selectize`), and four files that run no tests without their environment: `test-data-parity-db`, `test-data-shape-db`, `test-deployed-app-smoke`, `test-e2e-tabs-shinytest2`. If your numbers differ, keep your own recording; it is the baseline.

Because `test-source-encoding` already fails, it cannot catch non-ASCII bytes introduced by this work. Each task therefore also runs this check on the R files it changed (pass the paths as arguments):

```bash
"$RSCRIPT" -e 'f <- commandArgs(TRUE); bad <- f[vapply(f, function(p) any(as.integer(readBin(p, "raw", file.size(p))) > 127L), logical(1))]; if (length(bad)) { cat("NON-ASCII:", bad, "\n"); quit(status = 1L) }; cat("ascii ok\n")' R/helpers.R R/server_tab1.R R/server_tab5_traditional.R; echo "exit=$?"
```

Expected: `ascii ok` / `exit=0`.

- [ ] **Step 2: Write the move script**

Write `<SCRATCH>/task1_move.R`:

```r
# Task 1: move five pure Player Stats helpers from server_tab5_traditional.R
# into helpers.R. Pure move: closures are de-indented by exactly two spaces.
# Usage (from app/): Rscript task1_move.R <edit_tools.R> <tab5 path> <helpers path>
args <- commandArgs(trailingOnly = TRUE)
source(args[[1]])
tab5_path <- args[[2]]
helpers_path <- args[[3]]

tab5 <- read_lines_raw(tab5_path)
blocks <- list(
  list(start = "normalize_ts_result_cols <- function(df) {", end = "}", closure = FALSE),
  list(start = "ts_player_key <- function(team_id, player_id) {", end = "}", closure = FALSE),
  list(start = "  clean_ts_rows <- function(df) {", end = "  }", closure = TRUE),
  list(start = "  add_ts_usage_pct <- function(df) {", end = "  }", closure = TRUE),
  list(start = "  apply_ts_mode <- function(df, mode, x_poss = NA_real_, x_min = NA_real_) {", end = "  }", closure = TRUE)
)

ranges <- lapply(blocks, function(b) find_block(tab5$lines, b$start, b$end))
moved <- unlist(Map(function(b, r) {
  body <- tab5$lines[r]
  if (b$closure) body <- sub("^  ", "", body)
  c(body, "")
}, blocks, ranges), use.names = FALSE)

# Remove each block plus the single blank line that follows it.
drop <- unlist(lapply(ranges, function(r) {
  nxt <- max(r) + 1L
  if (!nzchar(strip_cr(tab5$lines[nxt]))) c(r, nxt) else r
}))
tab5$lines <- tab5$lines[-drop]
write_lines_raw(tab5_path, tab5)

helpers <- read_lines_raw(helpers_path)
anchor <- which(strip_cr(helpers$lines) == "# ---- Stat-filter column menus for the on/off tabs ----")
if (length(anchor) != 1L) stop("helpers.R anchor not found exactly once")
header <- c(
  "# ---- Player Stats (traditional) shared helpers ----",
  "# Moved verbatim from server_tab5_traditional.R (the three closures were",
  "# de-indented only) so the On/Off tab's Player Stats filter chips reuse the",
  "# Player Stats tab's own normalisation and per-game formulas instead of a",
  "# second copy. None of them reads input or session state.",
  ""
)
helpers$lines <- append(helpers$lines, c(header, moved), after = anchor - 1L)
write_lines_raw(helpers_path, helpers)
cat("moved", length(drop), "lines out of tab5;", length(moved) + length(header), "lines into helpers\n")
```

- [ ] **Step 3: Run the move**

```bash
"$RSCRIPT" <SCRATCH>/task1_move.R <SCRATCH>/edit_tools.R R/server_tab5_traditional.R R/helpers.R
```

Expected: `moved 137 lines out of tab5; 143 lines into helpers`

- [ ] **Step 4: Verify it is a move, by diff against HEAD**

```bash
diff -w \
  <(git diff -U0 HEAD -- R/server_tab5_traditional.R | grep '^-' | grep -v '^---' | sed 's/^-//' | tr -d '\r' | grep -v '^\s*$') \
  <(git diff -U0 HEAD -- R/helpers.R | grep '^+' | grep -v '^+++' | sed 's/^+//' | tr -d '\r' | grep -v '^\s*$' | grep -v '^# ') \
  && echo MOVE-IDENTICAL
echo "tab5 CR $(git show HEAD:app/R/server_tab5_traditional.R | tr -cd '\r' | wc -c) -> $(tr -cd '\r' < R/server_tab5_traditional.R | wc -c)"
echo "helpers CR $(git show HEAD:app/R/helpers.R | tr -cd '\r' | wc -c) -> $(tr -cd '\r' < R/helpers.R | wc -c)"
```

Expected: `MOVE-IDENTICAL`, `tab5 CR 1255 -> 1118`, `helpers CR 2648 -> 2780`. The helpers side gains 132 CRs, not 137, because the five blank separators are re-created as LF lines.

- [ ] **Step 5: Run the affected tests**

```bash
"$RSCRIPT" <SCRATCH>/run_tests.R --baseline <SCRATCH>/baseline.tsv \
  test-tab5-two-point-calcs test-tab5-multi-team-totals test-tab5-percentile-population \
  test-tab5-rate-base-col test-tab5-rate-threshold test-primary-table-render-smoke \
  test-server-tabs-smoke test-tab-parse test-euro-player-stats test-euro-clutch; echo "exit=$?"
```

Expected: `no new failures` / `exit=0`. Then run the ASCII check from Step 1 on `R/helpers.R R/server_tab5_traditional.R`.

- [ ] **Step 6: Commit**

```bash
git -c core.autocrlf=false add R/helpers.R R/server_tab5_traditional.R
git diff --cached --stat
```

Expected stat: `app/R/helpers.R | 143 +` and `app/R/server_tab5_traditional.R | 137 -`, nothing else.

```bash
git commit -m "$(cat <<'EOF'
shiny: move Player Stats normalisation helpers into helpers.R

Pure move so the On/Off tab can reuse Tab 5's own normalisation and per-game
formulas. clean_ts_rows, add_ts_usage_pct and apply_ts_mode were closures that
read no session state; they are de-indented only.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Extract the Israeli Player Stats readers

Tab 5 reads the Israeli season MV inside a two-league `cached_season_df()` call whose key includes the EuroLeague competition value even for Israeli data, and it reads filtered contexts through a session closure. Extract both so Tab 1 shares Tab 5's cache entry and SQL call. Tab 5's behaviour, guard key and SQL text do not change.

**Files:**
- Modify: `app/R/server_tab5_traditional.R`
- Modify: `app/R/helpers.R`
- Modify: `app/tests/testthat/helper-server-mocks.R`
- Create: `app/tests/testthat/test-player-traditional-readers.R`

**Interfaces:**
- Consumes (Task 1): `normalize_ts_result_cols(df)`.
- Produces:
  - `fetch_player_traditional_season_israel(pool, game_year, data_version)` returns the normalised `basketball_test.player_traditional_stats_mv` rows for one season, or `NULL` on a read error. Cached with `cached_season_df(list("player_traditional_stats_mv", as.integer(game_year), data_version), ...)`.
  - `run_player_traditional_israel(pool, session, guard_key, game_year, start_d, end_d, team_ids_csv, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter, min_gn, max_gn, last_n_games)` returns the raw reader frame, or a column-less `data.frame()` when `guard_heavy_request()` refuses.
  - Mock counters: `mock_db_query_count("player_traditional_mv")`, `mock_db_query_count("player_traditional_reader")`. Parameter capture: `mock_db_last_params("player_traditional_reader")` returns the last reader call's `params` list (cleared by `reset_mock_db_query_counts()`). Mock failure switch: `options(ibpl.mock_player_traditional_error = TRUE)` makes both mock reads `stop()`.

- [ ] **Step 1: Write the failing tests**

Create `app/tests/testthat/test-player-traditional-readers.R`:

```r
# The Israeli Player Stats readers, shared by Tab 5 and the On/Off tab's
# Player Stats filter chips (helpers.R).

test_that("the Israeli season reader normalises the Player Stats MV pull", {
  reset_mock_db_query_counts()
  out <- fetch_player_traditional_season_israel(pg_pool, "2026", "v1")
  expect_identical(mock_db_query_count("player_traditional_mv"), 1L)
  expect_true("Player" %in% names(out))
  expect_true(all(c("game_year", "team_id", "player_id", "gp", "pts") %in% names(out)))
})

test_that("the Israeli season reader fails to NULL, which cached_season_df never caches", {
  withr::local_options(ibpl.mock_player_traditional_error = TRUE)
  expect_null(fetch_player_traditional_season_israel(pg_pool, 2026L, "v1"))
})

test_that("the Israeli filtered reader issues one per-game read when no clutch window is set", {
  reset_mock_db_query_counts()
  out <- run_player_traditional_israel(
    pg_pool, session = NULL, guard_key = "test_reader",
    game_year = 2026L, start_d = as.Date("2025-11-01"), end_d = as.Date("2026-02-01"),
    team_ids_csv = NA_character_, game_type_csv = NA_character_, opp_ids_csv = NA_character_,
    home_away = NA_character_, outcome = NA_character_,
    opp_rank_side = NA_character_, opp_rank_n = NA_integer_, opp_rank_metric = NA_character_,
    max_margin = NA_integer_, margin_status = NA_character_,
    max_time_remaining = NA_integer_, ot_margin_filter = FALSE,
    min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_
  )
  expect_identical(mock_db_query_count("player_traditional_reader"), 1L)
  expect_identical(nrow(out), 3L)
})

test_that("Tab 5 reads Israeli Player Stats through the shared readers", {
  txt <- read_repo_txt("R", "server_tab5_traditional.R")
  expect_match(txt, "fetch_player_traditional_season_israel(pg_pool, gy_int, ts_data_version())", fixed = TRUE)
  # Shared helpers take the pool as an argument (review remark 4).
  reader_src <- paste(deparse(fetch_player_traditional_season_israel), collapse = "\n")
  expect_false(grepl("pg_pool", reader_src, fixed = TRUE))
  expect_match(txt, 'guard_key = "tab5_player_traditional"', fixed = TRUE)
  expect_no_match(txt, "run_player_traditional_dynamic", fixed = TRUE)
  # The EuroLeague season pull keeps its competition-qualified key.
  expect_match(txt, 'list("euro_player_traditional_stats_mv", ts_competition(), gy_int, ts_data_version())', fixed = TRUE)
})
```

Also repoint the existing Israeli-reader contract in `app/tests/testthat/test-euro-clutch.R` (LF blob; Edit tool + plain `git add` are fine). It pins the Israeli reader's strings inside `server_tab5_traditional.R`, which this task moves; a planning dry-run of the move failed exactly these three expectations. Replace:

```r
  # Israel now shares the request classifier: non-clutch and the exact cached
  # standard preset use per-game facts; custom clutch retains the action scan.
  expect_match(traditional, '"get_player_traditional_from_games"', fixed = TRUE)
  expect_match(traditional, '"get_player_traditional_custom_clutch"', fixed = TRUE)
  expect_match(traditional, 'pergame = "get_player_traditional_from_games"', fixed = TRUE)
  expect_match(traditional, 'dynamic = "get_player_traditional_from_games"', fixed = TRUE)
```

with:

```r
  # Israel now shares the request classifier: non-clutch and the exact cached
  # standard preset use per-game facts; custom clutch retains the action scan.
  # The Israeli reader lives in helpers.R (run_player_traditional_israel) so
  # the On/Off tab's Player Stats filter chips reuse it.
  helpers <- read_repo_txt("R", "helpers.R")
  expect_match(helpers, 'reader_kind <- clutch_reader_kind(list(', fixed = TRUE)
  expect_match(helpers, '"get_player_traditional_from_games"', fixed = TRUE)
  expect_match(helpers, '"get_player_traditional_custom_clutch"', fixed = TRUE)
  expect_match(helpers, 'pergame = "get_player_traditional_from_games"', fixed = TRUE)
  expect_match(helpers, 'dynamic = "get_player_traditional_from_games"', fixed = TRUE)
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
"$RSCRIPT" <SCRATCH>/run_tests.R --baseline <SCRATCH>/baseline.tsv \
  test-player-traditional-readers test-euro-clutch; echo "exit=$?"
```

Expected: `exit=1`. `NEW FAILURES` lists the four reader tests (`could not find function "fetch_player_traditional_season_israel"` and the text contract) and the `test-euro-clutch` test "Tab 5's EuroLeague reader routes through the shared classifier".

- [ ] **Step 3: Teach the mock DB the MV read, the counters and the failure switch**

In `app/tests/testthat/helper-server-mocks.R` (LF-only, so the Edit tool is fine):

(a) Directly above `db_get_query <- function(pool, query, params = NULL) {`, add:

```r
# Traditional Player Stats rows shared by the MV mock and the reader mock.
mock_player_traditional_df <- function() {
  data.frame(
    player_id = c(11L, 12L, 21L),
    team_id = c(1L, 1L, 2L),
    team_name = c("Team A", "Team A", "Team B"),
    player_name = c("Player A", "Player C", "Player B"),
    gp = c(5L, 4L, 5L),
    pts = c(100, 72, 90),
    reb = c(40, 24, 35),
    oreb = c(12, 7, 10),
    dreb = c(28, 17, 25),
    ast = c(30, 18, 20),
    stl = c(10, 7, 8),
    blk = c(6, 4, 5),
    dfl = c(5, 3, 4),
    tov = c(14, 11, 13),
    fgm = c(38, 28, 34),
    fga = c(70, 55, 66),
    `3pm` = c(11, 8, 9),
    `3pa` = c(29, 22, 26),
    ftm = c(13, 8, 13),
    fta = c(16, 10, 16),
    fg_pct = c(55.0, 51.0, 50.0),
    tp_pct = c(38.0, 36.0, 34.0),
    ft_pct = c(84.0, 80.0, 79.0),
    efg = c(62.8, 58.2, 56.8),
    ts = c(60.0, 58.0, 57.0),
    poss_on_floor = c(300, 220, 280),
    minutes = c(150, 120, 145),
    check.names = FALSE
  )
}

```

(b) In `db_get_query`, replace the opening of the existing reader branch:

```r
  if (grepl("get_player_traditional_dynamic", q, fixed = TRUE) ||
      grepl("get_player_traditional_pergame", q, fixed = TRUE) ||
      grepl("get_player_traditional_from_games", q, fixed = TRUE) ||
      grepl("get_player_traditional_custom_clutch", q, fixed = TRUE)) {
    out <- data.frame(
```

and everything down to and including that `data.frame(`'s closing lines

```r
      minutes = c(150, 120, 145),
      check.names = FALSE
    )
```

with:

```r
  if (grepl("basketball_test.player_traditional_stats_mv", q, fixed = TRUE)) {
    increment_mock_db_query_count("player_traditional_mv")
    assign("player_traditional_mv_pool", pool, envir = .mock_db_last_params)
    if (isTRUE(getOption("ibpl.mock_player_traditional_error"))) stop("mock Player Stats failure")
    out <- mock_player_traditional_df()
    out$game_year <- 2026L
    return(out)
  }

  if (grepl("get_player_traditional_dynamic", q, fixed = TRUE) ||
      grepl("get_player_traditional_pergame", q, fixed = TRUE) ||
      grepl("get_player_traditional_from_games", q, fixed = TRUE) ||
      grepl("get_player_traditional_custom_clutch", q, fixed = TRUE)) {
    increment_mock_db_query_count("player_traditional_reader")
    assign("player_traditional_reader", params, envir = .mock_db_last_params)
    assign("player_traditional_reader_pool", pool, envir = .mock_db_last_params)
    if (isTRUE(getOption("ibpl.mock_player_traditional_error"))) stop("mock Player Stats failure")
    out <- mock_player_traditional_df()
```

(c) Directly below the existing `mock_db_query_count <- function(name) { ... }` definition, add the parameter recorder the forwarding tests read:

```r
# Last parameter list each counted read received, so tests can assert exactly
# what a reader forwarded ($1, $2, ... in order).
.mock_db_last_params <- new.env(parent = emptyenv())

mock_db_last_params <- function(name) {
  if (exists(name, envir = .mock_db_last_params, inherits = FALSE)) {
    get(name, envir = .mock_db_last_params, inherits = FALSE)
  } else {
    NULL
  }
}
```

and make `reset_mock_db_query_counts()` clear it too, by adding this as the last line of its body:

```r
  rm(list = ls(envir = .mock_db_last_params, all.names = TRUE), envir = .mock_db_last_params)
```

`reset_mock_db_query_counts()` is defined above `.mock_db_last_params`, but it only looks the environment up when called, so the order is fine.

The remainder of that branch (team csv / home-away narrowing, `return(out)`) stays as it is.

- [ ] **Step 4: Write the extraction script**

Write `<SCRATCH>/task2_readers.R`:

```r
# Task 2: extract the Israeli Player Stats readers into helpers.R.
# Usage (from app/): Rscript task2_readers.R <edit_tools.R>
source(commandArgs(trailingOnly = TRUE)[[1]])
tab5_path <- "R/server_tab5_traditional.R"
helpers_path <- "R/helpers.R"

# 1. Move the filtered-reader closure out of Tab 5.
tab5 <- read_lines_raw(tab5_path)
r <- find_block(tab5$lines,
  "  run_player_traditional_dynamic <- function(pool, game_year, start_d, end_d,", "  }")
body <- sub("^  ", "", tab5$lines[r])
if (!identical(strip_cr(body[5]), "                                           min_gn, max_gn, last_n_games) {")) {
  stop("unexpected closure signature shape")
}
body <- c(
  "run_player_traditional_israel <- function(pool, session, guard_key, game_year, start_d, end_d,",
  "                                          team_ids_csv, game_type_csv, opp_ids_csv,",
  "                                          home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric,",
  "                                          max_margin, margin_status, max_time_remaining, ot_margin_filter,",
  "                                          min_gn, max_gn, last_n_games) {",
  body[-(1:5)]
)
key_line <- which(strip_cr(body) == '    session, key = "tab5_player_traditional",')
if (length(key_line) != 1L) stop("guard key line not found")
body[key_line] <- sub('key = "tab5_player_traditional"', "key = guard_key", body[key_line], fixed = TRUE)
drop <- if (!nzchar(strip_cr(tab5$lines[max(r) + 1L]))) c(r, max(r) + 1L) else r
tab5$lines <- tab5$lines[-drop]
write_lines_raw(tab5_path, tab5)

# 2. Tab 5's filtered call site.
splice(tab5_path,
r"---(        run_player_traditional_dynamic(
          pg_pool, game_year = gy_int,)---",
r"---(        run_player_traditional_israel(
          pg_pool, session = session, guard_key = "tab5_player_traditional",
          game_year = gy_int,)---")

# 3. Tab 5's season pull: the Israeli branch goes through the shared reader.
splice(tab5_path,
r"---(    # Raw season pull shared across sessions; per-session filters run below.
    out <- cached_season_df(
      list(if (ts_is_euro()) "euro_player_traditional_stats_mv" else "player_traditional_stats_mv",
           ts_competition(), gy_int, ts_data_version()),
      function() {
        raw <- tryCatch(
          if (ts_is_euro()) {
            db_get_query(
              pg_pool,
              "SELECT *
                 FROM euroleague.player_traditional_stats_mv
                WHERE competition = $1::text AND game_year = $2::int4",
              params = list(ts_competition(), gy_int)
            )
          } else {
            db_get_query(
              pg_pool,
              "SELECT *
                 FROM basketball_test.player_traditional_stats_mv
                WHERE game_year = $1",
              params = list(gy_int)
            )
          },
          error = function(e) NULL
        )
        if (is.null(raw)) return(NULL)
        normalize_ts_result_cols(raw)
      }
    ))---",
r"---(    # Raw season pull shared across sessions; per-session filters run below.
    # The Israeli pull is fetch_player_traditional_season_israel() so the On/Off
    # tab's Player Stats filter chips read the same cache entry.
    out <- if (ts_is_euro()) {
      cached_season_df(
        list("euro_player_traditional_stats_mv", ts_competition(), gy_int, ts_data_version()),
        function() {
          raw <- tryCatch(
            db_get_query(
              pg_pool,
              "SELECT *
                 FROM euroleague.player_traditional_stats_mv
                WHERE competition = $1::text AND game_year = $2::int4",
              params = list(ts_competition(), gy_int)
            ),
            error = function(e) NULL
          )
          if (is.null(raw)) return(NULL)
          normalize_ts_result_cols(raw)
        }
      )
    } else {
      fetch_player_traditional_season_israel(pg_pool, gy_int, ts_data_version())
    })---")

# 4. Insert both readers into helpers.R, above the on/off menu section.
readers <- c(
  "# ---- Israeli Player Stats readers (Tab 5 and the On/Off Player Stats chips) ----",
  "# One season of basketball_test.player_traditional_stats_mv, normalised. The",
  "# key carries no league dimension beyond the table name: this is the Israeli",
  "# reader. NULL on a read error, which cached_season_df() never caches. The",
  "# pool is an argument, not part of the key: it changes where, not what.",
  "fetch_player_traditional_season_israel <- function(pool, game_year, data_version) {",
  "  gy <- suppressWarnings(as.integer(game_year))",
  "  cached_season_df(",
  "    list(\"player_traditional_stats_mv\", gy, data_version),",
  "    function() {",
  "      raw <- tryCatch(",
  "        db_get_query(",
  "          pool,",
  "          \"SELECT *",
  "             FROM basketball_test.player_traditional_stats_mv",
  "            WHERE game_year = $1\",",
  "          params = list(gy)",
  "        ),",
  "        error = function(e) NULL",
  "      )",
  "      if (is.null(raw)) return(NULL)",
  "      normalize_ts_result_cols(raw)",
  "    }",
  "  )",
  "}",
  "",
  "# Filtered-context reader, moved from server_tab5_traditional.R. The caller",
  "# names its own rate-limit bucket (guard_key) so tabs do not share a budget.",
  "# A guard refusal returns a column-less data.frame().",
  body,
  ""
)
helpers <- read_lines_raw(helpers_path)
anchor <- which(strip_cr(helpers$lines) == "# ---- Stat-filter column menus for the on/off tabs ----")
if (length(anchor) != 1L) stop("helpers.R anchor not found exactly once")
helpers$lines <- append(helpers$lines, readers, after = anchor - 1L)
write_lines_raw(helpers_path, helpers)
cat("done\n")
```

- [ ] **Step 5: Run it and review the diff**

```bash
"$RSCRIPT" <SCRATCH>/task2_readers.R <SCRATCH>/edit_tools.R
git diff --stat -- R/helpers.R R/server_tab5_traditional.R
git diff --ignore-cr-at-eol -- R/server_tab5_traditional.R
```

Expected: `done`. The Tab 5 diff shows only the removed closure, the renamed call with `session = session, guard_key = "tab5_player_traditional",`, and the restructured season pull. Nothing else.

- [ ] **Step 6: Run the tests**

```bash
"$RSCRIPT" <SCRATCH>/run_tests.R --baseline <SCRATCH>/baseline.tsv \
  test-player-traditional-readers test-primary-table-render-smoke test-server-tabs-smoke \
  test-euro-player-stats test-euro-clutch test-tab-parse test-idle-restore-bookmarking; echo "exit=$?"
```

Expected: `no new failures` / `exit=0` (the `test-idle-restore-bookmarking` baseline failure is listed as ignored). Then run the ASCII check on `R/helpers.R R/server_tab5_traditional.R`. `test-primary-table-render-smoke.R` still renders Tab 5, whose smoke inputs use a narrowed date range and so exercise `run_player_traditional_israel()`.

- [ ] **Step 7: Commit**

```bash
git -c core.autocrlf=false add R/helpers.R R/server_tab5_traditional.R
git add tests/testthat/helper-server-mocks.R tests/testthat/test-player-traditional-readers.R tests/testthat/test-euro-clutch.R
git diff --cached --stat
```

Expected: only those five files; `test-euro-clutch.R` shows about 7 insertions and 4 deletions. `helpers.R` gains about 85 lines, and `server_tab5_traditional.R` shows roughly 60 deletions against about 25 insertions.

```bash
git commit -m "$(cat <<'EOF'
shiny: share the Israeli Player Stats readers

Tab 5's season MV pull and filtered reader move to helpers.R so the On/Off
tab can read the same cache entry and SQL call. Tab 5's guard key and SQL
text are unchanged; the Israeli cache key no longer carries the unrelated
EuroLeague competition value.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Pure Player Stats filter helpers

**Files:**
- Modify: `app/R/helpers.R`
- Create: `app/tests/testthat/test-onoff-player-stat-filters.R`

**Interfaces:**
- Consumes: `clean_ts_rows`, `add_ts_usage_pct`, `apply_ts_mode`, `ts_player_key`, `normalize_ts_result_cols` (Task 1); `fetch_player_traditional_season_israel`, `run_player_traditional_israel` (Task 2); existing `apply_stat_filters`, `normalize_stat_filter_cols`, `ONOFF_SUMMARY_FILTERABLE_COLS`, `ONOFF_FF_FILTERABLE_COLS`.
- Produces:
  - Constants: `PLAYER_STAT_FILTERABLE_COLS` (label -> `ps_*`), `PLAYER_STAT_FILTER_SOURCE` (`ps_*` -> source column), `ONOFF_SAMPLE_FILTER_COLS`, `ONOFF_STARTER_FILTER_FIELDS`, `PLAYER_STAT_FILTER_ERROR_TEXT`, `PLAYER_STAT_STARTERS_CHIP_NOTE`.
  - `is_player_stat_filter(filter) -> logical(1)`; `has_player_stat_filters(filters) -> logical(1)`; `split_stat_filters(filters) -> list(player_stats = list, onoff = list)`; `retain_stat_filters_for_cols(filters, filterable_cols) -> list(kept, dropped)`.
  - `onoff_stat_filter_groups(filterable_cols) -> named list of column vectors`; `stat_filter_choices(cols, groups = NULL)`; `stat_filter_chips_ui(prefix, state, filterable_cols, percent_hint = NULL, choice_groups = NULL)`.
  - `onoff_starter_restriction_active(filters) -> logical(1)`; `onoff_drop_starter_filters(filters) -> filters`; `onoff_player_stats_note(starters_active) -> character(1)`.
  - `player_stat_filter_projection(df, game_year) -> data.frame(game_year, team_id, player_id, ps_gp, ps_min_pg, ps_pts_pg, ps_reb_pg, ps_ast_pg, ps_stl_pg, ps_blk_pg, ps_3p_pct, ps_efg_pct, ps_ts_pct, ps_usg_pct)`.
  - `join_player_stat_filter_frame(df, ps_frame, game_year) -> df` with every `ps_*` column appended.
  - `apply_player_stat_filters(df, ps_frame, filters, game_year) -> list(df, error)`.
  - `round_display_1(x) -> numeric`: one-decimal rounding identical to `DT::formatRound(x, 1)` (JavaScript `toFixed(1)`); `NA`/non-finite stay `NA`.
  - `fetch_player_stat_filter_frame(pool, ctx, session) -> projected frame or NULL`, where `ctx` is `list(game_year, fast, start_d, end_d, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, min_gn, max_gn, last_n_games, data_version)`.

- [ ] **Step 1: Write the failing tests**

Create `app/tests/testthat/test-onoff-player-stat-filters.R`:

```r
# Player Stats filter chips on the On/Off Impact tab.
# Spec: docs/superpowers/specs/2026-09-15-onoff-player-stat-filters-design.md

ps_filter <- function(id, label, col, op = "ge", value = 0) {
  list(id = id, label = label, col = col, op = op, value = value)
}

trad_rows <- function() {
  data.frame(
    player_id = c(11L, 11L, 12L, 13L),
    team_id = c(1L, 2L, 1L, 1L),
    team_name = c("Team A", "Team B", "Team A", "Team A"),
    Player = c("Player A", "Player A", "Player C", "Player Z"),
    gp = c(5L, 2L, 4L, 0L),
    minutes = c(150, 40, 120, 12),
    pts = c(100, 30, 72, 4),
    reb = c(40, 8, 24, 1),
    ast = c(30, 2, 18, 0),
    stl = c(10, 1, 7, 0),
    blk = c(6, 0, 4, 0),
    tov = c(14, 3, 11, 1),
    fga = c(70, 25, 55, 3),
    fta = c(16, 4, 10, 0),
    tp_pct = c(38, 25, 36, NA),
    efg = c(62.8, 50, 58.2, 33.3),
    ts = c(60, 52, 58, 40),
    usg_pct = c(24.1, 18, 20.5, 10),
    poss_on_floor = c(300, 80, 220, 20),
    check.names = FALSE
  )
}

onoff_rows <- function() {
  data.frame(
    Team = c("Team A", "Team B", "Team A"),
    team_id = c(1, 2, 1),
    player_id = c(11, 11, 99),
    `Net RTG Diff` = c(5, -2, 7),
    pr_net = c(0.9, 0.2, 0.95),
    check.names = FALSE
  )
}

# ---- classification -------------------------------------------------------

test_that("Player Stats filters are classified by the ps_ column namespace, not the label", {
  expect_true(is_player_stat_filter(ps_filter(1L, "PTS/G", "ps_pts_pg")))
  expect_false(is_player_stat_filter(ps_filter(2L, "Net", "Net RTG Diff")))
  # A Player Stats-looking label does not make a filter one.
  expect_false(is_player_stat_filter(ps_filter(3L, "TS%", "ts")))
  expect_false(is_player_stat_filter(list(id = 4L, label = "no col")))
  expect_false(has_player_stat_filters(list()))
  expect_true(has_player_stat_filters(list(
    ps_filter(2L, "Net", "Net RTG Diff"), ps_filter(1L, "PTS/G", "ps_pts_pg")
  )))
})

test_that("split_stat_filters separates a mixed list without losing ids or order", {
  filters <- list(
    ps_filter(1L, "Net", "Net RTG Diff"), ps_filter(2L, "PTS/G", "ps_pts_pg"),
    ps_filter(3L, "On Poss", "ON Poss"), ps_filter(4L, "TS%", "ps_ts_pct")
  )
  out <- split_stat_filters(filters)
  expect_identical(vapply(out$player_stats, `[[`, integer(1), "id"), c(2L, 4L))
  expect_identical(vapply(out$onoff, `[[`, integer(1), "id"), c(1L, 3L))
  expect_identical(split_stat_filters(list()), list(player_stats = list(), onoff = list()))
})

test_that("the Player Stats menu adds unique labels to every On/Off view menu", {
  expect_identical(unname(PLAYER_STAT_FILTERABLE_COLS), names(PLAYER_STAT_FILTER_SOURCE))
  expect_true(all(startsWith(unname(PLAYER_STAT_FILTERABLE_COLS), "ps_")))
  for (menu in list(ONOFF_SUMMARY_FILTERABLE_COLS, ONOFF_FF_FILTERABLE_COLS, ON_SP_FILTERABLE_COLS)) {
    combined <- c(menu, PLAYER_STAT_FILTERABLE_COLS)
    expect_identical(anyDuplicated(names(combined)), 0L)
    expect_true(all(ONOFF_SAMPLE_FILTER_COLS %in% unname(menu)))
  }
})

# ---- projection ------------------------------------------------------------

test_that("the projection derives per-game values with the Player Stats tab's formulas", {
  out <- player_stat_filter_projection(trad_rows(), 2026)
  expect_named(out, c("game_year", "team_id", "player_id", names(PLAYER_STAT_FILTER_SOURCE)))
  a1 <- out[out$team_id == 1L & out$player_id == 11L, ]
  expect_identical(a1$game_year, 2026L)
  expect_equal(a1$ps_gp, 5)
  expect_equal(a1$ps_min_pg, 30)
  expect_equal(a1$ps_pts_pg, 20)
  expect_equal(a1$ps_reb_pg, 8)
  expect_equal(a1$ps_ast_pg, 6)
  expect_equal(a1$ps_stl_pg, 2)
  expect_equal(a1$ps_blk_pg, 1.2)
  # Percentages stay on the displayed 0-100 scale.
  expect_equal(a1$ps_3p_pct, 38)
  expect_equal(a1$ps_efg_pct, 62.8)
  expect_equal(a1$ps_ts_pct, 60)
  expect_equal(a1$ps_usg_pct, 24.1)
})

test_that("round_display_1 matches DT::formatRound(x, 1), i.e. JavaScript toFixed(1)", {
  # Expected strings were produced by node: (x).toFixed(1). Base round() gets
  # the first two wrong (10.2, 2.0) and 0.05 wrong (0.0).
  x <- c(41 / 4, 1.95, 0.05, 100.6 / 10, 10.05, 12.35, 1.45, 0.15, 43 / 4, -10.25, 38.1, 1.2)
  js <- c("10.3", "1.9", "0.1", "10.1", "10.1", "12.3", "1.4", "0.1", "10.8", "-10.3", "38.1", "1.2")
  expect_identical(sprintf("%.1f", round_display_1(x)), js)
  expect_identical(round_display_1(c(NA, NaN, Inf)), c(NA_real_, NA_real_, NA_real_))
  expect_identical(round_display_1(numeric(0)), numeric(0))
})

test_that("thresholds compare the displayed one-decimal value, not the raw average", {
  rows <- trad_rows()[1, ]
  rows$pts <- 41          # 41 / 4 = 10.25, displayed 10.3
  rows$gp <- 4L
  rows$minutes <- 40.24   # 40.24 / 4 = 10.06, displayed 10.1
  ps <- player_stat_filter_projection(rows, 2026)
  expect_equal(ps$ps_pts_pg, 10.3)
  expect_equal(ps$ps_min_pg, 10.1)
  onoff <- data.frame(team_id = 1, player_id = 11)
  keeps <- function(col, op, value) {
    nrow(apply_player_stat_filters(onoff, ps, list(ps_filter(1L, "x", col, op, value)), 2026)$df) == 1L
  }
  # The remark's example: a raw 10.06 would fail >= 10.1; the displayed 10.1 passes.
  expect_true(keeps("ps_min_pg", "ge", 10.1))
  expect_false(keeps("ps_min_pg", "ge", 10.2))
  expect_true(keeps("ps_pts_pg", "ge", 10.3))
  expect_false(keeps("ps_pts_pg", "ge", 10.4))
  expect_true(keeps("ps_pts_pg", "le", 10.3))
  expect_false(keeps("ps_pts_pg", "le", 10.2))
})

test_that("zero games played gives NA per-game values, never zero", {
  out <- player_stat_filter_projection(trad_rows(), 2026)
  z <- out[out$player_id == 13L, ]
  expect_equal(z$ps_gp, 0)
  per_game <- c("ps_min_pg", "ps_pts_pg", "ps_reb_pg", "ps_ast_pg", "ps_stl_pg", "ps_blk_pg")
  expect_true(all(is.na(unlist(z[per_game]))))
  expect_true(is.na(z$ps_3p_pct))
})

test_that("an empty Player Stats frame projects to an empty frame with every column", {
  out <- player_stat_filter_projection(trad_rows()[0, ], 2026)
  expect_identical(nrow(out), 0L)
  expect_named(out, c("game_year", "team_id", "player_id", names(PLAYER_STAT_FILTER_SOURCE)))
})

# ---- join and filter -------------------------------------------------------

test_that("the join is keyed on team and player, so a traded player is filtered per team", {
  ps <- player_stat_filter_projection(trad_rows(), 2026)
  joined <- join_player_stat_filter_frame(onoff_rows(), ps, 2026)
  expect_equal(joined$ps_pts_pg, c(20, 15, NA))
  expect_identical(names(joined)[1:5], names(onoff_rows()))
  # Ranks computed on the full population ride along untouched.
  expect_identical(joined$pr_net, onoff_rows()$pr_net)
})

test_that("the join ignores Player Stats rows from another season", {
  ps <- player_stat_filter_projection(trad_rows(), 2025)
  joined <- join_player_stat_filter_frame(onoff_rows(), ps, 2026)
  expect_true(all(is.na(joined$ps_pts_pg)))
})

test_that("the join tolerates a frame without id columns", {
  ps <- player_stat_filter_projection(trad_rows(), 2026)
  joined <- join_player_stat_filter_frame(data.frame(x = 1:2), ps, 2026)
  expect_true(all(is.na(joined$ps_gp)))
})

test_that("Player Stats filters fail closed on unmatched rows and form ranges", {
  ps <- player_stat_filter_projection(trad_rows(), 2026)
  ge10 <- ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 10)
  res <- apply_player_stat_filters(onoff_rows(), ps, list(ge10), 2026)
  expect_false(res$error)
  # Player 99 has no Player Stats row, so an active chip excludes it.
  expect_equal(res$df$ps_pts_pg, c(20, 15))
  range <- list(ge10, ps_filter(2L, "PTS/G", "ps_pts_pg", "le", 18))
  expect_equal(apply_player_stat_filters(onoff_rows(), ps, range, 2026)$df$team_id, 2)
})

test_that("with no Player Stats filters the frame passes through untouched", {
  res <- apply_player_stat_filters(onoff_rows(), NULL, list(), 2026)
  expect_identical(res, list(df = onoff_rows(), error = FALSE))
})

test_that("a missing Player Stats frame is an error, never an unfiltered table", {
  f <- list(ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 10))
  res <- apply_player_stat_filters(onoff_rows(), NULL, f, 2026)
  expect_true(res$error)
  expect_identical(nrow(res$df), 0L)
  expect_true(apply_player_stat_filters(onoff_rows(), data.frame(), f, 2026)$error)
})

# ---- menus and view switches -------------------------------------------------

test_that("switching views keeps Player Stats and sample-size chips and drops view-only ones", {
  filters <- list(
    ps_filter(1L, "Net", "Net RTG Diff"), ps_filter(2L, "PTS/G", "ps_pts_pg"),
    ps_filter(3L, "Min", "minutes"), ps_filter(4L, "On Off 2PT%", "on_off_fg2_pct")
  )
  out <- retain_stat_filters_for_cols(filters, c(ONOFF_FF_FILTERABLE_COLS, PLAYER_STAT_FILTERABLE_COLS))
  expect_identical(vapply(out$kept, `[[`, integer(1), "id"), c(2L, 3L))
  expect_identical(vapply(out$dropped, `[[`, integer(1), "id"), c(1L, 4L))
  expect_identical(
    retain_stat_filters_for_cols(list(), ONOFF_FF_FILTERABLE_COLS),
    list(kept = list(), dropped = list())
  )
})

test_that("the On/Off menu groups results, sample size and Player Stats", {
  cols <- c(ONOFF_FF_FILTERABLE_COLS, PLAYER_STAT_FILTERABLE_COLS)
  groups <- onoff_stat_filter_groups(cols)
  expect_named(groups, c("Impact and on/off results", "Sample size", "Player Stats"))
  expect_identical(groups[["Sample size"]], ONOFF_SAMPLE_FILTER_COLS)
  expect_identical(groups[["Player Stats"]], unname(PLAYER_STAT_FILTERABLE_COLS))
  expect_false(any(c(ONOFF_SAMPLE_FILTER_COLS, unname(PLAYER_STAT_FILTERABLE_COLS)) %in% groups[[1]]))

  choices <- stat_filter_choices(cols, groups)
  expect_identical(choices[["Choose..."]], "")
  expect_identical(names(choices[["Player Stats"]]), names(PLAYER_STAT_FILTERABLE_COLS))
  expect_identical(unlist(choices[["Sample size"]], use.names = FALSE), c("Min", "On Poss", "Off Poss"))
})

test_that("without groups the menu is the flat list it always was", {
  cols <- ONOFF_FF_FILTERABLE_COLS
  expect_identical(stat_filter_choices(cols), c("Choose..." = "", names(cols)))
})

test_that("the real stat-filter popover renders the groups as optgroups", {
  # helper-server-mocks.R stubs stat_filter_chips_ui(); load the real one.
  real <- new.env()
  sys.source(repo_file("R", "helpers.R"), envir = real)
  state <- list(filters = shiny::reactiveVal(list()), next_id = shiny::reactiveVal(1L))
  cols <- c(ONOFF_SUMMARY_FILTERABLE_COLS, PLAYER_STAT_FILTERABLE_COLS)
  ui <- shiny::isolate(real$stat_filter_chips_ui(
    "on", state, cols,
    percent_hint = onoff_player_stats_note(TRUE),
    choice_groups = onoff_stat_filter_groups(cols)
  ))
  html <- htmltools::renderTags(htmltools::tagList(ui))$html
  expect_match(html, '<optgroup label="Player Stats">', fixed = TRUE)
  expect_match(html, '<option value="PTS/G">PTS/G</option>', fixed = TRUE)
  expect_match(html, "not Player Stats.", fixed = TRUE)
})

# ---- starter-count disclosure --------------------------------------------------

test_that("the starter-count disclosure appears only when a starter restriction is active", {
  expect_identical(onoff_player_stats_note(FALSE),
                   "Player Stats use the selected games. Percentages use 0-100.")
  expect_match(onoff_player_stats_note(TRUE),
               "Starter-count filters affect On/Off possessions, not Player Stats.", fixed = TRUE)
  none <- list(num_starters_off_mode = "", num_starters_off = "",
               num_starters_def_mode = "", num_starters_def = "")
  expect_false(onoff_starter_restriction_active(none))
  expect_false(onoff_starter_restriction_active(list()))
  expect_false(onoff_starter_restriction_active(modifyList(none, list(num_starters_off_mode = "gte"))))
  expect_true(onoff_starter_restriction_active(
    modifyList(none, list(num_starters_def_mode = "lte", num_starters_def = "2"))
  ))
  dropped <- onoff_drop_starter_filters(
    modifyList(none, list(num_starters_off_mode = "gte", num_starters_off = "3", home_away = "home"))
  )
  expect_false(onoff_starter_restriction_active(dropped))
  expect_identical(dropped$home_away, "home")
})

# ---- frame fetch -----------------------------------------------------------------

ps_ctx <- function(...) {
  utils::modifyList(list(
    game_year = 2026L, fast = TRUE,
    start_d = as.Date("2025-10-01"), end_d = as.Date("2026-07-01"),
    game_type_csv = NA_character_, opp_ids_csv = NA_character_,
    home_away = NA_character_, outcome = NA_character_,
    opp_rank_side = NA_character_, opp_rank_n = NA_integer_, opp_rank_metric = NA_character_,
    min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_,
    data_version = "v1"
  ), list(...))
}

test_that("the filter frame reads the season MV on the fast path and the per-game reader otherwise", {
  reset_mock_db_query_counts()
  fast <- fetch_player_stat_filter_frame(pg_pool, ps_ctx(), session = NULL)
  expect_identical(mock_db_query_count("player_traditional_mv"), 1L)
  expect_identical(mock_db_query_count("player_traditional_reader"), 0L)
  expect_equal(fast$ps_pts_pg[fast$team_id == 1L & fast$player_id == 11L], 20)

  slow <- fetch_player_stat_filter_frame(pg_pool, ps_ctx(fast = FALSE), session = NULL)
  expect_identical(mock_db_query_count("player_traditional_reader"), 1L)
  expect_equal(slow$ps_pts_pg[order(slow$player_id)], fast$ps_pts_pg[order(fast$player_id)])
})

test_that("a failed frame read returns NULL", {
  withr::local_options(ibpl.mock_player_traditional_error = TRUE)
  expect_null(fetch_player_stat_filter_frame(pg_pool, ps_ctx(), session = NULL))
  expect_null(fetch_player_stat_filter_frame(pg_pool, ps_ctx(fast = FALSE), session = NULL))
})

test_that("the shared frame fetcher uses the pool it is given (review remark 4)", {
  src <- paste(deparse(fetch_player_stat_filter_frame), collapse = "\n")
  expect_false(grepl("pg_pool", src, fixed = TRUE))
  sentinel <- structure(list(id = "sentinel"), class = "mock_pool")
  reset_mock_db_query_counts()
  fetch_player_stat_filter_frame(sentinel, ps_ctx(fast = TRUE), session = NULL)
  expect_identical(mock_db_last_params("player_traditional_mv_pool"), sentinel)
  fetch_player_stat_filter_frame(sentinel, ps_ctx(fast = FALSE), session = NULL)
  expect_identical(mock_db_last_params("player_traditional_reader_pool"), sentinel)
})
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
"$RSCRIPT" <SCRATCH>/run_tests.R --baseline <SCRATCH>/baseline.tsv test-onoff-player-stat-filters; echo "exit=$?"
```

Expected: `exit=1`; `NEW FAILURES` lists the helper tests (`could not find function "is_player_stat_filter"` and similar).

- [ ] **Step 3: Write the helpers script**

Write `<SCRATCH>/task3_helpers.R`:

```r
# Task 3: pure Player Stats filter helpers + optgroup support in helpers.R.
# Usage (from app/): Rscript task3_helpers.R <edit_tools.R>
source(commandArgs(trailingOnly = TRUE)[[1]])
helpers_path <- "R/helpers.R"

# 1. stat_filter_chips_ui() gains optional optgroups; the flat default is unchanged.
splice(helpers_path,
r"---(stat_filter_chips_ui <- function(prefix, state, filterable_cols, percent_hint = NULL) {)---",
r"---(# Choices for the stat-filter column select. `groups` (group label -> internal
# column names) renders native <optgroup>s; each group is passed as a list so a
# one-item group still renders as a group. Without groups this is the flat
# vector the menu has always used.
stat_filter_choices <- function(cols, groups = NULL) {
  labels <- names(cols)
  if (is.null(groups)) return(c("Choose..." = "", labels))
  grouped <- lapply(groups, function(group_cols) {
    in_group <- labels[unname(cols) %in% group_cols]
    stats::setNames(as.list(in_group), in_group)
  })
  grouped <- grouped[vapply(grouped, length, integer(1)) > 0L]
  ungrouped <- labels[!unname(cols) %in% unlist(groups, use.names = FALSE)]
  c(list("Choose..." = ""), grouped, stats::setNames(as.list(ungrouped), ungrouped))
}

stat_filter_chips_ui <- function(prefix, state, filterable_cols, percent_hint = NULL,
                                 choice_groups = NULL) {)---")

splice(helpers_path,
r"---(        choices = c("Choose..." = "", choices),)---",
r"---(        choices = stat_filter_choices(cols, choice_groups),)---")

# 2. onoff_fallback_needed() uses the shared starter predicate (same logic).
splice(helpers_path,
r"---(    (nzchar(f$num_starters_off_mode %||% "") && nzchar(f$num_starters_off %||% "")) ||
    (nzchar(f$num_starters_def_mode %||% "") && nzchar(f$num_starters_def %||% ""))
)---",
r"---(    onoff_starter_restriction_active(f)
)---")

# 3. The Player Stats filter block, above the on/off menu section.
anchor <- "# ---- Stat-filter column menus for the on/off tabs ----"
splice(helpers_path, anchor, paste0(
r"---(# ---- Player Stats filter chips on the On/Off tab ----
# The On/Off `+ Filter` chip can narrow rows by a player's traditional stats
# over the same selected games. These columns never render: they are joined
# onto the ranked On/Off population and filtered BEFORE the table is built, so
# they stay out of the grouped-header column contract and cannot move a
# percentile rank. The ps_ prefix is the classification; labels are display.
PLAYER_STAT_FILTERABLE_COLS <- c(
  "GP"    = "ps_gp",
  "MIN/G" = "ps_min_pg",
  "PTS/G" = "ps_pts_pg",
  "REB/G" = "ps_reb_pg",
  "AST/G" = "ps_ast_pg",
  "STL/G" = "ps_stl_pg",
  "BLK/G" = "ps_blk_pg",
  "3P%"   = "ps_3p_pct",
  "eFG%"  = "ps_efg_pct",
  "TS%"   = "ps_ts_pct",
  "USG%"  = "ps_usg_pct"
)

# Player Stats column behind each ps_* column, in menu order. Counting stats
# become per-game through apply_ts_mode("Per Game"), exactly as Tab 5 shows
# them; the four percentages are already on the 0-100 display scale.
PLAYER_STAT_FILTER_SOURCE <- c(
  ps_gp = "gp", ps_min_pg = "minutes", ps_pts_pg = "pts", ps_reb_pg = "reb",
  ps_ast_pg = "ast", ps_stl_pg = "stl", ps_blk_pg = "blk",
  ps_3p_pct = "tp_pct", ps_efg_pct = "efg", ps_ts_pct = "ts", ps_usg_pct = "usg_pct"
)

# Sample-size columns every On/Off view menu carries under the same names.
ONOFF_SAMPLE_FILTER_COLS <- c("minutes", "ON Poss", "OFF Poss")

ONOFF_STARTER_FILTER_FIELDS <- c("num_starters_off_mode", "num_starters_off",
                                 "num_starters_def_mode", "num_starters_def")

PLAYER_STAT_FILTER_ERROR_TEXT <- "Player Stats filters could not be evaluated. Remove them or try again."
PLAYER_STAT_STARTERS_CHIP_NOTE <- "Player Stats ignore starter-count filters"

is_player_stat_filter <- function(filter) {
  isTRUE(startsWith(as.character(filter$col %||% ""), "ps_"))
}

has_player_stat_filters <- function(filters) {
  any(vapply(filters, is_player_stat_filter, logical(1)))
}

split_stat_filters <- function(filters) {
  ps <- vapply(filters, is_player_stat_filter, logical(1))
  list(player_stats = filters[ps], onoff = filters[!ps])
}

# Chips whose column the given menu still offers, and those it does not.
retain_stat_filters_for_cols <- function(filters, filterable_cols) {
  allowed <- unname(normalize_stat_filter_cols(filterable_cols))
  keep <- vapply(filters, function(f) as.character(f$col %||% "") %in% allowed, logical(1))
  list(kept = filters[keep], dropped = filters[!keep])
}

onoff_stat_filter_groups <- function(filterable_cols) {
  cols <- unname(normalize_stat_filter_cols(filterable_cols))
  sample <- intersect(ONOFF_SAMPLE_FILTER_COLS, cols)
  player_stats <- intersect(unname(PLAYER_STAT_FILTERABLE_COLS), cols)
  list(
    "Impact and on/off results" = setdiff(cols, c(sample, player_stats)),
    "Sample size" = sample,
    "Player Stats" = player_stats
  )
}

# A starter-count restriction is active only when both its mode and its value
# are set -- the same rule onoff_fallback_needed() applies.
onoff_starter_restriction_active <- function(filters) {
  set <- function(field) nzchar(as.character(filters[[field]] %||% ""))
  (set("num_starters_off_mode") && set("num_starters_off")) ||
    (set("num_starters_def_mode") && set("num_starters_def"))
}

# The game context Player Stats can honour: the traditional reader aggregates
# whole games, so the starter-count segment restriction is removed.
onoff_drop_starter_filters <- function(filters) {
  filters[ONOFF_STARTER_FILTER_FIELDS] <- ""
  filters
}

onoff_player_stats_note <- function(starters_active) {
  note <- "Player Stats use the selected games. Percentages use 0-100."
  if (isTRUE(starters_active)) {
    note <- paste(note, "Starter-count filters affect On/Off possessions, not Player Stats.")
  }
  note
}

# Normalised Player Stats rows -> one row per team-player with the ps_* columns.
# The multi-team TOTAL row is never built here, so each On/Off team row is
# judged on that team's stats alone.
player_stat_filter_projection <- function(df, game_year) {
  ps_cols <- names(PLAYER_STAT_FILTER_SOURCE)
  gy <- suppressWarnings(as.integer(game_year))
  if (is.null(df) || !nrow(df)) {
    out <- data.frame(game_year = integer(0), team_id = integer(0), player_id = integer(0))
    out[ps_cols] <- rep(list(numeric(0)), length(ps_cols))
    return(out)
  }
  per_game <- apply_ts_mode(add_ts_usage_pct(clean_ts_rows(df)), "Per Game")
  out <- data.frame(
    game_year = rep(gy, nrow(per_game)),
    team_id = suppressWarnings(as.integer(per_game$team_id)),
    player_id = suppressWarnings(as.integer(per_game$player_id))
  )
  out[ps_cols] <- lapply(unname(PLAYER_STAT_FILTER_SOURCE), function(src) {
    v <- per_game[[src]]
    if (is.null(v)) rep(NA_real_, nrow(per_game)) else suppressWarnings(as.numeric(v))
  })
  # Thresholds compare what the Player Stats table shows. GP is a count and is
  # displayed unrounded; everything else is displayed to one decimal.
  shown <- setdiff(ps_cols, "ps_gp")
  out[shown] <- lapply(out[shown], round_display_1)
  out
}

# One-decimal rounding identical to DT::formatRound(x, 1), which the Player
# Stats table uses. That is JavaScript toFixed(1): it rounds the double's EXACT
# decimal value and takes the larger candidate on a tie. Base round() does not:
# round(41 / 4, 1) is 10.2 where the table shows 10.3, and round(1.95, 1) is 2.0
# where it shows 1.9. sprintf("%.60f") prints the exact expansion, so the
# hundredths digit alone decides. Verified against node on 3,135,580 per-game
# and percentage values with zero mismatches (plan review remark 1).
round_display_1 <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  out <- rep(NA_real_, length(x))
  ok <- is.finite(x)
  if (!any(ok)) return(out)
  s <- sprintf("%.60f", abs(x[ok]))
  dot <- regexpr(".", s, fixed = TRUE)
  whole <- as.numeric(substr(s, 1L, dot - 1L))
  tenths <- as.numeric(substr(s, dot + 1L, dot + 1L))
  up <- as.numeric(substr(s, dot + 2L, dot + 2L)) >= 5
  out[ok] <- sign(x[ok]) * (whole * 10 + tenths + up) / 10
  out
}

# Appends every ps_* column to `df` by season + team + player. Row order and
# every existing column (percentile ranks included) are untouched; a row with
# no Player Stats match gets NA, which an active chip then excludes.
join_player_stat_filter_frame <- function(df, ps_frame, game_year) {
  ps_cols <- names(PLAYER_STAT_FILTER_SOURCE)
  ps <- ps_frame[suppressWarnings(as.integer(ps_frame$game_year)) %in%
                   suppressWarnings(as.integer(game_year)), , drop = FALSE]
  idx <- rep(NA_integer_, nrow(df))
  if (all(c("team_id", "player_id") %in% names(df)) && nrow(df)) {
    idx <- match(ts_player_key(df$team_id, df$player_id),
                 ts_player_key(ps$team_id, ps$player_id))
    idx[is.na(suppressWarnings(as.integer(df$team_id))) |
          is.na(suppressWarnings(as.integer(df$player_id)))] <- NA_integer_
  }
  df[ps_cols] <- lapply(ps[ps_cols], function(col) col[idx])
  df
}

# list(df, error). No Player Stats chips: df unchanged. A missing or malformed
# frame: an error with zero rows -- never the unfiltered table.
apply_player_stat_filters <- function(df, ps_frame, filters, game_year) {
  if (!length(filters) || is.null(df)) return(list(df = df, error = FALSE))
  required <- c("game_year", "team_id", "player_id", names(PLAYER_STAT_FILTER_SOURCE))
  if (is.null(ps_frame) || !all(required %in% names(ps_frame))) {
    return(list(df = df[0, , drop = FALSE], error = TRUE))
  }
  joined <- join_player_stat_filter_frame(df, ps_frame, game_year)
  list(df = apply_stat_filters(joined, filters), error = FALSE)
}

# The projected Player Stats frame for one On/Off game context, or NULL when it
# cannot be read. `ctx$fast` selects the shared default-season MV pull;
# otherwise the per-game reader answers with no team and no clutch filter.
fetch_player_stat_filter_frame <- function(pool, ctx, session) {
  raw <- if (isTRUE(ctx$fast)) {
    fetch_player_traditional_season_israel(pool, ctx$game_year, ctx$data_version)
  } else {
    out <- tryCatch(
      run_player_traditional_israel(
        pool, session = session, guard_key = "tab1_player_stat_filters",
        game_year = ctx$game_year, start_d = ctx$start_d, end_d = ctx$end_d,
        team_ids_csv = NA_character_,
        game_type_csv = ctx$game_type_csv, opp_ids_csv = ctx$opp_ids_csv,
        home_away = ctx$home_away, outcome = ctx$outcome,
        opp_rank_side = ctx$opp_rank_side, opp_rank_n = ctx$opp_rank_n,
        opp_rank_metric = ctx$opp_rank_metric,
        max_margin = NA_integer_, margin_status = NA_character_,
        max_time_remaining = NA_integer_, ot_margin_filter = FALSE,
        min_gn = ctx$min_gn, max_gn = ctx$max_gn, last_n_games = ctx$last_n_games
      ),
      error = function(e) NULL
    )
    # A guard refusal is a column-less frame: not an answer, so fail closed.
    if (is.null(out) || !ncol(out)) NULL else normalize_ts_result_cols(out)
  }
  if (is.null(raw)) return(NULL)
  player_stat_filter_projection(raw, ctx$game_year)
}

)---", anchor))
cat("done\n")
```

- [ ] **Step 4: Run it, then the tests**

```bash
"$RSCRIPT" <SCRATCH>/task3_helpers.R <SCRATCH>/edit_tools.R
"$RSCRIPT" <SCRATCH>/run_tests.R --baseline <SCRATCH>/baseline.tsv \
  test-onoff-player-stat-filters test-league-shared-helpers test-mobile-layer \
  test-primary-table-render-smoke; echo "exit=$?"
```

Expected: `done`, then `no new failures` / `exit=0`. Then run the ASCII check on `R/helpers.R`. `test-league-shared-helpers.R` pins `onoff_fallback_needed()` (lines 211-255) and must stay green after step 2 of the script.

- [ ] **Step 5: Commit**

```bash
git -c core.autocrlf=false add R/helpers.R
git add tests/testthat/test-onoff-player-stat-filters.R
git diff --cached --stat
```

Expected: only those two files; `helpers.R` shows about 175 insertions and 2 deletions.

```bash
git commit -m "$(cat <<'EOF'
shiny: pure helpers for On/Off Player Stats filter chips

ps_* filter columns, namespace classification, per-game projection through
Tab 5's formulas, a team+player join that leaves ranks untouched, a
fail-closed filter step, view-switch retention, and optgroup support in the
shared stat-filter popover.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Wire Player Stats filtering into Tab 1

**Files:**
- Modify: `app/R/server_tab1.R`
- Modify: `app/tests/testthat/test-onoff-player-stat-filters.R` (append server tests)

**Interfaces:**
- Consumes (Task 3): `PLAYER_STAT_FILTERABLE_COLS`, `split_stat_filters`, `has_player_stat_filters`, `onoff_drop_starter_filters`, `fetch_player_stat_filter_frame(pool, ctx, session)`, `apply_player_stat_filters(df, ps_frame, filters, game_year)`, `PLAYER_STAT_FILTER_ERROR_TEXT`; mock `mock_db_last_params()` (Task 2).
- Produces: `server_tab1()` now returns `invisible(list(stat_filter_state, player_stat_frame, filtered_result))`. `filtered_result()` is `list(df, error)`. `app.R` ignores the value.

- [ ] **Step 1: Append the failing server tests**

Append to `app/tests/testthat/test-onoff-player-stat-filters.R`:

```r
# ---- Tab 1 server --------------------------------------------------------------

tab1_app <- function(input, output, session) {
  session$userData$tab1 <- server_tab1(input, output, session, shared = make_shared())
}

# Season-bounds dates (2025-10-01 .. 2026-07-01 in make_shared) keep the fast path.
set_onoff_context <- function(session, mode = "Summary", ...) {
  inputs <- list(
    main_tabs = "onoff", game_year = "2026", onoff_view_mode = mode,
    date_range = as.Date(c("2025-10-01", "2026-07-01")), teams = character(0),
    on_game_type = character(0), on_opponents = character(0),
    on_home_away = "", on_outcome = "",
    on_opp_rank_side = "", on_opp_rank_n = "", on_opp_rank_metric = "",
    on_num_starters_off_mode = "", on_num_starters_off = "",
    on_num_starters_def_mode = "", on_num_starters_def = "",
    on_gn_min = "", on_gn_max = "", on_last_n = "",
    min_all_poss = 0, min_on_poss = 0
  )
  do.call(session$setInputs, utils::modifyList(inputs, list(...)))
  session$elapse(500)
  session$flushReact()
}

set_stat_filters <- function(session, ...) {
  session$userData$tab1$stat_filter_state$filters(list(...))
  session$flushReact()
}

# The On/Off DataTable is server-side: row data never appears in the widget
# JSON, only the container. Use it for headers (the error table's "Info").
table_text <- function(value) paste(deparse(value), collapse = "")

test_that("On/Off issues no Player Stats query without a Player Stats chip", {
  reset_mock_db_query_counts()
  shiny::testServer(tab1_app, {
    set_onoff_context(session)
    output$onoff_dt
    set_stat_filters(session, ps_filter(1L, "Net", "Net RTG Diff", "ge", 10))
    output$onoff_dt
    session$userData$tab1$filtered_result()
    expect_identical(mock_db_query_count("player_traditional_mv"), 0L)
    expect_identical(mock_db_query_count("player_traditional_reader"), 0L)
  })
})

test_that("a Player Stats chip reads once, filters by team and player, and keeps ranks", {
  reset_mock_db_query_counts()
  shiny::testServer(tab1_app, {
    set_onoff_context(session)
    tab1 <- session$userData$tab1
    unfiltered <- tab1$filtered_result()$df

    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 19))
    res <- tab1$filtered_result()
    expect_false(res$error)
    expect_identical(as.integer(res$df$player_id), 11L)
    # Narrowing rows never recomputes the league-relative ranks.
    expect_identical(res$df$pr_net, unfiltered$pr_net[unfiltered$player_id == 11])
    expect_identical(mock_db_query_count("player_traditional_mv"), 1L)

    # Threshold edits and ranges reuse the fetched frame.
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 17))
    expect_setequal(as.integer(tab1$filtered_result()$df$player_id), c(11L, 21L))
    set_stat_filters(session,
      ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 17),
      ps_filter(2L, "PTS/G", "ps_pts_pg", "le", 19))
    expect_identical(as.integer(tab1$filtered_result()$df$player_id), 21L)
    expect_identical(mock_db_query_count("player_traditional_mv"), 1L)
    expect_identical(mock_db_query_count("player_traditional_reader"), 0L)
  })
})

test_that("Player Stats and On/Off chips compose with AND in the table data", {
  # The renderer hands exactly these two inputs to onoff_summary_datatable().
  expect_match(read_repo_txt("R", "server_tab1.R"),
               "onoff_summary_datatable(df, onoff_filters, pivot = pivot_targets)", fixed = TRUE)
  shiny::testServer(tab1_app, {
    set_onoff_context(session)
    tab1 <- session$userData$tab1
    table_players <- function() {
      onoff_filters <- split_stat_filters(tab1$stat_filter_state$filters())$onoff
      df <- onoff_clean_display_names(tab1$filtered_result()$df)
      onoff_summary_datatable(df, onoff_filters)$x$data$Player
    }
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 17))
    expect_setequal(table_players(), c("Player A", "Player B"))

    set_stat_filters(session,
      ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 17),
      ps_filter(2L, "Net", "Net RTG Diff", "ge", 10))
    expect_identical(table_players(), "Player A")
  })
})

test_that("starter counts and teams reuse the Player Stats frame", {
  reset_mock_db_query_counts()
  shiny::testServer(tab1_app, {
    set_onoff_context(session)
    tab1 <- session$userData$tab1
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 0))
    tab1$filtered_result()
    expect_identical(mock_db_query_count("player_traditional_mv"), 1L)

    # Starter counts restrict On/Off segments, not the games Player Stats read.
    set_onoff_context(session, on_num_starters_off_mode = "gte", on_num_starters_off = "3")
    tab1$filtered_result()
    # The reader's team filter only drops team-game rows, so a team change
    # cannot alter a team-player row; the frame is reused.
    set_onoff_context(session, on_num_starters_off_mode = "gte", on_num_starters_off = "3", teams = "1")
    tab1$filtered_result()
    expect_identical(mock_db_query_count("player_traditional_mv"), 1L)
    expect_identical(mock_db_query_count("player_traditional_reader"), 0L)
  })
})

# One row per game-context control Tab 1 forwards to
# get_player_traditional_from_games(). `param` is the SQL placeholder ($n) the
# value must arrive in; `opp_ids` feeds shared$selected_opp_ids_on.
reader_forwarding_cases <- list(
  list(name = "start date", inputs = list(date_range = as.Date(c("2025-11-01", "2026-02-01"))),
       param = 2L, expected = as.Date("2025-11-01")),
  list(name = "end date", inputs = list(date_range = as.Date(c("2025-11-01", "2026-02-01"))),
       param = 3L, expected = as.Date("2026-02-01")),
  list(name = "game type", inputs = list(on_game_type = "1"), param = 5L, expected = "1"),
  list(name = "opponents", inputs = list(on_opponents = c("3", "4")), opp_ids = c(3L, 4L),
       param = 6L, expected = "3,4"),
  list(name = "home/away", inputs = list(on_home_away = "home"), param = 7L, expected = "home"),
  list(name = "outcome", inputs = list(on_outcome = "win"), param = 8L, expected = "win"),
  list(name = "opp rank side", inputs = list(on_opp_rank_side = "top"), param = 9L, expected = "top"),
  list(name = "opp rank count", inputs = list(on_opp_rank_side = "top", on_opp_rank_n = "4"),
       param = 10L, expected = 4L),
  list(name = "opp rank metric",
       inputs = list(on_opp_rank_side = "top", on_opp_rank_n = "4", on_opp_rank_metric = "off"),
       param = 11L, expected = "off"),
  list(name = "GN min", inputs = list(on_gn_min = "2"), param = 16L, expected = 2L),
  list(name = "GN max", inputs = list(on_gn_min = "2", on_gn_max = "9"), param = 17L, expected = 9L),
  list(name = "last N", inputs = list(on_last_n = "3"), param = 18L, expected = 3L)
)

tab1_app_mutable_opps <- function(input, output, session) {
  shared <- make_shared()
  # app.R derives this reactive from input$on_opponents; build_onoff_db_args()
  # reads the reactive, so the test must drive it, not just the input.
  opp_ids <- shiny::reactiveVal(NULL)
  shared$selected_opp_ids_on <- opp_ids
  session$userData$opp_ids <- opp_ids
  session$userData$tab1 <- server_tab1(input, output, session, shared = shared)
}

test_that("every game-context control invalidates the frame and reaches the reader exactly", {
  reset_mock_db_query_counts()
  shiny::testServer(tab1_app_mutable_opps, {
    tab1 <- session$userData$tab1
    for (i in seq_along(reader_forwarding_cases)) {
      case <- reader_forwarding_cases[[i]]
      # Start from the default (fast-path) context, then change one control.
      session$userData$opp_ids(NULL)
      set_onoff_context(session)
      set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", i))
      tab1$filtered_result()
      before <- mock_db_query_count("player_traditional_reader")

      session$userData$opp_ids(case$opp_ids)
      do.call(set_onoff_context, c(list(session), case$inputs))
      tab1$filtered_result()
      expect_identical(mock_db_query_count("player_traditional_reader"), before + 1L,
                       label = paste(case$name, "reader calls"))

      params <- mock_db_last_params("player_traditional_reader")
      expect_identical(params[[case$param]], case$expected, label = case$name)
      # Never forwarded: season stays, no team list, no clutch window.
      expect_identical(params[[1]], 2026L, label = paste(case$name, "$1 season"))
      expect_identical(params[[4]], NA_character_, label = paste(case$name, "$4 teams"))
      expect_identical(params[12:15], list(NA_integer_, NA_character_, NA_integer_, FALSE),
                       label = paste(case$name, "$12-$15 clutch"))

      # A threshold edit in the same context reuses the frame.
      set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", i + 0.5))
      tab1$filtered_result()
      expect_identical(mock_db_query_count("player_traditional_reader"), before + 1L,
                       label = paste(case$name, "reader calls after threshold edit"))
    }
  })
})

test_that("a failed Player Stats read shows an error, keeps the chips, and retries", {
  expect_match(read_repo_txt("R", "server_tab1.R"),
               "data.frame(Info = PLAYER_STAT_FILTER_ERROR_TEXT", fixed = TRUE)
  withr::local_options(ibpl.mock_player_traditional_error = TRUE)
  shiny::testServer(tab1_app, {
    set_onoff_context(session)
    tab1 <- session$userData$tab1
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 0))
    res <- tab1$filtered_result()
    expect_true(res$error)
    expect_identical(nrow(res$df), 0L)
    # The table is the one-column Info error table, not the On/Off grid.
    expect_match(table_text(output$onoff_dt), "<th>Info", fixed = TRUE)
    expect_length(tab1$stat_filter_state$filters(), 1L)

    options(ibpl.mock_player_traditional_error = FALSE)
    # A different value: reactiveVal ignores an identical set.
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 1))
    expect_false(tab1$filtered_result()$error)
  })
})

test_that("every On/Off view renders with a Player Stats chip, including an empty result", {
  shiny::testServer(tab1_app, {
    for (mode in c("Summary", "Four Factors", "Shot Profile")) {
      set_onoff_context(session, mode)
      set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 19))
      expect_silent(txt <- table_text(output$onoff_dt))
      expect_false(grepl("<th>Info", txt, fixed = TRUE))
      set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 1000))
      expect_silent(output$onoff_dt)
    }
  })
})

test_that("auto min-possessions still reads the unfiltered On/Off sources", {
  txt <- read_repo_txt("R", "server_tab1.R")
  start <- regexpr("setup_onoff_auto_min(", txt, fixed = TRUE)
  end <- regexpr("setup_gn_last_n_sync(", txt, fixed = TRUE)
  expect_gt(start, 0)
  expect_false(grepl("ps_filter|player_stat|filtered_result", substr(txt, start, end)))
})
```

- [ ] **Step 2: Run the tests to verify the new ones fail**

```bash
"$RSCRIPT" <SCRATCH>/run_tests.R --baseline <SCRATCH>/baseline.tsv test-onoff-player-stat-filters; echo "exit=$?"
```

Expected: `exit=1`; the Task 3 tests pass and `NEW FAILURES` lists only the new server tests (e.g. `attempt to apply non-function` on `session$userData$tab1$filtered_result`). The auto-min contract test already passes.

- [ ] **Step 3: Write the Tab 1 wiring script**

Write `<SCRATCH>/task4_tab1.R`:

```r
# Task 4: Player Stats filtering in server_tab1.R.
# Usage (from app/): Rscript task4_tab1.R <edit_tools.R>
source(commandArgs(trailingOnly = TRUE)[[1]])
f <- "R/server_tab1.R"

# 1. Every view's menu carries the Player Stats columns.
splice(f,
r"---(  on_stat_filter_cols <- reactive({
    switch(input$onoff_view_mode %||% "Summary",
      "Four Factors" = ONOFF_FF_FILTERABLE_COLS,
      "Shot Profile" = ON_SP_FILTERABLE_COLS,
      ONOFF_SUMMARY_FILTERABLE_COLS
    )
  }))---",
r"---(  # Player Stats chips are common to every view (PLAYER_STAT_FILTERABLE_COLS).
  on_stat_filter_cols <- reactive({
    c(switch(input$onoff_view_mode %||% "Summary",
      "Four Factors" = ONOFF_FF_FILTERABLE_COLS,
      "Shot Profile" = ON_SP_FILTERABLE_COLS,
      ONOFF_SUMMARY_FILTERABLE_COLS
    ), PLAYER_STAT_FILTERABLE_COLS)
  }))---")

# 2. Lazy frame, memo and upstream filtering, above the renderer.
splice(f, "  # --- Render Table ---",
r"---(  # ---- Player Stats filter chips ----
  # ps_* chips are evaluated here, upstream of the renderers, so they never
  # enter the grouped-header column contract. The frame is read only while a
  # ps_* chip is active, for this tab's game context WITHOUT the starter-count
  # restriction: the traditional reader aggregates whole games (the chips row
  # discloses this). Teams are not in the context either -- the reader's team
  # filter only drops team-game rows, so a team-player row's values cannot
  # depend on it, and leaving it out lets a team change reuse the frame.
  on_filter_split <- reactive(split_stat_filters(on_stat_filter_state$filters()))

  ps_filter_context <- reactive({
    rng <- debounced_range()
    req(rng)
    req(!is.na(rng[1]), !is.na(rng[2]))
    db_args <- build_onoff_db_args()
    list(
      game_year = as.integer(shared$selected_game_year()),
      # `list()`, not `input`: the On/Off tab also reads the RAW GN inputs so
      # it leaves the MV while a round is still being typed. Here that would
      # yield fast = FALSE with the (still debounced) GN arguments unset -- a
      # full-season reader query thrown away 150ms later. Decide the path
      # from the same resolved values the reader receives.
      fast = !isTRUE(onoff_fallback_needed(
        rng, shared$season_date_bounds(shared$selected_game_year()),
        onoff_drop_starter_filters(debounced_on_filters()), gn_params(),
        list(), onoff_cfg$prefix
      )),
      start_d = as.Date(rng[1]), end_d = as.Date(rng[2]),
      game_type_csv = db_args$game_type_csv, opp_ids_csv = db_args$opp_ids_csv,
      home_away = db_args$home_away, outcome = db_args$outcome,
      opp_rank_side = db_args$opp_rank_side, opp_rank_n = db_args$opp_rank_n,
      opp_rank_metric = db_args$opp_rank_metric,
      min_gn = db_args$min_gn, max_gn = db_args$max_gn,
      last_n_games = db_args$last_n_games,
      data_version = on_data_version()
    )
  })

  # One read per distinct context: threshold edits reuse the last frame. A
  # failed read (NULL) is not remembered, so the next chip change retries.
  ps_frame_memo <- new.env(parent = emptyenv())
  ps_filter_frame <- reactive({
    if (!has_player_stat_filters(on_stat_filter_state$filters())) return(NULL)
    ctx <- ps_filter_context()
    if (identical(ps_frame_memo$ctx, ctx)) return(ps_frame_memo$frame)
    frame <- fetch_player_stat_filter_frame(pg_pool, ctx, session)
    if (!is.null(frame)) {
      ps_frame_memo$ctx <- ctx
      ps_frame_memo$frame <- frame
    }
    frame
  })

  # result_df() is already ranked and min-poss filtered; ps_frame is forced
  # only when a Player Stats chip is active.
  ps_filtered_result <- reactive({
    apply_player_stat_filters(result_df(), ps_filter_frame(),
                              on_filter_split()$player_stats,
                              shared$selected_game_year())
  })

  # --- Render Table ---)---")

# 3. The renderer reads the Player Stats-filtered rows and the remaining chips.
splice(f,
r"---(    df <- onoff_clean_display_names(result_df()))---",
r"---(    ps_result <- ps_filtered_result()
    if (isTRUE(ps_result$error)) {
      return(DT::datatable(
        data.frame(Info = PLAYER_STAT_FILTER_ERROR_TEXT, check.names = FALSE),
        rownames = FALSE, options = list(dom = "t")
      ))
    }
    df <- onoff_clean_display_names(ps_result$df)
    onoff_filters <- on_filter_split()$onoff)---")

splice(f,
r"---(return(onoff_summary_datatable(df, on_stat_filter_state$filters(), pivot = pivot_targets)))---",
r"---(return(onoff_summary_datatable(df, onoff_filters, pivot = pivot_targets)))---")

splice(f,
r"---(return(onoff_four_factors_datatable(df, on_stat_filter_state$filters(),)---",
r"---(return(onoff_four_factors_datatable(df, onoff_filters,)---")

splice(f,
r"---(df_final <- apply_stat_filters(df_final, on_stat_filter_state$filters()))---",
r"---(df_final <- apply_stat_filters(df_final, onoff_filters))---")

# 4. Handles for the server tests.
splice(f,
r"---(                     "on_num_starters_def_mode", "on_num_starters_def"))
})---",
r"---(                     "on_num_starters_def_mode", "on_num_starters_def"))

  # Returned for the server tests; app.R ignores it.
  invisible(list(
    stat_filter_state = on_stat_filter_state,
    player_stat_frame = ps_filter_frame,
    filtered_result = ps_filtered_result
  ))
})---")
cat("done\n")
```

- [ ] **Step 4: Run it, then the tests**

```bash
"$RSCRIPT" <SCRATCH>/task4_tab1.R <SCRATCH>/edit_tools.R
"$RSCRIPT" <SCRATCH>/run_tests.R --baseline <SCRATCH>/baseline.tsv \
  test-onoff-player-stat-filters test-primary-table-render-smoke test-server-tabs-smoke \
  test-companion-query-counts test-tab-wiring; echo "exit=$?"
```

Expected: `done`, then `no new failures` / `exit=0` (the `test-companion-query-counts` EuroLeague baseline failure is listed as ignored). Then run the ASCII check on `R/server_tab1.R`. If the empty-result render test fails inside a shared renderer on a 0-row frame, that is a real defect the spec requires fixing: follow superpowers:systematic-debugging, fix it in the renderer with a failing test first, and do not weaken the test.

- [ ] **Step 5: Commit**

```bash
git -c core.autocrlf=false add R/server_tab1.R
git add tests/testthat/test-onoff-player-stat-filters.R
git diff --cached --stat
```

Expected: only those two files; `server_tab1.R` about 80 insertions and 6 deletions.

```bash
git commit -m "$(cat <<'EOF'
shiny: filter On/Off rows by Player Stats chips

A ps_* chip reads the Player Stats frame for the tab's game context (MV on
the default season, the per-game reader otherwise), joins it onto the ranked
On/Off population by season+team+player and filters before rendering. No
query without a chip; threshold edits reuse the frame; a failed read shows an
error instead of an unfiltered table.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: View-switch retention and the starter-count disclosure

**Files:**
- Modify: `app/R/server_tab1.R`
- Modify: `app/tests/testthat/test-onoff-player-stat-filters.R` (append)

**Interfaces:**
- Consumes (Task 3): `retain_stat_filters_for_cols`, `onoff_starter_restriction_active`, `onoff_player_stats_note`, `onoff_stat_filter_groups`, `PLAYER_STAT_STARTERS_CHIP_NOTE`, `stat_filter_chips_ui(..., percent_hint, choice_groups)`; existing `onoff_filter_values`.
- Produces: no new interfaces.

- [ ] **Step 1: Append the failing tests**

```r
test_that("switching views keeps Player Stats and sample chips and drops view-only ones", {
  shiny::testServer(tab1_app, {
    set_onoff_context(session, "Summary")
    tab1 <- session$userData$tab1
    set_stat_filters(session,
      ps_filter(1L, "Net", "Net RTG Diff", "ge", 0),
      ps_filter(2L, "PTS/G", "ps_pts_pg", "ge", 0),
      ps_filter(3L, "On Poss", "ON Poss", "ge", 0))
    session$setInputs(onoff_view_mode = "Four Factors")
    session$flushReact()
    ids <- function() vapply(tab1$stat_filter_state$filters(), `[[`, integer(1), "id")
    expect_identical(ids(), c(2L, 3L))
    session$setInputs(onoff_view_mode = "Shot Profile")
    session$flushReact()
    expect_identical(ids(), c(2L, 3L))
  })
})

test_that("Reset to defaults clears Player Stats chips", {
  shiny::testServer(tab1_app, {
    set_onoff_context(session)
    tab1 <- session$userData$tab1
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 0))
    session$setInputs(reset_defaults = 1)
    session$flushReact()
    expect_length(tab1$stat_filter_state$filters(), 0L)
  })
})

test_that("the On/Off chips row groups the menu and discloses the starter-count scope", {
  txt <- read_repo_txt("R", "server_tab1.R")
  expect_match(txt, "choice_groups = onoff_stat_filter_groups(on_stat_filter_cols())", fixed = TRUE)
  expect_match(txt, "percent_hint = onoff_player_stats_note(starters_active)", fixed = TRUE)
  expect_match(txt, "PLAYER_STAT_STARTERS_CHIP_NOTE", fixed = TRUE)
  expect_no_match(txt, "observeEvent(input$onoff_view_mode, {\n    reset_stat_filters", fixed = TRUE)
})
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
"$RSCRIPT" <SCRATCH>/run_tests.R --baseline <SCRATCH>/baseline.tsv test-onoff-player-stat-filters; echo "exit=$?"
```

Expected: `exit=1`; the view-switch test FAILS (ids are empty because the current observer resets everything); the chips-row contract test FAILS; the reset test already PASSES (existing behaviour, kept as a regression guard).

- [ ] **Step 3: Write the script**

Write `<SCRATCH>/task5_tab1.R`:

```r
# Task 5: view-switch retention + starter-count disclosure in server_tab1.R.
# Usage (from app/): Rscript task5_tab1.R <edit_tools.R>
source(commandArgs(trailingOnly = TRUE)[[1]])
f <- "R/server_tab1.R"

splice(f,
r"---(  observeEvent(input$onoff_view_mode, {
    reset_stat_filters(on_stat_filter_state)
  }, ignoreInit = TRUE))---",
r"---(  # Chips the new view can still evaluate survive a view switch -- every
  # Player Stats and sample-size chip does, since each view's menu carries
  # them. Only chips on the previous view's own columns are removed, visibly.
  observeEvent(input$onoff_view_mode, {
    split <- retain_stat_filters_for_cols(on_stat_filter_state$filters(), on_stat_filter_cols())
    if (!length(split$dropped)) return()
    on_stat_filter_state$filters(split$kept)
    dropped_labels <- vapply(split$dropped, function(x) as.character(x$label), character(1))
    showNotification(
      sprintf("Removed filters not available in %s: %s",
              input$onoff_view_mode, paste(dropped_labels, collapse = ", ")),
      type = "message", duration = 5
    )
  }, ignoreInit = TRUE))---")

splice(f,
r"---(    build_filter_chips(
      "on", input, shared$season_date_bounds,)---",
r"---(    starters_active <- onoff_starter_restriction_active(
      onoff_filter_values(input, onoff_cfg$prefix, game_type_id = onoff_cfg$game_type_id)
    )
    build_filter_chips(
      "on", input, shared$season_date_bounds,)---")

splice(f,
r"---(      extra_children = stat_filter_chips_ui("on", on_stat_filter_state, on_stat_filter_cols))---",
r"---(      extra_children = c(
        stat_filter_chips_ui("on", on_stat_filter_state, on_stat_filter_cols,
                             percent_hint = onoff_player_stats_note(starters_active),
                             choice_groups = onoff_stat_filter_groups(on_stat_filter_cols())),
        # Visible with the popover closed: Player Stats values come from whole
        # games, while the starter-count filters narrow On/Off possessions.
        if (starters_active && has_player_stat_filters(on_stat_filter_state$filters())) {
          list(tags$span(class = "filter-chip-note text-muted small",
                         PLAYER_STAT_STARTERS_CHIP_NOTE))
        }
      ))---")
cat("done\n")
```

- [ ] **Step 4: Run it, then the tests**

```bash
"$RSCRIPT" <SCRATCH>/task5_tab1.R <SCRATCH>/edit_tools.R
"$RSCRIPT" <SCRATCH>/run_tests.R --baseline <SCRATCH>/baseline.tsv \
  test-onoff-player-stat-filters test-primary-table-render-smoke test-server-tabs-smoke \
  test-filter-chip-date-guards test-chips-row; echo "exit=$?"
```

Expected: `done`, then `no new failures` / `exit=0`. Then run the ASCII check on `R/server_tab1.R`.

- [ ] **Step 5: Commit**

```bash
git -c core.autocrlf=false add R/server_tab1.R
git add tests/testthat/test-onoff-player-stat-filters.R
git diff --cached --stat
```

Expected: only those two files; `server_tab1.R` about 30 insertions and 3 deletions.

```bash
git commit -m "$(cat <<'EOF'
shiny: keep common On/Off chips across views; disclose starter scope

A view switch now drops only chips the new view cannot evaluate and says so;
Player Stats and sample-size chips survive. The filter menu is grouped, the
popover explains the Player Stats scale and scope, and the chips row notes
when starter-count filters do not apply to Player Stats.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Cross-tab validation and latency against the production database

Not committed. The script runs the real helpers against the read-only role.

**Assumptions, declared before running:**
- Join key and grain are the committed ones: `(game_year, team_id, player_id)`, one row per team-player-season (`ff_ranked_df` already joins the two On/Off MVs this way).
- Tab 5's displayed Per Game values are `apply_ts_mode(add_ts_usage_pct(add_ts_two_point_stats(clean_ts_rows(normalize_ts_result_cols(raw)))), "Per Game")` over the reader called **with** the team list, which is exactly `live_result_df()`. Sourcing `server_tab5_traditional.R` only defines functions and constants.
- Invariant to reconcile against: the default-season MV and `get_player_traditional_from_games()` over the full season window are two independently computed relations for the same games, so they should agree per team-player. Any disagreement is reported, not rationalised; it would affect Tab 5 identically and is not introduced by this feature. The window must be the app's `season_date_bounds_for_year()`, not a typed date. During planning a hand-typed 2025-10-01 start produced 21 "disagreements", all caused by the season's two 2025-09-21 games; with the whole season both relations agreed on all 313 rows.
- Comparisons are exact (`TOL <- 1e-9`). Both sides are *displayed* values: the filter frame applies `round_display_1()`, and the Tab 5 side applies `round_display_1()` to its Per Game double, which is what `DT::formatRound(..., 1)` shows (review remark 1). For A, any difference is reported as a count; none is tolerated away.

**Files:** `<SCRATCH>/task6_validate.R` only.

- [ ] **Step 1: Write the script**

```r
# Task 6: cross-tab validation of On/Off Player Stats filter values (not committed).
suppressPackageStartupMessages({ library(shiny); library(dplyr) })
setwd("C:/Users/ariel/documents/on_off_israel_pbp/app")
readRenviron(".Renviron")
source("R/helpers.R")
source("R/server_tab5_traditional.R")

pg_pool <- DBI::dbConnect(
  RPostgres::Postgres(), host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
  sslmode = "require", bigint = "numeric", connect_timeout = 15L
)
db_get_query <- function(pool, query, params = NULL) {
  if (is.null(params)) DBI::dbGetQuery(pool, query) else DBI::dbGetQuery(pool, query, params = params)
}
cached_season_df <- function(key_parts, query_fun) query_fun()
guard_heavy_request <- function(...) TRUE

GY <- 2026L
PS_COLS <- names(PLAYER_STAT_FILTER_SOURCE)
TOL <- 1e-9

timed <- function(expr) {
  t0 <- Sys.time()
  value <- expr
  list(value = value, secs = round(as.numeric(difftime(Sys.time(), t0, units = "secs")), 2))
}

# The app's own season window (global.R), lifted without sourcing global.R,
# which builds the pool at source time. A hand-typed Oct 1 window silently
# drops the 2026 season's two 2025-09-21 games (found in planning).
.global_exprs <- parse("R/global.R")
for (e in .global_exprs) {
  if (is.call(e) && identical(as.character(e[[1]]), "<-") &&
      as.character(e[[2]])[1] %in% c("SEASON_DATE_BOUNDS", "season_date_bounds_for_year")) eval(e)
}
BOUNDS <- season_date_bounds_for_year(GY)

base_ctx <- list(
  game_year = GY, fast = FALSE,
  start_d = BOUNDS$start, end_d = BOUNDS$end,
  game_type_csv = NA_character_, opp_ids_csv = NA_character_,
  home_away = NA_character_, outcome = NA_character_,
  opp_rank_side = NA_character_, opp_rank_n = NA_integer_, opp_rank_metric = NA_character_,
  min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_,
  data_version = "validation"
)

compare_frames <- function(a, b, label) {
  # The fetcher fails closed to NULL on a read error; say so instead of letting
  # merge() report an opaque "'by' must specify uniquely valid columns".
  if (is.null(a) || is.null(b)) stop(label, ": a Player Stats read returned NULL (read error)")
  m <- merge(a, b, by = c("game_year", "team_id", "player_id"), suffixes = c(".a", ".b"), all = TRUE)
  bad <- vapply(PS_COLS, function(col) {
    x <- m[[paste0(col, ".a")]]; y <- m[[paste0(col, ".b")]]
    sum(xor(is.na(x), is.na(y)) | (!is.na(x) & !is.na(y) & abs(x - y) > TOL))
  }, integer(1))
  cat(sprintf("\n[%s] rows a=%d b=%d merged=%d\n", label, nrow(a), nrow(b), nrow(m)))
  print(bad)
  invisible(bad)
}

# Tab 5's displayed Per Game frame for a context, with the team list applied.
tab5_per_game <- function(ctx, team_ids_csv) {
  raw <- run_player_traditional_israel(
    pg_pool, session = NULL, guard_key = "validation", game_year = ctx$game_year,
    start_d = ctx$start_d, end_d = ctx$end_d, team_ids_csv = team_ids_csv,
    game_type_csv = ctx$game_type_csv, opp_ids_csv = ctx$opp_ids_csv,
    home_away = ctx$home_away, outcome = ctx$outcome,
    opp_rank_side = ctx$opp_rank_side, opp_rank_n = ctx$opp_rank_n,
    opp_rank_metric = ctx$opp_rank_metric,
    max_margin = NA_integer_, margin_status = NA_character_,
    max_time_remaining = NA_integer_, ot_margin_filter = FALSE,
    min_gn = ctx$min_gn, max_gn = ctx$max_gn, last_n_games = ctx$last_n_games
  )
  df <- raw %>% normalize_ts_result_cols() %>% clean_ts_rows() %>%
    add_ts_two_point_stats() %>% add_ts_usage_pct()
  per_game <- apply_ts_mode(df, "Per Game")
  out <- data.frame(game_year = GY, team_id = as.integer(per_game$team_id),
                    player_id = as.integer(per_game$player_id))
  out[PS_COLS] <- lapply(unname(PLAYER_STAT_FILTER_SOURCE), function(s) as.numeric(per_game[[s]]))
  # What Tab 5's table shows: formatRound(..., 1) on every column except GP.
  shown <- setdiff(PS_COLS, "ps_gp")
  out[shown] <- lapply(out[shown], round_display_1)
  out
}

onoff_rows <- function(ctx, team_csv = NA_character_) {
  db_get_query(pg_pool, paste0(
    'SELECT team_id, player_id, "ON Poss" AS on_poss FROM basketball_test.onoff_compute(',
    "$1::date,$2::date,$3::text,$4::int4,$5::int4,$6::numeric,$7::text,",
    "$8::text,$9::text,$10::text,$11::text,$12::text,$13::int4,$14::text,",
    "$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4)"),
    params = list(ctx$start_d, ctx$end_d, team_csv, 0L, 0L, -1e9, as.character(GY),
                  ctx$game_type_csv, ctx$opp_ids_csv, ctx$home_away, ctx$outcome,
                  ctx$opp_rank_side, ctx$opp_rank_n, ctx$opp_rank_metric,
                  ctx$min_gn, ctx$max_gn, ctx$last_n_games,
                  NA_integer_, NA_integer_, NA_integer_, NA_integer_, NA_integer_, NA_integer_))
}

coverage <- function(onoff, ps, label) {
  key_o <- ts_player_key(onoff$team_id, onoff$player_id)
  key_p <- ts_player_key(ps$team_id, ps$player_id)
  missing <- onoff[!key_o %in% key_p, , drop = FALSE]
  cat(sprintf("[%s] on/off rows=%d unmatched=%d unmatched with ON Poss>=100=%d\n",
              label, nrow(onoff), nrow(missing), sum(missing$on_poss >= 100, na.rm = TRUE)))
}

# A. Default season: MV fast path vs the per-game reader over the full window.
fast <- timed(fetch_player_stat_filter_frame(pg_pool, modifyList(base_ctx, list(fast = TRUE)), NULL))
full <- timed(fetch_player_stat_filter_frame(pg_pool, base_ctx, NULL))
cat(sprintf("A timings: MV %.2fs, full-window reader %.2fs\n", fast$secs, full$secs))
compare_frames(fast$value, full$value, "A MV vs full-window reader")
coverage(db_get_query(pg_pool, 'SELECT team_id, player_id, "ON Poss" AS on_poss FROM basketball_test.onoff_default_mv WHERE "Year" = $1::int4', list(GY)),
         fast$value, "A default")

teams <- sort(unique(fast$value$team_id))
team_csv <- paste(teams[1:2], collapse = ",")

# B. Restricted dates + two teams (teams narrow On/Off only).
ctx_b <- modifyList(base_ctx, list(start_d = as.Date("2025-12-01"), end_d = as.Date("2026-03-01")))
b <- timed(fetch_player_stat_filter_frame(pg_pool, ctx_b, NULL))
cat(sprintf("B timing: %.2fs\n", b$secs))
b_tab5 <- tab5_per_game(ctx_b, team_csv)
compare_frames(b$value[b$value$team_id %in% teams[1:2], ], b_tab5, "B filter frame vs Tab 5 with teams")
coverage(onoff_rows(ctx_b, team_csv), b$value, "B dates+teams")

# C. Opponents + game-number range.
ctx_c <- modifyList(base_ctx, list(opp_ids_csv = paste(teams[3:4], collapse = ","), min_gn = 5L, max_gn = 20L))
c_frame <- timed(fetch_player_stat_filter_frame(pg_pool, ctx_c, NULL))
cat(sprintf("C timing: %.2fs\n", c_frame$secs))
compare_frames(c_frame$value, tab5_per_game(ctx_c, NA_character_), "C filter frame vs Tab 5")
coverage(onoff_rows(ctx_c), c_frame$value, "C opponents+GN")

DBI::dbDisconnect(pg_pool)
```

- [ ] **Step 2: Run it**

```bash
"$RSCRIPT" <SCRATCH>/task6_validate.R 2>&1 | grep -v renv
```

Expected: A, B and C report **0** for every `ps_*` column. Measured during planning on production (2026-09-15, 3 clean runs): A 313/313 rows, B 33, C 157, all zero; every context `unmatched with ON Poss>=100=0`; MV 0.49-0.54s, reader 0.29-0.80s. One earlier run's full-window read returned NULL; it did not reproduce in three instrumented reruns and its message was not captured. If `a Player Stats read returned NULL` appears, rerun once and report how often it happens. It is exactly the case the Tab 1 error state exists for.

B and C are the Tab 5 match (acceptance criterion 3), and B also proves the teams-out-of-context decision. Record all counts and timings verbatim. **If any B/C count is non-zero, or A disagrees, stop and report the numbers. Do not introduce a tolerance.**

---

### Task 7: Full suite, visual check, and hand-off

**Files:** none changed unless a check fails.

- [ ] **Step 1: Full suite and comparison with the baseline**

```bash
"$RSCRIPT" <SCRATCH>/run_tests.R --baseline <SCRATCH>/baseline.tsv ALL; echo "exit=$?"
"$RSCRIPT" -e 'f <- commandArgs(TRUE); bad <- f[vapply(f, function(p) any(as.integer(readBin(p, "raw", file.size(p))) > 127L), logical(1))]; if (length(bad)) { cat("NON-ASCII:", bad, "\n"); quit(status = 1L) }; cat("ascii ok\n")' R/helpers.R R/server_tab1.R R/server_tab5_traditional.R; echo "exit=$?"
```

Expected: `no new failures` / `exit=0` (only the baseline's seven are listed as ignored), then `ascii ok` / `exit=0`.

- [ ] **Step 2: Launch the app locally**

Run in the background (Run App semantics, never select-all + Ctrl+Enter):

```bash
"$RSCRIPT" -e "shiny::runApp('.', port = 7788, launch.browser = FALSE)"
```

Wait for `Listening on http://127.0.0.1:7788`. The served page should contain 11 `nav-link` occurrences.

- [ ] **Step 3: Browser checks (Playwright MCP; if its first connection fails, retry once)**

On the On/Off Impact tab at 1280x900, then at 400x900:

1. Open `+ Filter`. The column select shows three groups: "Impact and on/off results", "Sample size", "Player Stats" (11 items). The note reads "Player Stats use the selected games. Percentages use 0-100."
2. Add `PTS/G >= 10`. The chip reads `PTS/G >= 10 ×`; row count drops; heat colours on remaining rows are unchanged from before the chip (screenshot both).
3. Add `Net >= 5`. Both chips show and rows satisfy both.
4. Switch to Four Factors, then Shot Profile. `PTS/G` persists; `Net` is removed with a "Removed filters not available in Four Factors: Net" notification.
5. Set Own lineup starters `>= 3`. The chips row shows "Player Stats ignore starter-count filters", and the popover note gains the starter sentence.
6. Pick a player and a context (narrowed dates). In Player Stats (Per Game, same dates) read their PTS/G. Back in On/Off, set `PTS/G >=` that value and confirm they are included, then set it 0.1 above and confirm they are excluded.
7. The popover stays above the DataTable header and horizontal scroller; long labels don't overflow; chips wrap; dark theme is consistent.
8. `Reset to defaults` clears every chip.

Capture the browser console. There must be no new errors compared with a load of `main` (the known Home-load defects are pre-existing).

Stop the app when done.

- [ ] **Step 4: Finish the branch**

Use superpowers:finishing-a-development-branch. Pushing to `main` deploys to Connect Cloud, so merge/push only with the user's explicit go-ahead. After deploy, repeat checks 1-4 of Step 3 on the live app (acceptance criterion 10) and read the server log:

```bash
"$RSCRIPT" -e 'rsconnect::showLogs(appPath="app", account="ibpl-stats", server="shinyapps.io", entries=1500)'
```

Report to the user: Task 6 numbers and timings, Step 1 suite comparison, screenshots, and the spec decisions listed at the top of this plan (especially the teams-out-of-context deviation).
