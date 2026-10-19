# On/Off Player-Stat Filters Handoff

**Date:** 2026-09-15  
**Branch:** `shiny/onoff-player-stat-filters`  
**Plan:** `docs/superpowers/plans/2026-09-15-onoff-player-stat-filters.md`  
**Spec:** `docs/superpowers/specs/2026-09-15-onoff-player-stat-filters-design.md`

## Current status

Tasks 1-4 are committed. Task 5 is implemented in the working tree and its
focused tests pass, but it has not been committed. Task 6 production validation
started and exposed a real fast/filtered-frame normalization failure that still
needs diagnosis. Task 7 has not started.

Do not merge or deploy this branch yet.

## Committed feature work

| Task | Commit | Summary |
|---|---|---|
| 1 | `c154f8a` | Move Player Stats normalization helpers into `helpers.R` |
| 2 | `0e45e29` | Share the Israeli Player Stats MV and filtered readers |
| 3 | `275a376` | Add `ps_*` filter helpers, displayed-value rounding, optgroup support, fail-closed filtering, and tests |
| 4 | `ef21a44` | Wire lazy Player Stats filtering into Tab 1 |

The committed Task 4 implementation is in `app/R/server_tab1.R`. It:

- adds the eleven Player Stats fields to every On/Off view's filter choices;
- reads Player Stats only while a `ps_*` chip is active;
- uses the season MV for the default context and the filtered reader otherwise;
- memoizes the last successful context so threshold edits do not re-query;
- joins by season + team + player before filtering;
- keeps Player Stats columns out of the visible DataTable contract;
- sends only non-Player-Stats filters to the existing renderers;
- fails closed with a one-column error table when the Player Stats frame cannot
  be evaluated.

## Uncommitted Task 5 work

The working tree currently modifies:

- `app/R/server_tab1.R`
- `app/tests/testthat/test-onoff-player-stat-filters.R`

The Task 5 changes:

- preserve Player Stats and sample-size chips across Summary, Four Factors,
  and Shot Profile;
- remove only view-specific incompatible chips and show a notification;
- group the `+ Filter` menu into impact/on-off, sample size, and Player Stats;
- add the Player Stats scale/context note to the popover;
- show a persistent note when starter-count filters and Player Stats filters
  are active together;
- expose `player_stat_context` through the test-only server return;
- add the previously missing context-forwarding/invalidation test for game
  type, opponent, home/away, outcome, opponent rank, GN, and last N.

These changes were made with `apply_patch`; preserve unrelated working-tree
changes when continuing.

## Verification completed

Run through a clean R startup (`Rscript --vanilla`) because the repository
startup profile caused otherwise focused commands to run for several minutes
without reporter output.

The following pass with exit code 0:

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' --vanilla -e "invisible(parse(file='R/helpers.R')); invisible(parse(file='R/server_tab1.R')); invisible(parse(file='R/server_tab5_traditional.R')); cat('PARSE_OK\n')"

& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' --vanilla -e "testthat::test_file('tests/testthat/test-onoff-player-stat-filters.R', reporter='summary')"

& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' --vanilla -e "testthat::test_file('tests/testthat/test-player-traditional-readers.R', reporter='summary')"

& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' --vanilla -e "testthat::test_file('tests/testthat/test-server-tabs-smoke.R', reporter='summary')"

& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' --vanilla -e "testthat::test_file('tests/testthat/test-primary-table-render-smoke.R', reporter='summary')"

& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' --vanilla -e "testthat::test_file('tests/testthat/test-tab-wiring.R', reporter='summary')"
```

The output contains many locale warnings because `LC_ALL=C.UTF-8` is not
available in this Windows environment, plus a warning that DT was built under R
4.4.3. There were no test failures.

## Production validation blocker

A read-only Task 6 script was created at:

`tmp/validate_onoff_player_stat_filters.R`

It connects using `app/.Renviron` and compares:

1. the default-season Player Stats MV with the full-window reader;
2. a restricted date/team context with the Player Stats reader;
3. an opponent + GN context with the Player Stats reader.

The sandboxed run could not open the Supabase connection. The approved
read-only run connected successfully, then failed before comparisons with:

```text
Error in filter():
In argument: !is.na(Player)
Caused by error: object 'Player' not found
```

The stack is:

```text
fetch_player_stat_filter_frame()
  -> player_stat_filter_projection()
  -> clean_ts_rows()
```

A separate read-only schema query confirmed that
`basketball_test.player_traditional_stats_mv` for 2026 contains:

```text
game_year, team_id, player_id, team_name, firstname, lastname, player_name,
gp, poss_on_floor, minutes, pts, reb, oreb, dreb, ast, stl, blk, dfl, tov,
fgm, fga, 3pm, 3pa, ftm, fta, fg_pct, tp_pct, ft_pct, efg, ts, usg_pct
```

`normalize_ts_result_cols()` is intended to rename `player_name` to `Player`,
so the next step is to instrument the validation script immediately before
`player_stat_filter_projection()` and identify which path/frame loses the
column. Do not paper over the problem by weakening `clean_ts_rows()` until the
actual frame shape is known. The mocks currently always provide the expected
name shape and therefore did not catch this production failure.

Useful checks for the next session:

```r
raw_fast <- fetch_player_traditional_season_israel(pg_pool, 2026L, "debug")
print(names(raw_fast))

raw_slow <- run_player_traditional_israel(
  pg_pool, session = NULL, guard_key = "debug", game_year = 2026L,
  start_d = as.Date("2025-10-01"), end_d = as.Date("2026-07-01"),
  team_ids_csv = NA_character_, game_type_csv = NA_character_,
  opp_ids_csv = NA_character_, home_away = NA_character_,
  outcome = NA_character_, opp_rank_side = NA_character_,
  opp_rank_n = NA_integer_, opp_rank_metric = NA_character_,
  max_margin = NA_integer_, margin_status = NA_character_,
  max_time_remaining = NA_integer_, ot_margin_filter = FALSE,
  min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_
)
print(names(raw_slow))
print(names(normalize_ts_result_cols(raw_slow)))
```

Add a regression test using the exact production column shape once the cause is
identified, then rerun `tmp/validate_onoff_player_stat_filters.R`.

## Remaining work

1. Diagnose and fix the production-frame `Player` normalization failure.
2. Add a regression test that fails on the observed production shape.
3. Rerun the focused tests listed above.
4. Rerun Task 6 validation and record row counts, mismatch counts, and timings.
   Filtered contexts B and C must have zero mismatches. Investigate rather than
   relax tolerance if they do not.
5. Review the Task 5 diff, then commit only:
   - `app/R/server_tab1.R`
   - `app/tests/testthat/test-onoff-player-stat-filters.R`
6. Run the full suite and compare failures with the baseline recorded in the
   implementation plan. Do not require pre-existing failures to disappear.
7. Launch the app locally and complete desktop/mobile browser checks for menu
   optgroups, chip addition/removal, AND behavior, view-switch retention,
   starter disclosure, wrapping, z-index, and dark theme.
8. Resolve or explicitly accept the mixed-line-ending `git diff --check`
   reports before merge. The committed feature files preserve the repository's
   existing mixed CRLF/LF pattern, so many added CRLF lines appear as trailing
   whitespace to Git.
9. Do not merge, push, or deploy without explicit user approval.

## Working-tree caution

The repository was already heavily dirty before this feature. Current unrelated
tracked changes include `.gitignore`, `AGENTS.md`, `CLAUDE.md`, `PROJECT.md`, the
rsconnect DCF, and `test-team-hub-ui.R`; there are also many unrelated untracked
files and directories. Stage only the two Task 5 files (plus a deliberate
regression-test fix if needed). The plan and spec are themselves currently
untracked.

