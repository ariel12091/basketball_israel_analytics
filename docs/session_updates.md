# Session Updates

## 2026-07-28
- Completed the Tab 2 starters fast path from
  `docs/superpowers/plans/2026-07-27-starters-fast-path.md`.
- `mv_lineup_totals_by_day` and `lineup_four_factors_by_game` now key rows by
  both own starters (`num_starters`) and `opp_starters`, with
  `NULLS NOT DISTINCT` identity and starter-selective indexes.
- Minutes retain the canonical segment-time budget and are assigned per
  contiguous opponent-count window only when that window has offense rows.
  The current source has zero segments with multiple opponent counts; the
  window logic is retained for future data without changing current totals.
- `fetch_lineups_all` and `fetch_lineups_four_factors` now treat only
  margin/time parameters as clutch. Starters-only queries use uniform own/opp
  predicates on the pre-aggregated MVs; the raw clutch path remains in place.
- Execution found a plan-assumption mismatch: the old FF raw branch used the
  wrong per-type starters mapping. Correct uniform semantics change FF starter
  row counts (5v5: 833 to 751), matching Summary's 751 rows.
- Verification: all 15 general cases and clutch+5v5 are byte-identical;
  collapsed poss/points/shooting differences are zero; the team FF snapshot is
  identical; local tests and both DB test files pass.
- Pooler medians (3 runs): Summary 5v5 `0.998s -> 0.427s`; FF 5v5
  `1.109s -> 0.356s`; FF own-5 `2.848s -> 0.375s`.

## 2026-03-03
- Fixed Game Logs tab date-range mismatch in `app/R/server_tab4.R`.
- Root cause: tab used static defaults (`DEFAULT_START`/`DEFAULT_END`) while active season default is `2026`.
- Change: `gl_dates` now syncs to `shared$season_date_bounds(input$game_year)` on season change.
- Change: reset action now restores season bounds (including min/max) instead of setting dates to `NA`.
- Impact: Game Logs loads correctly on default season without manual date adjustment.

- Fixed Traditional rank delta rendering in `app/R/server_tab3.R`.
- Change: `show_delta` now follows `tr_delta_enabled()` instead of hard-disabled flag.
- Impact: rank delta arrows now appear in Traditional mode (team/opponent) when baseline rules allow deltas.

## 2026-03-07
- Aligned clutch margin semantics in `sql/functions/get_player_traditional_dynamic.sql` with other clutch-enabled SQL functions.
- Change: clutch margin/status checks now use pre-possession margin (subtracting `team_score` where relevant), matching the logic already used in:
  - `sql/functions/fetch_lineups_all.sql`
  - `sql/functions/fetch_lineups_four_factors.sql`
  - `sql/functions/get_team_ratings_dynamic.sql`
  - `sql/functions/get_team_four_factors_dynamic.sql`
- Impact: clutch filtering behavior is now consistent across Tab 2/3 and Tab 5 Traditional outputs for boundary possessions.
