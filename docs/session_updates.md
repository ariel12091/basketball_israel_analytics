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
- Live deployment is complete: rebuilt the L2-L4 objects, redeployed both
  lineup fetch functions, rebuilt dependent team metrics, and reapplied the
  app security grants. Final DB parity and shape suites passed.
- Ongoing maintenance requires no special ETL branch: `etl_full()` Phase 4
  refreshes `mv_lineup_totals_by_day` at L2 and
  `lineup_four_factors_by_game` at L3. `rebuild_all_mvs()` recreates both from
  their updated SQL files, including the starter indexes; SQL functions remain
  a separate deployment step.
- Merged and pushed to `main` as `23e6c51`. No starters-fast-path work remains.

## 2026-07-20
- Completed and documented the offensive-rebound point-impact audit and the
  implications for the four-factor regression.
- Validated direct OREB weight: `0.4114` points per 100 possessions per +1pp
  OREB%, with context-adjusted estimates near `0.415`.
- Starter-count controls did not materially change the estimate.
- Saved the full methodology, statistical interpretation, decisions, and
  proposed next-stage possession model in:
  - `docs/four_factor_impact_research_memory_2026-07-20.md`

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

## 2026-07-27
- SQL function performance tuning (branch `sql/perf-function-tuning`, plan `docs/superpowers/plans/2026-07-27-sql-function-perf-tuning.md`). All seven app functions redeployed; outputs verified byte-identical across 15 before/after cases.
- `get_team_ratings_dynamic`: single clutch-filtered scan with per-game pre-aggregation (`game_agg`) replaces the former double raw-MV scan; corner-3 flags via `LEFT JOIN shot_zones` instead of two per-row `EXISTS`; `team_names` CTE replaces `full_rosters` dedupe GROUP BY (verified: one team_name spelling per season, all years).
- All functions: correlated last-N-games subquery replaced by one windowed `schedule_ranked` CTE (pattern from `fetch_lineups_all`); `SET plan_cache_mode = force_custom_plan` added.
- Why force_custom_plan: during A/B testing the OLD `get_player_traditional_dynamic` hit the 120s statement timeout after repeated calls on one connection — the plpgsql generic-plan cliff, reproduced live. Cost of the insurance: ~0.1-0.3s extra planning per call on the big lineup queries.
- Measured (min-of-7, pooler): onoff last-N −40% (0.92→0.56s); team_ff filtered −20% (0.33→0.26s); team_rt clutch −16% (0.47→0.40s); lineup functions +0.1-0.35s planning premium; trad_full ~equal (and no more cliff).
- Findings deferred (output-changing, need a decision): (1) `four_factors_compute` emits a duplicate row for player_id 1094 / team_id 8 in 2026 — cross-season name variant in `full_rosters`, year-scoped roster join would fix it; (2) `fetch_lineups_four_factors` non-clutch filtered branch silently ignores `p_min_gn`/`p_max_gn`/`p_last_n_games` (sync-drift vs `fetch_lineups_all`); (3) stale 23/25-arg overloads of `fetch_lineups_all` still deployed in DB — sparse named calls are ambiguous.
- New helper: `scripts/deploy_sql_functions.R` (transactional whole-file deploy; ALWAYS re-run `scripts/apply_db_security.R` with `CONFIRM_DB_SECURITY_APPLY=1` after — DROP FUNCTION wipes app_readonly EXECUTE grants). Verify harness: `scripts/perf_tuning_baseline.R`.
- FOLLOW-UP (same day): fixed deferred finding (2) — `fetch_lineups_four_factors` non-clutch filtered branch now applies `p_min_gn`/`p_max_gn`/`p_last_n_games` (schedule_ranked pattern). Parity vs `fetch_lineups_all` verified exact (was 1966-vs-706 lineups on last-N=5); 15 unaffected cases byte-identical. Deployed + grants restored.
- FOLLOW-UP 2 (same day): fixed deferred finding (1) — `four_factors_compute` roster join now season-scoped (`fr2.game_year = p_game_year` + MIN names). Root cause was provider id recycling, not a spelling variant: 1094/8 = Imri Shavit (2025, Netanya) vs Gil Noyovitch (2026, Haemek); 2026 output had a phantom Shavit row with Noyovitch's stats. Only that row removed; all else identical. Deployed + grants restored.
- FOLLOW-UP 3 (same day): fixed deferred finding (3) — dropped stale 23/25-arg overloads of `fetch_lineups_all` and `fetch_lineups_csv_v2` (migration `sql/migrations/2026-07-27_drop_stale_lineup_fn_overloads.sql`). Only 29-arg versions remain; sparse named-arg calls no longer ambiguous; security audit clean.
