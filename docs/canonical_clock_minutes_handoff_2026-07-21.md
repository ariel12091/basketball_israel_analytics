# Canonical clock/minutes implementation handoff — 2026-07-21

## Status at handoff

- Repository: `C:\Users\ariel\documents\on_off_israel_pbp`
- Branch/HEAD: `main` at `797f30bc45d3de0d193acb0a61c8c1a41206f819`
- Objective: make lineup/player/team minute calculations robust to dubious source action clocks, add data-quality checks, and backfill persisted minute outputs only because they actually depend on the old calculation.
- Source clocks are deliberately preserved. The implementation adds canonical elapsed/segment timing fields; it does not rewrite `quarter`, `time`, or `end_game_seconds_remaining`.
- Code changes are present in the dirty worktree but are not committed.
- The production database backfill and all dependent refreshes completed successfully on 2026-07-22 through the resumable online path.
- Database state: five canonical columns populated, supporting index valid, canonical MVs live with app grants, no staging leftovers, no idle transaction or lock waiter, and rolling minute parity at zero mismatches.
- Game 115 validates at 39.867 minutes for both teams. Canonical timing validation reports zero missing rows, zero negative/inconsistent durations, and zero team-coverage failures above five seconds.
- The database-backed test suite passes. The DQ report still fails on known source/data defects: incomplete game 160, four 2–5 second jitter rows in games 62506/62511, historical player-minute conservation defects, and the existing source clock/clutch anomalies.

The worktree already contained many unrelated user changes and untracked files. Preserve them. Only the files listed under “Implementation” below belong to this clock/minutes work (plus the temporary diagnostic scripts listed separately).

## Why the change is needed

The 2025–26 source contains many impossible derived shot clocks. The prior audit found:

- 221 games examined.
- 29,678 shots.
- 1,845 impossible shots (6.22%), all with a negative derived shot clock.
- Seven games with a clear backward period/clock insertion: game IDs `115`, `122`, `127`, `146`, `158`, `166`, and `168`.

The possession rows generally still exist; many events are merely stacked on or inserted at dubious timestamps. This means event counts and possession counts are usually much less affected than duration calculations.

The old unfiltered minute calculation was vulnerable because it used a segment’s broad action-clock range, effectively `MAX(end_game_seconds_remaining) - MIN(end_game_seconds_remaining)`. One delayed action from an earlier period can therefore make a lineup segment span a large false interval.

Concrete example: game 115, action `1150630` is a Q3 assist at a displayed clock corresponding to `1200` seconds remaining, parent action `1150578`, but it appears between Q4 actions whose clocks are around `600`. The ordered ETL clock therefore goes `600 -> 1200 -> 600`. The old range made the affected segment 600 seconds instead of about 103 seconds and produced about 48.150 team minutes instead of about 39.867.

Other period regressions identified:

| Game | Action | Source period/clock | Type/parent | Immediately follows |
|---|---:|---|---|---|
| 115 | 1150630 | Q3, 1200 | assist, parent 1150578, segment 36 | Q4, 600 |
| 122 | 1220434 | Q2, 1800 | assist, parent 1220377, segment 27 | Q3, 1200 |
| 127 | 1270809 | Q3, 600 | assist, parent 1270480, segment 51 | Q4, 0 |
| 146 | 1460400 | Q1, 1800 | foul drawn, self-parent, segment 24 | Q2, 1200 |
| 158 | 1580656 | Q3, 1200 | rebound, parent 1580625, segment 42 | Q4, 600 |
| 166 | 1660418 | Q2, 1800 | assist, parent 1660320, segment 25 | Q3, 1200 |
| 168 | 1680415 | Q1, 1800 | assist, parent 1680064, segment 24 | Q2, 1200 |

## Alternatives tested and rejected

### Group by quarter

This does not solve the problem and changes too much normal data. In the 2026 audit, quarter grouping changed 434 of 442 team-games. It undercounts legitimate lineup segments that remain unchanged across quarter breaks. It also left game 115 at about 47.983 minutes because the corrupted segment contains legitimate Q3 rows as well as the delayed Q3 row.

### First/last action within each segment

Ordered endpoints were much better than `MAX-MIN`: they changed 38 of 442 team-games, reduced team-minute deviations greater than one minute from 22 rows to 10, and fixed game 115 to about 39.867. However, action endpoints do not represent the full time a lineup remains on court after its last recorded action, especially at game end.

### `final_end_id` boundaries

Rejected because every team-game was missing a usable final boundary.

## Chosen calculation

Use a canonical game-elapsed clock and consecutive lineup-segment starts:

1. Derive `event_elapsed_seconds` without changing raw source fields:
   - regulation: `2400 - end_game_seconds_remaining`
   - overtime: `2400 + (quarter - 5) * 300 + (300 - end_game_seconds_remaining)`
   - invalid period/range combinations remain null.
2. For each `(game_id, team_id, lineup_hash, segment_id)`, define the segment start from the canonical elapsed value on its earliest action ID.
3. Define its end as the next segment’s start for that game/team. For the last segment, use the maximum retained canonical event time.
4. Store `segment_seconds = GREATEST(segment_end_elapsed_seconds - segment_start_elapsed_seconds, 0)`.
5. Aggregate unfiltered minutes from one duration per segment, using `MAX(segment_seconds)` across the repeated action rows.
6. For filters that genuinely select action subsets, use canonical ordered endpoints. This is safer than the raw clock range, but see the remaining clutch-window caveat below.

Audit results supporting this choice:

- 2025–26: all 442 team-games had segment starts; there were no regressing consecutive segment starts; only six team-game minute rows exceeded a one-minute deviation, all belonging to incomplete games `184`, `380`, and `160`; game 115 became 39.8667 minutes.
- All retained seasons: 878 team-games; no missing segment starts. Only games `62506` and `62511` (both teams) showed small 2–5 second consecutive-start jitter. Total team coverage was still conserved within five seconds.
- The first segment began at elapsed zero in every audited team-game.

## Implementation in the worktree

New production files:

- `sql/functions/refresh_segment_clock_fields_for_games.sql`
  - populates `event_elapsed_seconds`, `clock_regression_seconds`, `segment_start_elapsed_seconds`, `segment_end_elapsed_seconds`, and `segment_seconds`;
  - accepts an optional game-ID scope;
  - joins calculated durations back on the full segment key, including `lineup_hash` and `segment_id`.
- `sql/migrations/2026-07-21_canonical_segment_clock_minutes.sql`
  - adds the five nullable fields to `basketball_test.df_pts_poss_lineups_longer_mv`;
  - adds an index on `(game_id, team_id, segment_id)`.
- `etl/backfill_canonical_segment_minutes.R`
  - defaults to a complete transactional rehearsal followed by `ROLLBACK`;
  - delegates `--apply` to the resumable online path;
  - keeps a bounded statement timeout for rollback rehearsals so a lost client cannot leave an unbounded backend.
- `etl/apply_canonical_segment_minutes_online.R`
  - commits the metadata-only columns immediately, creates the supporting index concurrently, batches base/player refreshes, builds MVs under staging names, cuts them over in a short retryable rename transaction, and refreshes remaining consumers independently;
  - sets server-side statement/lock timeouts and uses a session advisory lock.
- `etl/finish_canonical_segment_minutes_refresh.R`
  - resumable finalizer for sub-lineups and rolling metrics;
  - refreshes sub-lineups in 50-game batches with `enable_nestloop = off`, then refreshes `team_metrics_rolling_mv` concurrently.
- `app/tests/testthat/test-clock-minute-contracts.R`
  - checks that raw clocks are not updated, canonical timing drives minutes, the old raw `MAX-MIN` formula is absent from production SQL, and the new DQ checks exist.

Modified production files:

- `sql/materialized_views/df_pts_poss_longer.sql`
- `sql/functions/refresh_df_pts_poss_lineups_longer_for_games.sql`
- `sql/materialized_views/sub_lineups_by_day.sql`
- `sql/materialized_views/lineup_four_factors_by_game.sql`
- `sql/materialized_views/player_four_factors_by_game.sql`
- `sql/materialized_views/player_traditional_stats_mv.sql`
- `sql/materialized_views/onoff_mv.sql`
- `sql/functions/refresh_sub_lineups.sql`
- `sql/functions/refresh_sub_lineups_incremental.sql`
- `sql/functions/refresh_onoff_default_for_games.sql`
- `sql/functions/refresh_player_four_factors_by_game_for_games.sql`
- `sql/functions/fetch_lineups_all.sql`
- `sql/functions/fetch_lineups_four_factors.sql`
- `sql/functions/get_player_traditional_dynamic.sql`
- `app/R/server_tab3.R`
- `app/R/server_tab6_team_stats.R`
- `etl/etl_full.R`
- `etl/run_data_quality_report.R`
- `app/tests/testthat/test-data-shape-db.R`

Important behavior changes:

- The full and incremental ETL now require and invoke the segment-clock helper after rebuilding affected game rows.
- Unfiltered persisted minutes use one canonical duration per segment.
- Filtered app/RPC paths use canonical ordered endpoints rather than raw clock extrema.
- The ETL’s team-minute integrity check uses canonical segment durations.
- DQ check `T` uses canonical segments; player-minute conservation check `X` uses them as well.
- New DQ check `AH_canonical_segment_timing` checks missing canonical timing, inconsistent/negative duration, and team coverage conservation within five seconds.
- Existing source anomaly checks `AA`–`AD` remain useful. The raw anomaly is exposed separately via `clock_regression_seconds`.

Materialized-view rebuild order encoded in the backfill is important: drop `team_four_factors_mv`, `lineup_four_factors_by_game`, `mv_lineup_four_factors_by_game`, and `player_traditional_stats_mv`; then recreate the latter three before `team_four_factors_mv`. The dependency audit found `team_four_factors_mv` as the relevant dependent relation.

## Validation completed

- R parsing passed for the backfill, ETL, DQ report, both changed app servers, and both affected test files.
- Targeted `git diff --check` passed; only existing line-ending warnings appeared.
- A repository search confirmed the production `sql/`, `etl/`, and `app/` paths no longer contain the old raw `MAX(end_game_seconds_remaining) - MIN(...)` minute formula.
- Read-only audit queries produced the results described above.
- The full database rehearsal has **not yet been observed reaching its final success/rollback message**. That is the immediate unfinished validation.

## Deployment outcome and remaining next steps

Completed on 2026-07-22:

- Online base timing backfill: 528,033 action/team rows in five committed batches.
- Staged MV builds and atomic cutover with exact row-count parity: 45,246 lineup-day rows, 45,246 lineup-four-factor rows, 28 team-four-factor rows, and 568 traditional-player rows.
- Player four-factor refresh: nine committed batches covering all 439 retained games.
- Sub-lineups refresh: ten committed 50-game-or-smaller batches, 73,033 touched rows. Adding `SET enable_nestloop = off` changed this from a non-completing multi-hour statement to roughly seconds per batch.
- `team_metrics_rolling_mv` refreshed concurrently; minute mismatches against `team_metrics_by_game_mv` are zero.
- Database-backed tests passed. Latest DQ artifacts are under `etl/logs/data_quality/` (`data_quality_report_20260722_070833.md` and matching summary CSV).
- Post-refresh health: 851 MB database, no lock waiters/idle transactions, autovacuum current, and negligible dead rows on refreshed targets.

Remaining work:

1. Review the focused dirty-tree diff and commit only the clock/minutes files if requested. Unrelated user changes and untracked artifacts remain in the repository.
2. The Shiny app source changes in Tab 3/6 are not deployed. Do not deploy the whole dirty app blindly; `app/rsconnect/shinyapps.io/ibpl-stats/onoff-shiny.dcf` and other files contain unrelated changes.
3. Decide separately whether to fix non-contiguous clutch-window interval summing, the historical player-minute conservation rows, and cold-storage stale-key behavior. They are not blockers for this canonical timing deployment.

## Known limitations and follow-up

- The raw feed remains unreliable; this implementation isolates timing damage rather than pretending to repair the source history.
- Small 2–5 second segment-start jitter in games `62506` and `62511` moves a few seconds between adjacent lineups but preserves team coverage. Keep it visible in DQ rather than silently editing action clocks.
- Games `160`, `184`, and `380` are incomplete/early-ending data cases. In particular, game 160 remains around 36.75 team minutes. Canonical timing should not manufacture missing game time.
- Filtered/clutch queries now use canonical ordered endpoints, but a filter selecting multiple non-contiguous time windows can still bridge the gaps between those windows. A rigorous follow-up would intersect each canonical segment interval with the selected period/clock windows and sum the intersections. That is separate from the present source-regression fix and should not be claimed as solved.
- `etl/cold_storage.R` has a separate stale-row risk: combining new and old rows by distinct keys can retain obsolete keys, while the live table is cleaner. This was identified during the broader DQ review but is not changed in this implementation.
