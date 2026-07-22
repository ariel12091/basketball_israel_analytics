# Canonical Clock and Lineup Minutes

## Why this exists

Provider play-by-play actions are not always ordered chronologically by action ID. Delayed annotations can carry an earlier quarter or clock into a later part of the action stream. Raw clock fields must remain unchanged for auditing, but they are not safe endpoints for lineup-minute calculations.

The observed 2025-26 defects included seven games with period regressions. In game 115, for example, action `1150630` is a delayed Q3 assist inserted among Q4 actions. Using raw `MAX(clock) - MIN(clock)` segment ranges inflated the team total to roughly 48 minutes even though all possessions were present.

## Canonical model

`basketball_test.df_pts_poss_lineups_longer_mv` keeps the source clock and adds:

- `event_elapsed_seconds`
- `clock_regression_seconds`
- `segment_start_elapsed_seconds`
- `segment_end_elapsed_seconds`
- `segment_seconds`

Valid regulation events use:

```text
event_elapsed_seconds = 2400 - end_game_seconds_remaining
```

Overtime events use:

```text
event_elapsed_seconds =
  2400 + (quarter - 5) * 300 + (300 - end_game_seconds_remaining)
```

For each `(game_id, team_id, lineup_hash, segment_id)`:

1. The segment starts at the canonical elapsed time of its lowest action ID.
2. It ends at the next segment's start.
3. The final segment ends at the maximum retained elapsed time for that game/team.
4. `segment_seconds` is `GREATEST(end - start, 0)`.

Consumers deduplicate at the full segment key and count the duration once. Possession, point, and shot statistics remain split by `type_lineup`.

Do not restore minute formulas based on raw clock extrema, `first()`/`last()` source clocks, or quarter-level grouping. Quarter grouping undercounts legitimate lineup continuity across period boundaries; raw endpoints remain vulnerable to delayed actions.

## Runtime integration

- Full table definition and canonical backfill logic: `sql/materialized_views/df_pts_poss_longer.sql`
- Incremental canonical refresh: `sql/functions/refresh_segment_clock_fields_for_games.sql`
- Normal ETL entry point: `refresh_df_pts_poss_lineups_longer_for_games()` calls the canonical refresh for touched games.
- Existing-schema migration record: `sql/migrations/2026-07-21_canonical_segment_clock_minutes.sql`
- Minute consumers: lineup, on/off, player traditional, team, and sub-lineup SQL definitions under `sql/functions/` and `sql/materialized_views/`
- Data-quality checks: `etl/run_data_quality_report.R`, especially checks AA through AH
- Contract tests: `app/tests/testthat/test-clock-minute-contracts.R`

The production `basketball_test` schema was backfilled and validated on 2026-07-22. Raw source clocks were not rewritten.

## Database deployment lessons

- Do not hold an `ACCESS EXCLUSIVE` table lock across a long multi-object refresh. Commit metadata-only DDL quickly, build supporting indexes or replacement materialized views concurrently when possible, update large tables in bounded game batches, and keep cutover transactions brief.
- A killed or timed-out database client can leave its server transaction and lock running. After a disconnect during DDL, inspect `pg_stat_activity` and `pg_locks`, then terminate any confirmed orphan before retrying.
- `refresh_segment_clock_fields_for_games()` locally sets `enable_nestloop = off`. PostgreSQL severely under-estimated the function's large CTE update join and selected a catastrophic nested loop. Keep the planner guard scoped to this function.
- The one-time production backfill and analysis runners are intentionally local-only and are not supported runtime ETL entry points.

## Remaining distinctions

- Impossible derived shot-clock values and source clock regressions are source-quality signals; they do not automatically imply missing possessions.
- Clutch filters still use action-level source timing, so the data-quality report flags cases where bad ordering can change clutch membership.
- Team metric offense/defense minute allocation semantics are separate from canonical total floor-time conservation.
