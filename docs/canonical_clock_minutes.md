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

Before assigning segment boundaries, order actions by ID within each game/team
and compute a monotonic timeline as the running maximum of
`event_elapsed_seconds`. This prevents a provider clock that jumps backward and
then catches up from counting the repeated interval twice. The raw
`event_elapsed_seconds` and `clock_regression_seconds` audit fields remain
unchanged.

For each `(game_id, team_id, lineup_hash, segment_id)`:

1. The segment starts at the monotonic elapsed time of its lowest action ID.
2. It ends at the next segment's start.
3. The final segment ends at the buzzer of the final period -- `2400 + 300 * (max(quarter) - 4)`, or the maximum monotonic elapsed time if that is later -- when the feed reaches the fourth period or an overtime. The clock runs out with that lineup on the floor, and a substitution would have been an event. When the feed stops before the fourth period (games 184 and 380), the final segment ends at the maximum monotonic elapsed time: that game is truncated and keeps partial time.
4. `segment_seconds` is `GREATEST(end - start, 0)`.

Consumers deduplicate at the full segment key and count the duration once. Possession, point, and shot statistics remain split by `type_lineup`.

Do not restore minute formulas based on raw clock extrema, `first()`/`last()` source clocks, or quarter-level grouping. Quarter grouping undercounts legitimate lineup continuity across period boundaries; raw endpoints remain vulnerable to delayed actions.

## Runtime integration

- Full table definition and canonical backfill logic: `sql/materialized_views/df_pts_poss_longer.sql`
- Incremental canonical refresh: `sql/functions/refresh_segment_clock_fields_for_games.sql`
- Normal ETL entry point: `refresh_df_pts_poss_lineups_longer_for_games()` calls the canonical refresh for touched games.
- Existing-schema migration record: `sql/migrations/2026-07-21_canonical_segment_clock_minutes.sql`
- Backward-clock correction migration: `sql/migrations/2026-09-12_monotonic_segment_clock_minutes.sql`
- Final-buzzer correction (2026-10-19): the function above plus a recompute of every game; see the section below
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

## Final-buzzer seconds (2026-10-19)

Until 2026-10-19 the last segment ended at the game's last recorded event. When the clock ran out with nothing happening, those seconds were dropped: 864 of 950 team-games were short, about 10 s on average and 195 s in game 160 (its feed shows only clock ticks over the last 3:15, then `end-of-game`). Team and player minutes came out about 0.4% low and pace about 0.4% high. Found by `scripts/compare_official_boxscores.R` against the official box scores.

After the change every one of the 946 team-games whose feed reaches the fourth period totals exactly 2400 s (+ 300 per overtime). Game 62511 moved from 2402 s to 2400 s. Games 184 and 380 stop in Q2 and Q3 and are unchanged.

Games 140 and 143 were stored from a feed that stopped about 50 s early with points missing; the provider's feed now has those events. Their final ~50 s now go to the last stored lineup. Reloading them (`etl_full(game_ids = c(140, 143))`) replaces that with the real lineups.
