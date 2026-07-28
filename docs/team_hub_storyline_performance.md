# Team Hub Storyline Performance

Date: 2026-07-28

## Problem

The Team Hub Storylines card originally appeared about 10-15 seconds after the
rest of the Home tab. During that time there was no visible indication that
more content was loading, and the late card caused a layout jump.

The main cause was six sequential calls to
`basketball_test.get_team_ratings_dynamic()`:

1. Starter-heavy lineups
2. Bench-heavy lineups
3. Clutch
4. Last 10 games
5. Games against the Top 4
6. Games against the Bottom 4

## Changes made

### 1. Added an immediate loading state

`team_hub_ui()` now includes a Storylines placeholder in the initial HTML. It:

- reserves the final card's space;
- displays a spinner and `Analyzing team splits...`;
- is replaced in place when Storylines finish;
- resolves to an explicit empty state if no storyline qualifies.

Files:

- `app/R/mod_team_hub.R`
- `app/www/app.css`

Commit: `c3ffd1e`

### 2. Prioritized Storylines before season prewarming

The Storylines output now has a higher Shiny output priority and is allowed to
run even when the Home tab is temporarily hidden.

A season-specific readiness handoff coordinates startup:

1. Select the Home team.
2. Calculate Storylines.
3. Flush the Storylines result to the browser.
4. Release the lower-priority season-data prewarm.

This prevents the general cache prewarm from taking the database connection
before Storylines. The source-time `SELECT 1` SSL/pool handshake still happens
first because all database-backed outputs require a live connection.

Files:

- `app/app.R`
- `app/R/mod_team_hub.R`

Commit: `0588558`

### 3. Batched six database requests into one

The six storyline contexts are now sent in one parameterized `UNION ALL`
request. PostgreSQL still evaluates all six filtered contexts, but the app now
uses:

- one pool checkout instead of six;
- one network round trip instead of six;
- one shared cache entry per season and ETL version.

The combined result includes a `hub_variant` column and is split back into the
six storyline datasets in R. Storyline calculations and links are unchanged.

Files:

- `app/R/mod_team_hub.R`
- `app/tests/testthat/helper-server-mocks.R`
- `app/tests/testthat/test-team-hub-ui.R`

Commit: `0d89a08`

## Measured result

Read-only benchmarks against the configured live database returned the same 84
rows across all six variants:

| Method | Elapsed time |
|---|---:|
| Six sequential requests, first run | 6.720 s |
| Six sequential requests, warm run | 3.620 s |
| One batched request, run 1 | 2.190 s |
| One batched request, run 2 | 2.160 s |
| Exact parameterized app query | 2.160 s |

The database portion is therefore approximately 2-3 times faster, depending on
connection and database cache state. Process-wide caching means later sessions
can reuse the completed batch until the cache expires or the ETL version
changes.

## Validation

- Exact parameterized query returned all six expected variants and 84 rows.
- Team Hub and Compare regression set: 178 passed, 0 failed.
- R source files parsed successfully.
- No browser smoke test was performed.

## Remaining bottleneck

Batching removes connection and network overhead, but PostgreSQL still runs
`get_team_ratings_dynamic()` six times inside the single request. A larger
future optimization would require a dedicated database function or
precomputed table that calculates all storyline contexts from fewer underlying
scans. That would be a database/ETL design change rather than another Shiny
request-scheduling improvement.

## Suggested next optimizations (2026-07-28 review)

Where the remaining ~2.2 s goes: `get_team_ratings_dynamic()` has a single
`RETURN QUERY` with no pre-aggregate branch. Every variant scans
`df_pts_poss_lineups_longer_mv` (action-level, whole season, all teams) with a
`shot_zones` LEFT JOIN — six full scans per batch. Ranked by impact per
effort:

### 1. Serve last10 / top4 / bottom4 from `team_metrics_by_game_mv` in R (no DB change)

These three variants only *select games*; they never filter within a game.
`team_metrics_by_game_mv` already stores per-game team grain with
`off_points_raw` / `def_points_raw` / `off_poss_raw` / `def_poss_raw`, `gn`,
and `opp_team_id`:

- **last10**: per team, take the 10 highest `gn` rows, sum points/poss,
  derive net rating.
- **top4 / bottom4**: join `opp_team_id` to `team_ppp_ratings_mv` ranks
  (already cached in the hub), filter, sum.

One small cached season pull (~500 rows) replaces three of the six UNION
arms — the batch roughly halves. Gate: verify output-identical against the
SQL function for a season before switching (same numbers, same qualifying
games), and confirm `app_readonly` has SELECT on the table.

### 2. Precompute the within-game contexts at ETL (removes the rest)

`starters_hi/lo` and `clutch` genuinely need row-level filtering, but the hub
uses fixed definitions (3+/≤2 starters; margin ≤5, last 5 min). Add per-game
context columns to `team_metrics_by_game_mv` (or a sibling game-grain table):
off/def points+poss for `starters3plus`, `bench2minus`, `clutch`. Then all six
storylines become R-side sums over one indexed read (<100 ms), and game-grain
composes with last-N / opponent-rank slices for free (future storylines like
"clutch in the last 10" cost nothing). Incremental refresh already exists
(`refresh_team_metrics_by_game_for_games`). Size impact: ~500 rows/season × a
few numerics — negligible against the 500 MB tier.

### 3. If the SQL path stays: profile, then slim the scan

- `EXPLAIN (ANALYZE, BUFFERS)` each UNION arm first — data before tuning;
  clutch and the rank variants likely dominate.
- The scan computes 5 shot-mix columns and joins `shot_zones`, none of which
  storylines use (only net rating + poss). A `p_include_shot_mix bool DEFAULT
  TRUE` guard (single function preserved as shared source of truth) would cut
  I/O per arm.
- `force_custom_plan` means six plannings per batch — known premium, minor
  here.

### 4. Scheduling / perceived latency

- **ExtendedTask (promises/mirai) for the batch**: the 2.2 s query currently
  runs synchronously inside the single R process, blocking every concurrent
  session's interactions, not just the loading card. This is the existing
  scalability-backlog item; the storyline batch is its best first target.
- **Process-level warm**: the batch is cached per season + ETL version, but
  the first visitor after each deploy/ETL still pays it. Warming it in
  `prewarm_for_year` (it now runs after the storylines flush, so this only
  covers sessions that never render the hub) or via a post-ETL ping makes the
  first paint cached for everyone.

Recommended order: (1) now — pure R; (4a) if concurrency hurts before the ETL
change lands; (2) as the real fix; (3) only if EXPLAIN shows a cheap win in
the interim. With (1)+(2) the storyline card drops to a single ~100 ms read
and the loading spinner becomes vestigial.
