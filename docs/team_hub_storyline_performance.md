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
