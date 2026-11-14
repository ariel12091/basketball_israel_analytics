# Handoff: Supabase disk I/O and Game Flow

Date: 2026-11-13

## Release status

The fix is deployed to the maintained Vercel app:
https://ibpl-game-logs.vercel.app

- Production commit: `e0bc1382bc3d667c0eef3a0b04b2b01d264db59c`.
- PR: https://github.com/ariel12091/basketball_israel_analytics/pull/37
- Vercel deployment: `dpl_GSJ1aQuCrMm1Bw1bkfzbwiNTiYug`, observed `READY`
  with the production domain assigned to that commit.
- Shiny is retired, per the user's clarification. Its deployment was not
  verified and is not the acceptance target for this release.
- The release contains only the I/O fix, tests, and investigation report.
  Thirteen unrelated EuroLeague commits on the working branch were excluded
  by releasing from a separate branch based on `main`.

## What the warning means

Supabase's disk I/O budget measures read/write performance capacity. When
the burst budget is exhausted, the instance falls back to baseline disk
performance. This can slow database work even when storage remains available.

We found strong evidence of excessive database reads and temporary writes.
We did not establish that the disk was full, or obtain the exact timestamp
at which the Supabase budget ran out.

## The bug

EuroLeague/EuroCup game and team IDs are PostgreSQL `bigint` columns.
RPostgres connections configured with `bigint = "numeric"` return them as R
numeric values. When these are inserted into SQL using `DBI::sqlInterpolate`,
the resulting literals can be typed as `float8`:

```sql
-- Problematic comparison
WHERE game_id = 781::float8

-- Correct comparison
WHERE game_id = 781::bigint
```

In the problematic plan, PostgreSQL converts the indexed `game_id` column
to floating point for comparison. The ordinary integer index cannot serve
that predicate. Through `ribbon_segments_v`, this also stops the selected
game from constraining the lineup mapping early, producing full-table scans
and an aggregate across lineups from all games.

The Game Flow events/ratings reader adds further work through
`ribbon_events_v`. It amplified an existing ID-typing problem; the display
of runs and ratings itself was not shown to be the root cause.

EXPLAIN confirmed sequential scans with the float comparison and indexed,
game-constrained scans with the bigint comparison. No new index was needed.

## Evidence and scope

Retained statement statistics began on November 3. At inspection:

| Query family | Calls | Database block reads | Temporary writes |
| --- | ---: | ---: | ---: |
| EuroLeague/EuroCup Game Flow | 561 | 940.04 GiB | 189.30 GiB |
| Israeli Game Flow | 810 | 0.63 GiB | 0 GiB |
| Other statements | 784,400 | 533.74 GiB | 209.24 GiB |

Euro Game Flow accounted for approximately 64% of retained statement block
reads. The worst events variant averaged 55-63 seconds, depending on role.

These are cumulative PostgreSQL counters, not today's usage or exact device
I/O: a PostgreSQL block read can be satisfied by the operating system cache.
Both `postgres` and `app_readonly` contributed. Those roles do not distinguish
local tests from production users.

The audit found a confirmed second occurrence in the player-minutes live
regression test: 85 calls and 21.242 GiB of block reads. Another recorded
occurrence was a one-off diagnostic EXPLAIN ANALYZE of ribbon events.
No additional affected active feature reader was found in the source and
retained-query audit.

Crucially, a newer non-float Euro Game Flow variant already averaged 38 ms
over 209 calls. The evidence does not support saying that every current web
request was doing the expensive scan.

## The fix

Cast the parameters in SQL to their column types, leaving indexed columns
untouched:

- Israeli reader: `$1::int4` / `$2::int4`.
- EuroLeague and EuroCup reader: `$1::bigint` / `$2::bigint`.

This protects both bound parameters and interpolated R numeric values.
EuroLeague IDs retain their 64-bit database type. Do not replace this with a
global conversion of every numeric parameter: genuine measurements and rates
still need decimals.

Changed files:

- `app/R/global.R`: both canonical `RIBBON_SQL_*` reader constants.
- `web/src/data/gameflowSql.json`: regenerated web copy of those readers.
- `app/tests/testthat/test-stint-ribbon-readers.R`: typed ID predicates in
  both minutes reconciliation queries.
- `app/tests/testthat/test-gameflow-index-types-db.R`: live regression for
  integer, double-bound, and double-interpolated callers in all three leagues.
- `docs/verification/supabase_disk_io_2026-11-13.md`: detailed investigation.

No UI/styling, basketball definitions, stored data, database schema, views,
indexes, or compute plan were changed. No database migration was required.

## Why this affects the app and database

For the database, a request that should read one game can instead read and
aggregate data across games, then write intermediate results to temporary
files. Repeating that work consumes I/O capacity and competes with normal
traffic, ETL, and materialized-view refreshes.

For the app, an affected request can take much longer or time out. Because
all features share the database instance, resource pressure can also delay
other pages whose own queries are healthy. Those wider effects are the
expected mechanism; this investigation did not measure an outage on every
page.

Israeli Game Logs mainly use precomputed summaries. Israeli Game Flow also
uses action data, but its observed workload was much cheaper. The especially
expensive Euro path combined the float-ID comparison with lineup mapping
joins through the ribbon views.

The fix removes that unnecessary work. It does not erase historical counters,
restore burst credits immediately, or eliminate the separate I/O cost of ETL
and refreshes. There is no basis here for a guaranteed percentage reduction
in production device I/O or a promise that all app requests become faster.

## Verification completed

- Exact full-payload parity for Israeli game 390, EuroLeague game 781, and
  EuroCup game 671 using integer baselines, double binding, and double
  interpolation.
- Fixed reader measurements: 81.4 ms, 17.8 ms, and 23.3 ms respectively.
  These individual warm-cache executions had zero shared block reads,
  zero temporary writes, and no full scans of the main fact tables.
- Existing credited-event DB checks and all nine new index-regression
  assertions passed. The regression uses EXPLAIN without executing a
  potentially regressed full scan.
- All 62 focused web tests passed. Existing offline reader checks passed;
  database-only cases were skipped in that offline run.
- Local browser checks passed at 390px and 1440px for all three leagues,
  including phone quarter switching and horizontal overflow checks.
- GitHub Preview build, live browser checks, and R checks passed before
  merge. The merged production commit's R checks also passed.
- After deployment, all three production Game Flow endpoints returned
  HTTP 200 with nonempty lanes/events and expected team names.

## Follow-up for the next owner

1. Compare fresh counter deltas with Supabase Database Health over a normal
   traffic/ETL cycle. Historical totals will remain large after the fix.
2. If I/O pressure persists, separate reader activity from refresh/ETL
   spills; materialized-view refreshes were another substantial contributor.
3. Keep SQL ID casts in the canonical readers and regenerate the web SQL
   when changing them. The exporter also rewrites `gameflowLayout.json`;
   avoid committing unrelated generated layout changes.
4. For future bigint lookups from R, check how numeric IDs are quoted and
   verify that predicates use the existing indexes. Start suspicious-query
   diagnosis with EXPLAIN, not EXPLAIN ANALYZE of the old float variant.

Reference: https://supabase.com/kb/troubleshooting/exhaust-disk-io
