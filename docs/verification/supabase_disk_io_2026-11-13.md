# Supabase disk I/O investigation — 2026-11-13

Read-only PostgreSQL inspection at approximately 12:29 UTC. The initial
inspection made no changes. Subsequent local code fixes are recorded below;
production views, indexes, and data were not changed.

## Finding

EuroLeague/EuroCup Game Flow is the largest source of reads in retained
`extensions.pg_stat_statements` statistics. Statistics were last reset on
2026-11-03 05:32:40 UTC; these are cumulative counters, not today's usage.
They do not establish the exact time Supabase exhausted its burst budget.
PostgreSQL block reads can be served from the operating system cache, so
they are not a direct measurement of physical device traffic.

| Query family | Calls | Execution seconds | Block reads (GiB) | Temporary writes (GiB) |
| --- | ---: | ---: | ---: | ---: |
| Euro Game Flow | 561 | 5096.3 | 940.04 | 189.30 |
| Israeli Game Flow | 810 | 84.5 | 0.63 | 0.00 |
| Other statements | 784400 | 17147.1 | 533.74 | 209.24 |

Euro Game Flow accounts for approximately 64% of retained statement block
reads. ETL/materialized-view refreshes are another substantial contributor.
Database size was 7155 MB; allocated disk capacity was not available from
these queries, so free storage capacity was not established.

## Root cause

The worst recorded Game Flow statement interpolates game/team IDs as
`::float8`. Comparing an integer `game_id` to this value converts the column
to double precision and prevents the existing game-leading indexes from
serving the predicate. The join through `ribbon_segments_v` then aggregates
lineups across all games. The new events payload adds more reads through
`ribbon_events_v`, amplifying an older issue.

Confirmed with EXPLAIN only (no execution):

- `SELECT * FROM euroleague.ribbon_segments_v WHERE game_id=123::float8`:
  sequential scans of `matchup_segments_actions` and `lineup_totals_by_game`,
  full lineup hash aggregate, estimated total cost 28174.63.
- Same query with `123::bigint`: game-constrained index scans on both tables,
  estimated total cost 192.43.

Worst statement ID `8051232658373646137`, with events and float IDs:

| Role | Calls | Mean seconds | Block reads (GiB) | Temp writes (GiB) | Tracked since (UTC) |
| --- | ---: | ---: | ---: | ---: | --- |
| app_readonly | 31 | 55.459 | 425.89 | 122.00 | Nov 10 14:54 |
| postgres | 25 | 63.491 | 275.41 | 53.03 | Nov 12 11:09 |

Both administrator and readonly workloads contributed. Roles alone cannot
distinguish production traffic from local scripts/tests.

The largest newer non-float events variant (ID `3218887207638004937`) has
209 calls, mean 38 ms, 0.06 GiB block reads, zero temporary writes. Thus the
evidence does not justify attributing the heavy historical totals to every
current website request. Current `fetch_stint_ribbon()` already coerces IDs
to integer; the web uses bound parameters. Identify remaining interpolation
callers before assuming the deployed application is still issuing floats.

## Recommended remediation

1. Audit Game Flow SQL interpolation and diagnostic/test callers for floating
   IDs. Explicitly cast SQL game/team parameters to their integer column types
   in the canonical reader, then regenerate `web/src/data/gameflowSql.json`.
2. Check full reader plans for both leagues and both parameter binding and
   interpolation paths. Single-game readers must use game-constrained scans.
3. Avoid rerunning the expensive float variants while investigating. Snapshot
   counters around normal traffic/ETL to distinguish ongoing from historical
   consumption; correlate with Supabase Database Health budget timestamps.
4. Optimize refresh spill separately if I/O remains high after fixing callers.

Sources: `app/R/global.R`, `web/src/server/gameflowRepo.ts`,
`web/src/data/gameflowSql.json`, EuroLeague migrations 053, 061, and 062.
Diagnostic scripts: `scratchpad/investigate_disk_io.R` and
`scratchpad/investigate_disk_io_detail.R` (read-only sessions).

Supabase budget explanation:
https://supabase.com/kb/troubleshooting/exhaust-disk-io

## Remediation and verification

Fixed locally on November 13; not deployed. No database DDL/data changes.

- Both canonical Game Flow readers cast every game/team parameter to the
  actual column types: Israeli `int4`, EuroLeague/EuroCup `bigint`.
  Regenerated the web SQL from those constants. The casts also protect
  interpolation of numeric-valued IDs, without narrowing EuroLeague IDs
  to 32 bits.
- Fixed both minutes reconciliation queries in
  `app/tests/testthat/test-stint-ribbon-readers.R`. Its EuroLeague caller
  interpolates IDs read with `bigint="numeric"`; this is a confirmed source
  of the recorded bad query shape, rather than just a theoretical risk.
- Audited retained statements for other float ID equality predicates and
  searched active application/script interpolation callers. Outside Game
  Flow, the minutes test had 85 calls / 21.242 GiB block reads. The only other
  unprotected recorded case was a one-off `EXPLAIN ANALYZE` of ribbon events
  (1 call / 1.952 GiB). No further affected active feature reader was found.
  The older counters cannot establish that a query is still running today.
- Added a live read-only regression test for all three competitions. It
  compares integer-bound old-reader results with double-bound and
  double-interpolated fixed-reader results and rejects full fact-table scans
  in EXPLAIN. This guards the actual failing RPostgres caller shape.
- Exact full-payload parity passed on Israeli game 390, EuroLeague game 781,
  and EuroCup game 671. Their fixed, double-interpolated reader measurements
  were 81.4 ms, 17.8 ms, and 23.3 ms respectively, with zero shared block
  reads and zero temp writes in those warm-cache measurements. No full
  scans of the main fact tables appeared in the plans. These are individual
  warm-query measurements, not production latency guarantees.
- Existing credited-event DB checks passed; the new index regression passed
  all nine assertions. Offline ribbon reader checks passed (DB cases skipped)
  and all 62 focused web Game Flow/model/rating/run/summary tests passed.
  R emitted existing locale/package-build warnings.
- Live local browser checks passed at 390px and 1440px for all three
  competitions. Phone quarter switching reached Q2, charts rendered after
  hydration, and no horizontal overflow was detected. Screenshots are in
  `output/playwright/io-fix-<league>-<width>.png`. No UI code/styling changed.
