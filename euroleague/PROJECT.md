# EuroLeague project handoff

Last updated: 2026-09-01

This is the current project handoff and the first document to read before
changing the EuroLeague ETL, schema, or app integration. `CLAUDE.md` is a
historical record and must not be edited or treated as the current schema
contract.

## Current state

- Migration 050 was **applied** on 2026-09-01. The exact standard-clutch Team
  preset now has one additive dashboard reader that materializes
  `filtered_team_game_facts` once and returns Ratings, Four Factors, and
  Minutes together. Tab 9 is routed locally through one shared reactive for
  the current and previous-matchday results; per-game and custom-clutch paths
  deliberately retain their three existing readers because committed
  fresh-backend probes rejected both combined candidates. Both live parity
  presets returned the same 20 teams, security reconciliation and the final
  privilege/reachability audits passed, and all 264 Python tests passed. The
  app change has not been deployed.

- Israeli migration 051 was **applied** on 2026-09-01. Its deliberately narrow
  standard-clutch Four Factors + Minutes reader materializes one filtered
  action set and is routed locally through one shared Tab 3 reactive. It
  preserves Summary and all non-standard routes. Exact four-preset parity,
  repeated fresh-backend measurements, security reconciliation/audit, the
  static query-count contract, focused Shiny tests, and all 276 EuroLeague
  Python tests passed. The app change has not been deployed.

- Migration 049 was **applied in both schemas** on 2026-08-31. Team Ratings
  and Four Factors now round Net Rating once from additive counts and pass the
  permanent 12-route companion audit. The EuroLeague season Ratings MV now
  uses the same publication-eligible `team_four_factors_by_game` source as its
  per-game reader, preventing QA-blocked games 246, 493, 549 and 650 from
  leaking through the direct-actions season path. Rollback/apply gates
  preserved MV ownership, ACLs and indexes; security reconciliation/audits and
  all 238 Python tests passed.

- Migration 048 was **applied** on 2026-08-31. It records
  `refresh_actions_consumer_candidates(bigint[])` as one literal, replayable
  `CREATE OR REPLACE FUNCTION`, replacing migrations 015–016's catalog-text
  patching as the current source of truth. The rollback gate and committed
  apply preserved body MD5 `18b7329c289960f0825f0035f98a6bd8`, owner,
  security mode, settings and ACL exactly. Security reconciliation and the
  independent audit passed.

- Migration 047 was **applied** on 2026-08-31. It removed three orphaned
  functions and two orphaned views found by the first full audit of the SQL
  surface. The rollback gate and committed apply each preserved all 18 reader
  row counts; security reconciliation and the independent audit passed. See
  "Migration 047" below and `docs/sql_function_history_and_risk_2026-08-30.md`.

- Migration 046 was **applied in both schemas** on 2026-08-30. The filtered
  Four Factors path in Tab 8 and Tab 1 now makes one database call instead of
  two: `four_factors_dashboard_compute` returns the factor columns plus the
  rating/minutes fields the app previously re-derived with a second full
  `onoff_compute`. Additive - the existing functions are unchanged. Exact
  row parity on twelve presets; broad median 2.714 s -> 1.987 s.
  On 2026-08-31 the Israeli dashboard body was replaced with the same one-scan
  aggregation shape used by EuroLeague. The 12-preset apply gate was exact and
  measured 2.734 s -> 1.439 s broad. Both league behavioral matrices passed
  after commit, followed by the confirmed security apply and security audit.

- Israeli filtered Shot Profile now shares its ranked reactive with the
  auto-minimum observers. The table and both possession bars therefore make
  one `onoff_compute` call per filter change instead of two. A server-level
  query-count test guards this boundary; Four Factors routing is unchanged.

- Migration 045 was **applied as function-only query alignment** on 2026-08-29.
  `euroleague.onoff_compute` and `euroleague.four_factors_compute` now read
  `player_four_factors_by_game` directly, matching the Israeli app-facing
  access shape. No index or function-local `work_mem` setting was added; the
  existing physical indexes are unchanged.

- Migration 043 was applied on 2026-08-27. Non-clutch Team Minutes now reads
  the existing per-game lineup fact, with one additive `action_span_seconds`
  column preserving the Israeli first-to-last-action duration convention. It
  adds no table or unit expansion. Eight live presets matched the former
  direct reader exactly; broad warm median improved from 1.889 seconds in the
  apply comparison to 0.213 seconds, and last-10 measured 0.111 seconds. The
  required database security audit and hardening pass both succeeded. The app
  code is updated locally but was not deployed.
- Migration 044 was applied on 2026-08-28 after explicit acceptance of a 0.550-
  second median gate. The custom Player Stats function now reuses resolved IDs
  from `lineup_totals_by_game`, runs at the Israeli PL/pgSQL function boundary,
  disables per-call JIT, and deduplicates lineup identities before unnesting.
  It adds no table, column, index, backfill, or publication work. The retained
  run matched all bounded rows exactly, improved all three bounded medians, and
  measured 0.519 seconds median / 0.547 seconds p90 over 15 broad warm samples.
  The preceding rollback gate measured 0.524 seconds median; its first 2.961-
  second sample was recorded as cache warming rather than hidden. The required
  security audit and hardening pass succeeded. A separate 15-call check through
  the configured pooled connection measured 0.528 seconds median; its first two
  cold/session calls were 7.587 and 2.715 seconds. No app deployment was
  performed.
- A paired Tab 8 direct-fact source plus `(game_id, team_id)` index remains
  rejected: it preserved broad/narrow results and improved broad ON/OFF from
  9.876 to 0.878 seconds, but missed its 0.500-second gate; JIT-off regressed it
  to 1.050 seconds. A scalar-`lineup_key` action-fact experiment also rolled
  back: bounded rows were exact, but seven broad warm samples had a 0.583-
  second median, proving that persistent array identity was not the final
  bottleneck. That rewrite left physical free-space bloat even after autovacuum;
  guarded `VACUUM (FULL, ANALYZE)` preserved all 665,814 rows and reduced the
  fact plus indexes from 1,210,531,840 to 583,819,264 bytes, below its roughly
  677 MB pre-experiment baseline. No scalar-key column or experimental Tab 8
  index/function remains live.

- On 2026-08-24, same-definition concurrent index compaction reduced the live
  schema from 2,342 MB to 2,138 MB (about 204 MiB / 8.7%) without changing a
  table, index definition, read function, or analytical result. The largest
  fact, `player_stats_actions_by_game`, fell from 802 MB to 677 MB. Thirteen
  reviewed indexes were rebuilt one at a time; all are valid/ready and no
  concurrent-reindex shells remain. Use
  `scripts/reindex_storage_candidates.py` for the guarded, repeatable process.
- The apparently redundant 72 MB
  `euroleague_player_stats_actions_team_idx` was retained. A transactional
  A/B/A gate found exact results and a major Lineups improvement without it,
  but a repeat apply gate measured broad custom Team Ratings at 0.900 s with
  the index versus 1.040 s without it, exceeding the 10%/100 ms no-regression
  limit. `scripts/benchmark_storage_indexes.py` preserves this acceptance
  test and explicitly disables the loader helper's autocommit before testing
  transactional DDL.
- Post-compaction app-path medians with the full live index set were 0.878 s
  broad custom Team Ratings, 0.248 s broad custom Four Factors, 0.095 s
  one-team custom Ratings, 0.275 s broad custom Team Minutes, and 6.301 s
  custom five-player Lineups. All compared result hashes were identical.
- Storage verification now counts only table/materialized-view roots so
  indexes are not counted twice, and the lineup-duration gate uses migration
  015's `effective_period()` for provider-reset overtime clocks. The corrected
  duration gate has zero mismatches across the live season.

### Current app-query timing overview (2026-08-28)

A fresh read-only overview was run through the configured pooled connection
after migrations 043-044, the rejected scalar-key experiment, and the guarded
table compaction. Every probe fetched the complete result. The first execution
was recorded as cold/session-start latency; the reported warm value is the
median of the next three calls in the same connection. Each call had a 30-
second statement cap. These are execution observations, not publication gates:

| App-facing route | Rows | Cold | Warm median | Warm samples |
|---|---:|---:|---:|---|
| Tab 8 ON/OFF, broad | 358 | 10.548 s | 3.288 s | 6.775, 3.288, 0.863 s |
| Tab 8 ON/OFF, last 10 | 288 | 0.296 s | 0.276 s | 0.280, 0.276, 0.274 s |
| Tab 8 Four Factors, broad | 358 | 1.061 s | 0.945 s | 0.934, 0.945, 0.953 s |
| Tab 8 Four Factors, last 10 | 288 | 0.292 s | 0.288 s | 0.325, 0.288, 0.285 s |
| Player Stats, per-game broad | 340 | 0.497 s | 0.138 s | 0.132, 0.138, 0.141 s |
| Player Stats, custom broad | 265 | 3.771 s | 0.515 s | 1.660, 0.515, 0.515 s |
| Team Ratings, per-game | 20 | 0.892 s | 0.128 s | 0.134, 0.128, 0.123 s |
| Team Ratings, custom | 20 | 14.671 s | 0.346 s | 0.822, 0.346, 0.343 s |
| Team Four Factors, per-game | 20 | 0.380 s | 0.128 s | 0.128, 0.126, 0.130 s |
| Team Four Factors, custom | 20 | 0.404 s | 0.355 s | 0.359, 0.350, 0.355 s |
| Team Minutes, per-game | 20 | 3.203 s | 0.141 s | 0.181, 0.137, 0.141 s |
| Lineups, per-game size 5 | 10,016 | 8.265 s | 1.724 s | 2.046, 1.724, 1.645 s |
| Lineups, per-game size 2 | 2,451 | 15.163 s | 9.274 s | 10.344, 8.160, 9.274 s |
| Lineups, standard clutch size 5 | 1,098 | 1.654 s | 1.234 s | 1.196, 1.234, 1.361 s |
| Lineups, custom clutch size 5 | 751 | 24.252 s | 5.445 s | 8.792, 5.445, 3.209 s |

The remaining warm-over-500-ms work is therefore broad Tab 8, all measured
five-player Lineups routes, the deliberately deprioritized two-player Lineups
edge case, and custom Player Stats at a near-threshold 0.515 seconds. Team
Ratings, Team Four Factors, Team Minutes, last-10 Tab 8, and per-game Player
Stats are under 500 ms once warm.

Cold execution is the next investigation, separate from warm query-shape work.
The most useful cases are routes whose warm computation is already healthy but
whose first call is disproportionately slow: custom Team Ratings (14.671 vs
0.346 seconds), Team Minutes (3.203 vs 0.141), custom Player Stats (3.771 vs
0.515), and per-game Team Ratings (0.892 vs 0.128). Broad Tab 8 and Lineups
remain warm-query problems as well and must not be misdiagnosed as cold-only.

For the cold-call pass:

1. Measure connection checkout/wake time separately from server execution.
2. Capture `EXPLAIN (ANALYZE, BUFFERS)` on genuinely fresh pooled sessions and
   compare shared reads, cache hits, planning time, JIT, and execution time.
3. Separate relation-cache warming from function/custom-plan warming; do not
   infer either from one first-call sample.
4. Start with custom Team Ratings and Team Minutes because their warm paths are
   already below 500 ms, making cold overhead easier to isolate.
5. Require repeated cold-session samples and exact results before retaining a
   connection, function-setting, index, or prewarming change.
6. Keep the two-player Lineups route deprioritized unless product usage changes.
7. Do not deploy the app as part of database diagnosis; deployment remains an
   explicit separate action.

Cold-call phase 1 isolated the startup cost further. The database is PostgreSQL
17.4. The first diagnostic used the pooler's session-mode port 5432: connection
establishment was 0.49-0.75 seconds and a trivial first round trip was
0.078-0.093 seconds. On backend PID 4024165, the first custom
Team Ratings call took 12.523 seconds and its second call 3.303 seconds; later
new client sessions assigned to that same backend were already at 0.346-0.353
seconds. Team Minutes showed the same per-reader first-use shape: 4.547 seconds,
then 0.850 seconds, then 0.136-0.150 seconds on later sessions using that
backend. All result row counts and SHA-256 digests were identical.

This is not principally custom-versus-generic plan selection. After both paths
were warm, session-local `DISCARD PLANS` left custom Team Ratings at 0.358
seconds and Team Minutes at 0.145 seconds. A different pooled PostgreSQL backend
(PID 4024178) reproduced the startup penalty under `EXPLAIN (ANALYZE, BUFFERS)`:
Team Ratings took 4.266 seconds with 206,767 shared-buffer hits and only 82
reads; Team Minutes took 1.065 seconds with 19,062 hits and only 19 reads.
Planning reported by the outer call was at most 0.006 seconds. The evidence
therefore points to backend-local first-use initialization plus memory/cache
locality, not network connection time, disk reads, or ordinary cached-plan
invalidation alone. PostgreSQL 17 also predates PostgreSQL 18's change that
caches old-style SQL-function plans across successive outer queries.

The app currently makes this startup behavior user-visible: `app/R/global.R`
configures `dbPool()` with `minSize = 0`, `maxSize = POOL_MAX` (default 3), and
`idleTimeout = 15000` seconds. Its `onCreate` callback sets only
`statement_timeout`. The first database request therefore creates the first
connection lazily; later connections can introduce another cold backend when
concurrency grows. Because the idle timeout is about 4.2 hours, this is an app
process/pool-expansion cost rather than a cost on every ordinary request.

The exact R `dbPool` path was then measured with the app's actual port 6543,
which Supabase defines as transaction mode. With `minSize = 0`, pool creation
was immediate, the first control query took 2.480 seconds, custom Team Ratings
ran 7.790 / 0.790 / 0.530 / 0.510 seconds, and Team Minutes ran 1.600 / 0.310 /
0.310 / 0.310 seconds. With `minSize = 1`, pool creation took 1.250 seconds and
the first control query 0.520 seconds, but the first reader calls still took
2.420 and 0.960 seconds; subsequent calls settled at roughly 0.51-0.53 and
0.31-0.48 seconds. `minSize = 1` alone therefore moves client connection setup
to startup but does not warm the readers.

The same R pool on Supabase session mode (port 5432, the documented mode for a
persistent backend) improved first-use latency without changing results. With
`minSize = 0`, Team Ratings was 5.250 seconds cold and 0.53-0.55 warm; Team
Minutes was 1.000 cold and 0.30-0.31 warm. With `minSize = 1`, creation cost
1.230 seconds, Ratings was 1.360 seconds on its first use and 0.52-0.59 warm,
and Minutes was 0.31-0.33 seconds throughout. A targeted startup warm-up on the
one persistent connection, ordered Minutes then Ratings, appeared promising in
an already-warm synthetic sample: it shifted 0.950 seconds to startup and made
the next Ratings call 0.520 seconds. An earlier colder warm-up cost 4.510
seconds. Every comparison returned the same 20 rows and digest.

The connection-mode candidate was then checked for cross-league regression.
All eight representative Israeli readers returned identical rows and digests.
Session mode improved ON/OFF and Four Factors, was effectively neutral for the
Team routes, and was 3-4% slower for Traditional Stats in the initial small
sample. The initial Israeli Lineups result looked 21% slower, but a 10-sample
interleaved rerun reversed it: port 6543 measured 1.685 seconds median / 1.854
seconds p90, while port 5432 measured 1.490 / 1.692 seconds with the same digest.
There was therefore no evidence supporting separate league pools; connection
management must remain one shared app-level policy.

The complete `app/R/global.R` startup test nevertheless rejected the warm-up.
Real startup took 26.450 seconds and reported the warm-up successful, but the
first subsequent Ratings calls were still 7.530, 1.670, 0.530, and 0.520
seconds. Team Minutes was 3.730 seconds first and 0.310-0.320 thereafter. A
single startup call does not establish the steady state on this PostgreSQL 17
path, so it would move substantial work into startup without fixing the user-
visible cold call. The app pool/port/warm-up experiment was fully reverted; the
app still has its one original shared `dbPool` and no deployment was performed.

The next cold-call implementation should therefore be query-level, not a pool
workaround:

1. Test a combined additive Team reader that calculates Ratings and Four
   Factors from one filtered action set and attaches Minutes in the same app
   call, removing a duplicate scan and network round trips.
2. Retain it only if exact outputs and both cold and warm complete-call latency
   improve for the Israeli-shaped and EuroLeague consumers; do not introduce a
   league-specific connection policy.
3. The PostgreSQL-17 definition-time function-body experiment produced a
   partial improvement, recorded below, but needs broader preset parity before
   it could be retained.
4. Do not prewarm Lineups or two-player combinations. Avoid `pg_prewarm`: the
   slow new-backend samples were already overwhelmingly shared-buffer hits, and
   indiscriminate prewarming would consume shared resources used by the Israeli
   schema too.

The definition-time parsing experiment used an ungranted temporary
`BEGIN ATOMIC` copy of `get_team_ratings_direct` and four simultaneous, distinct
PostgreSQL session backends. Candidate creation took 0.251 seconds. Baseline
first calls were 8.485 and 5.657 seconds (7.071 median); atomic-candidate first
calls were 6.333 and 5.351 seconds (5.842 median), a 17.4% improvement. The
20-row result digest matched exactly. Warm median was unchanged within noise:
0.351 seconds baseline versus 0.348 candidate; measured warm p90 was 3.348
versus 0.379 seconds. The candidate was dropped successfully. This proves that
definition-time parsing helps backend startup without warm regression in the
broad case, but it does not make the cold call sub-500-ms. Do not apply it until
the remaining custom filter presets and the exact app call pass parity and
no-regression gates.

Cold-call phase 2 measured the deployed application boundary with standalone
Playwright. The bundled in-app browser control could not initialize because its
local Codex runtime failed before navigation with `failed to write kernel
assets: The system cannot find the path specified. (os error 3)`; the standard
Windows temp and application-data paths existed, so this was a local browser-
kernel problem rather than evidence about the app. After explicit permission to
use Playwright outside the sandbox, a fresh named browser session opened
`https://ibpl-stats.shinyapps.io/onoff-shiny/` in 38.5 seconds. A snapshot then
confirmed that the complete home UI had rendered. An immediate same-session
reload took 10.1 seconds end to end; the browser Navigation Timing entry
reported 4.471 seconds through `loadEventEnd`, including a 1.839-second response
start and 4.469-second DOM completion. A second fresh browser session while the
app was warm took 10.5 seconds. No application, database, or deployment change
was made for this measurement.

The roughly 28-second cold-versus-warm-fresh-browser difference is a combined
deployed cold-start tax, not a pure shinyapps.io sleep number. It includes
instance wake, R/package/application startup, creation of the process-local
`dbPool`, first database connection work, and any database reader first-use
cost. The independent database experiment above measured broad Team Ratings at
5.842 seconds cold for the atomic candidate and 7.071 seconds cold for the
baseline, versus roughly 0.35-0.52 seconds warm in the relevant runs. Those
measurements establish a material cold database component, but they were not
captured inside the same browser request and therefore must not be subtracted
from 28 seconds as an exact decomposition.

The two remedies are complementary. Keeping a shinyapps.io instance running or
raising its instance idle timeout can avoid instance/R restart and preserve the
worker's connection pool, but consumes active hours and depends on the hosting
plan and dashboard settings. An external keep-alive would mainly emulate that
cost and is not the preferred solution. Query-level/backend-first-use work can
reduce the remaining database component; `dbPool` by itself cannot survive an
application instance being put to sleep. Do not change hosting settings, add a
keep-alive, instrument production, or deploy without explicit approval.

For an exact next decomposition, add monotonic startup/request timestamps for
R process start, completion of global sourcing, pool creation, first checkout,
first `SELECT 1`, completion of the first app-facing reader, and UI-ready/flush.
Correlate those with shinyapps.io logs and browser Navigation Timing across
multiple genuinely sleeping starts. This requires a deliberately approved
instrumented deployment; until then, retain `38.5 s cold`, `10.5 s warm fresh
browser`, `4.471 s warm navigation`, and the separate `5.842-7.071 s` cold
Ratings evidence without claiming a more precise layer split.

- The project uses an isolated `euroleague` schema in the existing PostgreSQL
  database. Never write EuroLeague data to the Israeli `basketball` or
  `basketball_test` schemas.
- The recorded live load is `E/2025/1-84` under completed `load_run_id=4`:
  84 requested, 84 successful, zero failed, rounds 1-9.
- Parser `0.2.1` and offline checkpoint format `6` add the labelled
  compound-penalty endpoint fix. The extraction dependency remains pinned to
  `euroleague-api==0.1.1`.
- Migrations 010-012 completed the simplified event-schema cutover. The
  canonical `actions` table now contains every typed package PBP field, both
  package lineups, parser grouping, and the possession endpoint annotation.
- The recorded post-cutover counts are 47,608 `actions_raw` rows, 47,608
  `actions` rows, 95,216 event/team-perspective rows, and 11,554 team matchup
  segments. The schema occupied 254 MB for 84 games.
- Player ON/OFF, player four factors, team four factors, team ratings, schedule
  filtering, and EuroLeague Shiny tabs are implemented. The app now also has
  game logs in Summary and Four Factors modes, backed by the existing
  game-level read facts; no shot-type mode is exposed.
- EuroLeague Team Ratings now follows the Israeli minutes/pace pattern. Applied
  migration 018 exposes `get_team_minutes_dynamic()`, which sums canonical
  `matchup_segments_actions.segment_seconds` per game/team under active filters;
  the app derives Off Pace and Def Pace only after aggregating possessions and
  minutes. The function is a scoped `SECURITY DEFINER` path because
  `app_readonly` must not read the segment table directly.
- Statistics for 2-, 3-, 4-, and 5-player lineup units are implemented
  (migrations 013-014) and surfaced by a third EuroLeague Shiny tab.
- EuroLeague clutch filtering is implemented in the app and its database
  migration (019) is applied to the live `euroleague` schema (2026-08-12).
  Team Ratings and Lineup Data reuse the Israeli `clutch_filter_ui()`,
  `resolve_clutch_params()`, reset, chip, and clear helpers. The EuroLeague
  read layer evaluates score margin/status from the pre-event team
  perspective and intersects canonical lineup segments with score-state/time
  windows, so clutch minutes and pace do not bridge excluded stretches.
  Regulation uses the selected time limit; overtime always qualifies unless
  the user opts into the margin/status restriction.
- Migration 020 is applied to the live `euroleague` schema (2026-08-13). It
  adds an incrementally refreshed per-game additive cache for the dominant
  standard preset (pre-event margin <= 5, final 5:00, all score states, and
  unrestricted overtime). Non-clutch requests continue to read
  `lineup_totals_by_game`; custom clutch definitions continue to use the exact
  action-level migration-019 path. Publication refreshes only changed games.
- EuroLeague Player Stats is implemented as the shared Israeli/EuroLeague
  Player Stats tab. Migrations 021-027 are applied to the live schema:
  it adds `player_traditional_stats_mv` as the indexed season fast path and
  `get_player_traditional_dynamic()` for date/team/phase/opponent/home-away/
  outcome/opponent-strength/round/last-N/clutch filters. It reuses official
  per-game `full_rosters.boxscore_stats` for counting totals, while TS% and
  USG% use canonical PBP free-throw-trip, turnover, team-possession, and lineup
  exposure facts; no duplicate player/game base table is introduced. The
  provider has no deflection event, so EuroLeague DFL remains unavailable
  rather than being inferred from steals or blocks.
- Migration 022 adds the player-attributed standard-clutch cache used by the
  Player Stats reader. It is refreshed per changed game alongside migration
  020; custom clutch definitions remain action-grained.
- Migration 027 adds the private, incrementally refreshed
  `player_stats_actions_by_game` fact for custom Player Stats clutch requests.
  It preserves the Israeli action/team-perspective grain and feeds the same
  CTE calculation; it does not pre-aggregate basketball outcomes. The current
  season has 548,644 rows and occupies about 229 MB including indexes.
- All four EuroLeague tabs now render their filter chips through the Israeli
  `build_filter_chips()` rather than three hand-rolled builders, and
  `fmt_rank_cell()` is shared. This uncovered and fixed a wrong-season date
  reset on tabs 8 and 9 and a chip bar on tab 10 that showed none of the
  filters it was applying. The later Game Logs tab also uses the same filter
  and rank helpers.
- The latest focused commit is `b1c80c2` (`Optimize EuroLeague default clutch
  filtering`). Earlier commits in the same working window added Game Logs,
  corrected EuroLeague auto possession minimums, aligned lineup filters and
  loading, fixed the lineup fast path, and extracted shared Israeli/EuroLeague
  tab plumbing. Unrelated worktree changes remain outside those commits.
- The `euroleague` schema is now inside the repository-wide database security
  contract. RLS is enabled with the `app_readonly_select_all` read policy on all
  19 base tables, `PUBLIC`/`anon`/`authenticated` hold nothing, and
  `app_readonly` has a curated relation list plus an eight-function EXECUTE
  allowlist. Applied to the live database on 2026-08-12; see the security
  section below.
- Migrations 018-027 are applied to the isolated `euroleague` schema.
  Further live loads or DDL changes still require explicit approval.

## Project goal

Provide EuroLeague versions of the Israeli application's core basketball
analytics while preserving EuroLeague source semantics:

- player ON/OFF ratings;
- player ON/OFF four factors;
- 2-, 3-, 4-, and 5-player lineup-unit statistics;
- team offensive, defensive, net, and four-factor ratings;
- additive game-level facts that can be filtered and aggregated without
  averaging stored ratios.

Compatibility means comparable grains, lineup exposure, possession meaning,
and metric formulas. It does not require copying the Israeli physical schema or
its historical intermediate tables.

## Player Stats performance reference and rules

EuroLeague Player Stats migrations 021-027 are applied. The final interactive
design shares the Israeli UI and calculation shape while retaining EuroLeague
provider semantics:

- ordinary totals use official box-score facts;
- TS%, USG%, possession exposure, and clutch filters use canonical PBP;
- the standard clutch preset reads its incremental per-game cache;
- custom clutch reads the private, incrementally refreshed
  `player_stats_actions_by_game` action/team-perspective fact;
- custom minutes use the Israeli qualifying-action segment convention;
- regulation and overtime are separate `UNION ALL` branches so regulation can
  use the existing time/margin index without changing overtime behavior;
- the app calls the standard or custom reader directly. Do not route interactive
  custom requests through the generic PL/pgSQL selector because that boundary
  hides filter values from the inner PostgreSQL planner.

The decisive performance defect was a EuroLeague-only roster join inside the
`stats` CTE. It compared filtered actions with the full season roster even
though `type_lineup` already determines the relevant team perspective and the
final names/roster join removes irrelevant rows. The Israeli function uses
`stats FROM acts`; EuroLeague now does the same. Removing that join avoided
roughly 66 million comparisons in the broad measured preset. Exact full-row
parity was verified before and after both query-shape changes.

Warm full-season reference timings measured on 2026-08-13 are:

| Preset | Israeli 2026 | EuroLeague 2025 |
|---|---:|---:|
| Standard: margin <= 5, final 5:00, unrestricted OT | 0.76 s | about 0.8 s |
| Custom: margin <= 3, final 4:00, unrestricted OT | 0.68 s | 1.68 s |
| Custom: trailing, margin <= 7, final 2:00, filtered OT | 0.99 s | 1.00 s |

Treat these as performance regression references, not guarantees across cold
caches or different season sizes. For future Israeli-to-EuroLeague analytics:

1. Compare the working Israeli function line by line before designing a new
   EuroLeague path: source, joins, predicates, grouping keys, materialization,
   function boundaries, and app routing all count as part of the reference.
2. Match the Israeli execution shape wherever provider semantics allow. A CTE
   with the same name or basketball grain is not sufficient evidence of parity.
3. Document every EuroLeague-only join or intermediate relation and the source,
   integrity, or queryability requirement that makes it necessary.
4. Remove redundant work before adding caches, indexes, or new facts. In
   particular, do not resolve roster or lineup membership per action when a
   filtered fact or downstream identity join already provides the guarantee.
5. Benchmark identical presets through the actual app-called function, both
   cold and warm. Also benchmark direct inner functions when a wrapper exists;
   nested PL/pgSQL boundaries can change parameter planning materially.
6. Require exact full-row result parity for query-shape optimizations. Timing
   parity alone is not a correctness gate.
7. Inspect predicate shape before adding an index. Mixed regulation/overtime
   `OR` conditions can defeat an otherwise appropriate compound index; prefer
   mutually exclusive branches when they preserve the basketball semantics.
8. Add a physical fact only after direct adaptation and plan inspection show it
   is needed. Keep it at the narrowest established basketball grain, private
   from the app role, incrementally refreshed per changed game, and additive or
   reproducible rather than storing final ratios.

### Team and Lineup clutch audit (2026-08-13)

The Player Stats lessons were applied to the Team Ratings and Lineup readers.
The audit separated warm computation from cold source reads and found:

- standard cached Team Ratings, Four Factors, and Minutes are healthy at about
  0.6-0.7 seconds;
- the custom `clutch_segment_durations()` calculation itself is about 0.35
  seconds warm, and `select_team_game_facts()` is about 0.6-0.7 seconds warm;
- `filtered_team_game_facts()` can exceed the 10-second cap because it passes a
  computed game array and clutch parameters through nested analytical function
  boundaries. This is the same parameter-planning failure found in Player
  Stats; removing only one wrapper did not fix it;
- cold custom Team Ratings and Four Factors exceeded 10 seconds, while the
  broad custom Team Minutes request measured 6.4 seconds before warming;
- standard five-player Lineups measured 2.94 seconds warm in the direct parity
  run. Restricting lineup identity resolution to the filtered fact set before
  unit expansion reduced it to 1.12 seconds with exact 844-row full parity;
- candidate lineup indexes greatly reduced buffer traffic but did not improve
  elapsed time in the temporary benchmark, so they were not added.

Migration 028 repairs a publication-critical
lineage bug in migration 027: after `player_stats_action_context` was repointed
to the physical fact, the incremental refresh function would delete a changed
game and then select from that same deleted target. Migration 028 sources the
refresh directly from canonical `action_team_context_actions`. The currently
loaded fact is intact because its full backfill occurred before the view was
repointed, but migration 028 must be applied before the next EuroLeague game
publication.

Migration 029 contains only the verified
filter-before-expand Lineups query change. It adds no index or new relation.

Migration 030 is applied. It extends the existing private
`player_stats_actions_by_game` fact with additive starter and team-event fields
and routes custom clutch facts through that grain. The first bounded live
benchmark completed in 19.7 seconds for broad custom Team Ratings, so this is
an exactness/lineage improvement but not yet an under-10-second solution.

Migration 031 fixes that remaining query-shape error by adapting the Israeli
Team functions directly: filtered schedule, one materialized action set,
game/team/side aggregation, then final metrics. Team Ratings and Four Factors
no longer traverse the generic lineup/minutes fact pipeline for custom reads;
the standard 5/all/5:00 preset remains on its incremental cache. For the broad
margin <= 3/final 4:00 preset, Four Factors improved from 19.08 to 7.52 seconds
cold with exact full-season parity. Warm direct timings were 4.30 seconds for
Team Ratings and 1.10 seconds for Four Factors. The legacy full-season Ratings
reader exceeded its 20-second cap and the first cold direct call was 17.60
seconds; bounded one-team parity was exact and improved from 3.99 to 0.31
seconds. Thus the direct execution shape is fixed, while truly cold broad Team
Ratings remains above the desired ten-second ceiling.

Migration 031 was then tightened to the Israeli predicate literally: one
action-table scan per reader, with the overtime bypass embedded in the time,
margin, and status predicates rather than separate regulation/overtime scans.
That removed the warm execution gap (0.28 seconds for both direct readers,
versus 0.25 seconds for the measured Israeli companions). Migration 032 adds a
59 MB covering index for only the direct Team inputs, avoiding first-read scans
of the 326 MB player/lineup-bearing heap. The index is used in live calls; the
first measured post-index pass was 2.16 seconds for Team Ratings and 0.57
seconds for Four Factors. No additional fact table or backfill was introduced.

### Remaining over-one-second audit (2026-08-13 handoff)

Work stopped here to preserve the weekly agent budget. Migrations 033-036 are
applied live and reuse the existing action fact; they add no new fact table and
require no backfill.

- Migration 033 adds Israeli-shaped direct custom Team Minutes: one schedule
  filter, one eligible action set, and max-minus-min duration per canonical
  segment. Migration 034 adds its 35 MB covering index. Broad custom Minutes
  improved from a 10-second timeout to 0.57 seconds first measured / 0.22
  seconds warm. One-team full-row parity was exact (43.000 minutes) and timing
  improved from 3.45 to 0.13 seconds.
- Migration 035 adds the direct custom Lineups reader. It filters actions once,
  derives event counts and segment duration from that set, and bypasses
  `sub_lineups` for five-player units exactly as the Israeli function does.
  One-team full-row parity was exact (29 rows) and improved from 2.62 to 0.36
  seconds.
- Migration 036 adds the Lineups covering index. `CREATE INDEX CONCURRENTLY`
  repeatedly starved on continuous app reads; invalid shells were removed.
  A bounded normal build then succeeded. It is compatible with SELECTs and was
  run while no EuroLeague publication was active.
- Full-season custom five-player Lineups improved from a 10-second timeout to
  5.09 seconds. This is materially faster but still above the one-second goal.
- The direct standard-clutch Lineups path exceeded 10 seconds, so the app keeps
  the standard 5/all/5:00 preset on `fetch_lineups_dynamic` and its existing
  cache (last measured 6.10 seconds). Only non-standard custom clutch requests
  route to `fetch_lineups_direct`.

Next steps, in order:

1. Inspect `EXPLAIN (ANALYZE, BUFFERS)` for `fetch_lineups_direct` after the
   new index; determine whether remaining time is action aggregation, lineup
   identity resolution, or the 844/557-row name/unit output.
2. Optimize the standard cached Lineups reader separately. It should read the
   default-clutch per-game cache and five-player identity directly, without
   calling `filtered_team_game_facts()` or expanding through `sub_lineups`.
3. For custom Lineups, consider an incremental additive per-game five-player
   clutch candidate only if plan evidence shows aggregation—not identity/name
   rendering—is the remaining bottleneck. Do not add another raw action fact.
4. Recheck sizes 2-4 independently; unlike size 5, they legitimately require
   `sub_lineups` expansion and may need different performance expectations.
5. Require exact full-row parity for representative team-scoped queries and a
   full-season result-key/count comparison before changing live routing.

### The non-clutch routing gap (migrations 037-038, 2026-08-13)

The 031-036 audit above measured **clutch presets**, where a margin/time
predicate keeps the action scan small. Those results hold. The gap it never
measured is a **filtered but non-clutch** request -- a phase, an opponent, a
last-N, a narrowed date range -- which the app routed to the same `_direct`
readers with no predicate to narrow the scan. Migrations 037 and 038 close it
on the Team and Lineups tabs respectively. Both are additive: one or two new
functions, no fact table, no backfill, no index.

- **Migration 037** adds `get_team_ratings_pergame` and
  `get_team_four_factors_pergame`, reading `team_four_factors_by_game`
  (21,204 rows, 6.4 MB) instead of the 494 MB action fact. Broad filtered
  Tab 9 went from ~26s total to ~1.3s. Parity: 30/30 full ordered-row
  comparisons identical against the `_direct` readers across 15 non-clutch
  presets. Team Minutes was left alone -- it has no per-game counterpart and
  migration 033's reader is already fast at 0.77s.
- **Tab 9 reader routing cleanup (2026-08-31)** makes all nine ratings, Four
  Factors, and Minutes reader names fully qualified literals in a fail-closed
  map. A repository contract test cross-checks them against the direct-reader
  manifest, so reachability audits, grep, and review can no longer miss routes
  assembled from fragments. This is app/test-only; it changes no SQL object.
- **Migration 038** adds `fetch_lineups_pergame`. Its cause is *not* 037's.
  A probe of the live schema showed `fetch_lineups_dynamic` never touches the
  action fact on a non-clutch request: `select_team_game_facts` (migration 020)
  already branches to `lineup_totals_by_game` when margin and time are absent,
  and returns that table row for row. The 21-24s was query shape --- two nested
  function boundaries, then a *second* join of `lineup_totals_by_game` on a
  five-element `text[]` purely to recover `lineup_key` and `player_ids` that
  the fact rows already carried, then expansion through `sub_lineups` even at
  size 5. The new reader reads the fact once and groups it.
- Measured through the app's own query as `app_readonly` on the pooler: the
  default Tab 10 view 24.39s -> 1.22s (20x), phase 14.39 -> 1.03, own-starters
  9.68 -> 0.44, size 3 broad 21.50 -> 5.61. Parity: 29/29 presets identical on
  all 33 columns across all four unit sizes, both player-membership filters and
  `min_poss`, plus 8/8 identical again through the app query shape.
- Two grain facts were verified before the SQL was written, not assumed:
  `own_starters` is functionally determined by (game, team, lineup) -- zero
  violating instances -- so both starter bounds are plain row predicates; and
  all 8,240 season lineups have exactly one size-5 `sub_lineups` row with
  `unit_key = lineup_key` and identical `player_ids`, which makes the size-5
  bypass a row-set identity rather than an approximation.
- The per-game readers deliberately take fewer parameters than the clutch
  readers (19 vs 23 for team, 23 vs 27 for lineups). The per-game facts have no
  time or margin dimension, so a mis-routed clutch request fails at the call
  site instead of silently returning unfiltered numbers.
- App routing for both tabs now goes through one shared classifier,
  `clutch_reader_kind()` in `app/R/helpers.R`: no clutch predicate ->
  `_pergame`; exactly 5/all/5:00 -> `_dynamic` and its cache; any other clutch
  -> `_direct`. Clutch behaviour is unchanged on both tabs.

Tab 8's `onoff_compute` was examined on 2026-08-14 and **nothing was shipped**;
the live functions are byte-identical to migration 004 with their grants intact.
The 11.88s does not reproduce warm (2.8-3.1s broad, 0.3-1.5s filtered; the cold
first call is 15.3s). It does have a real shape defect -- both on/off readers
reach the per-game fact through `player_game_context`, whose two unused
schedule joins are 95% of the query's buffers -- but removing the view flips
the planner to a seq scan of the whole 624,478-row fact on any narrow filter,
so a source swap alone is a regression for the common case. It needs a
`(game_id, team_id)` index on `player_four_factors_by_game` first, then both
halves in one change behind one gate. Full evidence, including why timing on
this instance cannot arbitrate the decision, is in section 8 of
`docs/euroleague_query_perf_handoff_2026-08-13.md`.

Also still open: player traditional stats, which is broken in both leagues
(Israeli 91s live, EuroLeague over 120s).

### Optimization techniques used and lessons

Use this as the playbook for future EuroLeague analytical-query work:

1. **Start from the working Israeli companion.** Compare the complete public
   function, not only its lowest-level fact source. The important pattern was
   schedule filtering, one eligible action set, direct aggregation at the
   requested output grain, then final ratios/ranks.
2. **Remove nested analytical function boundaries.** Passing computed game-ID
   arrays and clutch parameters through SQL -> PL/pgSQL -> SQL prevented the
   inner planner from seeing useful constants. `force_custom_plan` alone did
   not repair this. Direct public readers did.
3. **Filter once and reuse the set.** A materialized eligible-action CTE feeds
   all additive metrics and, where needed, segment duration. This prevents
   duplicate scans and metric-semantic drift.
4. **Use one Israeli-style regulation/OT predicate.** Time, margin, and status
   each include the unrestricted-OT bypass. This was faster warm than separate
   regulation/OT `UNION ALL` branches and preserved every overtime period.
5. **Aggregate at the consumer grain.** Team Ratings and Four Factors aggregate
   directly to game/team/side; they do not build lineup rows or calculate
   minutes. Team Minutes groups only by game/team/segment. Lineups retain lineup
   identity because that grain is genuinely requested.
6. **Keep ratios late.** Facts and intermediate CTEs retain additive counts and
   seconds. PPP, TS%, eFG%, OREB%, TOV%, FTR, pace, and ratings are calculated
   only after the selected games are aggregated.
7. **Reuse the existing narrow action fact.** `player_stats_actions_by_game`
   was extended additively with starter/team-event fields. No second custom
   clutch action table was created. Incremental publication still refreshes
   only changed games.
8. **Use explicit cached/direct routing.** Full-season unfiltered reads use the
   established materialized views. The exact standard 5/all/5:00 clutch preset
   uses its incremental cache. Other custom presets use direct readers. Do not
   force every preset through one generic selector.
9. **Filter identity before expansion.** Lineup identities are restricted to
   filtered facts before unit expansion. This reduced the verified standard
   five-player query from 2.94 to 1.12 seconds in the earlier parity run.
10. **Bypass unnecessary unit maps.** For five-player units, `unit_key` equals
    `lineup_key`; direct Lineups therefore bypasses `sub_lineups`. Sizes 2-4
    retain that mapping because combinations are real required work.
11. **Use max-minus-min segment duration for custom filters.** This mirrors the
    Israeli interactive convention: last qualifying action minus first
    qualifying action within each game/team/segment. The exact standard preset
    remains on its precomputed window intersection.
12. **Add covering indexes only after query shape is correct.** The action fact
    is 326 MB because it carries player and lineup fields. Its pages were fully
    visible, so index-only scans were viable. Purpose-specific covering indexes
    reduced reads to narrower structures: 59 MB for Team metrics and 35 MB for
    Team Minutes; Lineups needs its own identity/metrics/duration coverage.
13. **Verify index use, not just elapsed time.** Check `pg_stat_user_indexes`
    scan deltas and relation sizes after a call. Warm-cache timing alone can
    falsely credit an unused index.
14. **Protect live availability during DDL.** Prefer `CREATE INDEX
    CONCURRENTLY`; cap statement and lock time. Continuous app reads starved
    the Lineups concurrent build, leaving invalid shells. Those shells were
    verified in `pg_index` and removed explicitly. A normal bounded build was
    used only after confirming it permits SELECTs and no EuroLeague publication
    was active.
15. **Measure cold and warm separately with database caps.** A first read and a
    repeated read answer different questions. Every slow benchmark used a
    statement timeout; builds also used bounded lock/statement timeouts.
16. **Require parity before routing.** Compare full ordered rows where feasible.
    When the legacy full-season query times out, use a bounded team scope plus
    algebraic eligibility checks. A 30-vs-29 Lineups mismatch exposed an extra
    zero-exposure lineup; recreating offense/defense zero rows from duration
    restored exact 29-row parity.
17. **Treat unsuccessful ideas as evidence.** Candidate generic lineup indexes
    reduced buffers but not elapsed time and were rejected. Repointing only a
    low-level clutch function retained expensive wrappers and produced 19.7
    seconds. Separate regulation/OT branches were slower warm. Direct standard
    Lineups exceeded 10 seconds, so it was not routed live.
18. **Keep security contracts synchronized.** Every new app-callable function
    must appear in both the grant and audit allowlists. The hardening script
    correctly rolled back when only one list was updated.

#### Shared-scan analytical reader rules (required)

These rules apply to new or changed EuroLeague readers and to Israeli companion
readers when cross-league work is in scope. They are acceptance constraints,
not optional optimization suggestions.

1. **One expensive filtered fact per user request.** Identify the schedule and
   action/fact eligibility once. When multiple requested outputs use identical
   eligibility, derive them from one narrowly projected materialized CTE or one
   pre-aggregated fact. Do not independently rescan action grain for metrics,
   duration, ranks, or chart data within the same request.
2. **Choose the lowest sufficient source by route.** Default-season requests
   use indexed materialized views; non-clutch filters use per-game additive
   facts; the exact standard-clutch preset uses an incrementally maintained
   standard-clutch fact where available; only custom clutch reaches action
   grain. A route may not accept and silently ignore a filter.
3. **Combine consumers, not unrelated features.** Share a filtered set only
   when consumers have the same selected-game, clutch, starter, and row-
   eligibility semantics. Keep the public reader narrow enough for what the UI
   actually renders. Do not make Summary pay for Four Factors, shot profile,
   Traditional, lineup expansion, or other unused work merely to create one
   universal dashboard function.
4. **One app owner for each heavy result.** A Shiny reactive owns the expensive
   query and all same-semantics tables, pace denominators, ranks, observers, and
   charts project from that cached result. Add a server-level query-count test
   for the render/filter boundary; standard acceptance is at most one action-
   grain database call per rendered view and filter change.
5. **A helper name is not reuse.** Separate calls to a SQL view, CTE-returning
   function, or analytical helper still evaluate separately and do not share a
   scan. Use one combined consumer call or a persisted/incremental fact. Do not
   use request-local temporary tables in the app path.
6. **Materialize narrowly and aggregate late.** Carry only required identity,
   additive metric, and canonical duration columns through the filtered fact.
   Aggregate counts and seconds at their correct grains, then calculate PPP,
   ratings, percentages, pace, and ranks after aggregation. A lineup change
   never creates a possession, and segment seconds are counted once at segment
   grain.
7. **Prefer durable pre-aggregation for common routes.** If a common filter
   repeatedly needs action grain, extend the appropriate per-game or standard-
   clutch additive fact and its incremental refresh lifecycle instead of
   copying another action table or repeatedly reconstructing the same rows.
   Keep provider schemas and evidence isolated.
8. **Prove structure and behavior.** Static tests must assert the intended
   primary-fact scan count, the one shared materialization boundary when reused,
   filter-before-expansion order, late ratios, and absence of nested companion
   scans. Behavioral gates compare complete non-vacuous results, including
   additive counts, duration, ranks, nulls, and provider extensions.
9. **Prove warm and backend-first performance.** Before routing, run at least
   15 alternating complete-fetch warm samples and report median and p90. Also
   run repeated candidate-first and legacy-first samples on distinct fresh
   backends and inspect `EXPLAIN (ANALYZE, BUFFERS, SETTINGS)`. A warm win alone
   is insufficient on PostgreSQL 17; reject a candidate that causes a material
   cold regression for its actual UI consumer.
10. **Measure the real UI composition.** Time the calls made by one rendered
    view, not the sum of mutually exclusive views. Record query count and full
    fetch time. Treat buffer reduction, network-round-trip reduction, and
    elapsed-time reduction as separate evidence.
11. **Apply additively and preserve rollback.** Keep existing readers as
    compatibility surfaces during cutover, revoke PUBLIC execution, update the
    app-role grant and independent audit allowlists together, run security and
    reachability audits, and do not deploy the app as an implicit part of DDL.
12. **Document retained duplication.** If separate readers remain because
    grains differ or a combined body regresses cold latency, record the measured
    reason. Historical duplication is not justification, but a demonstrated
    source, integrity, queryability, or latency benefit is.

Current Israeli result from the 2026-09-01 measurement: Summary and its Minutes
path remain unchanged. Migration 051 applies only to standard-clutch **Four
Factors + Minutes**, omitting Ratings ranks/records and Israeli shot-profile
work. It passed the parity, 15-sample warm, repeated fresh-backend, security,
and query-count gates and is routed locally but not deployed. See
`docs/plans/2026-09-01-israeli-standard-clutch-dashboard-measurement.md`.

The formerly planned custom Team/Lineup action-fact backfill is complete.
`player_stats_actions_by_game` already contains the canonical starter and
additive team-event fields, and the app already routes to cached-standard,
per-game, or direct-custom readers. Do not add a second custom-clutch action
table. Remaining work is the measured cold-backend behavior above and the warm
Lineups/Tab 8 paths, not another copy of the action grain.

## End-to-end ETL

```text
EuroLeague schedule + box score + PBP
        |
        v
Restartable per-game collectors and local cache
        |
        v
Offline staging
  - package PBP cleanup and Lineup_A / Lineup_B
  - deterministic parent and FT-trip grouping
  - possession endpoint and offense-team assignment
  - roster/identity resolution, reconciliation, and QA
        |
        v
One staged game snapshot
  - full_rosters
  - team_boxscores
  - actions_raw
  - actions
  - reconciliation_metrics
  - game_qa
  - qa_incidents
        |
        v
One PostgreSQL transaction per game
        |
        v
Actions-derived consumer facts
  - action_team_context_actions
  - matchup_segments_actions
        |
        v
Game-level player and team facts
        |
        v
App materialized views and dynamic filtered functions
```

### 1. Collection

`scripts/load_games.py` coordinates schedule metadata, box-score collection,
PBP collection, staging, publication, and verification. Cached per-game inputs
make collection restartable. Requests use throttling, bounded retry/backoff,
and deterministic game ordering.

The package is the extraction adapter and the lineup constructor. Project code
adds reliability, immutable persistence, deterministic possessions,
reconciliation, QA, and database mapping. Do not implement another lineup
engine unless a measured package failure establishes the need.

### 2. Offline staging

Staging has no database I/O. It converts one cached game into a complete
`GameSnapshot`, validates the schema coverage, and writes a checkpoint under
`data/staging/`. A checkpoint is reusable only when its format version and
input hashes match.

Package lineups are reconstructed from box-score starters and substitution
rows before the event snapshot is built. The complete package-enriched event,
including `Lineup_A`, `Lineup_B`, `IsHomeTeam`, and
`validate_on_court_player`, is preserved in `actions_raw.raw_event`.

### 3. Per-game publication

The game is the transaction and retry boundary:

1. Resolve/upsert shared teams, players, schedule, and immutable source
   artifacts.
2. Delete the game's replaceable rows child-first.
3. Insert the seven staged snapshot relations parent-first.
4. Rebuild the two actions-derived consumer facts.
5. Refresh player and team game facts.
6. Run database-side counts, structural checks, and reconciliation.
7. Commit only if every validation succeeds; otherwise roll back that game.

When the batch finishes, `refresh_app_materialized_views()` runs before the
load is marked completed. This is intentionally fail-closed so a successful
load cannot leave the app snapshots stale.

## Current schema contract

### Lineage, dimensions, and validation

| Relation | Grain and purpose |
|---|---|
| `load_runs` | One extraction/publication run, with package and collector lineage. |
| `source_artifacts` | Immutable schedule/PBP/box-score evidence and hashes. |
| `teams` | One provider team identity per competition. |
| `players` | One provider player identity per competition. This is not yet a cross-season person dictionary. |
| `schedule` | One row per `(competition, season, gamecode)`. |
| `full_rosters` | One named roster player per game and team, including starter and minutes evidence. |
| `team_boxscores` | One official box-score row per game and team. |
| `reconciliation_metrics` | PBP-versus-official metric comparison per game and team. |
| `game_qa` | One publication/QA summary per game and load run. |
| `qa_incidents` | Event-level or game-level review evidence. |

Only named box-score/PBP actors resolve to a normalized player foreign key.
Coach, bench, `Team`, `Total`, and other pseudo-actors remain in raw evidence
with `player_id` null or are excluded from normalized rosters as appropriate.

### `actions_raw`: immutable package-level evidence

Grain: one package-enriched PBP event per
`(game_id, source_event_order)`.

In this project, “raw” means the complete row returned by the pinned package
after its supported PBP cleanup and lineup enrichment. It is not limited to the
provider's wire payload. The table retains useful typed lookup columns and the
complete event in `raw_event JSONB`.

The JSON is retained only here so that:

- every package field can be reproduced and audited;
- future package changes can be detected;
- typed normalization can be re-run without recollecting the game;
- package lineups and validation flags are never lost.

### `actions`: canonical analytical event

Grain: exactly one row per `actions_raw` event, with the same primary key.

It contains:

- all 22 typed fields emitted by the pinned package after lineup enrichment;
- internal `team_id` and nullable `player_id` mappings;
- `lineup_a text[]` and `lineup_b text[]`, each exactly five players;
- source/package/load lineage;
- synthetic parent and free-throw-trip identities;
- grouping status, confidence, trace, and parser version;
- `end_possession` and endpoint reason;
- game and team possession sequence numbers and the offense team on endpoints.

`actions` deliberately has no JSON column: the complete immutable JSON already
exists in `actions_raw.raw_event`.

This is the end-goal event table discussed during the schema review: every PBP
event, its additional source and parser data, both on-court lineups, and an
explicit marker saying whether the possession ended on that event.

### Lineup identity

There is no current `lineup_id`, `lineup_a_id`, or `lineup_b_id`.

- `lineup_a` is the home lineup supplied by the package.
- `lineup_b` is the away lineup supplied by the package.
- They are event attributes, not offense/defense labels.
- `segment_id` is a deterministic sequence within `(game_id, team_id)`. It is
  not a reusable lineup identity.
- Cross-game or cross-season lineup units should be keyed from resolved
  internal player IDs, not provider names or a game-specific segment ID.

### `matchup_segments_actions`: lineup duration fact

Grain: one consecutive joint-lineup interval per `(game_id, team_id,
segment_id)`.

Each physical basketball interval becomes two rows, one from each team's
perspective. A row stores `own_lineup`, `opp_lineup`, starter counts, event
boundaries, canonical elapsed boundaries, and `segment_seconds`.

A segment begins when the home/away lineup pair changes. A lineup change does
not create or end a possession.

### `action_team_context_actions`: event/team analytical fact

Grain: two rows per canonical event, one for each team perspective:
`(game_id, source_event_order, team_id)`.

It supplies the fields repeatedly needed by analytics:

- `team_id` and `opponent_team_id`;
- own and opponent lineups;
- offense/defense context;
- starter context and segment identity;
- points, shot counts, rebounds, turnovers, steals, and FT attempts;
- possession endpoint flag;
- own and opponent running scores;
- canonical elapsed time.

The fixed two-row expansion is appropriately implemented with a two-value
lateral expansion. This is not a combinatorial join: a game has exactly two
team perspectives. Persisting the result prevents every analytics query from
repeating the same event expansion.

`actions` is conceptually similar to the Israeli `pws` row because it combines
an event, lineups, and possession information, but it does not carry a rival
team perspective. `action_team_context_actions` is the closer equivalent of
the Israeli two-perspective central fact.

### Game facts and app read layer

| Relation | Grain and purpose |
|---|---|
| `player_four_factors_by_game` | Player/game/team, ON or OFF, offense or defense, and starter context; raw additive counts and minutes. |
| `team_four_factors_by_game` | Game/team and starter context; separate additive offense and defense counts. |
| `final_schedule_mv` | Indexed team-perspective schedule for app filters. |
| `player_onoff_default_mv` | Default full-season player ON/OFF snapshot. |
| `player_advanced_stats_mv` | Default player four-factor snapshot. |
| `player_traditional_stats_mv` | Default official traditional player-stat snapshot; migration 021, applied. |
| `team_game_ratings_mv` | One row per game and team; therefore two team rows per game. |
| `team_ppp_ratings_mv` | Season team ratings calculated from summed points and possessions. |
| `team_four_factors_mv` | Default team four-factor snapshot. |
| `get_team_minutes_dynamic()` | Filtered team minutes summed from canonical matchup-segment seconds; used with team ratings to calculate pace. |
| `lineup_totals_by_game` | Game/team/five-player-lineup, offense or defense, and opponent starter context; additive counts plus floor seconds on offense rows only. |
| `sub_lineups` | Season mapping from a five-player lineup to each of its 26 sub-units. Identity only, no metrics. |
| `sub_lineups_stats_mv` | Default season snapshot per 2-5 player unit. |

The functions `onoff_compute()`, `four_factors_compute()`,
`get_team_ratings_dynamic()`, `get_team_four_factors_dynamic()`, and
`get_team_minutes_dynamic()` provide filtered paths. Default season requests
use indexed materialized results. Game Logs reads the existing game-level
facts directly and does not add another physical fact table.

### Removed relations

Migration 012 proved bidirectional output parity and then removed the obsolete
EuroLeague middle layer:

- `actions_clean`
- `possessions`
- `lineups`
- `lineup_players`
- `action_lineups`
- `stints`
- `pws`
- `action_team_context`
- `matchup_segments`

Do not restore these merely to resemble the Israeli schema. Migrations 001-009
remain in the bootstrap history, so some create the old objects temporarily;
migration 012 is the authoritative final cutover.

## Event and possession rules

- Primary event ordering is provider sequence expressed as
  `(season, gamecode, period, source_event_order)`. Game clock is never an
  identifier, and provider `NUMBEROFPLAY` is not assumed to be ordered.
- Every event has a same-game, same-period synthetic parent; singleton events
  parent themselves.
- Incident identity, FT-trip identity, and possession endpoints remain
  separate concepts.
- Every free throw resolves to exactly one trip or remains explicitly
  unresolved. Clock equality alone is insufficient.
- Provisional or contradictory sequences retain status, confidence, trace, and
  QA reasons; the parser never forces alternation just to make the result look
  regular.
- Rebound control is determined from rebound-team versus shooting-team context
  before trusting provider offensive/defensive rebound text.
- Only endpoint events increment possession counts. Non-endpoint actions remain
  in `actions` with `end_possession=false`.

The typed Python parser is canonical. The pure R implementation remains an
independent regression reference. New event behavior must be a general rule
with a labelled fixture; do not add gamecode-specific exceptions.

## Analytics flow

Points and possessions intentionally come from different event properties:

1. Made 2PT, 3PT, and FT actions contribute points using that action's lineup.
2. Only deterministic possession endpoints contribute a possession, using the
   endpoint action's lineup and offense-team assignment.
3. Every event is expressed from both teams' perspectives, which gives
   separate offense and defense rows.
4. Segment durations supply lineup and player minutes.
5. Every named player on the complete game roster receives ON and OFF contexts,
   including players who never entered the game.
6. Store and sum raw points, possessions, shot/rebound/turnover counts, and
   seconds first. Calculate PPP, ratings, percentages, and differences only
   after the requested games are aggregated.

The two team-perspective rows are essential. Without them there is no explicit
place to store a team's offense and the opponent's corresponding defense.
Defensive rating is opponent points per defensive possession, so lower is
better.

## Comparison with the Israeli ETL

### Israeli flow

```text
Provider JSON
  -> schedule + full_rosters + actions_clean + subs
  -> possessions
  -> stints + lineups_lookup
  -> pws
  -> df_pts_poss_lineups_longer_mv (two team perspectives)
  -> player, lineup, and team facts
  -> app aggregates
```

### EuroLeague flow

```text
Package-enriched cached inputs
  -> actions_raw + actions
  -> action_team_context_actions + matchup_segments_actions
  -> player and team game facts
  -> app aggregates
```

### Similarities

- Schedule and full-roster dimensions precede analytics.
- The canonical event order is preserved.
- Possessions are counted at deterministic endpoints.
- Lineup exposure is attached to action and time context.
- A two-team-perspective fact supports offense and defense.
- Player and team game facts retain additive components.
- Ratios are calculated after aggregation.

### Differences

| Concern | Israeli League | EuroLeague |
|---|---|---|
| Main implementation | R-centric orchestrator | Python collection/staging/publication; R is a possession reference |
| Lineups | Reconstructed by the ETL from starters/substitutions | Reconstructed by the package and preserved on every event |
| Event-to-final middle layer | `actions_clean -> possessions -> stints/lineups_lookup -> pws` | `actions_raw -> actions` |
| Central team-perspective fact | `df_pts_poss_lineups_longer_mv` | `action_team_context_actions` |
| Duration fact | Israeli stints/lineup lookup chain | `matchup_segments_actions` |
| Raw provenance | Provider fields and cold-storage exports | Explicit load runs, source artifacts, hashes, package version, and immutable package-event JSON |
| Publication | Phased R ETL and dependent incremental refreshes | Explicit one-game transaction and checkpointed retry |
| 2-5 player units | `sub_lineups` mapping plus `sub_lineups_stats` season table | Same shape: `sub_lineups` mapping plus `lineup_totals_by_game` and `sub_lineups_stats_mv` |
| Cold storage | Historical hot tables can be exported/truncated | No cold-storage lifecycle yet |

The Israeli intermediate tables primarily exist because its ETL must construct
and normalize lineups before it can create lineup stints. EuroLeague already
has event-level reconstructed lineups, so copying that entire middle layer
would add duplication without adding basketball information.

## Delivered: 2-5 player lineup units

Implemented in migrations 013 and 014 and surfaced by Shiny tab 10
(`euro_lineups`). The design is recorded in
`../docs/superpowers/specs/2026-08-10-euroleague-013-lineup-units-design.md`.

An earlier draft of this section proposed a per-game fact at *unit* grain. That
is not what was built. The Israeli `sub_lineups` shape was used instead, which
keeps the 26x expansion out of the facts entirely:

```text
action_team_context_actions (event metrics, possession endpoints)
  + matchup_segments_actions (segment seconds)
  + full_rosters (provider name -> internal player_id)
  -> lineup_totals_by_game     (fact, five-player-lineup grain, per game)
  -> sub_lineups               (season mapping, 26 units per lineup, no metrics)
  -> sub_lineups_stats_mv      (season roll-up, app fast path)
  -> fetch_lineups_dynamic()   (filtered path)
```

The expansion therefore multiplies distinct lineups per season, not
team-game-segments. Measured over the 84 loaded games: 17,144 fact rows, 83,824
mapping rows, 17,293 season units, and a 5-7 second full MV refresh.

### Identity

`lineup_key` is `md5` over the sorted internal `player_id` array; `unit_key`
uses the same construction over the unit's members, so `unit_key = lineup_key`
at size 5 by definition. Grouping happens on the provider name array, which is
valid *within* a game; identity is keyed on resolved internal IDs, which is what
survives across games. Nine lineups in the loaded season carry more than one
provider name spelling (for example `EBUKA, IZUNDU` versus `IZUNDU, EBUKA`);
name-based keying would have split each of them in two.

### Validation

`scripts/verify_lineup_units.py` holds nine gates, all passing over all 84
games. The load-bearing ones are G3 (per game and team, the summed lineup rows
equal `team_four_factors_by_game`), G4 (summed offense-row seconds equal segment
seconds and canonical game length), and G5 (the roll-up matches an independent
name-membership recomputation from the event fact).

Containment, duplicate-unit, and five-player-identity checks are deliberately
absent: this architecture makes all three tautologies, and asserting them would
repeat migration 009's mistake of checking what the schema already guarantees.

G5 compares two paths with deliberately different row populations -- the
roll-up is built from segments, the recomputation from events -- so it treats a
missing group as zero in one direction only, and separately asserts that no unit
the events prove exists is absent from the roll-up, and that no unit without
events carries counts.

### Not built

(Migration 019 has since delivered clutch filtering on this surface; see
below. This subsection is left as the historical record of why it was
deferred at the time the lineup-unit read layer shipped.)

Clutch filtering. It needs the pre-shot margin per event, which the
pre-aggregated fact cannot answer, so it requires a third query path against
`action_team_context_actions` and its own design pass.

## Delivered: shared filter chips and rank cells across both leagues

Branch `shiny/euro-tab1`, 2026-08-11. App-side only: no schema, SQL, or ETL
change, and no database connection.

This continues the code-unification work recorded in commits 1d959d7, 2fd1d5e,
8f5f02a, f456ee6 and 954b754. It closes the two largest remaining duplications
between the EuroLeague tabs and their Israeli companions, both of which were
audited and named before the work started.

### What was duplicated

`build_filter_chips()` lives in `app/R/global.R` and seven Israeli tabs use it.
All three EuroLeague tabs hand-rolled their own chip builder instead — roughly
70 lines in `server_tab8_euro.R`, 67 in `server_tab9_euro_team.R`, 25 in
`server_tab10_euro_lineups.R`. `setup_chip_clears()` was already shared and
already used by all three, so only the builder had drifted.

`fmt_rank_cell()` — the three-line value/rank/delta cell on the team-ratings
tabs — existed twice, in `server_tab3.R` and `server_tab9_euro_team.R`. The
copies differed only in that Tab 3's honoured a `show_delta` flag.

### What was done

`build_filter_chips()` was generalised rather than cloned, taking the league
dimension as arguments that all default to today's Israeli behaviour, so none
of the seven Israeli call sites changed. The argument list and the reasoning
behind each are recorded in the root `CLAUDE.md`, under the EuroLeague reuse
rule; that table is the reference, not this section.

Two structural fixes inside the builder were needed to serve a fourth and fifth
prefix at all: the teams branch now falls back to `<prefix>_teams` instead of
returning `NULL` for any unrecognised prefix, and the players-on/off block lost
its `prefix == "ld"` gate in favour of reading `<prefix>_players_on` / `_off`.
For prefix `ld` both resolve to the same inputs and the same clear ids, so Tab
2's behaviour is unchanged.

`fmt_rank_cell()` moved verbatim from `server_tab3.R` to `app/R/helpers.R`,
with `show_delta` promoted from a closure over the render scope to an argument
defaulting to `TRUE` — which is exactly what Tab 9's copy did. Tab 3's fifteen
call sites pass it explicitly; Tab 9's copy was deleted. The stub in
`tests/testthat/helper-server-mocks.R` gained the same argument.

### Three defects this surfaced

1. **Tab 10 was hiding its own filters.** Its chip bar showed only unit size,
   row count and min-possessions. Dates, phase, opponents, home/away, outcome,
   round range, last-N, starter bounds and opponent rank were all being sent to
   `fetch_lineups_dynamic()` with nothing on screen to say so — a filtered
   result was indistinguishable from an unfiltered one. The tab now renders the
   full chip bar and has `setup_chip_clears()` wired, which it never had. The
   three informational readouts survive as `extra_children`.
2. **The date chip cleared to the wrong season window.** `setup_chip_clears()`
   resolved its reset target with the Israeli `shared$season_date_bounds`, and
   Tabs 8 and 9 fed it a EuroLeague season read from `euro_game_year`. Season
   2025 therefore resolved to the Israeli 2024-25 window (Oct 2024 - Jul 2025)
   instead of Sep 2025 - Jul 2026. Fixed with a `bounds_fn` argument; all three
   EuroLeague tabs pass `euro_season_date_bounds`.
3. **Tab 10 used `"all"` as its blank sentinel** for Home/Away and Outcome,
   where every other tab in both leagues uses `""`. The read layer already
   coerces `''` to `'all'` (`COALESCE(NULLIF(btrim(p_home_away), ''), 'all')`),
   so the two were equivalent to SQL and the divergence bought nothing. The UI
   was aligned to `""` rather than teaching the shared builder a per-tab
   special case.

The EuroLeague chip bars also gained styling they never had: they were wrapped
in `.filter-chips-bar`, which has no rule anywhere in `app/www/app.css`. The
shared builder emits `.filter-chips`, which does.

### Deliberate display convergences

Where the EuroLeague wording differed from the Israeli for no league reason, it
now follows the Israeli: opponents read `vs 3 opps` rather than `vs 3 teams`,
and the two starter bounds collapse into one `Starters: Own ≥3, Opp ≤2` chip
instead of one chip per side. Round ranges stayed league-specific and read
`Rd≥1 Rd≤10`, via the `gn_label` argument — round-versus-GN is on the short
list of genuine league dimensions.

One behavioural note: the shared builder reads the round and last-N inputs
directly, where the old EuroLeague builders read the post-mutual-exclusion
`gn_params()`. If both a round range and a last-N are somehow set, both chips
now appear. This matches every Israeli tab, and the mutual exclusion applied to
the actual query is untouched.

### Verification at the time of the initial extraction

All `app/R/*.R` files parse. `test-tab-wiring.R` passes, 42 of 42. The full
suite (41 files, 890 `expect_` calls) was **not** run and remains outstanding —
`test-league-shared-helpers.R` is the other file worth running for this change.
Focused tests for shared helpers, Team Ratings regressions, Game Logs parsing,
and the minutes read layer now pass. Database and deployed-app tests remain
opt-in because they require external state.

Line endings needed repair: `global.R` and `server_tab3.R` are stored with
mixed CRLF/LF and the editor normalised both to all-CRLF, inflating the diff to
208 and 112 lines against 74 and 44 of real change. Untouched lines were
restored to their original endings, after which the raw `git diff --stat`
matches `--ignore-cr-at-eol` exactly. Check this on every edit to those two
files.

### Not done, and where tab unification stands

Tab 10 now has the same clutch controls and parameter semantics as Israeli Tab
2. Migration 019 is applied (see the delivered/validation-evidence sections
above); a local browser smoke test of both tabs' clutch controls remains
before the feature can be treated as fully deployed.

The agreed direction for the remaining duplication — extract into a league
descriptor plus shared helpers, never merge the tab files per pair — and the
measured per-file overlap are recorded in the root `CLAUDE.md` under the
EuroLeague reuse rule. Read that before continuing. What is left, in rough
cost order:

1. **`server_tab8_euro.R` against `server_tab1.R`** — the big one. 1,306 lines
   at roughly 81% overlap, the highest of any pair, and the only pair where a
   descriptor-driven extraction would pay for itself immediately. Nothing has
   been done here beyond the shared pieces listed in `CLAUDE.md`.
2. **`ui_tab8_euro.R` against `ui_tab1_onoff.R`** — 190 lines at 68%. The
   sidebar accordion is already built by a shared `global.R` builder; what
   remains is the per-tab input scaffolding around it.
3. **`setup_chip_clears()`'s teams handling.** The EuroLeague tabs still carry
   their own team-clear observers, because that helper picks between
   `character(0)` and `""` from a hardcoded Israeli id allowlist
   (`c("teams", "ts_teams")`) rather than from an argument. Small and
   well-understood; it would delete three near-identical observers.
4. **Tab 9 against Tab 3** — 34% overlap. Lower value, and Tab 9 is the smaller
   side of the pair, so the descriptor work above should land first and be
   reused here rather than derived twice.

   **Tab 10 against Tab 2 is now largely done** (2026-08-15, see the delivered
   section below): both tabs render through one pair of extracted helpers and
   Tab 10 uses Tab 2's sidebar. This was done without a descriptor — the two
   league differences are passed as a small `spec` list — and it is a useful
   precedent for what the descriptor should eventually absorb rather than a
   substitute for it.

The league descriptor itself does not exist yet. Until it does, each shared
piece is passed as an explicit argument, which is why `build_filter_chips()`
now takes seven of them. That is the right shape for two or three call sites
and the wrong shape for twenty — the descriptor is what replaces it, not
another round of arguments.

## Delivered: Tab 10 lineup table and sidebar parity with Tab 2 (2026-08-15)

Tab 10 no longer has a table of its own. `server_tab2.R`'s `renderDataTable`
body was extracted into `lineup_summary_datatable()` and
`lineup_ff_datatable()` in `app/R/helpers.R`, and both tabs now call them. A
`spec` list carries the only two league differences — the anchor class and the
click statement — so matching columns is a property of the code rather than a
claim about one day's screenshot.

| Commit | Change |
|---|---|
| `299ea6c` | Extract the two lineup DataTables into `helpers.R` (byte-identical move, verified by reversing the transform and diffing against `HEAD`) |
| `22a21d9` | Tab 10 renames its frame to Tab 2's column contract and renders through the shared helpers |
| `2a1b6e9` | Fix the lazy-default `raw = df` trap in `lineup_ff_datatable()` |
| `f7aeae6` | Tab 10 adopts Tab 2's sidebar, gains the shot-splits legend, FF explainer switches TS% → eFG% |

What Tab 10 gained: the 2PT/3PT shot-split cells and their legend, the
per-column `filter = "top"` boxes, `+/-`, `Off Pts`/`Def Pts`, `# Starters`,
Tab 2's paging (50, menu 25/50/100/200/1000), and Tab 2's sidebar order,
labels and widget types (group size is now an inline radio, the min-possessions
slider regained its help text).

Three design points worth carrying forward:

1. **Rank polarity lives in the rank value, not the palette.** The shared
   renderer applies `COLS_GRAD` to every `pr_*` column and never uses
   `COLS_REV`; Tab 2 bakes the inversion in upstream via `invert = TRUE`.
   Tab 10's `add_pct_ranks()` was changed to match and its `COLS_REV` heat list
   deleted. Any future tab calling these helpers must do the same or its
   defensive columns render backwards, and no test catches it.
2. **Counts are copied before rates are derived.** `off_tov` is a raw count in
   the EuroLeague schema and a rate in Tab 2's contract — the same name on both
   sides of the map. `to_tab2_contract()` copies every count column first and
   overwrites the rate names last. `off_fg3_made` and `off_fga` are copied
   rather than renamed because the Summary shot splits and the FTR denominator
   still read them.
3. **`ts_possessions` is unchanged.** Only the displayed column moved from TS%
   to eFG%; the EuroLeague TS% denominator is still computed and stored. The
   reversal is recorded in `euroleague/CLAUDE.md` beside the original decision.

**The bug this nearly shipped.** The extracted renderer took `raw = df` as a
default argument. R evaluates defaults lazily *in the function's own frame*, so
by the time `raw` was read — deep in the TOTAL-row block — `df` had already been
narrowed by `select(any_of(keep_cols))` and every count column was gone. Every
`sum(raw$...)` silently returned 0, so Tab 10's Four Factors TOTAL row would
have displayed zeros and NAs rather than erroring. Tab 2 was never affected
because it passes `raw = ld_data()` explicitly. The fix passes `raw` explicitly
at the call site *and* hardens the default to `raw = NULL` with
`if (is.null(raw)) raw <- df` as the first statement. The general lesson: when
extracting shared code, copy the working caller's call convention too — the one
part of the signature that was invented rather than copied is where the defect
was.

**Verification.** Offline replay against live EuroLeague data reconciled Off PPP
to 116.9827 against an independent `lineup_totals_by_game` aggregate. Both tabs
were then opened side by side in the running app, Summary and Four Factors, and
compared: identical headers, one TOTAL row, shot bars, filter boxes, sidebar.
The FF TOTAL row reads 121.6 / 57.6 / 32.0 / 16.6 / 26.6 — non-zero, which is
the `raw=` fix confirmed end to end. 297 tests pass, 0 fail.

**Open, and deliberately not fixed here.** Six of 8,240 EuroLeague units report
more offensive rebounds than rebound opportunities, so their OREB% exceeds 100.
`to_tab2_contract()` copies both columns verbatim with no arithmetic, so the
defect is upstream in `euroleague.sub_lineups_stats_mv`. It is an ETL fix, not
a UI one, and it is still outstanding.

**Delivered 2026-08-15** (migration `039_lineup_starters_numerator.sql`,
commits `3880747` Israeli / `94ba256` EuroLeague / `750a4dd` display). Full
account including every pitfall is in the root `PROJECT.md` under
"Session Update (2026-08-15): Real `# Starters` on Both Lineup Tabs" — read
that before touching this column again.

The design spec (and an earlier draft of this section) claimed Tab 2's
`# Starters` is simply a constant equal to the group size. That is true of only
one of `fetch_lineups_all`'s three branches. Verified 2026-08-15:

| Branch | `num_starters` |
|---|---|
| Fast path, line 101 | `s.num_lineup::numeric` — the constant |
| Filtered path A, line 315 | `ROUND(SUM(lt.num_starters * lt.total_poss) / NULLIF(SUM(lt.total_poss),0), 2)` |
| Filtered path B, line 485 | same weighted expression |

So the possession-weighted mean **already exists** on both filtered paths. The
live defect is that the column changes meaning depending on whether a filter is
active: a real weighted average when the query goes dynamic, a group-size
placeholder when it hits the MV. Applying any starter filter on Tab 2 shows
5-player units with values below 5, which is the filtered path telling the
truth.

Three facts that shape the fix:

- **`num_starters` and `own_starters` are the same value**, not two columns.
  `df_pts_poss_longer.sql` aliases both to `pws.num_starters_offense` on
  offense-perspective rows and to `pws.num_starters_defense` on defense rows,
  so both always mean "own starters on court". The filter and the display agree.
- **The existing weighting is over offensive *and* defensive possessions.**
  Line 315's `SUM(lt.total_poss)` carries no `type_lineup` filter, unlike every
  other aggregate in that SELECT. Coherent, since `num_starters` is always
  own-perspective — but it is not the offense-only
  `Σ(own_starters × off_poss) / Σ(off_poss)` the spec proposed. Whichever
  definition is chosen, the fast path must adopt the same one or the column
  changes meaning a second time.
- **The GRANT hazard is on the EuroLeague side, not the Israeli side.**
  `basketball_test.sub_lineups_stats` is a TABLE, populated by `INSERT INTO` in
  `refresh_sub_lineups_stats()` and `refresh_sub_lineups_stats_for_games()`;
  `ALTER TABLE ... ADD COLUMN` preserves its grants, and both refresh functions
  already read `df_pts_poss_lineups_longer_mv`, which carries `num_starters`, so
  the numerator needs no new join. `fetch_lineups_all` already declares
  `num_starters numeric` in its `RETURNS TABLE`, so `CREATE OR REPLACE` keeps
  the signature and its EXECUTE grants. By contrast
  `euroleague.sub_lineups_stats_mv` is a MATERIALIZED VIEW, whose query cannot
  be altered — that side needs DROP+CREATE, which wipes grants and drops
  `euroleague_sub_lineups_stats_mv_pk` and `..._size_idx`. The security
  apply/audit pass belongs there.

On the EuroLeague side the readers `035_direct_lineups_reader`,
`038_pergame_lineups_reader` and `014_lineup_units_read_layer` all filter on
`l.own_starters` but none of them return it, so the data is already joined and
in scope in every one of them.

What shipped on the EuroLeague side: `sub_lineups_stats_mv` and all three
readers now carry `starters_poss_num`, and `to_tab2_contract()` divides it by
`off_poss + def_poss` in R alongside the other rates — the read layer returns
the numerator, never a ratio. `fetch_lineups_pergame` was also added to both
security declarations; it had been missing since migration 038 and was
therefore outside the declared contract.

Two EuroLeague-specific traps, both now handled in 039 and both worth
remembering for the next MV change here:

- `CREATE FUNCTION` grants EXECUTE to PUBLIC by default. Recreating the three
  readers widened them to every role; the pre-migration ACL had none. The
  migration REVOKEs from PUBLIC before re-granting. Capture
  `pg_proc.proacl` **before** any DROP, and compare after.
- `lineup_totals_by_game.possessions` is `integer`, so
  `sum(own_starters * possessions)` is `bigint` and dividing it by
  `off_poss + def_poss` integer-divides. The MV numerator is cast `::numeric`
  for that reason; the readers were already safe because they cast in their
  final SELECT. The fast-vs-filtered invariant is what exposed it, and both
  paths now agree to 0.0000 across all 5,864 five-player units.

Branch `shiny/euro-tab1` — not merged, not deployed.

## Delivered: navbar and Home parity with the Israeli section (2026-08-15)

Cosmetic, but it completes the parity thread above at the level the user
actually navigates. The two leagues offer the same five surfaces and presented
them in a different order under different icons, so switching leagues moved the
tabs around under the cursor.

| Concern | Was | Now |
|---|---|---|
| Navbar order | Player Stats, On/Off, Team Ratings, Lineup Data, Game Logs | On/Off, Lineup Data, Team Ratings, Game Logs, Player Stats — Tabs 1-2-3-4-5 |
| On/Off icon | `bi-globe2` | `bi-toggles` (Tab 1) |
| Team Ratings icon | `bi-bar-chart-fill` | `bi-trophy-fill` (Tab 3) |
| Lineup Data icon | `bi-people` | `bi-people-fill` (Tab 2) |
| Game Logs icon | `bi-calendar-event` | unchanged — already matched Tab 4 |
| Home cards | On/Off, Team Ratings | the Israeli layout: On/Off + Lineup Data, Team Ratings + Game Logs, Player Stats |

Commits `b57de29` (order, icons, two Home cards) and the follow-up that moved
the shared tabs last. Three points worth carrying forward:

- **The EuroLeague section has five surfaces, not four.** Player Stats is not
  a per-league pair like 8↔1 or 10↔2 — it is *one* tab serving both leagues,
  switching on `input$league_select` (`server_tab5_traditional.R:427-430`
  onward: `ts_is_euro()` picks the season selector, the date bounds, the phase
  input, the cache prefix and the schema). That is why it is absent from
  `app.js`'s `TAB_LEAGUE` map and stays visible under both leagues, and it is
  the shape the "extract shared logic, do not merge the tab files" direction is
  aiming at. Only Compare is genuinely Israeli-only.
- **Shared tabs must sit last in the navbar.** One DOM order has to render
  correctly under both leagues, because app.js hides rather than reorders. With
  Player Stats at DOM position 5 it led the EuroLeague navbar (Home → Player
  Stats → On/Off) while sitting fifth in the Israeli one. Putting the Israeli
  block, then the EuroLeague block, then the shared tabs makes both sequences
  come out identical. Any future shared tab goes at the end for the same
  reason. Reordering is otherwise free: `TAB_LEAGUE` keys on the tab *value*
  (`euro`, `euro_lineups`, …), never on navbar index.
- **Home mirrors that.** Its EuroLeague Player Stats card reuses the Israeli
  `go_playerstats` input id rather than adding a `go_euro_playerstats` — there
  is no league-specific tab to dispatch to, only one of the two cards is ever
  visible, and the delegated `js-shiny-event` handler reads `data-input-id` off
  whichever was clicked. The block stays one card short of the Israeli one
  (Compare). The new cards carry no team prefill because the Home team selector
  is `league-only-il` — the Israeli `go_lineups`/`go_gamelogs` handlers write
  `shared$pending_ld_team` / `pending_gl_team` and the EuroLeague ones have
  nothing to write. `go_euro_lineups` does mirror the one non-navigational
  thing its Israeli twin does: it sets group size to 5, matching the card's
  "5-man units" copy.
- **`app/app.R` has mixed line endings**, and `sed`/`cat -A` under Git Bash
  strip CR before you see it — both reported the observer block as LF when the
  bytes are CRLF. Edit that file on bytes with an exact-match assertion, and
  check the staged blob's CR count against `HEAD`'s before committing (319 →
  330 here, exactly the lines added). A silent whole-file re-ending is the
  failure mode.

## Recent changes: 2026-08-11 to 2026-08-12

The following commits landed in the last day, in dependency order:

| Commit | Change | Practical result |
|---|---|---|
| `fb9e02a` | Shared filter-chip builder and rank-cell helper | EuroLeague tabs reuse Israeli filter UI, reset behavior, labels, and rank-cell rendering; wrong-season date reset and hidden Tab 10 filters were fixed. |
| `e7f14e0` | Shared Israeli/EuroLeague ON/OFF plumbing | Common descriptors, filter mapping, fast-path gates, local filters, and DataTable helpers now serve both leagues while retaining league-specific SQL and feature flags. |
| `edd7021` | Shared lineup/team-rating plumbing | Team and lineup tabs use neutral shared context helpers and shared metric-rank polarity rather than duplicate parameter conversion code. |
| `42a8a4a` | Lineup fast-path fix | EuroLeague lineup auto-minimum calculations use the correctly shaped filtered population before rendering. |
| `c9c32e6` | Lineup filters and extraction controls | Lineup filters re-enable auto thresholds when the data scope changes; collectors support fetch-only, box-score-only, cached-input reuse, missing-input handling, and bounded fetch batches with cooldowns. |
| `4c88ff3` | EuroLeague ON/OFF auto possession minimums | EuroLeague now starts from the same non-zero automatic minimum policy as Israeli ON/OFF, with regression coverage. |
| `33610db` | EuroLeague Game Logs | New Tab 11 supports Summary and Four Factors game-level views, shared filters/chips, W/L styling, rank heatmaps, CSV export, and per-game team-perspective facts. |
| `0ccaeaf` | Team minutes and pace | EuroLeague exposes filtered team minutes from canonical segment duration, adds Min/Off Pace/Def Pace to Team Ratings, and documents the `app_readonly` security boundary. |

### What this means for the current app

- Tabs 8-11 now share more of the Israeli application's interaction contract,
  but they still use EuroLeague-specific season, phase, round, possession, and
  schema semantics.
- Game Logs is an additive read surface; it does not create a new fact table.
  It adapts the EuroLeague team four-factor game fact into the shared Israeli
  game-log calculator and keeps both team perspectives explicit.
- Team pace is not a stored ratio. Minutes are summed first, and pace is
  calculated only after the requested games and starter context are aggregated.
  Overtime is therefore preserved.
- The local app uses `app_readonly`. The minutes function is intentionally
  `SECURITY DEFINER` with a fixed search path and schema-qualified relations;
  direct segment-table access remains denied.

### Follow-up thoughts

1. **Deploy/restart verification:** after pulling `0ccaeaf`, restart the local
   Shiny process so the new minutes reactive and Tab 11 sources are loaded. A
   live app bundle still needs deployment separately from the database migration.
2. **Add a direct app smoke test for minutes:** render EuroLeague Team Ratings
   through an `app_readonly` connection and assert non-empty `Min`, `Off Pace`,
   and `Def Pace` cells. This would have caught the original permission failure,
   which was hidden by a broad `tryCatch`.
3. **Keep read-layer permissions explicit:** any new function that reads an
   internal EuroLeague fact should document whether it is invoker or definer,
   fix its `search_path`, qualify relations, and include a role-level test.
4. **Run a focused extraction dry run before a live load:** use the new
   `--fetch-only`, `--skip-fetch`, and batch cooldown flags on a small gamecode
   range, then run staging/verification before requesting publication approval.
5. **Unify the remaining large tab pair:** Tab 8/Tab 1 still has the largest
   measured duplication. A league descriptor plus extracted render helpers is
   the next high-value refactor; do not merge the two server files wholesale.
6. **Refresh the handoff after deployment:** record the deployed app bundle,
   migration 019 application timestamp, and exact local/live test results so
   repository state and database state do not drift again.
7. **Apply and benchmark the standard clutch fast path:** migration 020 now
   implements the primary performance target: margin <= 5 points in the final
   5 regulation minutes, with overtime following the existing default. It
   preserves migration-019 pre-event score semantics and exact segment/window
   minutes in an additive per-game cache. Custom definitions remain dynamic,
   and non-clutch requests retain their existing aggregate path. The remaining
   step requires explicit live-DDL approval: run
   `scripts/apply_020_default_clutch_fast_path.py`, confirm bidirectional cache
   parity, record warm cached-versus-dynamic latency, then re-run the repository
   security apply/audit pass.

## Database security boundary (applied 2026-08-12)

The schema was created outside the Israeli security pass, so it had accumulated
its access rules from individual migration `GRANT` statements and nothing else.
An audit found 67 violations across both schemas:

| Violation | Count | What it was |
|---|---|---|
| `untrusted_routine_execute` | 28 | `anon` and `authenticated` held the default `PUBLIC` EXECUTE on all 14 EuroLeague functions, including the six mutating `refresh_*` publication functions and the `SECURITY DEFINER` `get_team_minutes_dynamic`. Only the absent schema `USAGE` stopped them. |
| `rls_disabled` | 19 | No RLS on any of the 18 EuroLeague base tables, and no policies at all. Plus one Israeli drift: `basketball_test.team_metrics_by_game_mv`, a physical table with an `_mv` name that a later migration created after the previous hardening pass. |
| `rls_unexpected_policy` | 14 | Israeli tables carrying a legacy `rls_read_all_app_readonly` policy alongside `app_readonly_select_all`. |
| `app_unexpected_routine_execute` | 6 | `app_readonly` could EXECUTE the six `refresh_*` mutating functions, for the same `PUBLIC`-default reason. |

The fix extends the existing `sql/security/*.sql` to take `euroleague` as a
third target schema rather than adding a parallel EuroLeague security script.
Two dimensions differ from the Israeli schemas, both deliberately stricter and
both enforced by the audit:

- **Curated relation grants.** Israeli schemas use `GRANT SELECT ON ALL TABLES`
  because their SQL functions are `SECURITY INVOKER` and read widely. Applying
  that here would newly expose `actions_raw`, `source_artifacts`, `game_qa`,
  `qa_incidents` and `reconciliation_metrics`. `app_readonly`'s existing grants
  already matched the read layer exactly, so that 18-relation list is pinned in
  both files. `app_required_relation_select_missing` catches a removal;
  `app_unexpected_relation_select` catches a widening.
- **No `service_role`.** The shadow schema stays outside Supabase's managed
  surface and must never join the Data-API exposed schemas.

The audit now reads RLS state from the catalog rather than trusting the apply
script to have seen every table — which is precisely how the Israeli
`team_metrics_by_game_mv` drift survived unnoticed.

Verified after applying, as `app_readonly` over the 6543 pooler: every
EuroLeague read and all six app functions still work, while reads of
`actions_raw`/`source_artifacts`, execution of `refresh_app_materialized_views`,
writes to any table, and `CREATE` in the schema are denied. The audit reports
zero violations. Publication is unaffected: every `euroleague` table is owned by
`postgres` and none sets `FORCE ROW LEVEL SECURITY`, so the owner bypasses its
own policies.

Two consequences for future work:

1. **Re-run the security pass after every EuroLeague migration.** `CREATE OR
   REPLACE FUNCTION` on a new signature and any `DROP FUNCTION` leave the
   function executable by `PUBLIC` and wipe `app_readonly`'s EXECUTE grants; a
   new base table arrives without RLS. This is how the 67 violations
   accumulated in the first place.
2. **A new app-facing relation or function must be added to the allowlists** in
   `../sql/security/enable_readonly_rls.sql` and
   `../sql/security/audit_app_access.sql`, in the same change that creates it,
   or the audit fails.

The applied database state is committed; the files that reproduce it were left
uncommitted in the working tree, so they still need a commit on an `infra/`
branch. Until then a future `apply_db_security.R` run from a clean checkout
would silently revert `euroleague` to unhardened.

## Validation evidence

All 79 EuroLeague Python tests pass, including the 31-test focused
migration-020 schema/backend suite. The focused EuroLeague clutch/lineup R tests
pass, and all changed R files parse.

Migration 020 was applied to the live `euroleague` schema on 2026-08-13 and
committed as `b1c80c2`. It creates
`default_clutch_lineup_totals_by_game`, backfills it from the exact
`clutch_team_game_facts(..., 5, 'all', 300, false)` result, and wires both the
per-game and grouped publication paths to refresh changed games after the
canonical action consumers. `select_team_game_facts()` uses explicit branches
so the standard preset cannot accidentally fall through to the dynamic path.
The cache contains 4,433 rows and matches the full dynamic calculation exactly
(4,433 rows; no missing or extra rows).

The first apply attempts appeared stalled because the verification script
recomputed the full-season dynamic clutch query for parity and then repeated it
for the benchmark. The DDL had already committed successfully; the apparent
timeout was verification cost, not a database lock or migration failure. The
repository security audit still needs to be rerun successfully: the R wrapper
failed with `bad_weak_ptr` while connecting, so no security conclusion should
be inferred from that failed audit attempt.

Migration 019 was applied transactionally to the live `euroleague` schema on
2026-08-12 via `scripts/apply_019_clutch_read_layer.py`, which validates the
DDL (only the four expected signature-changing `DROP FUNCTION IF EXISTS`
statements, each immediately superseded by its own `CREATE OR REPLACE`; no
`CASCADE`; no reference to `basketball`/`basketball_test`), applies it, and
verifies the result. The connection and safety guard are scoped to
`euroleague` only -- this migration did not touch, and the apply script cannot
touch, the Israeli schemas.

Non-clutch parity, checked by snapshotting each rewritten function's default
output before applying and diffing after:

- `get_team_ratings_dynamic`, `get_team_four_factors_dynamic`,
  `get_team_minutes_dynamic`: byte-for-byte identical, 20 rows each.
- `fetch_lineups_dynamic`: 8,240 rows before, 6,008 after, at `p_unit_size=5`
  and no other filters. Root-caused and accepted: the new
  `clutch_team_game_facts()` adapter (which all four functions now route
  through, to gain clutch parameters) omits lineup instances whose matchup
  segments all have `segment_seconds = 0` -- instantaneous back-to-back
  substitution artifacts with no floor time. All 4,263 of the missing
  `(game, team, own_lineup)` combinations were confirmed to have exactly zero
  seconds, zero possessions, and zero points across every one of their 4,700
  underlying segment rows; 2,232 season-level lineup units disappear entirely
  because every appearance they ever had was one of these zero-duration
  ghosts. This changes nothing about any real metric (every dropped row sums
  to zero) and arguably improves data quality by dropping pure noise, but it
  is a genuine behavior change from the pre-migration function, decided and
  accepted explicitly rather than silently passed through. The apply script
  documents this exact discrepancy so a future re-run treats it as expected
  and would still fail loudly on a mismatch of a different shape.

Also verified live, connecting as `app_readonly` over the same pooler the app
uses: `get_team_ratings_dynamic`, `get_team_minutes_dynamic`, and
`fetch_lineups_dynamic` all return sane clutch-filtered rows (e.g.
`p_max_margin=5, p_max_time_remaining=300`), and direct `SELECT` on
`euroleague.matchup_segments_actions` is still denied -- confirming the
`SECURITY DEFINER` scoping migration 018 required is intact for the new
functions too.

Browser-testing both tabs (Team Ratings and Lineup Data) with the clutch
controls live is still outstanding.

The exploratory 100-game sample established the parser and schema rules:

- 56,463 events and 14,684 possessions matched exactly between Python and R.
- All 4,122 free throws resolved.
- All 16 PBP-versus-box-score metrics matched across 200 team-games.
- Strict score progression passed 97/100 games; three exact adjacent
  one-event-ahead provider snapshots passed the bounded reconciliation rule,
  giving 100/100 reconciled games.
- Every event had two unique five-player package lineups.
- Thirty-seven package-invalid actor rows were retained as QA evidence rather
  than repaired by a second lineup engine.

For the recorded 84-game live load:

- raw and canonical event keys match exactly;
- all package fields round-trip from `actions` to `actions_raw.raw_event`;
- possession numbering is gap-free;
- official box-score and score-progression reconciliation passes all games;
- team and player four-factor facts reconcile;
- team ratings materialized and dynamic paths agree;
- the migration 012 actions-based outputs matched the removed model before the
  destructive cutover committed.

Seventy of the 84 games are marked `publication_status='review'`, primarily
because conservative possession QA flags small same-team-transition counts;
all hard publication gates passed. Review status is evidence for inspection,
not an exclusion from season aggregates.

## Migration 045 - Tab 8 query shape (applied 2026-08-29)

**Final status:** the two function source swaps are applied. The composite
index and function-local `work_mem` experiments were deliberately excluded.
Before that final decision, rollback was verified after every
one of the seven live runs by re-reading `pg_get_functiondef` hashes and
`pg_index`: both functions still read `player_game_context`,
`euroleague_pff_game_team_idx` does not exist, and neither function carries a
`work_mem` setting. Two runs were killed mid-flight and the server rolled both
back cleanly.

Historical pre-apply hashes used by the fixed applicator:

- `onoff` `083d6ff31f82cbe62083b82f36d6b4c17ac994e613d064317e7fe0b2ddbd4f82`
- `ff` `3bac5d68cb82f0e0a0f7d8e3367eb26b57f728af2649673e192ea59e8bad6c3a`

Spec: `docs/specs/2026-08-28-tab8-query-remediation-design.md` plus Addenda A,
A.1, A.2 and A.3. Plan: `docs/plans/2026-08-28-tab8-query-remediation.md`.
Also kept as a standalone document beside the plan:
`docs/plans/2026-08-28-tab8-query-remediation-handoff.md`.
Branch `sql/tab8-query-shape`, commit `aea2b13`.

### The defect

Both public functions aggregated the per-game fact through
`euroleague.player_game_context`. That view joins `euroleague.schedule` and the
two-perspective `euroleague.final_schedule` onto every fact row, and the
aggregation reads none of those columns. The `games` CTE inside each function
already resolves every schedule filter, so joining the fact to `games` on
`(game_id, team_id)` is the identical restriction.

The cost, from a faithful replica of the function's inner query:

```
Nested Loop                                    est=1      act=526,808
  Index Scan       player_four_factors_by_game  loops=796      buffers=69,476
  Index Only Scan  schedule                     loops=526,808  buffers=1,580,424
                                                -> 3.0 buffer accesses per loop,
                                                   96% of the 1,652,318 total
```

**1.65 million buffers is not 1.65 million pages read.** `euroleague.schedule`
is 11 pages and its primary key 4 - about 15 pages, permanently cached. Every
one of those accesses is a shared *hit*; there is no I/O at all. The cost is
CPU: 526,808 pointless B-tree descents to fetch columns nobody reads.

### Why the planner chose a per-row nested loop

Not a planner blunder - the correct choice for the estimate it had. Every join
node in that plan estimates **1 row** while producing 526,808, and the planner
costed the whole query at **163**.

| Join predicate | Estimated | Actual | Error |
|---|---:|---:|---|
| `game_id` only | 585,580 | 526,808 | 11%, fine |
| `team_id` only | 8,900,744 | 10,500,064 | 15%, fine |
| `game_id` AND `team_id` | 14,639 | 263,404 | **18x under** |

Each column is estimated well alone; together they collapse. `game_id` and
`team_id` are near-perfectly correlated - 589 games x 40 teams is 23,560
combinations if independent, but only **1,178 pairs actually exist**, because a
game determines its two teams. PostgreSQL assumes independence and multiplies
the selectivities, under-counting ~20x. There are no extended statistics on the
table to say otherwise.

The full chain:

1. `(game_id, team_id)` are correlated; the independence assumption under-counts
   ~20x per join.
2. Stacked through the real query's multi-level join, the estimate collapses
   from 526,808 to 1.
3. At one row, a nested loop with an index probe is unbeatable - total plan
   cost 163.
4. The view adds a second, redundant `schedule` join on top of that.
5. The "one" probe therefore executes 526,808 times, for 1,580,424 wasted
   buffer accesses.

**Why the fix is robust to this:** it does not try to correct the estimate. The
misestimate survives the migration, but with the view gone there is nothing left
to multiply. The only looped node becomes the fact index scan at 796 loops,
which is exactly right: one per game/team. That is why the buffer results were
identical to the digit across all seven runs while wall clock swung wildly.

With `enable_nestloop = off` the same query drops to 26,005 buffers via a merge
join, but its estimated cost is 20 billion, so it would never be chosen
naturally. Not a usable lever.

### What shipped

`sql/045_tab8_query_shape.sql` is additive, one transaction, and has no
`DROP FUNCTION`. It ships both function bodies with **exactly one line changed
each** - the aggregation
  source swaps from the view to the base fact, alias `c` retained so no other
  token differs. A test enforces byte-identity against candidate A.

No index or function setting changed. The proposed `(game_id, team_id)` index
and 16 MB `work_mem` remain separate experiments because their individual
benefit was not isolated and the Israeli companion does not carry that
composite index.

Signatures, volatility, `SECURITY` mode, defaults, return columns and ordering
are unchanged, so the Shiny call sites need no edit and no deploy.
`player_game_context` is **not** dropped - the migration 002 season aggregates
still read it.

### Historical bundled-candidate measurements

The table below measured the source swap bundled with physical tuning and must
not be attributed to the final function-only migration. Full-matrix
rollback-only run, 25 presets x both functions:

| preset | before | after | buffers | temp blocks |
|---|---:|---:|---|---|
| onoff broad, app dates | 3.787 s | **1.373 s** | 1,653,821 -> 47,623 | 9,240 -> 4,609 |
| ff broad, app dates | 4.338 s | **1.684 s** | 1,653,821 -> 47,623 | 16,794 -> 8,383 |
| onoff last 10, app dates | 0.915 s | **0.275 s** | 398,123 -> 6,133 | 0 -> 0 |
| onoff one team | 0.592 s | **0.135 s** | 141,245 -> 1,930 | 211 -> 0 |
| onoff one opponent | 0.609 s | **0.153 s** | 81,657 -> 3,555 | 225 -> 0 |
| onoff broad, NULL dates | 1.027 s | **0.810 s** | 71,988 -> 46,559 | 0 -> 0 |

**The spec measured the wrong call.** Tab 8 populates its date inputs from
`euro_season_date_bounds()`, so the app always sends an explicit
2025-09-01..2026-07-01 window. That call is ~4x slower than the NULL-date call
the spec benchmarked, for byte-identical output (358 rows either way). It was
added to the matrix as `broad app dates` and is the number that matters. The
same omission hid the single largest win in the exercise, `last 10 app dates`
at 398,123 -> 6,133 buffers.

### Candidates

| candidate | shape | verdict |
|---|---|---|
| A | direct fact source + `(game_id, team_id)` index | good; every preset faster |
| B | A + one filtered aggregation at `(player_id, team_id)` | **rejected** |
| C | B + 16 MB function-local `work_mem` | **rejected** |
| AC | A + 16 MB function-local `work_mem` | fast experiment; **not shipped** |
| final 045 | direct fact source only; existing indexes/settings | **applied** |

**B and C are rejected on measured evidence and must not be revisited without
new evidence.** Collapsing `agg`/`pivoted` into one aggregation needs 25
(ON/OFF) and 44 (Four Factors) accumulators per group instead of 7/12 over 4x
more groups. That state no longer fits `work_mem` and introduced an **on-disk
sort where none existed** - `onoff broad season` temp written 0 -> 9,240 blocks,
`ff broad season` 0 -> 16,794, `last 10` 0 -> 1,135/4,113. Wall clock followed:
`ff broad season` 0.935 -> 3.019 s. Candidate C removed the spill with 16 MB and
the shape was still ~2.5x slower than A, so the wide aggregate costs more CPU
than two narrow passes; the spill was a symptom, not the cause. Buffer counts
were identical to A's throughout, isolating the aggregation shape as the only
variable.

### The measurement problem

This consumed more of the session than the migration did, and the conclusions
matter for any future performance work on this instance.

The spec's 0.500 s gate was anchored to a sample that does not reproduce -
6.775 s warm became 0.993 s on re-measurement, a 7x spread driven by buffer
cache state. Worse, **the Israeli companion never reached 0.500 s either**, and
it already uses the exact shape this migration adopts:
`basketball_test.player_four_factors_by_game p JOIN sched s ON s.game_id =
p.game_id AND s.team_id = p.team_id`, with no context view. EuroLeague was the
outlier; this converges it. Gates were re-anchored to companion parity in
Addendum A.

Three instrument defects were found and fixed, none of which relaxed a
threshold:

1. **Shape-matched gates (A.2).** `basketball_test.onoff_compute` ends with
   `fs.game_date BETWEEN p_start_date AND p_end_date` and has no NULL guard, so
   the companion *cannot* make a NULL-date call - it returns zero rows. Gating
   EuroLeague's NULL-date presets against a dated companion call compared two
   different plans and produced a bogus p90 failure on a preset whose median was
   0.810 s. NULL-date presets are now report-only, and a `last 10 app dates`
   preset was added so the last-N gate is shape-matched too.
2. **Companion timed inside the transaction (A.2).** It was measured before the
   DDL, minutes earlier. Not close enough: the companion's own unchanged
   `four_factors_compute` measured 1.642 s and 1.199 s twenty minutes apart.
   `basketball_test` is untouched by the candidate DDL, so reading it inside the
   transaction cannot be contaminated by it.
3. **Trimmed estimator (A.3).** The instance injects multi-second stalls into
   whatever is running; one landed inside a baseline `EXPLAIN`, 13,207 ms on a
   query whose median is 1.2 s. A representative candidate sample:

   ```
   1.34 1.35 1.37 1.37 1.37 1.37 1.38 1.39 1.39 1.40 1.41 | 1.63 3.05 5.42 7.16
   twelve values inside 0.07 s                            | the instance

   raw     median 1.390   p90 5.420
   trimmed median 1.390   p90 1.630
   ```

   Every latency statistic is now computed over the central 60% of samples,
   applied identically to candidate, baseline and companion so no side gains an
   advantage. The median does not move; only the stall-dominated p90 does. Runs
   print the full sorted sample list with trimmed values marked, so a genuinely
   fat tail can never be mistaken for a discarded outlier.

The baseline half of a run is slow because it measures the *unfixed* functions -
a single `home` call costs ~6.8 s today. Baseline sample count was cut to 3 per
preset (the candidate keeps each preset's full count), taking a full run from
~50 minutes to ~18.

### What is settled

Stable across every completed run:

- exact full-row parity, 25 presets x 2 functions, including empty results,
  zero denominators, all four starter bounds, opponent-rank top/bottom, round
  ranges and the EuroCup competition;
- shared-buffer counts, identical to the digit;
- temp blocks, with narrow-preset spills eliminated (211/225/382/408 -> 0);
- no preset regressed under the `max(10%, 100 ms)` rule;
- privileges, `SECURITY` mode, signatures, and clean rollback;
- candidate trimmed median for `onoff broad app dates`: 1.357 / 1.373 / 1.390 s
  across three independent runs.

### What is open

Whether EuroLeague Four Factors is at parity with the Israeli companion. It
measured 1.651 s against the companion's 1.642 s in one run (pass) and 1.670 s
against 1.199 s in another (fail). The companion is the unstable side, and the
recorded comparisons are candidate-trimmed against companion-untrimmed because
its raw samples were not retained. One run with both sides trimmed settles it.
This is the only unresolved gate.

### Incidental findings, outside the scope of 045

- **Israeli `onoff_compute` silently returns zero rows on NULL input.** Its
  final filter is `WHERE fs.total_net_rtg >= p_min_net` and its date filter
  `fs.game_date BETWEEN p_start_date AND p_end_date`; neither is NULL-guarded,
  so a NULL yields an empty result rather than an error. The EuroLeague version
  guards both. Tab 1 always passes real values, so it is not currently biting.
- **No extended statistics on either league's fact table.**
  `CREATE STATISTICS (dependencies) ON game_id, team_id` would teach the planner
  the correlation described above and likely help other queries joining that
  pair. Deliberately not bundled into 045 - it would confuse attribution.
- **The Israeli side probably pays a smaller version of the same tax.** Same
  table shape, same missing statistics. Tab 1's broad call at 1.745 s is the
  candidate to look at.
- **JIT is already off server-wide**, so the spec's caution about not disabling
  it again is moot.
- **`app_readonly` already holds SELECT on the base fact.** The plan proposed a
  test asserting it could not; that assertion would have been false.

### Follow-up

1. Do not bundle the composite index or `work_mem` into migration 045.
2. Run `scripts/apply_db_security.R` with `CONFIRM_DB_SECURITY_APPLY=1`, then
   re-run the audit.
3. No app deployment is needed; the call interface is unchanged.

Operational notes are in `RUNBOOK.md` under "Migration 045 (Tab 8 query shape)":
direct port 5432 only, publication pre-flight, SHARE lock duration, and
kill-safety.

## Migration 046 - combined filtered player dashboard reader (applied 2026-08-30)

Session handoff, with the full measurement set and the design analysis:
`docs/plans/2026-08-30-combined-ff-reader-handoff.md`.

**Status: applied in both schemas.** `euroleague.four_factors_dashboard_compute`
and `basketball_test.four_factors_dashboard_compute` both exist and are on the
`app_readonly` EXECUTE allowlist. `four_factors_compute` and `onoff_compute` are
unchanged and still deployed in both schemas.

### The defect

On the filtered (non-MV) path, the Four Factors view issued **two** full fact
aggregations per filter change:

1. `four_factors_compute` for the 43 factor columns, and
2. a second `onoff_compute` with `p_team_ids => NULL`, `p_min_all => 0`,
   `p_min_on => 0` - a whole unrestricted on/off computation - solely to recover
   four columns: `Net RTG Diff`, `Off ON Diff`, `Def ON Diff`, `minutes`.

Both calls read the same per-game fact under the same filter set, then R
`left_join`ed them on `(player_id, team_id)`. The second call recomputed every
possession, rating and percentile the first call had already scanned the rows
for, and 43 of its 47 output columns were discarded.

### The fix

One additive function per schema returns `four_factors_compute`'s 43 columns
plus the four rating/minutes fields in one application/database call. The live
EuroLeague implementation uses one fact aggregation. The live Israeli
implementation is still the migration-046 wrapper: it preserves the 43 factor
columns by calling `four_factors_compute`, then performs one narrow second fact
aggregation for ratings/minutes. Ratios remain derived only after additive
sums.

- `euroleague/sql/046_player_dashboard_reader.sql` - 19 in-params, competition
  first, matching `euroleague.four_factors_compute`.
- `sql/functions/four_factors_dashboard_compute.sql` - 20 in-params, matching
  `basketball_test.four_factors_compute`.

The call interface for the *existing* functions is untouched, so nothing else
that reads them is affected.

### Gate evidence (Israeli, 2026-08-30)

`euroleague/scripts/gate_israeli_player_dashboard_reader.py` creates the
function inside a transaction, compares the combined result against the live
two-call result row-by-row keyed on `(player_id, team_id)` across all 47
columns, then rolls back unless `--apply` is passed.

Twelve presets, eleven of which return rows:

```
broad 362, last 10 328, game type 362, game type multi 362, opponents 332,
opponent rank 355, gn range 294, home 359, win 359, own starters 362,
opponent starters 359, empty 0
```

Exact parity on all twelve. Broad-call median latency **2.714 s two-call ->
1.987 s combined** (a second run: 3.488 s -> 2.374 s; wall-clock on this
instance swings widely, so treat the direction as the result, not the ratio -
the structural claim is one fewer full aggregation).

Two presets in the first version of this gate were vacuous and have been fixed;
both would have passed without exercising anything:

- `p_game_type_csv => '1'` - not a `game_type` this schedule uses. The 2026
  values are 5 (regular season, 194 games), 16, 17, 26, 34, 35.
- `p_opp_ids_csv => '1109,1110'` - `schedule.team1` holds external team codes;
  the id domain the functions filter on is the small-int `team_id` (2-15) used
  by `sched_long` and `onoff_default_mv`.

The gate now refuses to pass any preset other than `empty` that returns 0 rows.

### Israeli single-scan replacement (applied 2026-08-31)

The deployed `sql/functions/four_factors_dashboard_compute.sql` now matches the
EuroLeague execution shape: one eligible-games stage, one additive scan of
`player_four_factors_by_game`, then factors and ratings. Its public signature
and 47-column return contract are unchanged.

Pre-apply rollback evidence for the exact SQL:

- exact parity with the legacy two-call composition on 12/12 presets;
- exact parity with the live wrapper across all 47 columns on 12/12 presets;
- broad wrapper 2.302 s / 82,124 buffers versus single scan 1.759 s / 41,364;
- last-10 wrapper 0.664 s / 27,450 buffers versus single scan 0.491 s / 13,877;
- a separate broad gate measured 2.570 s two-call versus 1.268 s single scan.

The approved apply reran all 12 presets exactly, measured broad latency at
2.734 s two-call versus 1.439 s single scan, and committed. The post-commit
read-only audit passed every EuroLeague and Israeli preset. The confirmed
`apply_db_security.R` reconciliation committed and the independent
`audit_db_security.R` pass succeeded.

`euroleague/scripts/audit_player_dashboard_contracts.py` is the permanent
read-only drift guard. It compares each league's dashboard reader with its
established `four_factors_compute` + `onoff_compute` composition across the full
filter matrix and all 47 columns. Static tests additionally require both SQL
definitions to expose the same result contract and the same
one-scan `games -> agg -> rates -> p` structure.

Run from `euroleague/`:

```powershell
& .\.venv\Scripts\python.exe scripts\audit_player_dashboard_contracts.py --league both
& .\.venv\Scripts\python.exe scripts\gate_israeli_player_dashboard_reader.py
& .\.venv\Scripts\python.exe scripts\benchmark_israeli_dashboard_single_scan.py
```

The audit is always read-only. The gate remains non-persistent without
`--apply`, and the benchmark has no apply path at all.

### App changes

`ff_ranked_df` in both on/off tabs drops the second `onoff_compute` block and
reads the combined function directly on the fallback path. The MV path is
untouched.

- `app/R/server_tab8_euro.R` - `euroleague.four_factors_dashboard_compute`.
- `app/R/server_tab1.R` - `basketball_test.four_factors_dashboard_compute`.

### Security

The function name is on the Israeli and EuroLeague allowlists in
`sql/security/audit_app_access.sql`, `sql/security/enable_readonly_rls.sql` and
`app/tests/testthat/test-db-security-contracts.R`. Because this migration only
`CREATE OR REPLACE`s a new name and never `DROP`s an existing function, no
EXECUTE grant was wiped. Verified after apply: `proacl` is
`{postgres=X/postgres,service_role=X/postgres,app_readonly=X/postgres}` on the
Israeli function, `PUBLIC`/`anon`/`authenticated` have no EXECUTE, and
`audit_app_access.sql` returns zero violation rows.

### Ordering hazard seen in this change

The R edits for both leagues were made in one pass, but only the EuroLeague
function had been applied. Israeli Tab 1 Four Factors was therefore broken for
every filtered call - `UndefinedFunction` - while Summary, the MV path and all
of Tab 8 kept working, so a casual local check did not reveal it. A running
Shiny process also holds the pre-edit closure, since `app.R` sources `R/*.R`
once at startup and there is no `shiny.autoreload` in this project; a browser
reload does not re-source R. **When a change spans both leagues, apply both
functions before editing either tab, and restart R before believing a local
test.**


## Migration 047 - drop orphaned objects (applied 2026-08-31)

**Status: applied.** The three functions and two views are absent from the live
database. All guarded readers, security checks and both player-dashboard
behavioral matrices passed afterward.

047 drops three functions and two views that nothing reaches, found by the
first full audit of the schema's SQL surface. That audit is the single
place for this work - findings, migration detail and the apply
instructions all live in `docs/sql_function_history_and_risk_2026-08-30.md`.

Two things worth knowing without opening it:

- Before 047, `euroleague` carried **39 functions to the Israeli schema's 27**
  while covering fewer surfaces, and had **3 orphans to its 0**. It now has 36
  functions and zero known orphans.
- **`euroleague.player_game_context` must never be dropped.** It looks
  orphaned since migration 045 removed the function reads, but
  `scripts/load_games.py` reads it for the published-game QA check.

## Known gaps and risks

1. Clutch filtering is now available on the lineup-unit surface (migration
   019, applied 2026-08-12) -- see the delivered section and validation
   evidence above. Browser-testing both tabs with the controls live is still
   outstanding. `unit_key` is season-scoped, because `players` is not yet a
   cross-season person dictionary (gap 2); a unit's identity is stable within
   a season only.
2. `players` is keyed by competition/provider ID and is not yet a durable
   cross-season person identity. Build that identity layer before a second
   season is trusted.
3. EuroCup has not been collected or validated. Test one game before any batch.
4. Collection/publication is manual; there is no scheduler or last-success UI
   indicator.
5. QA review counts are not yet surfaced clearly in the UI.
6. There is no EuroLeague cold-storage policy; all published data remains hot.
7. Migration 012 changed the publication path, so the old 5-6 second/game
   benchmark is historical. Take a new multi-game timing sample before capacity
   planning.
8. EuroLeague four-factor impact-point annotations remain suppressed because
   their weights were fitted on Israeli data.
9. The database security audit runs automatically only after the *Israeli* ETL
   workflow. EuroLeague publication and migrations are manual (gap 4), so
   nothing runs the audit after a EuroLeague DDL change — it has to be run by
   hand until EuroLeague publication is scheduled.

## Recommended next sequence

0. **Commit the database security files** on an `infra/` branch. No design pass
   needed, and it is the cheapest item here: the live database is hardened but
   the four files that reproduce that state are uncommitted, so a future
   `apply_db_security.R` run from a clean checkout would silently revert
   `euroleague` to unhardened. Note this script also re-applies to
   `basketball`/`basketball_test`; running it is a cross-schema action, not a
   euroleague-only one, so treat it with the same care as any Israeli-schema
   change.

1. **Browser-test both clutch-enabled tabs.** Migration 019 is applied and
   database-side verified (functional correctness, non-clutch parity,
   `app_readonly` permissions); a local Shiny smoke test of the Team Ratings
   and Lineup Data clutch controls against live data has not been run yet.

The rest stay one-line backlog entries, roughly in dependency order. Each needs
its own design pass before it becomes work:

2. Durable cross-season player identity, built *with* the second season rather
   than after it (gap 2).
2b. Continue tab unification, starting with the tab 8 / tab 1 pair and the
   league descriptor. The direction, the measured overlap, and the ordered
   remaining targets are in the delivered section above and in the root
   `CLAUDE.md`. App-side only, no database work.
3. With explicit approval, continue the 2025-26 load through
   `scripts/load_games.py` and record a fresh publication benchmark (gap 7).
4. Surface `game_qa` review counts and a last-success indicator in the app
   (gap 5).
5. Scheduled collection/publication and operational monitoring (gap 4).
6. A EuroLeague cold-storage/retention policy, sized against the shared
   instance budget (gap 6).
7. Validate one EuroCup game before treating competition `U` as supported
   (gap 3).
8. Refit the four-factor impact weights on EuroLeague data, or keep the
   annotations suppressed (gap 8).

## Operations and tests

Use [RUNBOOK.md](RUNBOOK.md) for collection, dry-run, publication, verification,
rollback probing, prerequisites, and recovery instructions.

Run Python tests first:

```powershell
& .venv/Scripts/python.exe -m unittest discover -s tests -v
```

After grouping or possession changes, run the independent R tests from the
repository root:

```powershell
Set-Location etl/tests
& 'C:\Program Files\R\R-4.4.2\bin\Rscript.exe' test_euroleague_event_grouping_fixtures.R
& 'C:\Program Files\R\R-4.4.2\bin\Rscript.exe' test_euroleague_group_events.R
& 'C:\Program Files\R\R-4.4.2\bin\Rscript.exe' test_euroleague_count_possessions.R
```

Migration order is:

```text
001 -> 002 -> 004 -> 005 -> 006 -> 007 -> 008 -> 009 -> 010 -> 011 -> 012
  -> 013 -> 014 -> 015 -> 016 -> 017 -> 018 -> 019 -> 020 -> 021 -> 022
  -> 023 -> 024 -> 025 -> 026 -> 027 -> 028 -> 029 -> 030 -> 031 -> 032
  -> 033 -> 034 -> 035 -> 036 -> 037 -> 038
```

Migration 003 is superseded by 004 and must not be applied.
Migrations 028-038 are applied to the live schema. Migration 030 performed a
one-time refresh of the existing action fact; subsequent publications refresh
only changed games.
Migrations 020 through 024 are applied to the recorded live schema as of
2026-08-13. Migrations 023-024 give Player Stats the same explicit cached/custom
source-selection design as the team reader. Measured full-season latency was
0.77 seconds for the standard preset, 28.54 seconds for margin <= 3/final 4:00,
and 9.55 seconds for trailing/margin <= 7/final 2:00; standard-cache parity was
exact.
Migration 025 is applied with the shared Israeli-style custom-clutch duration
convention across player, team, and lineup readers. Standard clutch remains on
the exact precomputed cache; custom event counts and possessions remain exact.
Full-season Player Stats timings improved from 28.54 to 23.21 seconds for
margin <= 3/final 4:00 and from 9.55 to 8.47 seconds for trailing/margin <= 7/
final 2:00. Remaining custom-query cost is primarily event aggregation.
Migration 026 is applied and directly mirrors the Israeli Player Stats CTE
shape from one filtered action set (`lineup_map`, possession endpoints, player
usage, team possessions, segment time, player minutes, stats, and team usage).
Migration 027 is also applied. It stores only the narrow action/team fields
needed by that calculation, keeps the same action grain, and is refreshed per
changed game. Explicitly materializing the shared downstream CTE grains avoids
PostgreSQL recomputing them per player. The migration-026 function was then
reapplied with the Israeli `stats FROM acts` shape: `type_lineup` already
selects the actor's correct team perspective, and the final roster/name join
removes zero-valued opposite-perspective rows. Removing the redundant
action-to-season-roster join eliminated roughly 66 million comparisons in the
broad preset. Regulation and overtime are separate mutually exclusive branches
so the existing time/margin index is usable without changing overtime
semantics. The app calls the standard and custom functions directly because
routing a custom request through the generic PL/pgSQL selector hid the actual
filter values from the inner planner. Exact full-row parity was verified for
both measured custom presets. Direct live timings are about 0.8 seconds for
standard clutch, 1.7 seconds for margin <= 3/final 4:00, and 1.0 second for
trailing/margin <= 7/final 2:00, comparable to the Israeli reference.

After applying any migration, re-run the security pass from the repository root
(see the security section above for why this is not optional):

```powershell
& 'C:\Program Files\R\R-4.4.2\bin\Rscript.exe' scripts/audit_db_security.R
$env:CONFIRM_DB_SECURITY_APPLY = '1'
& 'C:\Program Files\R\R-4.4.2\bin\Rscript.exe' scripts/apply_db_security.R
```

`apply_db_security.R` is dry-run by default: without
`CONFIRM_DB_SECURITY_APPLY=1` it applies the hardening, runs the audit inside
the same transaction, and rolls back. Use that to preview a change. The
contract tests are `../app/tests/testthat/test-db-security-contracts.R`.

## Key files

- `RUNBOOK.md`: current load and recovery procedure.
- `src/euroleague_possessions/staging.py`: canonical per-game staging.
- `src/euroleague_possessions/transaction_writer.py`: transaction contract and
  seven-table snapshot order.
- `src/euroleague_possessions/postgres_backend.py`: database mapping,
  compatibility guard, validation, and fact refreshes.
- `src/euroleague_possessions/parser.py` and `counter.py`: deterministic event
  grouping and possession logic.
- `sql/010_canonical_actions.sql`: canonical typed `actions` table.
- `sql/011_actions_consumer_candidates.sql`: actions-derived event/team and
  matchup-segment facts.
- `sql/012_actions_consumer_cutover.sql`: consumer switch, parity gates, and old
  middle-table removal.
- `sql/013_lineup_units.sql`: lineup-grain per-game fact and season unit mapping.
- `sql/014_lineup_units_read_layer.sql`: season unit roll-up and
  `fetch_lineups_dynamic()`.
- `sql/018_team_minutes_read_layer.sql`: filtered team minutes from canonical
  matchup segments for Team Ratings.
- `sql/019_clutch_read_layer.sql`: shared pre-event clutch predicate,
  set-based score-state/segment duration intersection, and clutch-aware Team
  Ratings, Four Factors, minutes, and lineup-unit readers. Applied
  2026-08-12 via `scripts/apply_019_clutch_read_layer.py`, which validates,
  applies, and functionally verifies the migration (euroleague-only).
- `sql/020_default_clutch_fast_path.sql`: incremental per-game additive cache
  and explicit no-clutch/default/custom source selector. Pending live apply.
- `scripts/apply_020_default_clutch_fast_path.py`: guarded EuroLeague-only
  migration apply, exact cache-parity check, and default-preset benchmark.
- `docs/team_ratings_minutes.md`: minutes/pace and `app_readonly` security
  handoff.
- `../sql/security/enable_readonly_rls.sql`: schema-aware grants, EXECUTE
  allowlists, and RLS policies for both leagues. The EuroLeague relation and
  function allowlists live here.
- `../sql/security/audit_app_access.sql`: the same contract as assertions;
  expects zero rows.
- `../app/R/server_tab11_euro_gamelogs.R`: EuroLeague Game Logs server.
- `scripts/verify_lineup_units.py`: the nine lineup-unit validation gates.
- `scripts/verify_actions_schema.py`: actions/raw/fact verification.
- `scripts/probe_batched_publish.py`: real publication path followed by
  rollback and before/after proof.
- `../docs/database_context.md`: current Israeli schema and ETL reference.
- `CLAUDE.md`: historical exploration and validation narrative only.

## Non-negotiable boundary

EuroLeague schema work must remain isolated. Do not create, alter, load,
truncate, refresh, or otherwise modify objects in `basketball` or
`basketball_test`. Cross-league comparison is read-only design reference unless
the user explicitly authorizes a separate integration task.
