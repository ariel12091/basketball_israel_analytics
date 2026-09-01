# Israeli standard-clutch Team duplication measurement

**Date:** 2026-09-01
**Status:** narrow migration 051 applied; local app routing complete; app not deployed

## Question

Should Israeli Tab 3 replace its separate standard-clutch Team metric and
Minutes calls with the same full combined-dashboard pattern retained for
EuroLeague migration 050?

## Candidate and safety

`sql/candidates/051_israeli_standard_clutch_dashboard_probe.sql` constructs
one materialized clutch-filtered action set and derives the complete existing
Ratings contract (including Israeli shot profile), Four Factors/additive
contract, and filtered segment Minutes from it.

The warm harness created the function inside one transaction and always rolled
back. The cold harness used session-local `pg_temp` functions on simultaneously
held distinct PostgreSQL backends. It committed no catalog object and every
probe disappeared when its session closed.

## Parity

All fields matched exactly for four non-vacuous presets, each returning 14
teams:

- standard clutch (margin 5, status all, final 5:00);
- home games;
- last 10 games;
- own starters at least 3 and opponent starters at most 3.

The comparison covered Ratings/ranks/records, Israeli shot-profile attempts,
Four Factors and their additive counts, and filtered Minutes.

## Warm complete-call results

Fifteen alternating samples measured the two actual UI paths separately:

| UI path | Existing median / p90 | Combined median / p90 | Decision |
|---|---:|---:|---|
| Summary + Minutes | 0.488 / 0.522 s | 0.482 / 0.546 s | Reject: median neutral, p90 worse |
| Four Factors + Minutes | 4.561 / 4.793 s | 0.512 / 0.532 s | Retain for redesign: 88.8% median reduction |

The earlier three-reader parity harness measured 4.721 seconds versus 0.482
seconds, but that is not an app latency comparison because Tab 3 renders one
metric view at a time. It is retained only as evidence of total duplicated
work.

## Fresh-backend observations

The full candidate had material backend-first variance. Across two sets of
newly forced PIDs, candidate-first samples were 4.278, 0.490, 2.261, and 0.498
seconds. The corresponding legacy-first UI samples were:

- Summary + Minutes: 0.569 and 0.507 seconds;
- Four Factors + Minutes: 4.798 and 4.556 seconds.

Thus a full combined dashboard is unsafe for Summary: it can turn a roughly
0.5-second first call into 2-4 seconds. Four Factors remains a strong target:
even the slowest observed candidate-first sample was below the two observed
legacy Four Factors calls, while warm latency improved by almost 89%.

## Why the Four Factors gain is so large

The improvement is not primarily the removal of a network round trip. It comes
from eliminating repeated action-grain work.

The current Four Factors view performs two separate database computations:

1. `get_team_four_factors_dynamic()` filters the schedule and action fact,
   applies clutch and starter predicates, resolves parent-foul context for free
   throws, and aggregates the Four Factors numerators and denominators.
2. `fetch_team_game_minutes()` independently repeats the schedule, opponent-
   rank, clutch, and starter filtering over the action fact, then groups the
   surviving rows into lineup segments to calculate duration.

The candidate performs the expensive filtering once in `facts AS MATERIALIZED`.
Four Factors aggregates and segment-duration aggregation then consume that
smaller shared row set. This removes a complete large-relation scan plus its
duplicated schedule/rank/filter work. The roughly 89% warm reduction is
therefore plausible even though the final result contains only 14 team rows.

Summary is different. `get_team_ratings_dynamic()` already uses one
clutch-filtered, per-game pre-aggregation for Ratings, records, and the Israeli
shot profile. Its existing Summary + Minutes path is about 0.5 seconds warm.
The full candidate forces Summary to also pay for Four Factors-only parent-
foul, free-throw, rebound-opportunity, and additive-stat processing. Those
extra calculations offset the saved Minutes scan, leave warm latency neutral,
and materially enlarge backend-first initialization. This explains why the
same full-dashboard shape is beneficial for Four Factors but unsafe for
Summary.

The result should not be generalized as “one combined Team reader is always
faster.” The useful shared boundary is specifically the expensive filtered
action set needed by **Four Factors and filtered Minutes**. Summary has a
different optimal query shape.

## Decision

- Do not port the EuroLeague full-dashboard routing wholesale.
- Keep Israeli Summary and its existing Minutes path unchanged.
- Design the next candidate specifically as **Four Factors + Minutes**, omitting
  Ratings-only ranks, records, and shot-profile work. Measure that narrower
  body on repeated fresh backends before applying it.
- Do not change default-season, non-clutch, custom-clutch, Traditional, or shot-
  profile routing as part of this result.

That decision was used to produce the narrow migration 051 implementation
recorded below. The rejected full-dashboard probe remains measurement evidence
only.

## Suggested implementation shape

The next candidate should:

1. retain the current `get_team_four_factors_dynamic()` result columns and
   formulas exactly;
2. carry `id`, `game_id`, `team_id`, `lineup_hash`, `segment_id`, and canonical
   `event_elapsed_seconds` through its clutch-filtered action CTE;
3. derive Minutes by grouping those already-filtered rows at
   `(team_id, game_id, lineup_hash, segment_id)` and summing each segment once;
4. return one additional `minutes` column for the app's pace denominator;
5. omit wins/losses, output ranks, lay-up/dunk/corner-three fields, and all
   other Summary-only work;
6. expose it only to the standard-clutch Four Factors reactive; Summary,
   custom clutch, non-clutch, Traditional, and Shot Profile remain unchanged;
7. preserve the existing functions as compatibility readers and add the new
   function additively with PUBLIC execution revoked and the app role granted;
8. require exact parity over the measured filter matrix, 15 alternating warm
   calls, repeated fresh-backend candidate-first/legacy-first samples, and the
   repository security audit before app routing changes.

An acceptable candidate must retain the large Four Factors improvement without
the 2-4 second backend-first behavior observed from the oversized full
dashboard body. If the narrower body still has material cold regressions, keep
the current path despite its warm duplication.

## Narrow Four Factors + Minutes result

`sql/candidates/051_israeli_four_factors_minutes.sql` implemented the proposed
shape with one `facts AS MATERIALIZED` action set. It returns the exact existing
Four Factors contract plus `minutes`; it omits Summary ranks, records, shot
profile, and other unrelated work.

Exact parity passed for standard clutch, home, last 10, and starter-context
presets, with 14 teams in every result. Fifteen alternating warm samples
measured the real Four Factors + Minutes UI composition:

| UI path | Existing median / p90 | Narrow median / p90 | Median change |
|---|---:|---:|---:|
| Four Factors + Minutes | 4.452 / 4.570 s | 0.409 / 0.423 s | -90.8% |

Repeated fresh-backend pairs also favored the narrow reader. Candidate-first
calls were 0.500 and 0.406 seconds; the corresponding legacy-first compositions
were 4.470 and 4.457 seconds. Eight simultaneously held sessions used eight
distinct backend PIDs, so the result was not pooled-backend reuse disguised as
cold measurement.

## Migration 051 completion

The exact measured function body was promoted additively to
`basketball_test.get_team_four_factors_dashboard_dynamic` in
`sql/functions/get_team_four_factors_dashboard_dynamic.sql`. The rollback gate
and committed apply each repeated exact parity for all four presets before the
transaction completed. PUBLIC execution is revoked and `app_readonly` has
execution access; repository security reconciliation and the final application-
path security audit passed.

Israeli Tab 3 now routes only the exact standard-clutch Four Factors result and
its Minutes denominator through one shared Shiny reactive. Summary, per-game,
custom clutch, non-clutch, Traditional, and Shot Profile routing remain
unchanged. A static query-count/routing contract enforces this boundary. All
276 EuroLeague Python tests and the focused Shiny routing, database-security,
and parse contracts passed. No app deployment was performed.
