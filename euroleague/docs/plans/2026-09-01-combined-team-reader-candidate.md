# Combined Team reader candidate

Date: 2026-09-01
Status: standard-clutch migration 050 applied; local app routing complete; app not deployed

## Scope

The original migration-050 candidate added one wide Team dashboard reader for
each existing route kind:

- `get_team_dashboard_pergame`;
- `get_team_dashboard_dynamic`;
- `get_team_dashboard_direct`.

Each returns the current Ratings, Four Factors, and Minutes contract in one row
per team. The route capability boundary remains unchanged. Additive metrics and
duration are aggregated separately before the team-level join so lineup rows
cannot multiply event counts.

## Structural result

- Per-game: one materialized filtered-games CTE, one
  `team_four_factors_by_game` scan for Ratings/Four Factors, and one
  `lineup_totals_by_game` scan for Minutes.
- Standard clutch: one materialized `filtered_team_game_facts()` result supplies
  all outputs.
- Custom clutch: one materialized `player_stats_actions_by_game` scan supplies
  additive metrics and a separate segment-duration aggregate.

The existing nine public readers are unchanged. The app still calls them.

After cold testing, the three-reader candidate moved to
`sql/candidates/050_two_call_team_dashboard_readers.sql`. The retained migration
is now `sql/050_standard_clutch_team_dashboard.sql` and contains only
`get_team_dashboard_dynamic`.

## Rollback-only evidence

The gate created all three candidates inside a transaction and always rolled
back. Seven presets covered broad, last-N, starter, standard-clutch,
home-clutch, custom-clutch, and custom-clutch starter filters. All returned 20
teams and matched the existing three-reader composition exactly across all 25
columns.

Legacy-first elapsed observations:

| Route/preset | Existing three calls | Combined |
|---|---:|---:|
| per-game broad | 1.872 s | 0.264 s |
| per-game last 10 | 0.318 s | 0.128 s |
| per-game starter context | 0.381 s | 0.154 s |
| standard clutch | 1.942 s | 0.650 s |
| standard clutch home | 1.073 s | 0.359 s |
| custom clutch | 12.878 s | 1.526 s |
| custom clutch starters | 1.206 s | 0.478 s |

Candidate-first elapsed observations:

| Route/preset | Existing three calls | Combined |
|---|---:|---:|
| per-game broad | 0.463 s | 1.749 s |
| per-game last 10 | 0.307 s | 0.132 s |
| per-game starter context | 0.380 s | 0.156 s |
| standard clutch | 1.934 s | 0.727 s |
| standard clutch home | 1.078 s | 0.371 s |
| custom clutch | 6.104 s | 8.889 s |
| custom clutch starters | 1.176 s | 0.650 s |

These are complete-call observations, not an acceptance benchmark. The reverse
order shows a material first-use cost for the wider SQL function on broad
per-game and custom-clutch calls. Do not average that cost away or claim a cold
win from the legacy-first samples.

## Verification

- 248 Python tests passed.
- Migration safety validation found only the expected additive statements.
- The database gate has no apply mode, uses lock and statement timeouts, and
  rolls back on both success and failure.

## Next gate

Before applying migration 050 or changing Tab 9 routing:

1. measure repeated interleaved warm calls and shared buffers;
2. measure genuinely fresh-backend first calls in both orders;
3. retain only if the combined route improves the target latency distribution,
   not merely warm elapsed time after the legacy readers have primed the data;
4. after explicit approval, apply the migration, update the reachability
   manifest and app routing together, then run security reconciliation/audit.

## 2026-09-01 measured warm benchmark

A separate rollback-only benchmark then ran 15 complete, fetched samples per
route, alternating which side executed first. Both sides received two untimed
warm-up passes. One `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` comparison was
captured after the timed samples.

| Route | Three-call median / p90 | Combined median / p90 | Median change | Shared hits, old -> new |
|---|---:|---:|---:|---:|
| per-game broad | 0.443 / 0.467 s | 0.308 / 0.337 s | -30.5% | 32,059 -> 24,781 (-22.7%) |
| standard clutch | 2.047 / 2.164 s | 0.714 / 0.718 s | -65.1% | 4,560 -> 1,520 (-66.7%) |
| custom clutch | 1.245 / 1.502 s | 1.127 / 1.230 s | -9.5% | 607,187 -> 206,086 (-66.1%) |

The custom-clutch interpretation is mixed. Complete-call median and p90
improved and one 8.803-second legacy outlier did not appear on the combined
side. However, the buffer probe reported 869.818 ms summed execution for the
three existing calls versus 1,016.820 ms for the combined function despite its
much lower buffer count. Materializing and scanning the wider action tuple
twice saves I/O work but adds CPU/materialization overhead. That route is not a
clear retention win yet.

### Cold-measurement limitation

Sequential new client sessions all reused backend PID 184048. Only the first
sample could represent backend first use; it measured combined per-game at
1.801 seconds and combined custom clutch at 9.427 seconds when each ran first.

A follow-up held two session-mode connections open and obtained distinct PIDs
184696 and 184697. On those backends, creating the rollback-only candidate
functions itself took 2.5-3.8 seconds, after which per-game and standard-clutch
combined calls were already fast. Candidate creation therefore contaminates a
rollback-only cold test by moving some backend initialization into DDL. A
transient five-second catalog lock then stopped the direct-route batch; all
sessions closed and rolled back.

At that point, a valid production-like cold comparison required an explicitly
approved, temporarily committed set of disposable candidate functions so
untouched backends could call already-existing functions without running their
DDL. The following section records that completed experiment.

## 2026-09-01 committed disposable cold probes

Explicit approval was given to commit three uniquely named `_cold_probe`
functions temporarily. The deployment backend remained occupied while two
other session-mode backends called the already-existing probes in opposite
orders. Production names and app routing were unchanged. The probes were then
dropped and their absence verified after commit.

Baseline old-style SQL bodies:

| Route | Candidate-first combined | Legacy-first three calls |
|---|---:|---:|
| per-game broad | 2.988 s | 0.510 s |
| standard clutch | 0.729 s | 1.941 s |
| custom clutch | 15.136 s | 2.544 s |

The candidate-first and legacy-first values come from different untouched
backends by design. The companion call on the same backend was also exact, but
is not treated as cold after the first side ran.

A second explicitly approved disposable experiment converted the same three
bodies to definition-time `BEGIN ATOMIC` syntax:

| Route | Atomic candidate-first combined | Legacy-first three calls |
|---|---:|---:|
| per-game broad | 1.931 s | 0.683 s |
| standard clutch | 0.671 s | 1.867 s |
| custom clutch | 9.279 s | 4.646 s |

Definition-time parsing improved candidate-first per-game by 35% and custom
clutch by 39%, but did not remove their material regressions. Standard clutch
remained a clear cold and warm win. All six disposable definitions were removed
and verified absent after their respective runs.

### Resulting classification

- **Retain:** combined standard-clutch reader. It has exact results, a 65% warm
  median improvement, 67% fewer shared hits, and a cold candidate-first win.
- **Redesign:** combined per-game reader. Its 30% warm win does not justify a
  roughly 1.2-second atomic cold regression against the measured legacy-first
  call.
- **Redesign or reject:** combined custom-clutch reader. Its warm median gain is
  only 9.5%, its buffer probe has worse server execution despite fewer hits,
  and its atomic cold call remains roughly twice the measured legacy-first
  composition.

Do not apply migration 050 in its current three-reader form. The next candidate
should narrow the custom path to one combined Ratings/Four-Factors aggregation
while leaving Minutes on its established segment reader, and should consider
the same two-call shape for per-game. This trades some buffer savings for a
smaller function body and avoids materializing a wide action set solely to feed
two different grains.

## 2026-09-01 two-call follow-up

The proposed redesign was implemented and measured. Per-game and custom clutch
combined Ratings/Four Factors only; each retained the existing Minutes reader
as a second call. Standard clutch remained the accepted one-call shape. All
seven parity presets matched the established three-call composition exactly
across all 25 final columns.

Warm results:

| Route | Three-call median / p90 | Narrowed median / p90 | Median change | Shared hits, old -> new |
|---|---:|---:|---:|---:|
| per-game broad | 0.445 / 0.553 s | 0.314 / 0.391 s | -29.3% | 32,059 -> 25,896 (-19.2%) |
| custom clutch | 1.154 / 1.211 s | 0.786 / 0.886 s | -31.9% | 607,187 -> 401,093 (-34.0%) |

The custom series included one combined 2.945-second outlier after fourteen
samples between 0.765 and 0.886 seconds. A preceding attempt lost its database
connection after eight exact, similarly favorable samples; its transaction
rolled back and was not used as the retained measurement.

Committed disposable cold probes rejected both narrowed routes:

| Route | Old-style candidate-first | Atomic candidate-first | Corresponding legacy-first |
|---|---:|---:|---:|
| per-game broad | 3.695 s | 3.110 s | 0.479 / 0.648 s |
| custom clutch | 15.725 s | 21.119 s | 1.918 / 1.349 s |

The second number in the legacy column is the separate atomic experiment, not
a repeated sample from one backend. Both experiments used distinct candidate-
first and legacy-first PIDs, committed only uniquely suffixed probes, and
dropped and verified those probes afterward.

### Final local decision

- Retain only `get_team_dashboard_dynamic` in migration 050.
- Keep per-game and custom clutch on their current three readers. Their warm
  duplication is cheaper than the measured user-visible backend-first penalty.
- Preserve the rejected two-call SQL under `sql/candidates/` as measured
  evidence, not deployable migration input.
- Database apply, app routing, reachability registration, and security
  reconciliation were completed on 2026-09-01. Deployment remains undone.

### Completion record

Migration 050 was applied with `scripts/apply_050_standard_clutch_team_dashboard.py`.
Its rollback gate and committed apply each matched both the broad and home
standard-clutch presets exactly, with 20 non-vacuous team rows per preset. The
app now reuses one `et_dynamic_dashboard` reactive for Summary, Four Factors,
and Minutes, and one `et_prev_dynamic_dashboard` reactive for both trend
consumers. Per-game and custom-clutch routing is unchanged.

The central `app_readonly` allowlist and independent security audit were
updated for the new reader. Confirmed security reconciliation, the post-apply
security audit, and the function-reachability audit all passed; reachability
reported no missing, overloaded, or uncovered functions. The focused Shiny
contracts and all 264 EuroLeague Python tests passed. No app deployment was
performed.

### Actual UI-path confirmation

A corrected follow-up timed the mutually exclusive rendered views rather than
summing all three legacy readers. Fifteen alternating warm samples produced:

| UI path | Existing median / p90 | Migration 050 median / p90 |
|---|---:|---:|
| Summary + Minutes | 1.247 / 1.308 s | 0.634 / 0.661 s |
| Four Factors + Minutes | 1.278 / 1.331 s | 0.642 / 0.665 s |

Fresh distinct-backend samples also favored migration 050: Summary was 0.626
seconds candidate-first versus 1.243 seconds legacy-first, and Four Factors was
0.633 versus 1.296 seconds. This confirms that the retained full standard-
clutch reader follows the shared-scan rules for both actual EuroLeague views.
