# Israeli / EuroLeague Team dashboard parity audit

**Date:** 2026-08-29  
**Status:** audit complete; no application or database change applied  
**Scope:** Israeli Tab 3 and EuroLeague Tab 9 Team Ratings / Four Factors

## Decision

The EuroLeague implementation has the better execution architecture: explicit
default, non-clutch per-game, standard-clutch cache, and custom-clutch action
paths. The Israeli implementation has the richer public data contract: it
returns the additive four-factor counts needed to verify every displayed rate
and exposes a provider-supported shot-profile extension.

The target should combine those strengths. Do not make the physical schemas or
provider-only columns identical. Make the common request semantics, common
metric formulas, common output types, routing rules, and tests identical; keep
league adapters and league-specific extensions below or alongside that common
contract.

## Measured evidence

The reproducible read-only audit is
`scripts/audit_team_dashboard_parity.py`. It was run against the direct
PostgreSQL endpoint on 2026-08-29. The selected seasons were Israeli 2026 (14
teams) and EuroLeague E/2025 (20 teams).

| Reader | Broad warm median | Last 10 warm median | Broad shared hits | Broad temp blocks |
|---|---:|---:|---:|---:|
| Israel Ratings dynamic | 0.981 s | 0.359 s | 619,263 | 3,405 read / 3,415 written |
| Israel Four Factors dynamic | 0.156 s | 0.121 s | 18,025 | 295 read / 296 written |
| Euro Ratings per-game | 0.112 s | 0.091 s | 5,368 | 0 |
| Euro Four Factors per-game | 0.125 s | 0.093 s | 5,368 | 436 read / 437 written |

The Israeli `team_metrics_by_game_mv` is a 892-row, 631 KB physical table. A
read-only aggregate over it returned in 0.08-0.13 seconds and reproduced:

- all eight audited common Ratings values exactly for broad and last-10;
- all 33 current Four Factors output values exactly for broad and last-10;
- every team row in both cases.

That proves a safe non-clutch fast path already exists for filters that do not
need shot profile or action-time state. It does not prove starter-filter parity,
because `team_metrics_by_game_mv` deliberately collapses starter context.
`lineup_four_factors_by_game`, however, retains both `num_starters` and
`opp_starters`, so it can answer non-clutch starter filters without the raw
action scan.

Cold timings are workload/cache-state sensitive (Israel Ratings 7.30 s,
Euro Ratings 0.47 s in this run). The buffer and relation-size differences are
the stronger architectural evidence.

## Difference register

### Request and UI contract

| Area | Israel | EuroLeague | Assessment / preferred rule |
|---|---|---|---|
| League scope | Schema implies league | `competition` parameter | Deliberate provider scope; translate in an adapter |
| Competition stage | integer `game_type` CSV | text `phase` CSV | Deliberate provider difference |
| Team selection | no function team-list parameter | `team_ids_csv` | Common UI capability should be exposed by both adapters |
| Opponent, home/away, result | Supported | Supported | Common and materially aligned |
| Opponent rank | Season-wide ratings; top/bottom and off/def/net | Same semantics | Keep; centralize tests |
| Round / last N | `gn`; last N ranked before other narrowing | `round_number`; same last-N order | Common semantic contract |
| Clutch | margin, status, time, OT override | Same public semantics | Common contract; physical calculation remains provider-specific |
| Starter context | legacy exact values plus min/max bounds | min/max bounds only | Drop the two legacy exact arguments after caller migration; bounds are sufficient |
| Public arity | 23 parameters | 19 per-game, 23 clutch readers | Keep explicit typed DB parameters, but hide arity behind one named app request object and one router per league |

The shared UI labels the filters **Own lineup starters** and **Opponent lineup
starters**. Therefore the canonical meanings are `own_starters` and
`opp_starters`; the historical SQL names `num_starters_off/def` are misleading
and should be treated as compatibility names only.

### Application routing

| Request | Israel | EuroLeague | Better choice |
|---|---|---|---|
| Untouched full season | season MVs | season MVs | Equal and correct |
| Filtered, non-clutch | Ratings always raw action scan; Four Factors uses per-game only when no starter filter | both use starter-aware per-game fact | EuroLeague routing |
| Standard 5-point / 5-minute clutch | raw Ratings; raw Four Factors; selected storyline presets have a separate cache | incremental standard-clutch facts | EuroLeague routing, generalized |
| Custom clutch | raw action scan | direct action-fact scan | Necessary provider-specific slow path |
| Shot Profile | bundled into Israeli Ratings even when Summary only needs ratings | not supported | Split Israeli shot-profile extension from common Summary metrics |

There is one routing correctness defect to fix first:
`et_fallback_needed()` checks `max_margin` and `max_time_remaining` but omits
`margin_status` and `ot_margin_filter`. A status-only request can therefore
remain on the unfiltered season MV. The Israeli predicate checks
`margin_status`, but also omits `ot_margin_filter`. Both predicates should call
one shared helper that classifies the full four-field clutch request.

### Physical facts and performance

| Area | Israel | EuroLeague | Assessment |
|---|---|---|---|
| Schedule perspectives | 892 rows | 1,186 rows | Both appropriately tiny and indexed |
| Raw analytical action source | 528,033 rows / 262 MB | 665,814 rows / 584 MB | Never use for non-clutch metrics already stored additively |
| Starter-aware FF fact | 74,410 rows / 27.1 MB, long offense/defense grain | 25,334 rows / 8.1 MB, wide two-sided grain | Both valid; Euro wide grain is simpler for Team readers |
| Game-team combined fact | 892 rows / 631 KB with ratings, FF counts/rates and traditional stats | FF fact remains starter-aware; no separate collapsed equivalent needed | Israel fact is the best no-starter fast path; Euro fact is the best general non-clutch path |
| Standard clutch cache | preset-specific Ratings cache, plus other Israeli caches | general incremental standard-clutch fact | Euro approach is easier to reuse |

The Israeli long fact is not inherently wrong. Its offense/defense rows match
the source model. The Euro wide fact is easier to query because one row already
contains both perspectives and both starter counts. Do not rewrite the Israeli
fact merely for column parity; add a thin provider adapter or a starter-aware
game-team aggregate only if benchmarks show that the existing indexed long
fact is insufficient.

### Output columns

The current common intersection is:

`game_year`, `team_id`, `team_name`, `off_ppp`, `def_ppp`, `net_rtg`,
`off_poss`, and `def_poss`. Ratings additionally align on `games_played`,
`wins`, `losses`, and three ranks. Four Factors additionally align on offensive
and defensive `efg`, `ts`, `oreb`, `tov`, and `ftr`.

Meaningful output differences:

| Difference | Israel | EuroLeague | Preferred contract |
|---|---|---|---|
| ID/count types | `team_id` and many counts are `int4` | IDs/counts are generally `bigint` | Common output uses `bigint`; adapters cast upward |
| Column order | FF starts `team_id, game_year`; Ratings starts `game_year, team_id` | Both start `game_year, team_id` | One common order |
| Additive FF counts | exposes points, possessions, TS poss, rebounds/opportunities, turnovers, FTA/FGA/FGM/3PM | public reader exposes rates and possessions only, although its fact has the counts | Expose the same additive common counts; derive rates after aggregation |
| Shot profile | 12 attempt columns in Ratings | absent | Israeli extension, not a common-column requirement |
| Team filter | not present | present | Add at adapter/router boundary, not by changing provider facts |

The additive counts are the stronger interface. They make re-aggregation,
reconciliation, formula tests, and future consumers safe. The payload cost is
negligible at 14-20 team rows.

### Metric logic

Most formulas are materially aligned: PPP per 100 possessions, eFG%, TS%,
OREB%, TOV%, and FTR are computed from summed additive numerators and
denominators. Both providers rank opponents over the full season and apply
last-N before date/context narrowing.

One verified inconsistency is unhealthy: Net Rating rounding.

- Israeli Ratings subtracts unrounded rates and rounds once; Israeli Four
  Factors subtracts already-rounded one-decimal rates.
- EuroLeague Ratings subtracts already-rounded rates; EuroLeague Four Factors
  subtracts unrounded rates and rounds once.

This produced a 0.1 disagreement between Summary and Four Factors for 4 of 14
Israeli teams and 8 of 20 EuroLeague teams in the broad audit. Last-10 differed
for 3 Israeli and 5 EuroLeague teams. The canonical formula should be:

```sql
round(
  100.0 * sum(off_pts) / nullif(sum(off_poss), 0)
  - 100.0 * sum(def_pts) / nullif(sum(def_poss), 0),
  1
)
```

Round only the displayed result, never either input before subtraction.

Clutch implementations should remain provider-specific but must share the
observable contract: pre-event score, regulation time cutoff, and the same OT
margin override. EuroLeague centralizes these rules in clutch helper functions;
that is preferable to repeating CASE expressions in each reader.

### Function structure and maintenance

| Area | Israel | EuroLeague | Better choice |
|---|---|---|---|
| Language | large PL/pgSQL readers with internal branching | mostly SQL readers plus explicit app routing | Small SQL implementation readers |
| Filter CTEs | duplicated between Ratings and FF, and within clutch/non-clutch branches | duplicated between Ratings and FF across per-game/direct variants | Shared generated/tested query skeleton or one internal filtered-games contract |
| Routing location | mixed app/MV fallback and function branching | explicit app classification | One shared app classifier plus thin DB implementations |
| Plan policy | forced custom plan | per-game/direct forced custom; dynamic definition is inconsistent | Apply the same policy only where benchmarked necessary |
| Privilege model | invoker-style functions in source | `SECURITY DEFINER`, locked `search_path`, PUBLIC revoked, execute granted to app role | Euro hardened pattern, subject to the existing security audit |
| Names | historical `off/def starters` names | same historical names despite own/opponent meaning | Canonical app names `own/opp`; compatibility aliases at DB boundary |

Avoid solving duplicated filter SQL with a nested function call that creates an
optimization boundary. The preferred implementation is either a single common
Team metrics reader per route, or mechanically generated parallel SQL plus
contract tests. If separate Ratings and Four Factors public functions remain,
their shared CTE text and formulas must be generated from one source or tested
for structural equivalence.

## Target architecture

1. A shared R request object owns the common fields: season/date, team,
   competition stage, opponent, location, result, opponent strength, round / last
   N, clutch, and own/opponent starter bounds.
2. A league adapter maps `game_type` versus `phase`, integer versus bigint IDs,
   and league defaults.
3. One shared classifier chooses `season`, `pergame`, `standard_clutch`, or
   `custom_clutch`. It considers all four clutch fields.
4. Provider-specific implementation readers consume the best native fact:
   Israeli `team_metrics_by_game_mv` or `lineup_four_factors_by_game`, and
   EuroLeague `team_four_factors_by_game`; only custom clutch and Israeli shot
   profile reach action grain.
5. A common output projection returns identical names, order, types, additive
   counts, formulas, and null behavior. Provider-only fields are separate
   extensions.

This gives structural parity without pretending the provider columns or facts
are identical.

## Recommended implementation order

### P0 — correctness and contract lock

1. Fix both fallback predicates to use the shared four-field clutch classifier.
2. Add tests for status-only, OT-toggle, time-only, and margin-only routing.
3. Choose single-round Net Rating as the canonical formula and update every
   season MV, per-game reader, standard-clutch reader, and direct reader in one
   migration. Treat the expected 0.1 changes as an intentional correction.
4. Add cross-reader tests asserting Summary and Four Factors agree on PPP,
   possessions, and Net Rating for the same request.

### P1 — Israeli fast paths

1. Add an Israeli per-game Team reader over `team_metrics_by_game_mv` for
   non-clutch requests without starter filters. Preserve the current public
   output and compare exact rows over a filter matrix before routing traffic.
2. Route non-clutch starter filters through
   `lineup_four_factors_by_game`, using `num_starters` as own starters and
   `opp_starters` as opponent starters. Starter filters must not by themselves
   activate the raw clutch path.
3. Split shot-profile calculation from common Team Ratings so Summary requests
   do not pay for `shot_zones` and action-grain scans.
4. Benchmark broad, last-N, date, opponent, home/away, result, rank, GN, own
   starter, opponent starter, and empty-result presets. Require exact additive
   parity before comparing rates.

### P2 — common output and maintenance

1. Define the common output projection and cast both adapters to it.
2. Add EuroLeague additive FF counts to the public/common projection.
3. Normalize common column order and `bigint` IDs/counts.
4. Remove the two Israeli exact-starter parameters after all callers use
   min/max bounds.
5. Consolidate duplicated schedule/rank/filter SQL through one source template
   or one benchmark-safe internal contract, with snapshot tests preventing
   structural drift.

### P3 — optional call consolidation

Only after P0-P2, measure whether one combined Team metrics call is worthwhile.
The current Euro broad pair totals about 0.24 seconds warm, so this is a modest
latency/maintenance improvement, not the primary performance problem. Do not
trade clear routing or planner visibility for that small saving.

## Acceptance gates

- No live load and no provider-fact rewrite.
- Exact selected-game set across companion adapters for equivalent UI requests.
- Exact additive-count parity between old and candidate paths, except a
  separately approved semantic correction.
- Summary and Four Factors agree on common PPP, possession, and Net Rating
  columns.
- All four clutch fields participate in routing tests.
- Own/opponent starter filters have the same observable meaning in every view.
- No non-clutch Team request scans an action-grain relation unless it requests
  Israeli shot-profile fields.
- Candidate p95 and buffer counts do not regress against the current best route.
- Existing privilege/search-path and schema-isolation checks pass.

