# Cross-league Team metrics read layer — design specification

**Date:** 2026-08-29  
**Status:** proposed; implementation and live DDL require separate approval  
**Companion audit:** `docs/plans/2026-08-29-team-dashboard-parity-audit.md`  
**Implementation plan:** `docs/plans/2026-08-29-cross-league-team-metrics.md`

## 1. Goal

Give the Israeli and EuroLeague Team dashboards one deliberate application
contract while retaining the best native execution path and provider evidence
in each schema.

The design must achieve all four outcomes together:

1. **Correctness:** the Summary and Four Factors views must derive common
   metrics from the same additive totals and must never disagree because of
   routing or rounding.
2. **Performance:** non-clutch requests must use game-grain additive facts;
   action grain is reserved for custom clutch and the Israeli shot-profile
   extension.
3. **Maintainability:** one normalized request, one route classifier, and one
   combined Team metrics result replace parallel app calls and drifting SQL
   formulas.
4. **Provider integrity:** EuroLeague and Israeli physical relations remain
   independent and source-appropriate. Cross-league parity does not mean
   identical physical tables or provider-only columns.

## 2. Non-goals

This work does not:

- copy EuroLeague data into `basketball` or `basketball_test`;
- restore removed EuroLeague Israeli-named middle tables;
- rewrite either possession or lineup engine;
- require identical provider columns or identical event eligibility;
- add a new shot-location model to EuroLeague;
- make Israeli-result equality a EuroLeague publication gate;
- combine Team minutes/pace into the first contract revision;
- change player, lineup, on/off, traditional-stat, or gamelog contracts;
- authorize a live load, schema change, app deployment, or relation rebuild.

Minutes stay separate initially because the two providers obtain duration from
different canonical segment facts. A later measured change may add additive
seconds without blocking this remediation.

## 3. Design principles

### 3.1 Align behavior, not storage

Both adapters implement the same request and result semantics. Each adapter
reads its best native fact:

- Israel: `team_metrics_by_game_mv` when starter context is unrestricted;
  `lineup_four_factors_by_game` when own/opponent starter bounds are active;
  `df_pts_poss_lineups_longer_mv` only for custom clutch or shot profile.
- EuroLeague: `team_four_factors_by_game` for non-clutch;
  `default_clutch_lineup_totals_by_game` for the standard clutch preset; the
  approved action-derived Team fact for custom clutch.

No cross-schema SQL dependency is allowed.

### 3.2 Add first, divide last

Every rate is calculated after the selected games and contexts have been
reduced to additive numerators and denominators. Stored or child-level ratios
must never be averaged.

### 3.3 One public application call per Team metrics request

The app requests one combined row set containing Summary and Four Factors
fields. Existing Ratings and Four Factors functions remain temporarily as
compatibility projections, but the app no longer calls both for the same
request.

### 3.4 Explicit route capabilities

The application selects one of four execution routes:

1. `season`
2. `pergame`
3. `standard_clutch`
4. `custom_clutch`

Private implementation functions may use route-specific signatures. The app
does not construct those signatures directly; one adapter owns that mapping.

### 3.5 Provider extensions are separate

The Israeli shot-profile columns are a valid provider extension. They are not
null-filled into the EuroLeague common contract and are not bundled into an
Israeli Summary request.

## 4. Canonical request model

The R application owns a named `team_metrics_request` object. It is a logical
contract, not a positional argument list.

| Group | Canonical field | Type / normalized default | Meaning |
|---|---|---|---|
| Scope | `league` | `israel` or `euroleague` | Chooses adapter only |
| Scope | `competition` | nullable text | EuroLeague provider competition; null for Israel |
| Scope | `season` | integer, required | Provider season key |
| Games | `start_date`, `end_date` | nullable dates | Inclusive game-date bounds |
| Games | `team_ids` | nullable ID vector | Requested output teams |
| Games | `stage_values` | nullable vector | Israeli game types or Euro phases through adapter |
| Games | `opponent_ids` | nullable ID vector | Opponent restriction |
| Games | `home_away` | `all`, `home`, `away` | Team perspective |
| Games | `outcome` | `all`, `win`, `loss` | Team result |
| Strength | `opp_rank_side` | `all`, `top`, `bottom` | Season-wide opponent rank restriction |
| Strength | `opp_rank_n` | nullable integer | Size of rank band |
| Strength | `opp_rank_metric` | `net`, `off`, `def` | Rank dimension |
| Sequence | `round_min`, `round_max` | nullable integer | `gn` or `round_number` through adapter |
| Sequence | `last_n_games` | nullable positive integer | Most recent N per team in season |
| Clutch | `max_margin` | nullable nonnegative integer | Absolute pre-event margin |
| Clutch | `margin_status` | `all`, `leading`, `trailing`, `tied` | Pre-event team state |
| Clutch | `max_time_remaining` | nullable nonnegative integer | Regulation seconds remaining |
| Clutch | `filter_overtime_margin` | boolean, default false | Apply margin/status to OT when clutch is active |
| Context | `own_starters_min/max` | nullable 0-5 integer | Team lineup starter bounds |
| Context | `opp_starters_min/max` | nullable 0-5 integer | Opponent lineup starter bounds |

### 4.1 Validation

The request builder rejects rather than silently normalizes:

- start date after end date;
- minimum above maximum;
- starter values outside 0-5;
- negative margin/time/round/last-N;
- rank side without a positive `opp_rank_n`;
- unknown enumerated values;
- a competition supplied to the Israeli adapter;
- IDs that cannot be represented by the target adapter.

Blank UI strings become null before classification. Historical SQL parameter
names containing `off/def starters` are compatibility aliases only; their
canonical meanings are own/opponent.

### 4.2 CSV boundary

The R request keeps vectors as vectors. CSV serialization occurs only inside
the league DB adapter because the current PostgreSQL functions accept CSV
text. A later array-signature migration may be measured separately; it is not
required for this cutover.

## 5. Canonical route classifier

`team_metrics_route(request, season_bounds)` is the only app function allowed
to select a Team metrics execution path.

### 5.1 Clutch activity

```text
clutch_active =
  max_margin is set
  OR margin_status != "all"
  OR max_time_remaining is set
```

`filter_overtime_margin` changes how an active margin/status request treats
overtime. By itself, with no clutch predicate, it has no observable effect and
does not force an action scan.

### 5.2 Route rules

Routes are evaluated in this order:

1. `season` when the request is the exact full-season, all-team, all-context,
   non-clutch default supported by the season aggregate.
2. `pergame` when `clutch_active` is false. Date, team, stage, opponent,
   home/away, outcome, opponent rank, round, last-N, and both starter filters
   remain eligible for this route.
3. `standard_clutch` when margin is 5, status is `all`, time remaining is 300,
   and overtime-margin filtering is false.
4. `custom_clutch` for every other active clutch request.

The `season` predicate and `pergame` predicate must be shared by both league
tabs. League adapters may report that a particular fast path is unavailable,
in which case routing falls forward to the next semantically capable route,
never to an incapable route.

### 5.3 Route invariants

- A route cannot accept and ignore a filter.
- Starter filters alone never select `custom_clutch`.
- `margin_status` alone always selects `custom_clutch`.
- The standard preset with OT filtering enabled selects `custom_clutch`.
- An untouched season request never executes action grain.
- Previous-period/delta requests use the same classifier as current requests.

## 6. Selected-game semantics

Every implementation follows the same logical stages:

```text
normalize provider scope
  -> rank all season games by recency per team
  -> attach season-wide opponent ranks
  -> apply selected-game filters
  -> read the route's provider fact
  -> apply own/opponent starter bounds
  -> aggregate additive totals by team
  -> calculate common rates and ranks
  -> project provider extensions separately
```

### 6.1 Last N

`last_n_games` is assigned over all games for `(competition, season, team)` in
descending `(game_date, stable_game_id)` order before date, stage, opponent,
venue, outcome, and round filters are applied. This preserves current Team-tab
behavior. If product intent later changes to “last N within selected filters,”
that is a separate semantic change.

### 6.2 Opponent ranks

Opponent strength uses the complete season rating table for the provider and
does not shift with the requested date window. `bottom N` means ranks greater
than `team_count - N`; missing opponent ranks do not qualify for top/bottom.

### 6.3 Team selection and ranks

`team_ids` restricts the returned team population and its output ranks, matching
the current EuroLeague filtered behavior. It does not alter the season-wide
opponent-rank table. The Israeli adapter gains this optional capability even if
the initial Israeli UI does not expose a team selector.

### 6.4 Qualifying games

`games_played`, wins, and losses count distinct selected games having at least
one qualifying fact row after route and starter-context filters. A team with no
qualifying row is absent rather than returned with invented zero rates.

## 7. Canonical additive result

Each provider implementation returns one row per `(season, team_id)`. Common
IDs and counts are cast to `bigint`; `season` remains `integer`.

### 7.1 Identity and record counts

1. `game_year integer`
2. `team_id bigint`
3. `team_name text`
4. `games_played bigint`
5. `wins bigint`
6. `losses bigint`

### 7.2 Offensive additive fields

7. `off_pts bigint`
8. `off_poss bigint`
9. `off_ts_poss bigint`
10. `off_oreb_cnt bigint`
11. `off_oreb_opps bigint`
12. `off_tov_cnt bigint`
13. `off_fta bigint`
14. `off_fga_cnt bigint`
15. `off_fgm_cnt bigint`
16. `off_fg3m_cnt bigint`

### 7.3 Defensive additive fields

17. `def_pts bigint`
18. `def_poss bigint`
19. `def_ts_poss bigint`
20. `def_oreb_cnt bigint`
21. `def_oreb_opps bigint`
22. `def_tov_cnt bigint`
23. `def_fta bigint`
24. `def_fga_cnt bigint`
25. `def_fgm_cnt bigint`
26. `def_fg3m_cnt bigint`

### 7.4 Derived display fields

27. `off_ppp numeric`
28. `def_ppp numeric`
29. `net_rtg numeric`
30. `off_ts numeric`
31. `def_ts numeric`
32. `off_efg numeric`
33. `def_efg numeric`
34. `off_oreb numeric`
35. `def_oreb numeric`
36. `off_tov numeric`
37. `def_tov numeric`
38. `off_ftr numeric`
39. `def_ftr numeric`
40. `rank_net_rtg bigint`
41. `rank_off_ppp bigint`
42. `rank_def_ppp bigint`

The ordered contract is intentionally a superset of the existing Ratings and
Four Factors intersections. The 20-row payload cost is negligible; exposing
the additive terms makes correctness and re-aggregation testable.

### 7.5 Provider extensions

Israel exposes a separate `team_shot_profile` result containing:

- `off_fga`, `off_layup_att`, `off_dunk_att`, `off_fg3_att`, `off_c3_att`,
  `off_c3_known_att`;
- the corresponding defensive attempt fields.

EuroLeague may retain `def_steals` or other provider metrics in a separate
extension. Provider extension columns are not inserted as all-null fields into
the common result.

## 8. Formula contract

All formulas operate on aggregated additive fields. Each displayed value is
rounded once to one decimal place.

```text
off_ppp = 100 * off_pts / off_poss
def_ppp = 100 * def_pts / def_poss
net_rtg = 100 * off_pts / off_poss - 100 * def_pts / def_poss

off_ts  = 100 * off_pts / (2 * off_ts_poss)
off_efg = 100 * (off_fgm_cnt + 0.5 * off_fg3m_cnt) / off_fga_cnt
off_oreb = 100 * off_oreb_cnt / off_oreb_opps
off_tov  = 100 * off_tov_cnt / off_poss
off_ftr  = 100 * off_fta / off_fga_cnt
```

Defense uses the corresponding defensive terms. Every denominator is guarded
with `NULLIF(..., 0)`; a zero denominator yields null, not zero.

Net Rating specifically subtracts unrounded PPP expressions and rounds the
final difference once. This intentionally corrects the verified 0.1
Summary/Four-Factors disagreements in both leagues.

Ranks are `dense_rank()` over the unrounded expressions, with Net and offense
descending and defense ascending. Display rounding must not create or break a
rank tie.

## 9. Provider adapters

### 9.1 Israeli adapter

#### Season

Read the existing season aggregate during the compatibility phase. Its refresh
definition must adopt the canonical additive formulas and common types.

#### Non-clutch, no starter bounds

Read `basketball_test.team_metrics_by_game_mv`. It already stores the required
game/team schedule fields and additive Four Factors totals. The audit proved
exact current-output parity for broad and last-10 requests.

#### Non-clutch with starter bounds

Read `basketball_test.lineup_four_factors_by_game`, joined once to the filtered
game set. Map:

- `num_starters` -> own starter bounds;
- `opp_starters` -> opponent starter bounds.

Aggregate its long offense/defense rows into the common wide output. Starter
bounds must not set the old `v_clutch_active` flag.

#### Standard/custom clutch

Initially retain the current raw logic, but expose it through one combined
metrics implementation so Ratings and Four Factors share the same eligible
rows and additive aggregation. A later standard-clutch cache is optional and
must be justified by measured app traffic and refresh cost.

#### Shot profile

Move the `shot_zones` join and shot-tag counts into a separate function called
only by the Shot Profile view. It shares the canonical selected-game and clutch
semantics but is not required for Summary or Four Factors.

### 9.2 EuroLeague adapter

#### Season

Read the existing season aggregates during compatibility. Their formula and
common output projections are updated together.

#### Non-clutch

Read `euroleague.team_four_factors_by_game` joined to the filtered game set on
both `(game_id, team_id)`. Its `(own_starters, opp_starters)` grain answers all
starter bounds without action access.

#### Standard clutch

Read `default_clutch_lineup_totals_by_game` through the existing incremental
refresh lifecycle. The combined reader projects the same additive contract as
the per-game route.

#### Custom clutch

Use one approved action-derived Team fact and one filtered game set. Ratings
and Four Factors must not independently scan the same action fact. Clutch
qualification remains based on pre-event score and canonical elapsed time.

## 10. PostgreSQL function structure

Each schema adds route-specific combined implementation readers with the same
result columns:

```text
get_team_metrics_pergame(...non-clutch arguments...)
get_team_metrics_standard_clutch(...context arguments...)
get_team_metrics_custom_clutch(...all arguments...)
```

Names may receive a schema-specific suffix only if an existing signature
collides. The application adapter chooses the route and exact signature.

Existing functions remain during transition:

- `get_team_ratings_*` becomes a thin projection of the combined result;
- `get_team_four_factors_*` becomes a thin projection, including its historical
  column aliases/types where compatibility requires them.

The new app path calls the combined implementation directly, so compatibility
wrappers are not latency-critical. Do not make a hot implementation call a
nested helper if plan evidence shows an optimization boundary or repeated
materialization; inline or mechanically generate the shared SQL in that case.

SQL readers are `STABLE`. Follow the existing least-privileged app execution
model, lock `search_path` for any `SECURITY DEFINER` function, revoke PUBLIC,
and grant only the intended app role. Do not change security mode solely for
textual parity without running the repository security audit.

## 11. Application structure

Shared helpers own:

- request construction and validation;
- route classification;
- common output column assertions;
- formula/rank presentation metadata;
- DB adapter dispatch.

League server files own only provider input mapping and presentation-specific
extensions. They must not repeat the 19/23 positional signature builders.

Conceptually:

```r
request <- build_team_metrics_request(...)
result <- fetch_team_metrics(pg_pool, request)
```

`fetch_team_metrics()` selects the league adapter, route, SQL signature, and
parameter list. It returns the canonical ordered data frame or fails with a
contract error. Both current and previous-period reactives call this function.

Summary and Four Factors render different projections from the same combined
reactive result. Israeli Shot Profile invokes its extension fetch only while
that view is active.

## 12. Compatibility and cutover

This is an additive-first migration:

1. Add combined functions and tests without changing app routing.
2. Capture old-reader baselines for the complete preset matrix.
3. Validate new combined readers against additive totals and approved semantic
   corrections.
4. Deploy DB functions.
5. Switch the app to the shared adapter behind a feature flag.
6. Observe route, latency, error, and row-count telemetry.
7. Remove the feature flag after a stable observation window.
8. Deprecate, but do not immediately drop, legacy public functions and exact
   starter parameters.

Rollback switches the app feature flag back to existing readers. Additive DB
objects can remain unused; rollback must not require destructive DDL.

## 13. Observability

Every Team metrics request records, without sensitive values:

- league and selected route;
- normalized filter-class flags;
- complete-fetch duration;
- returned team-row count;
- whether the season cache or extension was used;
- a stable request-shape identifier;
- error class.

The implementation harness records `EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS,
FORMAT JSON)` for gated presets. App logging must make an unexpected action
route visible.

## 14. Acceptance criteria

### Logic

- Same selected-game keys for old and candidate paths unless the spec declares
  a correction.
- Exact additive-count parity for unchanged semantics.
- Summary and Four Factors have identical PPP, possessions, and Net Rating for
  the same request.
- Net Rating uses one-round unrounded-input semantics everywhere.
- Own/opponent starter labels have identical observable meaning in both tabs.
- Status-only clutch requests cannot reach a season or per-game cache.
- OT toggle affects standard/custom selection when a clutch predicate is active.

### Performance

- No non-clutch common Team request scans an action-grain relation.
- Israeli broad and last-10 combined requests meet an initial 0.25-second warm
  median target on the audited dataset, subject to the formal relative gates in
  the implementation plan.
- EuroLeague broad and last-10 do not regress beyond the larger of 10% or 100
  ms from the current per-game readers.
- No candidate adds an index without isolated plan/timing evidence.

### Maintenance

- One shared request builder and classifier serve both Team tabs.
- No league server contains a copied 19/23-position SQL signature builder.
- Common column order, types, formula text, and route tests are locked.
- Provider-only columns remain explicit extensions.
- Legacy wrappers are documented with a removal condition and usage audit.

### Safety

- EuroLeague and Israeli writes remain schema-isolated.
- Candidate DDL runs in rollback mode by default and verifies live function
  hashes before apply.
- Grants, owners, security mode, search path, and RLS expectations are tested.
- No load or destructive relation replacement is part of this change.

## 15. Decisions deliberately deferred

- Replacing CSV parameters with PostgreSQL arrays.
- Combining Team minutes/pace into the additive result.
- Adding an Israeli standard-clutch physical cache.
- Replacing existing season MVs with one unified physical table.
- Removing legacy Ratings/Four Factors functions.
- Exposing a team selector in Israeli Tab 3.

Each deferred item requires its own usage evidence, benchmark, and compatibility
decision. None is necessary to obtain the core correctness and performance
benefits.

