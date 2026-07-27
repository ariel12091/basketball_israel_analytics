# Starters Fast Path — Design Spec

**Date:** 2026-07-27 · **Status:** Approved, NOT yet executed · **Plan:** `docs/superpowers/plans/2026-07-27-starters-fast-path.md`

## Problem

The starters filters are the slowest filters in the app. Any starters parameter
(`p_num_starters_off/def` + min/max variants) flips `v_clutch_active` in the
Tab 2 lineup functions and Tab 3 team functions, forcing the raw
`df_pts_poss_lineups_longer_mv` action scan even when no clutch (margin/time)
filter is set.

Measured 2026-07-27 (min of 5, pooler, 2026 season):

| Case | Time | Path |
|---|---|---|
| Tab 2 Summary, no filters | 0.26s | fast (sub_lineups_stats) |
| Tab 2 Summary + own starters=5 | 0.75s | raw (forced) |
| Tab 2 Summary + 5v5 joint | 0.61s | raw |
| **Tab 2 Four Factors + starters** | **2.35s** | raw + FF recompute |
| Tab 3 ratings + starters | 0.53s | raw (fine post 2026-07-27 refactor) |
| Tab 3 FF + starters | 0.61s | raw |
| Tab 1 (either view) + starters | 0.58–0.65s | pre-agg (pff MV, starter-keyed) |

Tab 1 proves the target architecture: `player_four_factors_by_game` is keyed by
`(own_starters, opp_starters)`, so starters filtering is just a different SUM.

## Root cause

`mv_lineup_totals_by_day` and `lineup_four_factors_by_game` carry a single
own-side `num_starters` key. An opponent-side or joint starter condition is
inexpressible on them, so the functions route all starters queries to the raw
scan. The starters logic that exists in the non-clutch branches is dead code
AND semantically wrong (it maps `p_num_starters_def` to defense-perspective
rows' `num_starters`, which is the OWN count — the clutch path's actual
semantics filter `own_starters` and `opp_starters` uniformly on all rows).

## Primary use case (user, 2026-07-27)

Starters-vs-starters (joint own+opp condition, typically 5v5). This rules out
the cheap own-side-only routing fix — the joint condition needs an
`opp_starters` dimension in the pre-agg layer.

## Decision

1. **Add `opp_starters` as a grouping key** to both lineup MVs. `own_starters`
   is already there: it is functionally determined by the lineup composition
   (= existing `num_starters` column). Measured row growth ×1.64 (45k → 74k
   rows for 2026); both MVs are 12–13MB → ~+10–15MB total. Negligible against
   the (currently 851MB) DB.
2. **Gap-exclusive minutes** (user decision): within a segment, minutes are
   attributed per contiguous opponent-window (gaps-and-islands on
   `opp_starters` ordered by action id), summed per `opp_starters` value.
   Windows are computed across ALL rows (no type_lineup filter — full floor
   time, as today) and attached to offense rows only (as today).
3. **Gate split** in `fetch_lineups_all` / `fetch_lineups_four_factors`:
   - `v_clutch_active` = margins/time only → raw path, unchanged (incl. its
     starter predicates for clutch+starters combos).
   - `v_starters_active` = any starters param → pre-agg path with uniform
     filters: `num_starters` vs off params AND `opp_starters` vs def params,
     on ALL rows regardless of type_lineup.
   - Fast path (`sub_lineups_stats`) still requires no starters (not keyed).
     Exception under evaluation in the plan: lineups-FF's fast path reads
     `lineup_four_factors_by_game` directly, which WILL have the keys — 
     starters-only-no-other-filters may ride it.
   - The dead per-type starters branches are deleted.
4. **Tab 3 untouched** — measured fine (starters predicate actually speeds up
   its single scan post the 2026-07-27 `game_agg` refactor).

## Minutes semantics — what changes and when

- Poss/pts/shooting: exact per action row; unaffected by windowing; must stay
  byte-identical to the raw path.
- Minutes on starters-filtered queries: today's raw path takes
  first-to-last-matching-action span per segment, which INCLUDES interior gaps
  where the opponent count fell outside the filter (e.g. opp 5→3→5 under an
  `opp=5` filter). Gap-exclusive EXCLUDES those gaps → minutes can only
  shrink, and are equal whenever the lineup faced a single contiguous
  opponent window per segment (the common case; always true for lineups whose
  opponents never re-entered the same count non-contiguously).
- Minutes on non-starters queries: identical (day-level sum of windows equals
  the old segment span when no filter splits windows — verified by the plan's
  aggregation invariant).
- Rationale for gap-exclusive: it answers "how long did this matchup actually
  play", is simpler to precompute, and the exact-parity alternative would
  require keeping segment granularity in the MV (row explosion) purely to
  reproduce a less-correct number.

## NULL `opp_starters` contract

Raw-path comparisons exclude NULL rows whenever an opp filter is active; the
MV must preserve this. Therefore: keep NULL as NULL (never COALESCE to a
sentinel — `-1 <= max` would wrongly match max-filters), join windows with
`IS NOT DISTINCT FROM`, and make the unique indexes `NULLS NOT DISTINCT`
(PG15+) so REFRESH CONCURRENTLY identity holds if ETL uses it (recon item).

## Verification contract (plan Task 7)

- 15-case perf harness: byte-identical, all cases.
- Clutch+starters case (`st_clutch_5v5`): byte-identical (still raw path).
- Starters cases: byte-identical with the minutes column excluded; minutes
  row-wise ≤ baseline, equal for single-window rows.
- Targets: FF 5v5 2.4s → ≤0.5s; Summary 5v5 0.6–0.75s → ≤0.4s.
- `team_four_factors_mv` (L4, sums `lineup_four_factors_by_game`): totals
  unchanged after rebuild (re-sum invariant over the new key).

## Rejected alternatives

- **Own-side-only MV routing** (no schema change): doesn't cover the primary
  starters-vs-starters use.
- **Sentinel for NULL opp_starters**: breaks max-filter semantics (above).
- **Exact-parity (gap-inclusive) minutes**: needs segment-level MV granularity
  for a number that misattributes gap time; rejected with user sign-off.
- **Speeding up the raw path instead** (game_agg-style restructure of the
  lineups-FF clutch path): keeps 1.2–1.5s floor, doesn't generalize; may still
  be worth doing later for clutch+starters combos, independent of this work.

## Open items resolved during execution (recon, plan Task 1)

- How `player_four_factors_by_game` attributes `onoff_minutes` across its
  starter keys — mirror if sound, else record discrepancy (its minutes may
  predate the canonical clock model).
- Whether ETL refreshes these MVs with CONCURRENTLY (drives the NULLS NOT
  DISTINCT index requirement).
- Whether lineups-FF's fast path can take the starter predicates directly.

## Related

- `docs/adr_api_owns_query_construction.md` — at the Phase-2 trigger these
  functions retire; the `opp_starters` MV keys remain useful to any future
  query builder (the pre-agg layer outlives the plpgsql layer).
- 2026-07-27 perf review: `docs/superpowers/plans/2026-07-27-sql-function-perf-tuning.md`.
