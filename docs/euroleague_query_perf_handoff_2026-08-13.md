# EuroLeague vs Israeli query performance — handoff, 2026-08-13

Measurement of every app-facing EuroLeague query against its Israeli
companion, the one fix applied so far, and what remains. All timings are warm
medians as `app_readonly` through the pooler (port 6543) unless stated.

Every number below is reproduced in this document deliberately: the scripts
that produced them live in a session scratchpad under `%TEMP%` and are not
expected to survive a restart.

**Committed:** `a9d5f7f` on `shiny/euro-tab1` (migration 037). Not pushed.

---

## 1. Scale baseline

Scale does not explain any gap below.

| | games | teams |
|---|---:|---:|
| Israeli `game_year` 2026 | 221 | 14 |
| EuroLeague `E` / 2025 | 292 | 20 |

Largest relations: `euroleague.player_stats_actions_by_game` 494 MB,
`euroleague.action_team_context_actions` 307 MB, `euroleague.actions_raw`
271 MB, `basketball_test.df_pts_poss_lineups_longer_mv` 250 MB,
`euroleague.actions` 177 MB, `euroleague.sub_lineups` 128 MB.

## 2. Paired benchmark (before any change)

Fast paths were at parity everywhere. Every EuroLeague *filtered* path was
slower, and two surfaces were faster.

| Surface | Israeli | EuroLeague |
|---|---:|---:|
| On/off season MV | 0.30 | 0.39 |
| Player advanced MV | 0.32 | 0.31 |
| Team ratings MV | 0.25 | 0.25 |
| Team four factors MV | 0.25 | 0.30 |
| Player traditional MV | 0.28 | 0.25 |
| Reference lookups (teams / GN / players) | 0.31 / 0.28 / 0.28 | 0.27 / 0.28 / 0.27 |
| Game-log season pull | 1.58 + 0.90 | **0.66** |
| `onoff_compute` | 2.06 | **11.88** |
| `four_factors_compute` | 1.60 | 3.93 |
| Team ratings filtered | 1.22 | **12.61** direct / 9.50 dynamic |
| Team four factors filtered | 0.36 | **12.58** direct / 9.39 dynamic |
| Team minutes filtered | — | 0.77 direct / 9.22 dynamic |
| Lineups filtered | 0.90 / 2.72 | **19.86** |
| Lineups fast path | — (always SQL) | 4.04 |
| Player traditional filtered | **91.22** | timeout > 120s |
| Player traditional clutch | 20.52 | **0.76** standard / 15.67 custom |

Two reversals, both by design: EuroLeague standard clutch player stats read an
incremental per-game cache Israel has no equivalent of, and the EuroLeague
game-log fact is one join instead of two season pulls.

## 3. The actual defect

`PROJECT.md`'s 031-036 tuning was validated on **clutch presets**, where a
margin/time predicate makes the action scan small. Those hold up. The gap was a
class the audit never measured: a **filtered but non-clutch** request — a phase,
an opponent, a last-N, a narrowed date range — which the routing also sent to
the `_direct` readers, with no predicate to narrow the scan.

| Preset class | EuroLeague team ratings | team four factors |
|---|---:|---:|
| Custom clutch (m≤3 / 4:00) — tuned | 0.77 | 0.77 |
| Narrowing filter (last 10) | 1.70 | 1.78 |
| Broad, no clutch | **12.61** | **12.58** |

`use_direct_team_reader()` returned TRUE for anything that was not exactly the
5/all/5:00 preset — including a request with no clutch filter at all.

## 4. What was fixed (migration 037, committed)

Added `euroleague.get_team_ratings_pergame` and
`get_team_four_factors_pergame`, reading `team_four_factors_by_game`
(21,204 rows, 6.4 MB, keyed `game_id, team_id, own_starters, opp_starters`)
instead of the 494 MB action fact. Everything from `normalized` through
`games_filtered` is unchanged from the `_direct` readers.

`app/R/server_tab9_euro_team.R` now routes three ways via `team_reader_kind()`:
no clutch predicate → `_pergame`; exact 5/all/5:00 → `_dynamic` (cached);
any other clutch → `_direct`.

The per-game readers take **19 parameters, not 23** — the fact has no time or
margin dimension, so a mis-routed clutch request fails at the call site rather
than silently returning unfiltered numbers. Starter semantics follow `_direct`
(`off` bounds own starters, `def` bounds opponent), not Israel's.

**Verification:** 30/30 full ordered-row comparisons identical against the
`_direct` readers across 15 non-clutch presets (including starter bounds,
opponent-rank, phase, last-N, date windows). App suite `FAIL 0 | PASS 1177`.

| Tab 9 broad filtered request | before | after |
|---|---:|---:|
| Team ratings | 12.61 | **0.28** |
| Four factors | 12.58 | **0.29** |
| Team minutes | 0.77 | 0.77 (unchanged) |
| **Total** | **~26s** | **~1.3s** |

Israeli Tab 3's equivalent is ~1.6s. Team minutes was left alone: it has no
per-game counterpart and migration 033's `_direct` reader is already fast.

## 5. Lineups — DONE (migration 038), but not for the predicted reason

**Outcome:** `euroleague.fetch_lineups_pergame`, routed from
`server_tab10_euro_lineups.R`. Measured through the app's own query as
`app_readonly` on the pooler: default view **24.39s → 1.22s**, phase 14.39 →
1.03, own-starters ≥4 9.68 → 0.44, size 3 broad 21.50 → 5.61. Parity 29/29
presets on all 33 columns (four unit sizes, both player filters, `min_poss`),
then 8/8 again through the app query shape.

**The diagnosis below was wrong, and the probe is what caught it.** This
section assumed `fetch_lineups_dynamic` scans the action fact on a non-clutch
request, as `get_team_*_direct` did. It does not. `select_team_game_facts`
(migration 020) already branches to `lineup_totals_by_game` when margin and
time are absent, and returns that table row for row — 8,440 rows over a
40-game sample, zero differing in either direction. The 21-24s was query
*shape*, not data volume:

1. the fact arrives through two nested analytical function boundaries;
2. `lineup_identity` then joins `lineup_totals_by_game` a **second** time, on a
   five-element `text[]` equality, purely to recover `lineup_key` and
   `player_ids` — two columns the fact rows already carried;
3. the result expands through `sub_lineups` even at unit size 5.

The proposed fix was right anyway, which is the trap: had the probe been
skipped, the migration would have shipped with a false explanation attached to
it, and the next reader would have drawn the wrong lesson about where the cost
lives. A right answer for a wrong reason still fails the next question.

The three "unverified assumptions to check" resolved as: grain is
`(game_id, team_id, lineup_key, type_lineup, opp_starters)` with `own_starters`
functionally determined (zero violating instances), so starter bounds are plain
row predicates; `filtered_team_game_facts()` sources the identical numbers; and
the ghost-lineup discrepancy did not reappear — 24,701 all-zero rows exist but
contribute zero to every aggregate, and no unit exists only as ghosts.

Both tabs now share one classifier, `clutch_reader_kind()` in `app/R/helpers.R`,
replacing the copy that lived in `server_tab9_euro_team.R`. It has its own unit
tests in `test-euro-clutch.R`; that file passes 33/33.

### Original analysis, as written before the probe

`server_tab10_euro_lineups.R` has the identical structural gap:

```r
reader <- if (isTRUE(clutch_active) && !isTRUE(standard_clutch)) {
  "fetch_lineups_direct"      # custom clutch  -> 1.07s
} else {
  "fetch_lineups_dynamic"     # NON-CLUTCH     -> 19.86s broad, 3.91s last-10
}                             # standard clutch -> 6.10s (per PROJECT.md)
```

`fetch_lineups_dynamic` (migration 029) sources its metrics from
`filtered_team_game_facts()` — the nested analytical function boundary
`PROJECT.md` identifies as the parameter-planning failure — then joins
`lineup_totals_by_game` for identity and expands through `sub_lineups`.

For a non-clutch request none of that is needed. `lineup_totals_by_game`
(94,318 rows, 50 MB) is already the per-game lineup fact and carries every
column required: `lineup_key`, `player_ids`, `type_lineup`, `own_starters`,
`opp_starters`, all additive counts, and `seconds`.

Proposed `fetch_lineups_pergame`, mirroring migration 035's shape but sourcing
`lineup_totals_by_game` instead of scanning actions:

- filter the schedule once (byte-identical CTEs to 035's `n` / `sr` / `ranks` / `games`);
- aggregate `lineup_totals_by_game` over those games, applying the starter bounds;
- size 5: `unit_key = lineup_key`, bypass `sub_lineups` (PROJECT.md lesson 10);
- sizes 2-4: join `sub_lineups` for the unit mapping;
- minutes = `sum(seconds) FILTER (WHERE type_lineup = 'offense') / 60.0`, matching 029.

**Do the parity probe before writing SQL.** That is what de-risked 037: confirm
an aggregate of `lineup_totals_by_game` reproduces `fetch_lineups_dynamic`'s
output for the broad preset, and matches `sub_lineups_stats_mv` for the
unfiltered case. Only then write the migration.

Unverified assumptions to check in that probe:
- the exact grain of `lineup_totals_by_game` (is it split by starter context?);
- whether `filtered_team_game_facts()` sources the same numbers on the
  non-clutch path, or applies eligibility rules that would change totals;
- the zero-duration "ghost lineup instance" discrepancy already documented for
  migration 019 — expect it to reappear and decide deliberately.

Note `fetch_lineups_direct` **timed out (>20s)** on a non-clutch last-10
request, so it is not the answer here; it needs a clutch predicate to be fast.

## 6. Other open items

1. **Tab 8 `onoff_compute`** — examined 2026-08-14, see section 8. The shape
   defect is real but the obvious fix is not one; nothing was left applied.
2. **Israeli Tab 5** `get_player_traditional_dynamic` broad — **91s**, live.
   The EuroLeague equivalent exceeds 120s. Both leagues broken; Israel's ships.
3. **Statement timeout is not in force.** `app/R/global.R:431` sets it via the
   connection `options` string; Supabase's pooler discards that, and
   `SHOW statement_timeout` returns `2min`. A plain `SET statement_timeout`
   *does* work and persists. This is how a 91s query runs to completion.
   Not yet confirmed from inside a running Shiny session.
4. **Game 246** — Crvena Zvezda vs Dubai, Round 25, 2026-01-30, 95-92. Present
   in `final_schedule_mv` and counted by `team_ppp_ratings_mv`, but has no row
   in `team_four_factors_by_game` or `lineup_totals_by_game`. So Tab 9's
   *default* view counts a game every *filtered* view excludes: Crvena Zvezda
   reads 28 games / 2089 off poss / 118.4 off ppp in the season MV versus
   27 / 2017 / 117.9 everywhere else; Dubai 27 / 1998 / 121.1 versus
   26 / 1925 / 120.9. League-wide this is the only such game. Pre-existing,
   unrelated to migration 037.
5. ~~**`PROJECT.md` has no entry for migration 037**~~ — done. `PROJECT.md`
   now carries both 037 and 038 under "The non-clutch routing gap", and the
   migration order and applied-range lines are updated to 038.

## 7. Method notes worth reusing

- The parity gate is the point, not the timing. 037's apply script compares
  full ordered rows for 15 presets and refuses to proceed on any mismatch.
- Probe the data before writing SQL. One query comparing the candidate fact's
  aggregate to the live reader's output settled 037's feasibility in seconds
  and would have killed the idea just as cheaply.
- The apply script's own safety guard is text-based: a reference to an Israeli
  schema *inside a SQL comment* trips it. That is correct behaviour; reword the
  comment.
- Benchmark what the app actually calls. An early claim here that team minutes
  cost 11.6s with an 8.5 GB temp spill was measured against
  `get_team_minutes_dynamic`, which the app does not use on that path; the real
  reader is `_direct` at 0.77s. That error removed a whole item from the plan
  once corrected.

## 8. Tab 8 `onoff_compute` — examined 2026-08-14, nothing shipped

**State: the live functions are exactly as migration 004 left them.** A
migration was written, applied, parity-verified, then reverted the same
session. Both `euroleague.onoff_compute` and `euroleague.four_factors_compute`
are byte-identical to `euroleague/sql/004_app_read_layer.sql` and their
`app_readonly` EXECUTE grants are intact, both re-verified from `pg_proc` after
the revert.

### The 11.88s does not reproduce

Warm medians as `app_readonly` on the pooler, E/2025 broad season:

| | cold first call | warm median |
|---|---:|---:|
| `euroleague.onoff_compute` | 15.32 | 2.8-3.1 |

Narrow presets (last-10, phase, home, starter bounds) are 0.3-1.5s warm. So
Tab 8's steady state is roughly twice the Israeli 2.06s, not six times it, and
the 11.88s figure in section 2 was a cold read. **Cold cache, not steady state,
is what hurts this surface** — which also means timing on this instance cannot
adjudicate a change of this size (see "measurement" below).

### The shape defect (real, and it is neither 037's nor 038's)

Both functions already read a per-game fact. They read it through
`euroleague.player_game_context`, a view that joins `schedule` and
`final_schedule` onto `player_four_factors_by_game`. The `agg` CTE uses none of
the columns those joins add — every filter that needs schedule context reads
the `games` CTE instead. `EXPLAIN (ANALYZE, BUFFERS)`, broad season preset,
385,140 qualifying fact rows:

- `Index Only Scan using schedule_pkey ... loops=385140, Heap Fetches: 385140,
  Buffers: shared hit=1155420` — the view's `JOIN euroleague.schedule s`, run
  once per fact row. Postgres cannot remove it: inner joins are not removable
  even when the join key is unique.
- `Join Filter: (pf.team_id = "*VALUES*".column1), Rows Removed by Join Filter:
  385140` — `final_schedule` is a view over `CROSS JOIN LATERAL (VALUES ...)`,
  so every fact row is expanded to both team perspectives and half discarded.

That is 1,155,420 of the query's 1,216,268 buffers, 95%, doing nothing.

### Why swapping the source is not the fix

Reading `player_four_factors_by_game` directly is output-identical — 41/41
before/after preset comparisons returned identical ordered tuples through the
applier's snapshot gate, and 20/20 more through the inlined bodies beforehand.
It is also **not faster**, because the view's useless joins were accidentally
holding the plan together:

| buffers (deterministic; timings on this instance are not) | broad season | last-10 games |
|---|---:|---:|
| through `player_game_context` (004, live) | 1,216,268 | **10,105** |
| base fact direct | **59,096** | 21,017 |

With the view, the planner drives a parameterised nested loop from the game set
into `euroleague_pff_game_idx`. With the fact exposed directly and the game set
estimated at 650 rows, it prices 650 index probes (cost 166.88 each, because
`euroleague_pff_game_idx` is on `game_id` alone and each probe reads ~1,319
rows to keep ~659) above a `Seq Scan on player_four_factors_by_game` of all
624,478 rows, and takes the seq scan. Every narrow filter — the common case —
gets slower. The estimate that drives this is off by two orders of magnitude in
both directions: `rows=1` against 385,140 actual with the view, `rows=3107`
against 128,720 without it.

Forcing the nested loop back does work — `enable_seqscan=off` reproduces the
good shape on both presets — so the direction is right and the mechanism is
understood. What is missing is a plan the planner will choose on its own.

### What to try next, in order

1. **Index `player_four_factors_by_game (game_id, team_id)`.** The join
   predicate is exactly that pair; today's `game_id`-only index makes every
   probe read both teams and discard half, which is both the runtime waste and
   the reason the planner's per-probe cost is high enough to prefer a seq scan.
   Follow migration 036's lesson: `CREATE INDEX CONCURRENTLY` starved on
   continuous app reads, a bounded normal build succeeded, and invalid shells
   must be dropped first.
2. **Only then** swap the aggregation source, in the same change and behind the
   same before/after snapshot gate. Neither half is worth shipping alone.
3. `work_mem` is **2,184 kB** server-side, so the 385k-row sort always spills
   (`Sort Method: external merge  Disk: 13592kB`). A function-level
   `SET work_mem` was worth ~20%, and collapsing the two-level `agg` ->
   `pivoted` aggregation into one `GROUP BY (player_id, team_id)` with FILTER
   aggregates was worth ~15%. Both are separate changes with their own gates.

### Measurement — the thing that actually went wrong here

The first probe covered one preset class (broad season), found the view join,
and that was enough to write SQL. It was not enough to decide: the defect is
real, the fix was output-identical, and it still made the common case worse.
The handoff's own rule — probe before writing SQL — has to mean probe *every
preset class the app can produce*, not the first one that explains the headline
number.

Timing on this instance cannot arbitrate differences under ~2x. The same query
measured 0.62s and 2.98s in runs minutes apart, and one preset read 0.44s
before a change and 14.28s after it, purely from contention. **Use buffer
counts from `EXPLAIN (ANALYZE, BUFFERS)`** — they are deterministic — and treat
wall-clock as corroboration only.

### Two incidental findings

- `schedule.phase` for E/2025 holds `RS` (274), `PO` (15), `PI` (3). Migration
  004's header comment describes EuroLeague phases as `'REGULAR SEASON'`,
  `'PLAYOFFS'`. The comment is wrong; the data and the UI agree with each other.
- `player_four_factors_by_game` has no `competition` column, and `agg` puts no
  predicate on the fact at all. Of its 624,478 rows for season 2025, 385,140 are
  `E` and **239,338 are `U`** — the EuroCup rows are in every scan of this fact.
  `pf.game_year` is never distinct from `schedule.season` (0 of 624,478), so a
  redundant `c.game_year = p_game_year` predicate would be safe; it does not
  help on its own, since `game_year` alone does not separate the competitions.
