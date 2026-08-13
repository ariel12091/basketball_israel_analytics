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

## 5. Next: Lineups (analysis done, nothing written)

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

1. **Tab 8 `onoff_compute`** — 11.88s vs Israeli 2.06s. Shape not yet examined.
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
5. **`PROJECT.md` has no entry for migration 037** — deliberately deferred.
   It documents 028-036 in detail and that chain is what made this session
   possible; 037 should be added under the 2026-08-13 handoff.

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
