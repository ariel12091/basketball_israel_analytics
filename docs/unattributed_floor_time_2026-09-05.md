# Israeli lineup minutes lose ~0.59 min per team-game

Open defect, measured 2026-09-05, not fixed. Found while deciding whether to
change the Israeli end-of-game convention to match EuroLeague's — the answer was
no, and this is the reason the two numbers disagreed in the first place.

## The measurement

Summing one duration per segment straight from the base table gives the right
answer. The materialized view the app reads does not:

| Source | avg min / team-game | sd |
|---|---|---|
| `df_pts_poss_lineups_longer_mv`, `max(segment_seconds)` per segment, summed | **40.006** | 1.758 |
| `mv_lineup_totals_by_day`, `sum(minutes)` on offense rows | **39.421** | 1.759 |

Difference: **avg 0.586 min, max 2.633 min, affecting 804 of 878 team-games
(92%)**. The canonical clock is fine — 40.006 is essentially exact for a 40
minute game. The loss is in the aggregation.

## The mechanism, confirmed

`sql/materialized_views/sub_lineups_by_day.sql:135`:

```sql
SUM(wt.window_seconds) FILTER (WHERE wt.has_offense) / 60.0 AS minutes
```

`lineup_four_factors_by_game.sql:174` carries the identical line.

A window contributes its seconds **only if it contains an offensive possession**.
A lineup that is on the floor for a stretch without recording one contributes
zero minutes, even though the wall clock ran.

Confirmed against the data:

```
segments                    42,020
segments with no offense     2,531   (6.0%)
time inside them             466.1 min  =  0.531 min per team-game
```

0.531 of the measured 0.586 — **91% of the gap**. The residue is the finer
`opp_starters` window split, which subdivides segments further.

## Why the filter is there

`CLAUDE.md` records the intent: *"Floor time: compute `MAX - MIN` across ALL rows
per segment (no `type_lineup` filter), then SUM with offense filter to avoid
double-counting."* Every segment appears twice, once per perspective; summing
both would double the clock. Filtering to offense picks one copy.

The flaw is that it picks the copy by a property that is not always present.
`has_offense` is a proxy for "this is the offense-perspective row", and when a
window has no offensive possession at all the proxy selects nothing rather than
selecting one of two. Deduplicating on the perspective itself — a deterministic
pick per window rather than a filter on possession existence — would keep the
double-count protection without dropping real floor time.

## Impact

Low but systematic. A ~1.5% uniform undercount cancels in every within-league
comparison, and per-minute rates are only ever compared inside one league. It
matters for:

- **Any figure read as an absolute**, e.g. a player's minutes in Tabs 1/2/4/5/7.
- **Cross-league comparison.** EuroLeague computes minutes differently again
  (`euroleague.lineup_totals_by_game.seconds`, ending at the nominal period
  boundary rather than the last action), so Tab 4's Min and Tab 11's Min are
  already not comparable. See
  `docs/tab4_tab11_gamelog_parity_audit_2026-09-05.md` § L1-L4.

## What a fix would cost

`minutes` is persisted on `mv_lineup_totals_by_day`, `lineup_four_factors_by_game`,
`player_four_factors_by_game`, `onoff_default_mv`, `sub_lineups_stats` and
`player_traditional_stats_mv`. Changing the aggregation means a DROP+CREATE of
the affected MVs and an L1->L4 rebuild in dependency order, re-granting
everything the migration touches, plus re-baselining DQ checks T and X and the
minute-conservation assertions in `app/tests/testthat/test-clock-minute-contracts.R`.

Not attempted here. The end-of-game convention, which is what prompted the
investigation, accounts for only ~6 seconds of median tail and is NOT worth
changing on its own; this is the finding that actually explains the shortfall.
