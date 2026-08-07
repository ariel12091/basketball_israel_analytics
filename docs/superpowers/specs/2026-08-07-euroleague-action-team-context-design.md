# EuroLeague persisted event × team-perspective fact

Date: 2026-08-07
Status: approved, not implemented
Scope: `euroleague` schema only. No Israeli object is read or changed.

## Problem

`euroleague.refresh_player_four_factors_by_game_for_games()` rebuilds the
event × team-perspective expansion from `actions_raw` on every refresh: a regex
parse of `marker_time`, a running-maximum window for monotonic elapsed seconds,
a five-way event join, the offense/defense expansion, and six chained window
CTEs to derive joint segments and minutes. None of that varies between runs.

The Israeli pipeline does not do this. It reads
`df_pts_poss_lineups_longer_mv`, a persisted table that already carries
`type_lineup`, lineup identity, starter context and segment seconds, so its
four-factor function is a filtered `SUM`. This is item 7 of the table remarks
in `euroleague/CLAUDE.md`: *"any new metric means re-scanning `actions_raw` and
re-deriving perspective… it is the difference between one scan per load and one
scan per metric."*

Migration 007 (2026-08-07) fixed that function's query plan and took publication
from ~28-42s/game to ~5s. It did not close this gap; it made it stop hurting.
The gap still has to close before more metrics are added, because every new one
otherwise pays the derivation again.

## What the Israeli relation actually is

Worth stating precisely, because the names mislead.

- `df_pts_poss_lineups_longer_mv` is a **physical table**, not a materialized
  view, despite the `_mv` suffix. It is maintained per game by
  `refresh_df_pts_poss_lineups_longer_for_games(int4[])`.
- It is built as `pws UNION ALL pws-mirrored` — two rows per action, one per
  team perspective. "Longer" is the pivot-longer sense.
- The Israeli `pws` is a **wide action-level** table. EuroLeague's `pws` is a
  **narrow possession-endpoint bridge**, 1:1 with `possessions`. The two share
  a name and nothing else.

Measured on the live EuroLeague schema at 84 games:

| relation | rows | per game |
|---|---:|---:|
| `actions_raw` | 47,608 | 567 |
| `action_lineups` | 47,608 | 567 |
| `possessions` | 12,344 | 147 |
| `pws` | 12,344 | 147 |
| `stints` | 5,861 | 70 |
| `player_four_factors_by_game` | 182,868 | 2,177 |

`action_lineups` is exactly 1:1 with `actions_raw` (zero events without a
lineup row), and `stints` tile every event (zero events uncovered by a range
join). **The EuroLeague analogue of the Israeli `pws` is `action_lineups`, not
`pws`.** Lineups are already computed and attached per event; nothing about them
needs re-deriving. The expensive parts of the current chain are only the clock
and the joint segments.

## Decisions

| decision | choice | why |
|---|---|---|
| Grain | Long: one row per (action, perspective team) | Matches the Israeli model. Consumers filter and sum; no perspective `CASE` anywhere. The wide alternative forces every consumer to re-implement the perspective logic, which is exactly the duplication being removed. |
| Name | `euroleague.action_team_context` | `df_pts_poss_lineups_longer_mv` is a documented hazard in the parent repo (an `_mv` suffix on a physical table). This schema already uses the `_context` idiom in `player_game_context`. |
| `pws` | Subsumed and dropped | It is write-only today (PROJECT.md known issue). Everything it holds is a filtered read of the fact. |
| `type_lineup` | One value per row, single measure set | The current code emits both an offense and a defense row per (event, team) with one of them almost entirely zeros. Deriving the side from event type collapses that. |
| Rollout | Three migrations, verified between each | The fact's correctness, the consumer rewrite, and a table removal are three distinct risks. |

## The relation

`euroleague.action_team_context`, primary key
`(game_id, source_event_order, team_id)`. Approximately 1,134 rows per game.

| group | columns |
|---|---|
| keys | `game_id`, `source_event_order`, `team_id` (the perspective team), `opponent_team_id`, `period` |
| perspective | `type_lineup`, `own_lineup_id`, `opp_lineup_id`, `own_stint_id`, `opp_stint_id`, `own_starters`, `opp_starters` |
| event | `event_team_id`, `action_player_id`, `play_type`, `play_info`, `synthetic_ft_trip_id`, `parent_play_type`, `ft_reverse_order` |
| measures | `points`, `ts_possessions`, `orebounds`, `oreb_opportunities`, `turnovers`, `steals`, `ft_attempts`, `fga`, `fgm`, `fg2_made`, `fg2_att`, `fg3_made`, `fg3_att`, `layup_made`, `layup_att`, `dunk_made`, `dunk_att` |
| possession | `possession_flag`, `final_end_poss`, `endpoint_reason` |
| timing | `event_elapsed_seconds`, `segment_id`, `segment_seconds` |
| clutch | `own_team_score`, `opp_team_score` (cumulative through this event) |
| lineage | `load_run_id`, `derivation_version`, `derived_at` |

Constraints and indexes:

- `PRIMARY KEY (game_id, source_event_order, team_id)`
- `FOREIGN KEY (game_id, source_event_order)` → `actions_raw`, `ON DELETE CASCADE`
- `FOREIGN KEY (game_id, own_lineup_id)` and `(game_id, opp_lineup_id)` →
  `lineups (game_id, lineup_id)` — the same composite-key discipline as the rest
  of the schema, so a lineup can never be borrowed from another game
- `CHECK (type_lineup IS NULL OR type_lineup IN ('offense','defense'))`
- `CHECK (team_id <> opponent_team_id)`
- index on `(game_id, team_id, type_lineup)` for the aggregate path

`type_lineup` is nullable: the 35 coach/bench pseudo-actor rows have no side,
matching the Israeli `ELSE NULL`.

Three column meanings to pin down, since the names are not self-explanatory:

- `possession_flag` — 1 when this event is a possession endpoint **and** the
  possession belongs to this row's side, else 0. Summing it over a filtered
  scan is how any consumer counts possessions, replacing the join back to
  `possessions`. Derived from `possessions.endpoint_offense_team_id`, not from
  `event_team_id`; see Side assignment.
- `segment_id` — the **joint** segment: a number, dense within
  `(game_id, team_id)`, that increments whenever *either* the own lineup or the
  opponent lineup changes. It is finer than a team stint, which changes only
  when that team substitutes. Minutes and the `own_starters`/`opp_starters`
  partitioning both need the joint form, which is why the current code derives
  it with six chained window CTEs.
- `segment_seconds` — the duration of that joint segment, repeated on every
  event within it. Aggregate it with `MAX` per `segment_id` and then `SUM`,
  never `SUM` directly, or the segment is counted once per event. This is the
  same trap the Israeli project documents for floor time.

`play_info` is carried because the layup/dunk flags read it with `ILIKE`. That
derivation is separately questioned in `euroleague/CLAUDE.md` (free-text matching
where the Israeli side uses an enumerated tag). This spec preserves it unchanged;
correcting it is a different decision and must not be smuggled into an
output-identical refactor.

## Side assignment

The highest-risk part of the design, and the reason for the verification gates.

The side is a function of event type *relative to the perspective team*, not a
uniform "was this my team's event":

| event | for the acting team | for the opponent |
|---|---|---|
| shot, free throw, turnover, assist, foul drawn | offense | defense |
| steal, block, foul committed | defense | offense |
| rebound | side from the O/D label | mirrored |

The current code encodes exactly this, per metric, which is why it flips
`off_steals`/`def_steals` relative to every other pair.

Possession counting is different again: it keys off
`possessions.endpoint_offense_team_id`, not `event_team_id`. In every sequence
traced by hand this agrees with the type rule — a defensive rebound that ends a
possession lands on the missing team's offense from both directions — but
"agrees in the cases we traced" is not proof. It is settled by the 008 gate
below, not by argument.

## Refresh lifecycle

`euroleague.refresh_action_team_context_for_games(bigint[])`, the same
DELETE-by-game/INSERT shape as every other `refresh_*_for_games` function in this
schema and in the Israeli one.

The table is **derived**: the loader never inserts into it. It therefore joins
`player_four_factors_by_game` and `team_four_factors_by_game` in the
`assert_shadow_schema_compatible()` allowlist in `postgres_backend.py`, and
**not** `INSERT_ORDER`. That allowlist edit ships in migration 008; without it
publication refuses to start, by design.

Call order inside `PostgresTransactionBackend.validate_game()`, all within the
single per-game transaction so a game cannot commit without its analytics:

```
refresh_stint_timing
  → refresh_action_team_context
    → refresh_player_four_factors_by_game
    → refresh_team_four_factors_by_game
```

The `PERFORM refresh_stint_timing_for_games()` currently at the top of the player
four-factor function moves up to the fact's refresh, which is what needs stint
ids and segment seconds.

## Rollout

### 008 — create and backfill; nothing reads it

Creates the table, indexes and refresh function, wires the refresh into
`validate_game()`, extends the schema allowlist, and backfills all 84 loaded
games.

**Gate.** Prototype the replacement player four-factor query against the fact and
diff it against the stored rows: `EXCEPT ALL` in both directions across all
182,868 rows of `player_four_factors_by_game`, excluding `derived_at`. Zero rows
either way, or 008 does not land.

`derived_at` is excluded because it is meant to move on a re-derivation. Note
that comparing two fresh runs inside one transaction will not reveal a
`derived_at` difference, because `now()` is the transaction timestamp — compare
against stored rows.

### 009 — consumers read the fact

The verified prototype becomes the body of
`refresh_player_four_factors_by_game_for_games()`; the team-grain function is
rewritten the same way. Both should shrink to a filtered aggregate over the fact.

**Gate.** The same bidirectional `EXCEPT ALL` against stored rows, for both the
player and team grains, now as the permanent acceptance test.

### 010 — drop `pws`

First move its integrity property — every possession endpoint has a valid offense
and defense lineup and stint — into `game_qa` as an explicit gate, since today
that property is enforced only by four `NOT NULL` foreign keys on a table nothing
reads. Then remove the table and its entries in `INSERT_ORDER`, `DELETE_ORDER`,
`TABLE_COLUMNS`, staging, and the transaction-writer tests.

**Obstacle to resolve at 010, not before.** `apply_shadow_schema()` rejects any
DDL containing `DROP `. Migration 010 therefore cannot use the normal applier as
written. The options are a deliberate one-off path or narrowing the guard to
permit `DROP TABLE euroleague.<name>` while still refusing everything else.
Decide then; do not weaken the guard speculatively.

## Testing

- Add `action_team_context` to `PROJECTIONS` in
  `euroleague/scripts/probe_batched_publish.py`, so the rollback probe checks the
  fact the way it already checks lineups, stints and `pws`.
- Add a check to `scripts/load_games.py --verify-only` that every published game
  has fact rows, mirroring the existing "all games have team analytics".
- Python tests cover the allowlist change in `assert_shadow_schema_compatible`.
- After each migration: the rollback probe on gamecodes 1-3 and `--verify-only`
  on the full 84.

## Storage

Additive, on top of `player_four_factors_by_game` which stays: roughly 70-115 MB
for a full 402-game season against today's 199 MB for 84 games. Dropping `pws`
returns a little. Acceptable on the confirmed 5 GB headroom, but it is a real
cost and it is the price of not re-deriving.

## Non-goals

- **Correcting the layup/dunk `play_info` matching.** Carried forward unchanged.
  A metric change cannot ride inside an output-identical refactor.
- **Corner-3 and shot zones.** No shots endpoint has been collected; `c3_made`,
  `c3_att`, `c3_known_att` stay hardcoded 0.
- **Season-scoped lineup identity and the player identity layer.** Still
  PROJECT.md plan items 1 and 2, and still prerequisites for a second season.
  This fact keys on the existing per-game `lineup_id` and does not change lineup
  identity.
- **Any new app surface.** This makes clutch, lineup combos and game logs
  *buildable* by carrying the columns they need. It does not build them.
