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
| `player_four_factors_by_game` | 182,868 † | 2,177 † |

† Superseded. This was measured before the 2026-08-08 correction to 007 (see
"Correction to 007" under Rollout), which removed the never-occurred
combinations that made up 53.5% of the table. The corrected population is on the
order of 109,000 rows, roughly 1,300 per game. The figure is left here because
the problem statement above is about the *pre-correction* state; do not use it as
a target anywhere.

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
| timing | `event_elapsed_seconds`, `segment_id` (FK to `matchup_segments`) |
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

`type_lineup` is nullable, and the NULL population is much larger than a first
reading suggests. A side is assigned only to the enumerated basketball play
types in the Side assignment table below; everything else is NULL, because
substitutions, timeouts and administrative markers are not possession events and
have no offensive or defensive side to take.

Measured on EuroLeague gamecodes 1-3 (1,646 events):

| | events | share |
|---|---:|---|
| Inside the enumeration (gets a side) | 1,189 | 72.2% |
| **Outside the enumeration (NULL)** | **457** | **27.8%** |
| …of which have no `team_id` at all | 36 | |
| …of which **do** carry a `team_id` | **421** | |

`IN`/`OUT` substitutions alone are 374 of those events. An earlier draft of this
spec said "the 35 coach/bench pseudo-actor rows have no side", which counted only
`team_id IS NULL` and understated the NULL population by more than an order of
magnitude. That error is not cosmetic: it led an implementer to read the correct
NULLs as a defect and broaden the side rule to cover substitutions and timeouts,
which had to be reverted.

Carrying a `team_id` does not make an event a possession event. The rule is the
enumeration, not team attribution.

Three column meanings to pin down, since the names are not self-explanatory:

- `possession_flag` — 1 when this event is a possession endpoint **and** the
  possession belongs to this row's side, else 0. Summing it over a filtered
  scan is how any consumer counts possessions, replacing the join back to
  `possessions`. Derived from `possessions.offense_team_id`, not from the
  endpoint event's `team_id`; see Side assignment for why, given that the two
  never disagree on current data.
- `segment_id` — foreign key to `matchup_segments` (below). The **joint**
  segment: it changes whenever *either* the own lineup or the opponent lineup
  changes, so it is finer than a team stint, which changes only when that team
  substitutes. Minutes and the `own_starters`/`opp_starters` partitioning both
  need the joint form, which is why the current code derives it with six
  chained window CTEs.

Note that segment **duration** is deliberately *not* on this table. See below.

## `euroleague.matchup_segments`

The joint segment is an entity, and its duration is an attribute of that
entity, not of each event inside it. It therefore gets its own relation, keyed
`(game_id, team_id, segment_id)`:

| column | meaning |
|---|---|
| `game_id`, `team_id`, `segment_id` | primary key; `segment_id` dense within `(game_id, team_id)` |
| `own_lineup_id`, `opp_lineup_id` | the pair that defines the segment |
| `own_starters`, `opp_starters` | starter counts for the partition |
| `start_event_order` | first event in the segment |
| `start_elapsed_seconds`, `end_elapsed_seconds` | boundaries |
| `segment_seconds` | the duration, **once** |

Measured on the 84 loaded games: 11,554 rows, 137.6 per game, ~55k for a full
season — against ~456k for the fact. The cost is negligible.

### Why this deviates from the Israeli design

The Israeli pipeline stores `segment_seconds` denormalised onto every event row
of the backbone, and needs three separate mechanisms to keep that safe:

1. the backbone CTAS writes `NULL::numeric AS segment_seconds` and a separate
   ETL pass (`etl/apply_canonical_segment_minutes_online.R`) fills the value
   back onto every row of the segment;
2. every consumer must reduce before summing — `MAX(segment_seconds)` grouped
   by `segment_id`, then `SUM`. That convention appears four times across
   `sql/materialized_views/player_four_factors_by_game.sql` (lines 86, 185) and
   `sql/functions/refresh_player_four_factors_by_game_for_games.sql` (lines
   105, 204). Summing directly multiplies each segment by its event count;
3. a standing validator asserts the copies have not drifted apart —
   `count(DISTINCT segment_seconds) AS duration_values` per segment, with
   `duration_values <> 1` reported as an invalid segment.

The third is the tell: a data-quality check whose entire purpose is to confirm
that repeated copies of one number still agree. On a segment-grained relation
all three disappear. `duration_values <> 1` becomes unrepresentable, the `MAX`
convention is unnecessary, and minutes is a join and a `SUM`.

This is a deliberate improvement over the Israeli original, in the same
category as the composite same-game foreign keys and half-open stint intervals
already listed in `euroleague/CLAUDE.md`. It is not a semantic change: the
durations are identical, only their storage grain differs.

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

Possession counting keys off `possessions.offense_team_id`, not the endpoint
event's `team_id`. On current data the two never disagree — checked across all
12,344 endpoints in the 84 loaded games, zero divergence:

| endpoint_reason | endpoints | event by possessing team | endpoint play type |
|---|---:|---:|---|
| `made_field_goal` | 4,678 | 4,678 | 2FGM/3FGM |
| `miss_defensive_rebound` | 3,402 | 3,402 | 2FGA/3FGA |
| `turnover` | 2,138 | 2,138 | TO |
| `ordinary_ft_trip_final_make` | 1,163 | 1,163 | FTM |
| `final_ft_miss_defensive_rebound` | 307 | 307 | FTA |
| `blocked_shot_defensive_rebound` | 255 | 255 | 2FGA/3FGA |
| remaining seven reasons | 401 | 401 | |
| **total** | **12,344** | **12,344** | |

The reason is not obvious and is worth recording: for
`miss_defensive_rebound`, `blocked_shot_defensive_rebound` and
`final_ft_miss_defensive_rebound`, the endpoint is anchored on the **missed
attempt**, which the possessing team took — not on the defensive rebound. So
the endpoint event is always performed by the team in possession.

Use `offense_team_id` regardless:

1. It is the authoritative record of whose possession it was, produced by the
   possession state machine. The endpoint event's `team_id` is the acting team,
   a different concept that currently coincides.
2. The coincidence is an emergent property of the endpoint-anchoring rule, not
   a declared invariant, and nothing constrains it. A parser change that
   anchored an endpoint on a defensive event would silently attribute every
   affected possession to the wrong team.
3. `actions_raw.team_id` is nullable — the coach/bench pseudo-actors — while
   `offense_team_id` carries a foreign key to `teams`.

## Refresh lifecycle

`euroleague.refresh_action_team_context_for_games(bigint[])`, the same
DELETE-by-game/INSERT shape as every other `refresh_*_for_games` function in this
schema and in the Israeli one.

One function refreshes both relations, `matchup_segments` first, since the fact
carries a foreign key to it. Both are **derived**: the loader never inserts
into either, so both join `player_four_factors_by_game` and
`team_four_factors_by_game` in the `assert_shadow_schema_compatible()`
allowlist in `postgres_backend.py`, and **neither** goes in `INSERT_ORDER`.
That allowlist edit ships in migration 008; without it publication refuses to
start, by design.

Call order inside `PostgresTransactionBackend.validate_game()`, all within the
single per-game transaction so a game cannot commit without its analytics:

```
refresh_stint_timing
  → refresh_action_team_context
      writes matchup_segments, then action_team_context
    → refresh_player_four_factors_by_game
    → refresh_team_four_factors_by_game
```

The `PERFORM refresh_stint_timing_for_games()` currently at the top of the player
four-factor function moves up to the fact's refresh, which is what needs stint
ids and segment boundaries.

## Rollout

### 008 — create and backfill; nothing reads it

Creates the table, indexes and refresh function, wires the refresh into
`validate_game()`, extends the schema allowlist, and backfills all 84 loaded
games.

**Gate.** Prototype the replacement player four-factor query against the fact and
diff it against the stored rows: `EXCEPT ALL` in both directions across every row
of `player_four_factors_by_game`, excluding `derived_at`. Zero rows either way,
or 008 does not land.

The gate is the zero-diff, not a row count. An earlier draft named 182,868 rows;
that figure was invalidated on 2026-08-08 when migration 007's refresh was
corrected to stop generating rows for `(player, is_on_key, own_starters,
opp_starters)` combinations that never occurred — see "Correction to 007" below.
A stored row count legitimately moves whenever the grain is corrected, so
asserting one would turn a valid improvement into a spurious gate failure.

`derived_at` is excluded because it is meant to move on a re-derivation. Note
that comparing two fresh runs inside one transaction will not reveal a
`derived_at` difference, because `now()` is the transaction timestamp — compare
against stored rows.

#### Correction to 007 (2026-08-08)

Building the gate exposed a defect in the reference it diffs against.
`refresh_player_four_factors_by_game_for_games()` built its output population
with an unconditional cross join — roster players × every starter-count bucket
the team saw that game × `is_on_key {0,1}` × `type_lineup {offense,defense}` —
then LEFT JOINed the real counts. Combinations that never occurred survived as
rows zero on every measure. The stored population was exactly
`players × buckets × 4` for every team-game, and 53.5% of it was inert.

This had no Israeli counterpart to justify it. The Israeli materialized view is
observation-driven: `SELECT DISTINCT` player↔lineup associations INNER JOINed to
real lineup totals. Starter-count buckets are a consequence of which lineups
actually faced each other, not a dimension to densify.

The grid was restricted to genuinely observed combinations. Every season-level
rate came out byte-identical before and after — the removed rows contributed
zero to both numerator and denominator — so no displayed figure changed, and the
table is roughly 37% smaller. Migration 009's rewrite inherits the corrected
population, not the inflated one.

An open question deferred to 009: `is_on_key` and the starter-bucket
pre-aggregate are shapes the Israeli pipeline needs because it reconstructs
lineups from substitution events. EuroLeague lineups are first-class
(`lineups`, `lineup_players`) and every fact row already carries
`own_lineup_id`/`opp_lineup_id`, so `is_on_key` is a plain membership test.
Whether `player_four_factors_by_game` needs to exist as a persisted per-player
table at all — rather than being computed from the fact at query time — should
be decided in 009 rather than inherited by default.

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
for a full 402-game season against today's 199 MB for 84 games. `matchup_segments`
adds ~55k rows for a full season, which is noise beside the fact's ~456k.
Dropping `pws` returns a little. Acceptable on the confirmed 5 GB headroom, but
it is a real cost and it is the price of not re-deriving.

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
