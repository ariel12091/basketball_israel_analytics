# EuroLeague 2-5 player lineup units — design

Date: 2026-08-10
Status: design approved, not implemented
Scope: migrations 013 and 014 in the `euroleague` schema, the publication wiring
in `src/euroleague_possessions/postgres_backend.py`, and a new league-scoped
Shiny lineups tab.

## Goal

Give EuroLeague the 2-, 3-, 4-, and 5-player lineup-unit statistics the Israeli
app exposes in its Tab 2, by mimicking the *behaviour* of the Israeli
`sub_lineups` / `sub_lineups_stats` pair rather than its physical schema.

The deliverable runs from the database fact through to a working tab in the
deployed Shiny app, carrying the full EuroLeague filter set already established
by tabs 8 and 9. Clutch filtering is the one filter deliberately held back to a
later phase of this same work, because it needs a third query path against raw
events.

## What "mimic `sub_lineups`" means, and why it differs from `PROJECT.md`

`euroleague/PROJECT.md`'s "Next deliverable" section proposes a single per-game
fact at *unit* grain — `lineup_unit_game_stats`, keyed on
`(game_id, team_id, unit_size, unit_key, type_lineup, own_starters,
opp_starters)`. That is not how the Israeli side works, and it is the source of
that section's own "measure the fan-out before writing any DDL" warning.

The Israeli design keeps the fan-out out of the facts entirely:

- `mv_lineup_totals_by_day` is a **fact at five-player-lineup grain**, per game.
- `sub_lineups` is a **mapping only** — `sub_lineup_hash → lineup_hash`, scoped
  to `(team_id, game_year)`, carrying no metrics.
- `sub_lineups_stats` is a **cached season roll-up** of the join between them,
  used only for the unfiltered full-season case.

The consequence is that the 26× expansion multiplies *distinct lineups per
season*, not *team-game-segments*. This design follows that shape.

`PROJECT.md`'s "Next deliverable" section is superseded by this document and
must be rewritten as part of the rollout.

## Decisions

| Decision | Choice | Rejected alternative |
|---|---|---|
| Architecture | Israeli shape: lineup-grain fact + season mapping + season roll-up | Unit-grain per-game fact (`PROJECT.md`) |
| Deliverable depth | Data layer, read path, **and** Shiny tab | Stopping at the database |
| Count set | Summary **and** four-factor numerators in one fact | Israel's two-relation split, which exists only because its FF work came later |
| `opp_starters` | In the base fact's key | Dropping it, which would foreclose starters-context filtering |
| `own_starters` | Attribute, not key — determined by `lineup_key` | — |
| Season roll-up | Indexed MV inside the existing fail-closed refresh | Physical table with incremental refresh (Israel's shape, and its known stale-scope failure mode) |
| `game_id` in base key | Kept | Season grain only, which would force every filtered request back to raw events |
| Lineup grouping key | `own_lineup text[]` read from the consumer facts | Re-deriving home/away → own/opp from `actions.lineup_a`/`lineup_b` |
| Lineup identity key | `md5` over sorted internal `player_id`s | Hashing provider names or provider IDs |
| Stored ratios | None | Israel's stored `off_ppp`/`def_ppp`, which `AGENTS.md` forbids |
| Clutch | Phase 3 of this work | Shipping it with the tab, or dropping it entirely |

## Phases

| Phase | Contents | Gate to the next phase |
|---|---|---|
| 1 | Migration 013, publication wiring, backfill, validation gates G1-G4 and G7-G9 | Those gates pass over all 84 loaded games |
| 2 | Migration 014 (season MV + filtered function), Shiny tab, full filter set except clutch | Tab renders correct numbers against the verified fact |
| 3 | Clutch filtering — a third query path against `action_team_context_actions` | — |

## Source relations

Everything is derived from relations that already exist after migration 012.
No new provider data, no parser change, no restoration of a removed middle
table.

- `action_team_context_actions` — two rows per canonical event, one per team
  perspective. Carries `own_lineup`, `opp_starters`, `own_starters`,
  `type_lineup`, `segment_id`, running scores, and every additive count this
  design needs.
- `matchup_segments_actions` — one consecutive joint-lineup interval per
  `(game_id, team_id, segment_id)`, with `own_lineup`, `opp_starters`, and
  `segment_seconds`.
- `full_rosters` + `players` — provider `source_player_name` → internal
  `player_id`, excluding the `Team`/`Total` pseudo-actors.
- `schedule` / `final_schedule_mv` — `game_year = schedule.season` (the PROVIDER
  season; 2025 is the 2025-26 season), `competition`, `round_number`, `phase`.

Three properties of `action_team_context_actions` that this design depends on,
verified in `sql/011_actions_consumer_candidates.sql`:

1. Each event's metric values (`points`, `possession_flag`, `fg2_made`, …) are
   **mirrored identically onto both team perspectives**. A team's offensive
   points are therefore `SUM(points) FILTER (WHERE type_lineup = 'offense')`,
   and points allowed are the same sum filtered to `'defense'`. This matches the
   Israeli `team_score` convention.
2. `type_lineup` is **NULL** for events belonging to neither side —
   substitutions, timeouts, and other non-basketball rows. Those rows contribute
   to no context and are excluded.
3. `own_lineup` is already team-perspectived and already sorted, by
   `ARRAY(SELECT x FROM unnest(...) x ORDER BY x)`. It is a sorted array of
   provider *names*, not internal IDs.

## Phase 1 — the fact and the mapping

### `euroleague.lineup_totals_by_game` (migration 013)

Physical table. The analogue of the Israeli `mv_lineup_totals_by_day`.

```sql
CREATE TABLE euroleague.lineup_totals_by_game (
  game_id            bigint   NOT NULL REFERENCES euroleague.schedule(game_id)
                                ON DELETE CASCADE,
  team_id            bigint   NOT NULL REFERENCES euroleague.teams(team_id),
  lineup_key         text     NOT NULL,
  type_lineup        text     NOT NULL CHECK (type_lineup IN ('offense','defense')),
  opp_starters       smallint NOT NULL CHECK (opp_starters BETWEEN 0 AND 5),

  competition        text     NOT NULL,
  game_year          integer  NOT NULL,
  own_starters       smallint NOT NULL CHECK (own_starters BETWEEN 0 AND 5),
  own_lineup         text[]   NOT NULL CHECK (cardinality(own_lineup) = 5),
  player_ids         bigint[] NOT NULL CHECK (cardinality(player_ids) = 5),

  possessions        integer  NOT NULL DEFAULT 0,
  points             integer  NOT NULL DEFAULT 0,
  fg2_made           integer  NOT NULL DEFAULT 0,
  fg2_att            integer  NOT NULL DEFAULT 0,
  fg3_made           integer  NOT NULL DEFAULT 0,
  fg3_att            integer  NOT NULL DEFAULT 0,
  ts_possessions     integer  NOT NULL DEFAULT 0,
  fgm                integer  NOT NULL DEFAULT 0,
  fga                integer  NOT NULL DEFAULT 0,
  ft_attempts        integer  NOT NULL DEFAULT 0,
  orebounds          integer  NOT NULL DEFAULT 0,
  oreb_opportunities integer  NOT NULL DEFAULT 0,
  turnovers          integer  NOT NULL DEFAULT 0,
  steals             integer  NOT NULL DEFAULT 0,
  seconds            numeric  CHECK (seconds IS NULL OR seconds >= 0),

  load_run_id        bigint REFERENCES euroleague.load_runs(load_run_id),
  derivation_version text NOT NULL,
  derived_at         timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (game_id, team_id, lineup_key, type_lineup, opp_starters),
  CHECK ((type_lineup = 'offense') = (seconds IS NOT NULL))
);
```

`seconds` is populated on offense rows only and is NULL on defense rows, so a
naive `SUM` across both contexts cannot double-count floor time. The final
`CHECK` makes that a schema guarantee rather than a convention.

Index for the app's real access path, and nothing speculative:

```sql
CREATE INDEX euroleague_lineup_totals_by_game_season_idx
  ON euroleague.lineup_totals_by_game
     (competition, game_year, team_id, lineup_key, type_lineup);
```

### `euroleague.sub_lineups` (migration 013)

Physical table. Identity and mapping only — no metrics, exactly as on the
Israeli side. A unit maps to *many* lineups; that is the point of the relation.

```sql
CREATE TABLE euroleague.sub_lineups (
  competition  text     NOT NULL,
  game_year    integer  NOT NULL,
  team_id      bigint   NOT NULL REFERENCES euroleague.teams(team_id),
  lineup_key   text     NOT NULL,
  unit_key     text     NOT NULL,
  unit_size    smallint NOT NULL CHECK (unit_size BETWEEN 2 AND 5),
  player_ids   bigint[] NOT NULL CHECK (cardinality(player_ids) = unit_size),
  created_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (competition, game_year, team_id, lineup_key, unit_key)
);

CREATE INDEX euroleague_sub_lineups_unit_idx
  ON euroleague.sub_lineups
     (competition, game_year, team_id, unit_key, unit_size);
```

**Grain difference from the Israeli relation of the same name**, per the
`AGENTS.md` rule on reusing familiar names: the Israeli `sub_lineups` holds
sizes 2-4 only and synthesizes size 5 from the full `lineup_hash` elsewhere.
This one holds sizes 2-5 uniformly, and because `unit_key` and `lineup_key` use
the same `md5` construction, `unit_key = lineup_key` at size 5 automatically.

### Identity resolution, off the hot path

The largest cost in a naive implementation is resolving five provider names per
event row. That is avoided: the two aggregations group on `own_lineup`
directly, which is already present and already sorted on every source row, and
name resolution runs once per *distinct lineup per game* — roughly 9,000 rows
across the 84 loaded games.

```
identity := SELECT DISTINCT game_id, team_id, own_lineup
              FROM matchup_segments_actions
            JOIN real_roster ON source_player_name = ANY(own_lineup)
            -> player_ids = sorted array of internal player_id
            -> lineup_key = md5(array_to_string(player_ids, '_'))
```

`real_roster` is the CTE already verified in
`sql/012_actions_consumer_cutover.sql`, including its `lower(...) NOT IN
('team','total')` pseudo-actor exclusion. The unit refresh must reuse it, not
write a second resolution.

**Grouping key and identity key are deliberately different objects.** The name
array is a valid *grouping* key within a game, because `full_rosters` gives each
player exactly one `source_player_name` per game, making name-array ↔ id-array
1:1 there. It is not a valid *identity* key across games or seasons, because
provider names and IDs re-mint. So: group by names, key by resolved internal
IDs. Gates G1 and G2 police the seam between the two.

### Building `lineup_totals_by_game`

Two aggregations plus the identity join:

```
counts  := action_team_context_actions
           WHERE type_lineup IS NOT NULL
           GROUP BY game_id, team_id, own_lineup, type_lineup, opp_starters
           -> SUM(possession_flag) AS possessions,
              SUM(points), SUM(fg2_made), ... SUM(steals)

seconds := matchup_segments_actions
           GROUP BY game_id, team_id, own_lineup, opp_starters
           -> SUM(segment_seconds)

rows    := seconds CROSS JOIN (VALUES ('offense'),('defense')) AS side
           LEFT JOIN counts USING (game_id, team_id, own_lineup,
                                   opp_starters, type_lineup)
           JOIN identity USING (game_id, team_id, own_lineup)
           -> seconds attached on the offense side only
```

The row set is driven from **segments**, not from events, with the event counts
left-joined on. This is a deliberate improvement over the Israeli original,
which builds from its event fact and therefore silently loses a segment's
minutes if every event in it has a NULL context. Here a lineup that was on court
but recorded no offensive or defensive event still gets its row and its seconds,
with zero counts.

`own_starters` is carried through as an attribute. It is functionally determined
by `(game_id, team_id, lineup_key)` and so is not part of the key.

### Building `sub_lineups`

The expansion is a deterministic index expansion over the five sorted resolved
IDs, driven by a static 26-row `VALUES` list of index masks — 10 pairs, 10
triples, 5 quads, 1 quintet. It is never a cross join against a roster.

```
masks(unit_size, idxs) := VALUES
  (2, ARRAY[1,2]), (2, ARRAY[1,3]), ... (5, ARRAY[1,2,3,4,5])   -- 26 rows

INSERT INTO euroleague.sub_lineups
SELECT DISTINCT
    l.competition, l.game_year, l.team_id, l.lineup_key,
    md5(array_to_string(u.ids, '_')) AS unit_key,
    m.unit_size, u.ids
  FROM euroleague.lineup_totals_by_game l
  CROSS JOIN masks m
  CROSS JOIN LATERAL (
    SELECT ARRAY(SELECT l.player_ids[i] FROM unnest(m.idxs) i ORDER BY 1) AS ids
  ) u
ON CONFLICT DO NOTHING;
```

`player_ids` is already ascending and `idxs` is ascending, so the subset is
produced in sorted order and `unit_key` is order-independent by construction.
`{A,B}` and `{B,A}` cannot both exist.

Because the source is `lineup_totals_by_game`, this refresh performs **no name
resolution at all** — the resolution happened once, upstream.

### Refresh lifecycle

| Function | When | Notes |
|---|---|---|
| `euroleague.refresh_lineup_totals_by_game(bigint[])` | Per game, inside `PostgresBackend.validate_game()` | Immediately after `refresh_actions_consumer_candidates()`, before the mapping |
| `euroleague.refresh_sub_lineups(bigint[])` | Per game, immediately after the above | Additive; `ON CONFLICT DO NOTHING` |
| `REFRESH MATERIALIZED VIEW euroleague.sub_lineups_stats_mv` | Per batch, inside `refresh_app_materialized_views()` | Phase 2. Already fail-closed: a load cannot be marked `completed` if the refresh fails |

`refresh_lineup_totals_by_game` deletes the target games' rows before
reinserting, so a republished game replaces rather than accumulates.
`sub_lineups` rows are additive identity and are not deleted per game — a lineup
observed in an earlier game of the same season stays mapped.

Two changes outside SQL, both shipping in the same commit as migration 013:

1. `_assert_shadow_schema_compatible()` in
   `src/euroleague_possessions/postgres_backend.py` — add both new tables to the
   expected table set. **Publication refuses to run against the new schema until
   this lands.**
2. The `validate_game()` call sites above.

### Validation gates

This architecture makes three of `PROJECT.md`'s six proposed gates tautologies.
Units are *generated from* observed lineups, so containment cannot fail; the
primary key makes duplicate units impossible; and `unit_key = lineup_key` at
size 5 by construction makes the five-player identity check self-proving.
Asserting those would repeat migration 009's mistake of checking what the schema
already guarantees. They are replaced by checks that can fail.

All gates run over all 84 loaded games.

| # | Check | Failure it catches |
|---|---|---|
| G1 | Every distinct `(game_id, team_id, own_lineup)` resolves to exactly 5 internal `player_id`s | A lineup name missing from `full_rosters` |
| G2 | Within a `(game_id, team_id)`, distinct `own_lineup` arrays map to distinct `lineup_key` | Two lineups collapsing to one key and double-counting |
| G3 | Per `(game, team, type_lineup)`, the `SUM` over all lineup rows equals that team's row in `team_four_factors_by_game` | Any event lost, duplicated, or misattributed — checked against an already-verified relation |
| G4 | Per `(game, team)`, `SUM(seconds)` over offense rows equals `SUM(matchup_segments_actions.segment_seconds)` and equals canonical game length (`2400 + 300 × OT`) | Minutes double-counted across contexts, or lost on a segment with no scored events — the Israeli `T`-class defect |
| G5 | `sub_lineups_stats_mv`, recomputed by a **name-membership** path (ATC filtered to rows whose `own_lineup` contains all of the unit's `source_player_name`s), equals the **key-mapping** path the MV uses | The mapping join itself |
| G6 | Summing all `unit_size = 5` rows per team-season equals that team's `team_ppp_ratings_mv` season totals | Season roll-up and MV refresh drift |
| G7 | A unit's possessions are ≥ those of any larger unit containing it, and ≤ its team's | A wrong index mask in the 26-row expansion |
| G8 | Exactly 26 mapping rows per `lineup_key`, split 10/10/5/1 by `unit_size` | A mask typo |
| G9 | Refreshing a game twice produces byte-identical rows, excluding `derived_at` | Non-determinism in a refresh |

G3 and G5 are load-bearing. G5 is the only gate that exercises the mapping
through a different code path than the one under test; it must be written from
the source semantics, not by copying the MV's own SQL. G5 and G6 depend on
phase 2's MV and run when it lands; G1-G4 and G7-G9 gate phase 1.

## Phase 2 — read path and Shiny tab

### `euroleague.sub_lineups_stats_mv` (migration 014)

Materialized view. The season roll-up and the app's default fast path.

Grain: `(competition, game_year, team_id, unit_key)`, with a unique index on
exactly that so the relation is joinable and uniquely keyed.

The refresh is **not** `CONCURRENTLY`. `refresh_app_materialized_views()` runs
inside the publication transaction so that a load cannot be marked `completed`
with a stale snapshot, and `REFRESH ... CONCURRENTLY` cannot run in a
transaction block. Fail-closed publication and concurrent refresh are mutually
exclusive; this project has already chosen fail-closed for its existing MVs, and
this one follows.

Columns: `unit_size`, `player_ids`, `player_names text[]`,
`player_names_str text`, then the off/def split of every additive count in
`lineup_totals_by_game`, plus `minutes`.

No `off_ppp`, no `def_ppp`, no ranks. `AGENTS.md` requires additive counts and
seconds only; rates are derived after aggregation. This is a deliberate
deviation from the Israeli `sub_lineups_stats`, which stores rounded PPP.

Display names come from `players.display_name`, with a `'#' || player_id`
fallback matching the Israeli convention for a missing roster name.

### `euroleague.fetch_lineups_dynamic(...)` (migration 014)

The filtered path: `sub_lineups` → `lineup_totals_by_game` →
`final_schedule_mv`.

The parameter list mirrors the four existing dynamic functions
(`onoff_compute`, `four_factors_compute`, `get_team_ratings_dynamic`,
`get_team_four_factors_dynamic`) so the app's filter plumbing carries over
unchanged:

```
p_competition, p_game_year,
p_start_date, p_end_date,
p_team_ids_csv, p_phase_csv, p_opp_ids_csv,
p_home_away, p_outcome,
p_opp_rank_side, p_opp_rank_n, p_opp_rank_metric,
p_min_gn, p_max_gn, p_last_n_games,
p_num_starters_off_min, p_num_starters_off_max,
p_num_starters_def_min, p_num_starters_def_max,
```

plus four parameters specific to this surface:

```
p_unit_size            -- 2, 3, 4 or 5
p_players_on_csv       -- units must contain all of these player_ids
p_players_off_csv      -- units must contain none of these player_ids
p_min_poss             -- final filter on off_poss + def_poss
```

`p_players_on_csv` / `p_players_off_csv` apply against `sub_lineups.player_ids`,
which is why that array is stored alongside the hash rather than only the hash:
"these two players together" reads the array.

Returns one row per `(team_id, unit_key)` at the requested size, with
`player_ids`, `player_names`, off/def possessions and points, minutes, starter
context, the 2PT/3PT splits, and the four-factor numerators. Rates and ranks are
computed by the app after aggregation, never stored.

Conventions inherited from the existing functions:

- Last-N-games filtering uses the `schedule_ranked` windowed-CTE pattern, never
  a correlated per-row subquery.
- `SET plan_cache_mode = force_custom_plan`, as every heavy function in this
  project carries.
- The fast-path gate accepts an explicit full-season window as well as null
  dates, since the app always sends dates.

### Grants

`GRANT SELECT` on both new tables and the MV, and `GRANT EXECUTE` on the new
function, to `app_readonly`. A `DROP`+`CREATE` on any existing object in these
migrations wipes its grants, so re-grant everything each migration touches, not
only what it creates. Re-run `scripts/apply_db_security.R` with
`CONFIRM_DB_SECURITY_APPLY=1` after migration 014.

### Shiny tab

A new league-scoped tab alongside tabs 8 and 9, following their established
structure: `app/R/ui_tab10_euro_lineups.R` and
`app/R/server_tab10_euro_lineups.R`, sourced in `app.R`, reading the euro
competition/season selectors and reference lookups from `app/R/global_euro.R`.

**Filters.** The complete EuroLeague filter vocabulary already implemented on
tab 8, under `euro_ld_*` input IDs:

| Group | Controls |
|---|---|
| Scope | competition + season (existing global euro navbar selectors) |
| Schedule | `euro_ld_date_range`, `euro_ld_gn_min`/`euro_ld_gn_max` (round number), `euro_ld_last_n`, `euro_ld_phase` |
| Matchup | `euro_ld_teams`, `euro_ld_opponents`, `euro_ld_home_away`, `euro_ld_outcome`, `euro_ld_opp_rank_side`/`_n`/`_metric` |
| Starters | `euro_ld_num_starters_off`/`_def` plus their `_mode` selectors |
| Lineup | `euro_ld_group_size` (2/3/4/5), team + players-on / players-off, `euro_ld_minposs` |
| Display | `euro_ld_view_mode` (Summary / Four Factors), `euro_ld_filter_chips`, `euro_ld_reset` |

Clutch controls are **not** built in phase 2. They arrive in phase 3 with the
query path that backs them; a disabled control that silently does nothing is
worse than an absent one.

**Behaviours carried over from Israeli Tab 2:**

- **Summary / Four Factors toggle.** Summary shows off/def possessions, points,
  PPP, net, minutes, and the 2PT/3PT splits. Four Factors shows TS%, TOV%,
  OREB%, FTR on both sides, derived from the stored numerators after
  aggregation.
- **Auto minimum possessions.** A row-count target cap computed server-side on
  the team/player-filtered population *before* the min-poss filter, mirroring
  Israel's `auto_minposs_target_r()`. Manual slider use switches to manual; a
  filter change returns to auto.
- **Server-side ranking on the full population.** The main fetch requests
  `p_min_poss = 0` so percentile ranks are computed over the complete comparison
  set; the displayed threshold is applied afterwards.
- **TOTAL row** pinned at top: raw counts summed, rates derived from the sums,
  rank fields null, not clickable.
- **Lineup click → game log modal.** Resolves the clicked `unit_key` through
  `sub_lineups` to its constituent lineups, then reads
  `lineup_totals_by_game` joined to `final_schedule_mv` for the per-game rows.
  This is the payoff for keeping `game_id` in the base fact's key, and it costs
  no new relation.

**Isolation requirements**, per the standing rules for this integration:

- The tab owns its own reference lookups (`euro_fetch_teams`,
  `euro_fetch_round_values`, `euro_fetch_phases`, and a new
  `euro_fetch_players_basic` for the players-on/off pool) through
  `cached_ref_query()` keys that include competition and season. Never reuse an
  Israeli lookup's cache key.
- Cross-session caches key on competition + season + `euro_data_version()`.
- No view, query, or ranked table mixes the two leagues.
- Inherit the existing guardrails: `guard_heavy_request()`, statement timeout,
  idle-session restore, filter chips, loading skeletons.
- `app/app.R` has mixed line endings. Check `git diff --stat` after editing it
  and fix by re-applying on bytes if the diff is implausibly large.
- The view-mode selector is the navbar hover menu built from a hardcoded `CFG`
  array in `app/www/app.js`. Any new or renamed view mode must update that file
  too, not only the server-side radio.

## Phase 3 — clutch

Held back deliberately. Clutch cannot read the pre-aggregated fact, because the
margin test is per-event: it needs the pre-shot margin, derived from
`action_team_context_actions.own_team_score` / `opp_team_score` minus the current
event's points, at the moment of each event. That is a third query path,
structurally similar to Israel's clutch path, which filters the central action
fact and rebuilds segment stats for the selected subset.

Its own design pass covers: the four clutch parameters
(`p_max_margin`, `p_margin_status`, `p_max_time_remaining`,
`p_ot_margin_filter`), minutes attribution when a clutch window covers only part
of a segment, and whether the OT bypass convention should match Israel's.

The period constants themselves are not the difficulty: migration 011 already
establishes 4 × 600 seconds of regulation plus 300 per overtime, the same
arithmetic the Israeli side uses. The difficulty is that clutch membership is a
per-event test, so it can only be answered below the level at which this fact
aggregates.

## Sizing estimates

Derived from the recorded 84-game counts in `euroleague/PROJECT.md` (11,554 team
matchup segments, 95,216 event/team-perspective rows, 254 MB for 84 games).
**These are estimates, not measurements** — the sizing query in the rollout
confirms them.

| Relation | 84 games | ~400-game season | Bound |
|---|---|---|---|
| `lineup_totals_by_game` | ≤ 23k rows | ~110k rows | `segments × 2` contexts, before de-duplication |
| `sub_lineups` | ~65k narrow rows | ~230k narrow rows | `distinct lineups × 26` |
| `sub_lineups_stats_mv` | — | ≤ ~99k rows/season | Roster combinatorics: `20 teams × 4,928` units per 15-man roster |

All three are small against the schema's current footprint. The decisive
difference from the superseded unit-grain design is that no relation multiplies
team-game-segments by 26.

## Rollout

Every database-touching step needs explicit user approval before it runs, per
`euroleague/AGENTS.md`. Nothing here loads new games. Nothing here touches
`basketball` or `basketball_test`.

**Phase 1**

1. Sizing query, read-only. Confirm the row-count estimates. No longer
   load-bearing now that nothing fans out at game grain, but cheap.
2. Migration 013 — both tables, both refresh functions, indexes, grants.
3. `postgres_backend.py` — expected table set and the two `validate_game()` call
   sites, in the same commit as 013.
4. Backfill all 84 games, then run G1-G4 and G7-G9 over the full set.

**Phase 2**

5. Migration 014 — MV, unique index, `fetch_lineups_dynamic(...)`, registration
   in `refresh_app_materialized_views()`, grants. Then re-run
   `scripts/apply_db_security.R`.
6. Run G5 and G6.
7. Shiny tab: UI, server, `global_euro.R` lookup, `app.R` wiring, `app.js` view
   mode entry.
8. Verify the tab against the fact — pick several units and reconcile the
   displayed numbers against a direct query, including at least one where the
   filtered path and the fast path should agree.

**Phase 3**

9. Clutch design pass, then implementation.

**Throughout**

- `scripts/verify_lineup_units.py` holds G1-G9, following the shape of
  `scripts/verify_actions_schema.py`.
- The existing Python suite runs first; the three R tests run as
  unchanged-output regression, since the parser is not touched.
- Rewrite `euroleague/PROJECT.md`: its "Next deliverable" section describes the
  unit-grain design this document supersedes, and its relation inventory needs
  the three new objects, with `sub_lineups`'s grain difference from the Israeli
  relation of the same name documented explicitly.

## Non-goals

- No opponent-unit context. Adding it later is additive.
- No stored minimum-possession threshold. That is an app concern and must not be
  baked into the stored fact.
- No shot-profile columns. `layup_*` / `dunk_*` derive from
  `play_info ILIKE '%lay%up%'` / `'%dunk%'` free text, and the Israeli Tab 2 does
  not display them.
- No cross-league adapter, and no ranked view mixing leagues.
- No four-factor impact-point annotations. Their weights were fitted on Israeli
  data and remain suppressed for EuroLeague.
- No `frontend-v2` work. Shiny is the product; the React app is archival.

## Open items

- Whether the season roll-up stays an MV. Take a refresh timing sample during
  the backfill; switch to an incrementally maintained table only if the full
  recompute is actually slow at the target season size.
- Cross-season unit identity. `sub_lineups` is season-scoped, and `players` is
  still keyed by competition and provider ID rather than being a durable person
  dictionary. A unit's `unit_key` is therefore stable *within* a season only.
  This is gap 2 in `PROJECT.md` and must be resolved before a second season is
  trusted — not by this deliverable.
