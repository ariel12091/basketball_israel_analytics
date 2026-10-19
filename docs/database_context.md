# Database Context and Query Guide

Last reconciled: **2026-07-22** against the repository, the live PostgreSQL
catalog, and `etl/logs/data_quality/latest.md`.

This is the first file to read before writing or changing a database query. Its
purpose is to make the existing data model, query surfaces, filters, refresh
order, and known defects explicit enough that existing logic is reused instead
of reconstructed from memory.

Section 15 is the exhaustive live column dictionary: 35 non-staging relations
and all 843 deployed columns, including PostgreSQL types, nullability/defaults,
meaning, derivation, units/value domains, join warnings, and known caveats.
`docs/database_value_profiles.md` is its live-content companion: it records the
observed values for every column, exact value/count domains when there are at
most ten non-null values, general ranges for larger domains, and value-level
anomalies found during profiling.

The exact SQL remains authoritative when this document and code disagree:

- App SQL callers: `app/R/global.R` and `app/R/server_tab*.R`
- Read/query functions: `sql/functions/*.sql`
- Persisted derived relations: `sql/materialized_views/*.sql`
- Incremental refresh functions: `sql/functions/refresh_*.sql`
- ETL orchestration: `etl/etl_full.R`
- Current quality state: `etl/logs/data_quality/latest.md`
- Canonical time contract: `docs/canonical_clock_minutes.md`

`docs/db_relation_usage_map.md` and
`docs/db_tables_usage_relations_purpose.md` are useful March 2026 snapshots, but
they are no longer authoritative. They predate cold storage, stable player
identity, shot zones, canonical segment timing, and the conversion of several
`*_mv` relations into incrementally maintained physical tables.

## 1. Fast decision guide: use the query that already exists

| Question | Reuse this relation/function | Do not start from |
|---|---|---|
| Default season player on/off PPP, impact, shots, layups, dunks, corner 3s | `onoff_default_mv` | Raw action rows |
| Filtered player on/off PPP and shot profile | `onoff_compute(...)` | A new lineup/action join |
| Default season player four factors | `player_advanced_stats_mv` | Recomputing flags |
| Filtered player four factors/disruptions | `four_factors_compute(...)` | Raw event `CASE` expressions |
| Default/full-window 2-5 player lineup summary | `fetch_lineups_csv_v2(...)`; its fast path reads `sub_lineups_stats` | Expanding player combinations again |
| Filtered lineup summary | `fetch_lineups_csv_v2(...)` | Direct `sub_lineups` aggregation |
| Filtered lineup four factors | `fetch_lineups_four_factors_csv(...)` | New four-factor logic |
| Default season team ratings | `team_ppp_ratings_mv` | Summing schedule scores |
| Filtered/clutch team ratings and shot profile | `get_team_ratings_dynamic(...)` | New action scans |
| Default season team four factors | `team_four_factors_mv` | A custom lineup aggregation |
| Filtered/clutch team four factors | `get_team_four_factors_dynamic(...)` | New four-factor logic |
| Per-game team dashboard/game logs | `team_metrics_by_game_mv`, or the narrower `mv_lineup_totals_by_day` / `lineup_four_factors_by_game` pair | Reconstructing games from PBP |
| Rolling 3/5/10-game team trends | `team_metrics_rolling_mv` | Windowing the event fact in the app |
| Default season traditional player stats | `player_traditional_stats_mv` | Counting raw events again |
| Filtered/clutch traditional player stats | `get_player_traditional_dynamic(...)` | A new box-score query |
| Schedule, result, opponent, home/away, GN | `final_schedule_mv` | Raw `schedule` unless updating schedule logic |
| Teams and player dropdowns | `full_rosters` using the existing queries in `app/R/global.R` | Inferring names from action rows |
| Display identity across provider-ID corrections | `resolved_player_identity_v` | Ad hoc alias joins |
| Legacy alias-compatible correction feed for ETL | `player_identity_aliases_v` | Directly mixing both correction tables |
| Corner-three classification | `shot_zones` joined by `(game_id, id)` | Re-deriving from coordinates after cold purge |
| New-game detection/publication | `etl_processed_games` | Checking `actions_clean`, which is normally empty |
| Last successful clean ETL time | `app_meta` key `etl_full_last_success` | Local marker files in deployed code |

If a request matches a row above, use that surface first. Only drop to its
upstream relation when the existing output contract genuinely cannot answer the
question.

## 2. Environment, schemas, roles, and naming

### Active schema

The Shiny app currently hardcodes `basketball_test` in its SQL. ETL defines:

```r
SCHEMA <- if (APP_ENV == "prod") "basketball" else "basketball_test"
```

but the live catalog audit on 2026-07-22 found all application objects in
`basketball_test` and no corresponding relations in `basketball`. In practice,
`basketball_test` is the deployed application schema. Treat the `basketball`
branch as an undeployed environment abstraction until its objects are verified.

### Connections

- The app uses `app/.Renviron` and the `app_readonly` role.
- ETL and DDL use `etl/.Renviron` and a write-capable role.
- R version: 4.4.2; use `C:/Program Files/R/R-4.4.2/bin/Rscript.exe`.
- Port `6543` is the Supabase transaction pooler used by the app/normal ETL.
- Port `5432` is used for direct DDL when required, with the same pooler host.
- Pooler sessions do not preserve `search_path`. Qualify application reads and
  use `SET LOCAL search_path TO basketball_test, public` inside ETL transactions.
- `bigint = "numeric"` is set on the app pool to avoid R `integer64`/dplyr
  incompatibilities.

### Naming is historical, not a type guarantee

Live object type is authoritative. On 2026-07-22:

- Physical incremental tables despite `_mv`: `df_pts_poss_lineups_longer_mv`,
  `player_four_factors_by_game`, `onoff_default_mv`,
  `player_advanced_stats_mv`, `team_metrics_by_game_mv`.
- Actual materialized views: `final_schedule_mv`,
  `mv_lineup_totals_by_day`, `lineup_four_factors_by_game`,
  `player_traditional_stats_mv`, `team_ppp_ratings_mv`,
  `team_four_factors_mv`, `team_metrics_rolling_mv`.
- Ordinary views: `sched_long`, `player_identity_aliases_v`,
  `resolved_player_identity_v`.

Never issue `REFRESH MATERIALIZED VIEW` or `DROP MATERIALIZED VIEW` based only on
the suffix.

## 3. End-to-end data flow

```text
provider schedule + PBP + box/roster JSON
  -> schedule, full_rosters, actions_clean, subs
  -> possessions
  -> stints + lineups_lookup
  -> pws (events/possessions with offense and defense lineup hashes)
  -> df_pts_poss_lineups_longer_mv
       one event mirrored into each team's perspective
       + possession endings, cumulative scores, starter context
       + canonical event/segment timing
  -> lineup_four_factors_by_game / mv_lineup_totals_by_day
  -> player_four_factors_by_game
  -> team_metrics_by_game_mv
  -> onoff_default_mv / player_advanced_stats_mv
  -> team_ppp_ratings_mv / team_four_factors_mv
  -> player_traditional_stats_mv / team_metrics_rolling_mv
  -> Shiny query functions and direct reads

lineups_lookup (ON rows)
  -> lineups_lookup_on
  -> sub_lineups (2/3/4-player identities)
  -> sub_lineups_stats (2/3/4/5-player season totals)

full_rosters + correction tables
  -> player_identities + player_identity_map
  -> player_identity_aliases_v / resolved_player_identity_v

actions_clean coordinates
  -> shot_zones (persistent; never cold-purged)

successful run
  -> etl_processed_games + app_meta
  -> export actions_clean/possessions/pws/stints/subs to Parquet
  -> truncate those five hot tables
```

The central modeling decision is that
`df_pts_poss_lineups_longer_mv` holds **two team perspectives per source event**
when both lineups are known. One row attaches the event to the acting team's
lineup; the mirrored row attaches it to the opponent lineup and reverses the
offense/defense perspective. Never sum this relation across teams without
grouping or filtering by `team_id` and `type_lineup`.

## 4. Universal basketball and clock semantics

### Rating scale

All `off_ppp`, `def_ppp`, and `net_rtg` fields exposed by the app are points per
100 possessions, despite the historical `ppp` name:

```text
off_ppp = 100 * offensive points / offensive possessions
def_ppp = 100 * opponent points / defensive possessions
net_rtg = off_ppp - def_ppp
```

Lower `def_ppp` is better. Higher offensive and net values are better.

### Four factors and related metrics

The shared SQL definitions are:

```text
TS%   = points / (2 * ts_poss_count)
eFG%  = (FGM + 0.5 * 3PM) / FGA
OREB% = offensive_rebounds / oreb_opportunities
TOV%  = turnovers / possessions
FTR   = free_throw_attempts / FGA
Defensive disruptions / possession = (steals + deflections) / possessions
```

Team/lineup functions usually return these as percentages (`* 100`, rounded to
one decimal). `four_factors_compute()` returns its base `off_on_*`/`def_on_*`
rates as fractions and returns the named `* Diff` columns in percentage points.
Confirm scale before combining outputs.

`ts_poss_count` is not the NBA estimate `FGA + 0.44 * FTA`. It is the count of
shot actions plus one distinct personal-foul trip identified through
`parent_action_id`. Traditional TS% deliberately uses
`PTS / (2 * (FGA + 0.44 * FTA))`, so the two TS implementations answer slightly
different questions.

`oreb_opportunities` counts missed/blocked shots plus a missed final free throw
on a personal-foul trip. The `pct_ft = 1` flag identifies that final free throw.

### Event ownership

- `type_lineup = 'offense'` means the event belongs to that row's team's
  offensive perspective; `'defense'` means the opponent produced the event.
- Shots, free throws, assists, turnovers, and foul-drawn events are offensive.
- Steals, blocks, deflections, and fouls are defensive.
- Offensive rebounds are offensive; defensive rebounds are defensive.
- `event_owner_side` records the same event-level ownership classification.
- `is_on_key = 1` in player facts means that player is on the lineup represented
  by the row; `0` is the comparison/off-floor bucket.

### Score fields

- Raw `actions_clean.score` is provider text and is not trusted for analytics.
- `team_score` is the points contributed by the current made shot/free throw;
  otherwise it is null/zero in sums. It is an increment, not the game score.
- `own_team_score` and `opp_team_score` are cumulative scores in the row team's
  perspective.
- Clutch margin/status predicates use the **pre-event** score. On a scoring row,
  SQL subtracts the current `team_score` increment before comparing the margin.
- Official final scores and W/L come from `final_schedule_mv`, not from the
  reconstructed event total.

### Shot fields

- `parameters_points` is 2 or 3 and defines the 2PT/3PT split.
- `parameters_type` is a provider shot label such as `lay-up`, `jump-shot`,
  `dunk`, or `allyhoop`; it is not `2pt`/`3pt`.
- `parameters_made` uses values such as `made`, `missed`, and `blocked`.
- A dunk includes provider types `dunk` and `allyhoop` in current SQL.
- `shot_zones` contains only 3PA with coordinates. `is_corner3 = true` when the
  provider raw y coordinate is `<= 285`.
- Absence from `shot_zones` means unknown corner status, not above-the-break.
- Corner share uses `c3_att / c3_known_att`; never divide by all `fg3_att`.

### Canonical time and minutes

Raw provider clocks remain untouched for auditing. Canonical elapsed time is:

```text
regulation: event_elapsed_seconds = 2400 - end_game_seconds_remaining
overtime:   event_elapsed_seconds =
              2400 + (quarter - 5) * 300 + (300 - end_game_seconds_remaining)
```

For `(game_id, team_id, lineup_hash, segment_id)`, the segment starts at the
canonical time of the lowest action ID and ends at the next segment's start;
the final segment ends at the maximum retained event time. `segment_seconds` is
the nonnegative difference.

For unfiltered floor time, deduplicate the repeated event rows at the full
segment key and count `MAX(segment_seconds)` once. Do not group by quarter and
do not restore raw-clock `MAX - MIN` calculations. Action-subset queries such as
clutch use ordered canonical endpoints because the full segment duration would
include time outside the selected window.

Clutch membership itself still uses the source-derived
`end_game_seconds_remaining`. Known ordering defects can therefore affect which
events enter a clutch query even though canonical durations protect ordinary
minutes.

### Starter context

- `own_starters` is the number of starters on court for the row's `team_id`.
- `opp_starters` is the opponent starter count.
- In lineup per-type aggregates, `num_starters` is the relevant lineup's starter
  count: own starters on offense and the corresponding row-team context on
  defense.
- Exact starter inputs are compatibility aliases for ranges. SQL applies
  `COALESCE(min, exact)` and `COALESCE(max, exact)`.
- Valid statistical rows normally have counts 0-5. Missing/invalid context is a
  known quality problem for a small set of rows; do not silently coerce it.

## 5. Relation catalog

The grains and keys below were verified against the live catalog on 2026-07-22.
Approximate sizes/row counts are intentionally omitted from the durable
contract; use the catalog query in section 11 for a fresh snapshot.

### Schedule and operational relations

| Relation | Type and grain/key | Purpose and important columns |
|---|---|---|
| `schedule` | Table; one row per game. PK `(game_id, game_year)` plus unique `game_id`. | Normalized provider schedule. Holds `game_type`, `gn`, schedule-side team IDs/names, official scores, text/raw date fields, parsed `game_date`, live/PBP metadata. |
| `schedule_team_dict` | Table; one row per `(game_year, team_id_schedule)`. | Maps schedule IDs/names to roster/team IDs. Required because provider subsystems use different IDs. |
| `sched_long` | View; two rows per mapped game, one per team perspective. | Pivots team1/team2 into `team_id`, `opp_team_id`, scores, names, and `team_slot`. Reads `schedule` live. |
| `final_schedule_mv` | Materialized view; two rows per game, one per team. | App schedule dimension. Adds `margin`, `has_won`, and `is_home`; carries `game_year`, `game_date`, `gn`, `game_type`, team/opponent IDs/names and official scores. |
| `etl_processed_games` | Table; PK `game_id`. | Publication marker and new-game detection. A marker means base and downstream refreshes completed successfully. Do not infer this from hot tables. |
| `app_meta` | Table; PK `key`. | Operational key/value store. The app reads `etl_full_last_success`. |
| `games_ingest_status` | Table; PK `game_id`. | Legacy hash/status inventory (`source_hash`, `ingested_at`). No current repository code reads or writes it; do not depend on it without re-establishing ownership. |

### Roster and identity relations

| Relation | Type and grain/key | Purpose and important columns |
|---|---|---|
| `full_rosters` | Table; PK `(game_id, team_id, player_id)`. | Per-game roster/box identity source. Contains English/local names, team names, jersey, `inroster`, `is_on`, `starter`, and `game_year`. Names/dropdowns originate here. |
| `player_id_aliases` | Table; PK `(game_year, team_id, alias_player_id)`. | Legacy season-scoped source-ID correction to `canonical_player_id`; `canonicalize_base` controls whether base rows are rewritten. |
| `player_id_game_overrides` | Table; PK `(game_id, team_id, alias_player_id)`. | More specific per-game correction. Overrides season mappings. |
| `player_identities` | Table; PK `identity_id`; unique `identity_key`. | Stable cross-context identity and preferred display/local names. Generated IDs start far above provider IDs. |
| `player_identity_map` | Table; PK `map_id`; partial unique season/game source keys. | Maps provider/team/season/game source IDs to `identity_id` and current `canonical_player_id`; records scope, origin, evidence, reason, and active state. |
| `player_identity_aliases_v` | View; active corrections only. | Compatibility projection shaped like the legacy alias model. ETL correction loading should use this rather than reimplementing precedence. |
| `resolved_player_identity_v` | View; one resolved source context per roster game/team/player. | Applies active game mapping before season mapping, otherwise falls back to the provider source ID. Returns stable `identity_id`, canonical ID, display name, scope, and reason. |

Identity meanings are deliberately distinct:

- `source_player_id`: the provider ID observed in a specific source context.
- `canonical_player_id`: the ID used by current base/derived basketball tables.
- `identity_id`: stable dictionary identity intended to survive provider-ID
  splits/reuse.

Do not join players across teams/seasons on `player_id` alone. For app display,
join at least on `(game_year, team_id, player_id)`; for stable identity, use the
resolved view.

### Transient hot ETL relations and cold storage

| Relation | Type and key | Purpose and column inheritance |
|---|---|---|
| `actions_clean` | Table; PK `(id, game_id)`. | Clean event feed. Contains source action/parent IDs, player/team, action type, provider parameters, raw score text, row order, and derived remaining clocks. |
| `subs` | Table; PK `(game_id, id)`. | Substitution-action subset used to reconstruct on-court state. Carries the clean-action shape. |
| `possessions` | Table; PK `(id, game_id)`. | `actions_clean` plus `pct_ft`, `q_bucket`, point increment `team_score`, and possession flags `end_poss`, `sum_poss_poss`, `sum_block`, `sum_tech`, `final_end_poss`. |
| `stints` | Table; PK `(game_id, team_id, final_start_id, final_end_id)`. | Lineup interval map with offense/defense lineup hashes, opposing team, segment IDs, period bucket, and boundary action IDs. |
| `pws` | Table; PK `(game_id, id)`. | Possession/event rows joined to stint context: lineup hashes, opponent team, segment/boundary fields, and starter counts. It is the immediate source of the central fact table. |

These five tables are intentionally truncated after every fully successful ETL
run and therefore normally occupy only an empty table shell in PostgreSQL.
Their cumulative history lives in:

```text
exports/cold/actions_clean.parquet
exports/cold/possessions.parquet
exports/cold/pws.parquet
exports/cold/stints.parquet
exports/cold/subs.parquet
```

Use `scripts/restore_cold_storage.R` / the helpers in `etl/cold_storage.R` when
historical hot rows are genuinely required. Never treat an empty
`actions_clean`/`possessions`/`pws` table as data loss without checking the cold
storage and publication marker.

### Lineup identity and sub-lineups

| Relation | Type and grain/key | Purpose and important columns |
|---|---|---|
| `lineups_lookup` | Table; player-row grain for each action/team lineup state. PK `(id, game_id, team_id, quarter, end_game_seconds_remaining, player_id)`. | Maps action/state to the five-player `lineup_hash`; `is_on_verdict` distinguishes ON/OFF membership; carries `n_on`, `game_year`, and `num_starters`. |
| `lineups_lookup_on` | Table; PK `(player_id, lineup_hash, team_id, game_year)`. | Deduplicated ON-player composition of each five-player lineup across a season. |
| `sub_lineups` | Table; PK `(team_id, lineup_hash, sub_lineup_hash, game_year)`. | Maps full five-player lineups to generated 2/3/4-player combinations. Holds combination `lineup_id`, size, optional `player_ids`, and creation time. Five-player identity is the full `lineup_hash` itself. |
| `sub_lineups_stats` | Table; PK `(team_id, sub_lineup_hash, game_year)`. | Season aggregate for 2/3/4/5-player groups: player arrays/names, off/def points/possessions/PPP, minutes, and 2PT/3PT makes/attempts. Fast source for default lineup queries. |

Hash scope matters: always carry `team_id` and `game_year` with a lineup or
sub-lineup hash. `segment_id` repeats across games, so any segment key must also
contain `game_id` (and for minutes, `team_id` and `lineup_hash`).

### Central event-team fact

`df_pts_poss_lineups_longer_mv` is a physical incremental table, not a
materialized view. Its row grain is approximately one persisted action per
`(game_id, team_id, id)` after mirroring to both team perspectives; the data
quality report enforces uniqueness at that grain.

Column groups:

- Event: `id`, `parent_action_id`, `game_id`, `quarter`, `type`, `player_id`,
  `parameters_type`, `parameters_points`, `parameters_made`, `pct_ft`.
- Possession/score: `team_score`, `final_end_poss`, `own_team_score`,
  `opp_team_score`.
- Perspective/lineup: `team_id`, `event_owner_side`, `type_lineup`,
  `lineup_hash`, `segment_id`, `final_end_id`.
- Starters: `num_starters`, `own_starters`, `opp_starters`.
- Source time: `end_game_seconds_remaining`.
- Canonical time: `event_elapsed_seconds`, `clock_regression_seconds`,
  `segment_start_elapsed_seconds`, `segment_end_elapsed_seconds`,
  `segment_seconds`.

This is the lowest persisted live grain for filtered/clutch analytics. Use a
pre-aggregated relation whenever its grain can answer the question.

### Lineup and game aggregate relations

| Relation | Type and verified grain/key | Payload / consumers |
|---|---|---|
| `mv_lineup_totals_by_day` | Materialized view; unique `(lineup_hash, type_lineup, g_date, num_starters)`. | Per game/date/team lineup points, possessions, 2PT/3PT counts, starter count, and canonical minutes. Used by lineup summary filtered path and Game Logs. Although `team_id`, `game_id`, `game_year` are columns, they are not in the unique index; rely on the full semantic grain in joins. |
| `lineup_four_factors_by_game` | Materialized view; unique `(lineup_hash, team_id, game_id, type_lineup, num_starters)`. | Raw four-factor counts and minutes per full lineup/game/side/starter count. Used by team/lineup FF functions and Game Logs. |
| `team_metrics_by_game_mv` | Physical table; unique `(game_year, game_id, team_id)`. | Complete team-game row: schedule context, raw rating/factor numerators, computed ratings/factors, traditional team totals/rates, and minutes. Primary source for Tab 3 trend/previous-period SQL. |
| `team_metrics_rolling_mv` | Materialized view; same unique key as team game. | `team_metrics_by_game_mv` plus `team_game_seq`, rolling 3/5/10 off/def/net/TS/eFG, previous rolling-5 net, and rolling-5 delta. |

The four-factor raw count payload is intentionally retained so totals can be
reaggregated correctly. Never average displayed percentages across lineups or
games; sum numerators/denominators, then divide.

### Player aggregate relations

| Relation | Type and verified grain/key | Payload / consumers |
|---|---|---|
| `player_four_factors_by_game` | Physical table; unique `(player_id, team_id, game_id, is_on_key, type_lineup, own_starters, opp_starters)`. | Shared per-game player ON/OFF fact: points, possessions, four-factor counts, steals/deflections, player usage counts, canonical minutes, 2PT/3PT, layup/dunk/corner-3 counts, and `onoff_minutes`. It replaced the old `player_onoff_by_game` MV. |
| `onoff_default_mv` | Physical table; unique `("Year", team_id, player_id)`. | Default season player on/off result including percentiles, PPP/net splits, possession/minute eligibility fields, 16 ordinary shot-split fields, and 28 layup/dunk/corner-3 profile fields. Despite quoted display columns, identity keys remain numeric. |
| `player_advanced_stats_mv` | Physical table; unique `(game_year, team_id, player_id)`. | Default season player four-factor ON/OFF splits, possession counts, diffs, defensive disruptions, and selected percentiles. |
| `player_traditional_stats_mv` | Materialized view; unique `(game_year, team_id, player_id)`. | GP, on-floor possessions, canonical minutes, PTS/REB/OREB/DREB/AST/STL/BLK/DFL/TOV, shooting totals/rates, eFG, traditional TS, and usage. Default Tab 5 source. |

### Team season aggregate relations

| Relation | Type and grain/key | Payload / consumers |
|---|---|---|
| `team_ppp_ratings_mv` | Materialized view; one row per `(game_year, team_id)` (indexed, but not uniquely). | Off/def/net ratings, GP/W/L, possessions, dense ranks, and offense/defense FGA/layup/dunk/3PA/corner-known counts. Default Team Ratings source and opponent-rank reference. |
| `team_four_factors_mv` | Materialized view; unique `(team_id, game_year)`. | Offense/defense TS/eFG/OREB/TOV/FTR/PPP plus raw numerators and denominators. Default team FF source. |

`team_ppp_ratings_mv` is season-wide. Most dynamic functions use its season
ranks to filter top/bottom opponents. `onoff_compute()` is the exception: it
recomputes opponent ranks from the selected date/year/game-type window before
applying the top/bottom filter.

### Persistent shot-zone relation

`shot_zones(game_id, id, is_corner3)` is a table with PK `(game_id, id)`. It is
populated before cold purge for 3PT shots that have coordinates and was
historically backfilled from Parquet. Never add it to `COLD_TABLES` and never
inner-join it when counting all threes: missing rows represent unknown zone.

## 6. Shared filter contract

The main dynamic functions share a common filter vocabulary. The app sends
typed positional parameters, usually with `NULL` for inactive filters. Preserve
the exact parameter order in app calls and explicit PostgreSQL casts (`::int4`,
`::date`, `::text`, `::bool`, `::numeric`) because several arguments are null.

| Parameter | Meaning and accepted behavior |
|---|---|
| `p_game_year` | Season-ending year, e.g. `2026` for 2025-26. `onoff_compute` historically accepts text; other functions use integer. |
| `p_start_date`, `p_end_date` | Inclusive schedule-date bounds. Null means unbounded where the function permits it. |
| `p_team_ids` / `p_team_ids_csv` | Team scope. CSV wrappers strip spaces, split commas, cast to distinct sorted `int4[]`. Empty/null means all teams. |
| `p_player_ids_csv` | Required/included players for lineup queries. |
| `p_player_off_csv` | Excluded players; implemented as no overlap with the lineup player array. |
| `p_exact` | If selected-player count equals group size, require exactly that player set. If fewer players are selected than group size, containment is used. `FALSE` always means containment. |
| `p_min_poss` | Lineup threshold on `off_poss + def_poss`. The app often queries with zero, computes ranks, then applies its UI threshold locally. |
| `p_min_all` | Player on/off eligibility: the minimum possession count across all required ON/OFF and offense/defense buckets. |
| `p_min_on` | Player on/off eligibility: minimum ON possessions. |
| `p_min_net` | Final minimum `Net RTG Diff` in `onoff_compute`. |
| `p_game_type_csv` | Comma-separated integer schedule game types. |
| `p_opp_team_ids_csv` / `p_opp_ids_csv` | Explicit opponent team IDs. |
| `p_home_away` | `all`, `home`, or `away`; blank normalizes to `all`. |
| `p_outcome` | `all`, `win`, or `loss`; based on official schedule result. |
| `p_opp_rank_side` | `all`, `top`, or `bottom`. Blank is normalized to all/null depending on the historical function. |
| `p_opp_rank_n` | Size of top/bottom opponent group. Null disables rank filtering. |
| `p_opp_rank_metric` | `net`, `off`, or `def`; maps to the corresponding dense rank. |
| `p_min_gn`, `p_max_gn` | Inclusive schedule game-number range. |
| `p_last_n_games` | Last N games **per team within the season**, ordered by `game_date DESC, game_id DESC`. Ranking is against the season schedule, then intersected with other filters. |
| `p_max_margin` | Maximum absolute **pre-event** score margin. Null disables it. |
| `p_margin_status` | `all`, `leading`, `trailing`, or `tied`, evaluated at the pre-event score in the row-team perspective. |
| `p_max_time_remaining` | Maximum regulation `end_game_seconds_remaining`; e.g. 300 means the last five regulation minutes. Current SQL includes every OT event regardless of this limit. |
| `p_ot_margin_filter` | If false, OT bypasses margin and leading/trailing/tied restrictions. If true, those restrictions also apply in OT. It does not make the time-remaining limit apply to OT. |
| `p_num_starters_off`, `p_num_starters_def` | Legacy exact starter counts. |
| `p_num_starters_off_min/max`, `p_num_starters_def_min/max` | Preferred inclusive range filters. Own starters apply to offense; opponent starters apply to defense/team context as implemented by each aggregate. |

Important filter consequences:

- Top/bottom opponent ranks are normally season-wide ranks from
  `team_ppp_ratings_mv`; selected date/GN/home/outcome filters do not recompute
  those ranks. `onoff_compute()` is the documented exception.
- With clutch filters, `get_team_ratings_dynamic()` counts GP/W/L only for games
  with at least one qualifying event/possession.
- All functions use the team-perspective schedule row, so home/away, result, and
  opponent are unambiguous after joining on both `game_id` and `team_id`.
- Starter range filters make lineup/team FF functions take the detailed path;
  they cannot be answered by a season aggregate that discarded starter context.

## 7. Query function contracts and internals

All app-read functions are `STABLE`, `SECURITY INVOKER`. The app role therefore
needs SELECT access to their underlying relations. The live deployed signatures
were verified on 2026-07-22.

### `onoff_compute` — filtered player on/off summary

Canonical signature: 23 parameters: 7 core arguments followed by game context,
GN/last-N, and six starter exact/range arguments. Source:
`sql/functions/onoff_compute.sql`.

Returns one row per player/team for the requested window:

- Team/name and numeric identity.
- ON/OFF offensive/defensive rating and ON/OFF net rating.
- Offensive ON-minus-OFF and defensive ON-minus-OFF.
- Overall `Net RTG Diff = offensive_on_diff - defensive_on_diff`.
- ON/OFF possessions and ON minutes.
- Percentile columns; defense-specific percentiles are inverted where “better”
  must mean lower points allowed.
- 16 2PT/3PT split counts.
- 28 layup/dunk/corner-3 counts, including known-corner denominators.

Pipeline:

1. Normalize CSV and categorical inputs.
2. Build the opponent ranking population. Unlike the other functions, this
   recomputes team PPP from the selected date/year/game-type window.
3. Filter team-perspective schedule rows.
4. Sum the merged `player_four_factors_by_game` fact by player, ON/OFF, and
   offense/defense, applying starter context.
5. Calculate ratings and percentiles.
6. Join roster names scoped to qualifying schedule rows.
7. Enforce `p_min_all` and `p_min_on` before pivoting.
8. Pivot ON/OFF and offense/defense, calculate diffs and final percentiles.
9. Enforce `p_min_net` and order best net impact first.

Use `onoff_default_mv` only when the app's fast-path conditions are met. It
stores all players; the UI applies its minimum-ON threshold locally.

### `four_factors_compute` — filtered player ON/OFF four factors

Canonical signature: 20 parameters: season, dates, team/game context,
GN/last-N, and starter exact/ranges. Source:
`sql/functions/four_factors_compute.sql`.

It sums raw count columns from `player_four_factors_by_game`, computes rates,
pivots offense/defense and ON/OFF, attaches roster names, and returns raw split
rates, possessions, and percentage-point diffs. It also returns defensive
steal-plus-deflection disruptions per possession and the ON-minus-OFF
disruptions per 100 difference.

Use `player_advanced_stats_mv` for an unfiltered season. Do not average rows from
that table to produce another grouping; call the function so raw counts are
reaggregated.

### `fetch_lineups_csv_v2` / `fetch_lineups_all` — lineup summary

The app calls the 29-argument CSV wrapper. It parses team/included/excluded
player CSVs into arrays and delegates to the array-based inner function.
Sources: `sql/functions/fetch_lineups_csv.sql` and
`sql/functions/fetch_lineups_all.sql`.

Returns one row per `(team_id, sub_lineup_hash, game_year)` at the requested
group size with player IDs/names, off/def points, possessions, rating, net,
minutes, starter context, and 2PT/3PT makes/attempts.

Three execution paths exist:

1. **Full-window fast path:** reads `sub_lineups_stats`. This requires no game,
   opponent, outcome, rank, clutch, GN, last-N, or starter filter and either
   null dates or a window covering the full Oct 1-July 1 season convention.
2. **Filtered non-clutch path:** filters `final_schedule_mv`, maps full lineups
   through `sub_lineups`, and sums `mv_lineup_totals_by_day`.
3. **Clutch/starter detailed path:** filters the central action-team fact,
   rebuilds segment stats and shot flags for the selected action subset, and
   maps those full lineups to the requested sub-lineups.

The final minimum is `off_poss + def_poss >= p_min_poss`.

### `fetch_lineups_four_factors_csv` / `fetch_lineups_four_factors`

The app calls the 29-argument CSV wrapper; the inner function uses arrays.
Source: `sql/functions/fetch_lineups_four_factors.sql`.

Returns the same lineup identity plus off/def TS/eFG/OREB/TOV/FTR, rating,
points, possessions, minutes, and all raw numerators/denominators needed to
construct a correct TOTAL row.

Execution paths:

- With clutch/margin/time filters, it recomputes event flags from
  `df_pts_poss_lineups_longer_mv`.
- Without clutch filters, it sums `lineup_four_factors_by_game` after schedule,
  rank, player-set, and starter filtering. The full-window form avoids an event
  scan but still returns correctly reaggregated raw counts.

### `get_team_ratings_dynamic`

Canonical signature: 23 parameters: season/date/game context, clutch, GN/last-N,
and starter exact/ranges. Source:
`sql/functions/get_team_ratings_dynamic.sql`.

Pipeline:

1. Filter team-perspective schedule rows.
2. Join season-wide opponent ranks and apply top/bottom selection.
3. Select qualifying games with at least one event matching clutch/starter
   predicates.
4. Count wins/losses from those qualifying games.
5. Aggregate the same qualifying event subset by offense/defense.
6. Compute ratings, dense ranks, and shot-profile attempt counts.

It uses PK-backed `EXISTS` probes into `shot_zones` only for 3PA to avoid an
action-wide zone join. Use `team_ppp_ratings_mv` for the default full season.

### `get_team_four_factors_dynamic`

Same 23-argument context contract as team ratings. Source:
`sql/functions/get_team_four_factors_dynamic.sql`.

- Clutch path recomputes event-level counts from the central fact because
  margin/time filters cannot be applied to the preaggregated MV.
- Non-clutch path sums `lineup_four_factors_by_game` and preserves raw counts.
- Final output is one row per team with offense/defense rates, raw counts, PPP,
  possessions, points, and net rating.

Use `team_four_factors_mv` for the unfiltered season.

### `get_player_traditional_dynamic`

Canonical signature: 18 parameters. It supports season/date/team/game context,
opponent rank, clutch/margin/time, GN, and last-N, but it has no starter-count
arguments. Source: `sql/functions/get_player_traditional_dynamic.sql`.

It:

1. Filters schedule/team contexts.
2. Filters central fact actions with pre-event clutch semantics.
3. Resolves parent personal-foul trips for TS possessions.
4. Maps each full lineup to its five ON players through `lineups_lookup`.
5. Counts player GP and on-floor possessions.
6. Calculates filtered segment minutes from ordered canonical event endpoints.
7. Counts traditional events by `player_id` and team perspective.
8. Computes percentages, traditional TS, and team/on-floor-adjusted usage.
9. Drops rows with no usable name/team and no participation.

Use `player_traditional_stats_mv` for the default season; it uses persisted
canonical `segment_seconds` for unfiltered minutes.

### Write/refresh functions

These are ETL APIs, not app APIs:

| Function | Effect |
|---|---|
| `refresh_df_pts_poss_lineups_longer_for_games(int4[])` | Delete/reinsert the selected games in the central fact and invoke canonical timing refresh. Null convention should be checked in the source before use. |
| `refresh_segment_clock_fields_for_games(int4[])` | Recalculate five canonical time columns; locally disables nested loops to avoid a catastrophic planner choice. |
| `refresh_player_four_factors_by_game_for_games(int4[])` | Rebuild selected player/game ON/OFF count rows, including disruptions and shot profile. |
| `refresh_team_metrics_by_game_for_games(int4[])` | Rebuild selected team-game rows. |
| `refresh_onoff_default_for_games(int4[])` | Refresh season/team/player summary rows affected by selected games. |
| `refresh_player_advanced_stats_for_games(int4[])` | Refresh season/team/player FF rows affected by selected games. |
| `refresh_sub_lineups_stats_for_games(int4[])` | Incrementally rebuild impacted sub-lineup totals. |
| `refresh_sub_lineups_stats()` | Full rebuild; use only when incremental scope is unavailable or explicitly forced. |

Pass game IDs as an explicitly typed `int4[]`. These functions mutate data and
belong inside the documented ETL transaction/order, not ad hoc app requests.

## 8. App-to-database query registry

### Shared/reference queries (`app/R/global.R`, `app/app.R`)

- Teams: distinct/minimum team names from `full_rosters` by `game_year`.
- GN choices: distinct `gn` from `final_schedule_mv` by `game_year`.
- Players: `(team_id, player_id)` and roster name from `full_rosters` by season.
- Last update: `app_meta.value` where key is `etl_full_last_success`.
- These are cached briefly by `cached_ref_query()` to avoid repeated metadata
  traffic across sessions.

### Tab 1 — On/Off Impact (`app/R/server_tab1.R`)

- Default Summary: `onoff_default_mv`, season key `"Year"`.
- Filtered Summary: `onoff_compute(...)` with the full 23-argument call.
- Default Four Factors: `player_advanced_stats_mv`.
- Filtered Four Factors: `four_factors_compute(...)`.
- UI thresholds/stat filters may be applied in R after fetching the ranking
  population; do not assume every UI slider is pushed into SQL.

### Tab 2 — Lineup Data (`app/R/server_tab2.R`)

- Summary: always calls `fetch_lineups_csv_v2(...)`; the SQL function selects
  its own fast/non-clutch/clutch path.
- Four Factors: calls `fetch_lineups_four_factors_csv(...)`.
- Main ranking fetch often sets `p_min_poss = 0`; the displayed threshold and
  auto-minimum are applied after the complete comparison population is ranked.
- Modal identity: resolve `sub_lineup_hash` through `sub_lineups`; a five-player
  hash is already the full lineup hash.
- Modal schedule/name: `final_schedule_mv` and `sub_lineups_stats`.
- Modal Summary game rows: sum `mv_lineup_totals_by_day` by game/side.
- Modal FF game rows: sum `lineup_four_factors_by_game` raw counts by game/side.

### Tab 3 — Team Ratings (`app/R/server_tab3.R`)

- Summary: `get_team_ratings_dynamic(...)` for filtered contexts; default cache
  may read `team_ppp_ratings_mv`.
- Four Factors: `get_team_four_factors_dynamic(...)`; default may read
  `team_four_factors_mv`.
- Traditional/team profile and previous-period work uses direct CTE queries over
  `team_metrics_by_game_mv`, and filtered minutes use the central fact.
- Default shot profile columns come from `team_ppp_ratings_mv`.
- Previous-rank queries deliberately use a prior date/GN anchor; do not replace
  them with a simple row lag without preserving the chosen comparison window.

### Tab 4 — Game Logs (`app/R/server_tab4.R`)

- Schedule: all `final_schedule_mv` rows for the selected season.
- Summary payload: `mv_lineup_totals_by_day` rows for the season, then R
  aggregation/pivot by schedule team-game.
- Four Factors: `lineup_four_factors_by_game`, then raw-count aggregation/pivot.
- This is deliberately a broad season fetch with local filtering/caching; avoid
  introducing per-input repeated database queries without evidence.

### Tab 5 — Player Stats (`app/R/server_tab5_traditional.R`)

- Default season fast path: `player_traditional_stats_mv`.
- Filtered/clutch path: `get_player_traditional_dynamic(...)`.
- Roster display reference: `full_rosters`.
- Stable display identity: `resolved_player_identity_v` grouped by
  `(team_id, source_player_id, identity_id)`.
- Totals/per-game/per-60-possession/per-30-minute scaling happens in R; the SQL
  source stays at totals plus GP/possessions/minutes.

### Tab 6 files

`app/R/server_tab6_team_stats.R` contains direct filtered team-traditional SQL
over `final_schedule_mv`, `team_ppp_ratings_mv`,
`df_pts_poss_lineups_longer_mv`, and `full_rosters`. The current navbar treats
Tab 6 as dormant/unfinished; do not treat its query as a public stable API until
the tab is wired and verified.

### Tab 7 — Compare (`app/R/server_tab7_compare.R`)

Compare does not introduce a separate metric engine. It reuses the same
functions twice, once per side:

- Teams: `get_team_ratings_dynamic` or `get_team_four_factors_dynamic`.
- Lineups: `fetch_lineups_csv_v2` or
  `fetch_lineups_four_factors_csv`.
- Players: `get_player_traditional_dynamic`, `four_factors_compute`, and
  `onoff_compute` depending on the selected metric/detail.

Side A/B normalization is part of the contract. A compare bug should be traced
through its side-parameter builder before changing SQL shared with other tabs.

### React/Plumber code

`frontend-v2/server/plumber.R` contains older parallel copies of many query
wrappers. Per the 2026-07-22 architecture decision in `PROJECT.md`, Shiny is the
active product and both React prototypes are archival unless explicitly
revived. Do not modify a Plumber query and assume the live app changed.

## 9. ETL, publication, refresh order, and cold storage

Run the orchestrator in `etl/etl_full.R`; do not run the bare call at the bottom
of `etl/etl_onoff.R` as the operational pipeline.

```powershell
$env:APP_ENV='test'
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "source('etl/etl_full.R'); etl_full(dry_run=TRUE)"
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "source('etl/etl_full.R'); etl_full(dry_run=FALSE)"
```

The dry run previews scope; the write run performs the work. For a targeted
reprocess, pass explicit `game_ids`. Do not delete publication markers or
restore cold data merely to “make the ETL notice” a game without first tracing
the documented reprocess path.

### Phase order

1. **Setup and identity sync**
   - Load `etl/.Renviron`, connect, choose schema from `APP_ENV`.
   - Ensure/backfill `etl_processed_games`.
   - Ensure legacy correction and stable identity objects.
   - Sync `player_identities` and `player_identity_map`.
2. **Base table ETL, one transaction per game**
   - Fetch normalized schedule, PBP, and box/roster payloads.
   - Use the schedule table as fallback for explicit cup IDs missing from the
     external schedule feed.
   - Recover PBP participants missing from the game roster from same-season,
     same-team roster history. Then fail the game before its write transaction
     if any nonzero action-player context still lacks a roster row.
   - Clean events, apply player corrections, compute possessions, substitutions,
     stints, lineup lookup, starter counts, and `pws`.
   - Join actions to half-open stint ranges. Persist real action IDs as stint
     endpoints for FK integrity, but advance only the final range's temporary
     join upper bound by one so the game's maximum action remains attributable.
   - Commit successful games independently; failed games roll back.
   - Resync the identity dictionary after successful base loads.
   - Persist `shot_zones` before any purge.
3. **Sub-lineup identity generation**
   - Pull newly observed ON five-player lineups.
   - Add deduplicated `lineups_lookup_on` rows.
   - Generate 2/3/4-player combinations into `sub_lineups`.
4. **Derived refresh transaction**
   - L1 materialized view: `final_schedule_mv`.
   - Incremental table: `df_pts_poss_lineups_longer_mv`, including canonical
     timing for touched games.
   - L2 MVs: `mv_lineup_totals_by_day`, `team_ppp_ratings_mv`.
   - L3 MV: `lineup_four_factors_by_game`.
   - Incremental tables: `player_four_factors_by_game`,
     `team_metrics_by_game_mv`, `onoff_default_mv`,
     `player_advanced_stats_mv`.
   - Full MV: `player_traditional_stats_mv`.
   - L4 MVs: `team_metrics_rolling_mv`, `team_four_factors_mv`.
5. **Sub-lineup stats**
   - Prefer `refresh_sub_lineups_stats_for_games()` for touched games.
   - Fall back to `refresh_sub_lineups_stats()` only when required or forced.
6. **Validation and publication**
   - Base row floors; action-to-stint/lineup coverage; canonical team minutes.
   - Unique app-player keys; alias residue; identity ambiguity/unresolved checks.
   - Only a clean downstream pipeline inserts successful games into
     `etl_processed_games`.
7. **Cold storage**
   - Export the five hot tables to cumulative Parquet, with current DB rows
     winning on duplicate keys.
   - Widen export scope to rescue hot games absent from Parquet.
   - Require a coverage gate before truncation.
   - Truncate all five tables together after every export succeeds.
   - Record `app_meta.etl_full_last_success` only after a clean run.

### Rebuild/deployment cautions

- The current incremental refresh order above supersedes the old static L1-L4
  tree in early project notes.
- `DROP ... CASCADE` can remove dependent materialized views. Inventory
  dependencies before replacing a central relation.
- `sql/rebuild_all_mvs.R` reflects a historical rebuild registry; compare it
  with the current ETL and live object types before using it after later
  migrations.
- Long canonical-clock work must follow the resumable online scripts and avoid
  an `ACCESS EXCLUSIVE` lock across a large refresh.
- A killed client can leave a server transaction holding locks. Inspect
  `pg_stat_activity`/`pg_locks` before retrying timed-out DDL.
- The canonical clock refresh must keep its local `enable_nestloop = off`
  planner guard.
- A full sub-lineup refresh can be drastically slower than the incremental
  function. Use the game scope already available in the ETL.
- `upsert_by_like()` matches source and target columns by exact, case-sensitive
  name. Explicitly map mixed-case provider fields such as `GN` and `ExternalID`
  to `gn` and `game_id` before upsert.

## 10. Database security contract

Security DDL lives in `sql/security/enable_readonly_rls.sql`; the audit is
`sql/security/audit_app_access.sql`.

- `PUBLIC`, `anon`, and `authenticated` have analytics schema/table/function
  privileges revoked.
- `app_readonly` has schema USAGE, relation SELECT, no schema CREATE, no writes,
  no sequence privileges, and EXECUTE only on the allowlisted app function
  names.
- Functions are `SECURITY INVOKER`, so `app_readonly` requires broad SELECT on
  underlying tables/views.
- Physical tables have RLS enabled and a read-all policy only for
  `app_readonly`. Views and materialized views do not have table RLS.
- `service_role` retains explicit administrative read/execute access where that
  role exists.

The security audit must return zero rows. The apply wrapper
`scripts/apply_db_security.R` is dry-run by default and only commits when
`CONFIRM_DB_SECURITY_APPLY=1` is set.

Security is not a reason to interpolate user input. All app value filters must
remain DBI parameters. Dynamic SQL identifiers are only acceptable from fixed,
validated allowlists.

## 11. Existing diagnostic queries and commands

These are the preferred starting points for common investigations.

### Current data-quality state

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' etl/run_data_quality_report.R
```

Read `etl/logs/data_quality/latest.md` and its `details/*.csv` files. Do not copy
an old report's counts into a new diagnosis when a current report exists.

### Relation type, size, and approximate rows

```sql
SELECT
  c.relname,
  CASE c.relkind
    WHEN 'r' THEN 'table'
    WHEN 'm' THEN 'materialized_view'
    WHEN 'v' THEN 'view'
    WHEN 'p' THEN 'partitioned_table'
    ELSE c.relkind::text
  END AS kind,
  pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
  c.reltuples::bigint AS estimated_rows
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'basketball_test'
  AND c.relkind IN ('r', 'm', 'v', 'p')
ORDER BY c.relname;
```

Use exact `COUNT(*)` only where exactness matters; it can scan large relations.

### Column contract for any table, view, or MV

```sql
SELECT
  a.attnum,
  a.attname AS column_name,
  pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
  a.attnotnull,
  pg_get_expr(ad.adbin, ad.adrelid) AS column_default
FROM pg_attribute a
JOIN pg_class c ON c.oid = a.attrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_attrdef ad
  ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
WHERE n.nspname = 'basketball_test'
  AND c.relname = 'schedule' -- Replace with the relation being inspected.
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum;
```

### Deployed functions and overload drift

```sql
SELECT
  p.proname,
  pg_get_function_identity_arguments(p.oid) AS identity_arguments,
  pg_get_function_result(p.oid) AS result,
  p.provolatile,
  p.prosecdef
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'basketball_test'
ORDER BY p.proname, identity_arguments;
```

Run this before changing a return table or dropping a function. PostgreSQL
overloads are separate objects; `CREATE OR REPLACE` with a new signature does
not replace the old signature.

### Verified keys

```sql
SELECT
  c.relname AS relation,
  i.relname AS index_name,
  ix.indisprimary,
  ix.indisunique,
  pg_get_indexdef(ix.indexrelid) AS definition
FROM pg_index ix
JOIN pg_class c ON c.oid = ix.indrelid
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'basketball_test'
  AND (ix.indisprimary OR ix.indisunique)
ORDER BY c.relname, ix.indisprimary DESC, i.relname;
```

### Find staging leftovers

```sql
SELECT
  c.relname,
  pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
  c.reltuples::bigint AS estimated_rows
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'basketball_test'
  AND c.relkind = 'r'
  AND left(c.relname, 8) = '__stage_'
ORDER BY c.relname;
```

Confirm no active ETL/session owns a candidate before dropping it.

### One game through the durable pipeline

```sql
SELECT s.game_id, s.game_year, s.game_date, s.gn,
       COUNT(DISTINCT fs.team_id) AS schedule_team_rows,
       COUNT(DISTINCT d.team_id) AS fact_team_rows,
       COUNT(DISTINCT p.team_id) AS player_fact_teams,
       COUNT(DISTINCT tm.team_id) AS team_metric_rows,
       (eg.game_id IS NOT NULL) AS published
FROM basketball_test.schedule s
LEFT JOIN basketball_test.final_schedule_mv fs USING (game_id)
LEFT JOIN basketball_test.df_pts_poss_lineups_longer_mv d USING (game_id)
LEFT JOIN basketball_test.player_four_factors_by_game p USING (game_id)
LEFT JOIN basketball_test.team_metrics_by_game_mv tm USING (game_id)
LEFT JOIN basketball_test.etl_processed_games eg USING (game_id)
WHERE s.game_id = 62452 -- Replace with the game being inspected.
GROUP BY s.game_id, s.game_year, s.game_date, s.gn, eg.game_id;
```

For large diagnostic joins, preaggregate each relation by `game_id` before
joining if the planner expands a many-to-many intermediate.

### Canonical team minutes for one game

```sql
WITH segments AS (
  SELECT game_id, team_id, lineup_hash, segment_id,
         MAX(segment_seconds)::numeric AS segment_seconds
  FROM basketball_test.df_pts_poss_lineups_longer_mv
  WHERE game_id = 62452 -- Replace with the game being inspected.
    AND lineup_hash IS NOT NULL
    AND segment_id IS NOT NULL
    AND segment_seconds IS NOT NULL
  GROUP BY game_id, team_id, lineup_hash, segment_id
)
SELECT game_id, team_id,
       ROUND(SUM(segment_seconds) / 60.0, 3) AS minutes
FROM segments
GROUP BY game_id, team_id
ORDER BY team_id;
```

### Default app surfaces

```sql
SELECT *
FROM basketball_test.onoff_default_mv
WHERE "Year" = 2026 -- Replace with the season-ending year.
ORDER BY "Net RTG Diff" DESC;

SELECT *
FROM basketball_test.player_advanced_stats_mv
WHERE game_year = 2026; -- Replace with the season-ending year.

SELECT *
FROM basketball_test.team_ppp_ratings_mv
WHERE game_year = 2026 -- Replace with the season-ending year.
ORDER BY rank_net_rtg;

SELECT *
FROM basketball_test.team_four_factors_mv
WHERE game_year = 2026; -- Replace with the season-ending year.

SELECT *
FROM basketball_test.player_traditional_stats_mv
WHERE game_year = 2026 -- Replace with the season-ending year.
ORDER BY pts DESC;
```

### Safe current lineup-function invocation

Because obsolete deployed overloads currently exist, provide all 29 named
arguments so PostgreSQL must resolve the newest contract:

```sql
SELECT *
FROM basketball_test.fetch_lineups_csv_v2(
  p_num_lineup => 5,
  p_team_ids_csv => NULL,
  p_player_ids_csv => NULL,
  p_player_off_csv => NULL,
  p_exact => true,
  p_start_date => DATE '2025-10-01',
  p_end_date => DATE '2026-07-01',
  p_min_poss => 0,
  p_game_year => 2026,
  p_game_type_csv => NULL,
  p_opp_team_ids_csv => NULL,
  p_home_away => 'all',
  p_outcome => 'all',
  p_opp_rank_side => 'all',
  p_opp_rank_n => NULL,
  p_opp_rank_metric => 'net',
  p_max_margin => NULL,
  p_margin_status => 'all',
  p_max_time_remaining => NULL,
  p_ot_margin_filter => false,
  p_min_gn => NULL,
  p_max_gn => NULL,
  p_last_n_games => NULL,
  p_num_starters_off => NULL,
  p_num_starters_def => NULL,
  p_num_starters_off_min => NULL,
  p_num_starters_off_max => NULL,
  p_num_starters_def_min => NULL,
  p_num_starters_def_max => NULL
);
```

For application code, copy the established typed DBI wrapper in
`app/R/server_tab2.R` instead of embedding a new call shape.

### Security audit

Execute `sql/security/audit_app_access.sql` as a catalog-capable role. Expected:
zero rows. Static repository contracts live in
`app/tests/testthat/test-db-security-contracts.R`.

### Locks after a failed DDL/backfill client

```sql
SELECT pid, usename, application_name, state, wait_event_type, wait_event,
       xact_start, query_start, left(query, 300) AS query
FROM pg_stat_activity
WHERE datname = current_database()
ORDER BY xact_start NULLS LAST, query_start;
```

Only terminate a backend after confirming it is the abandoned session and not
an active ETL/app request.

## 12. Known current problems and anomalies

This section distinguishes current quality failures from reviewed exceptions
and repository/deployment drift. Counts are a snapshot; the latest DQ report is
the live source of truth.

### Current data-quality status (2026-07-22 20:01 IDT)

Overall status: **FAIL** with error checks configured not to abort the report.

| Area | Current finding | Practical impact |
|---|---|---|
| Player correction residue (`C`, `F`) | Six base residue groups and nine lineup-derivative residue groups. Active season aliases remain for `1982 -> 1143` (team 6), `2052 -> 1110` (team 9), and `2046 -> 1165` (team 13). | Duplicate/split identities and stale lineup/sub-lineup hashes remain in 2026 aggregates until scoped cleanup/backfill runs. |
| Same name, multiple IDs (`B`) | Three warning groups: NOAM AVIVI, BEN ALTSHULER, and the reviewed `NEW NEW` placeholders. | Do not merge solely on normalized name. The first two correspond to active alias residue; placeholders require manual evidence. |
| Placeholder players (`P0`, `P`) | Three 2025 `NEW NEW` IDs (33406/33407/33408) in game 62426; three placeholder rows are visible in app player aggregates but have no participation. | They do not affect points, possessions, lineups, or minutes, but may appear in broad identity/player outputs. |
| Event-to-lineup mismatch (`Q`) | 486 event-team rows in four games cannot resolve to a five-player ON lineup: 178 (362), 62452 (116), 209 (6), 62479 (2). | Affected events are absent from lineup/player-on-floor attribution; team/event totals can diverge from lineup totals. |
| Invalid lineup states (`R`) | 52 states across 24 game/team groups have other than exactly five ON players. Largest defects: game 178 teams 4/11 and game 62452 team 11. | Lineup hashes, minutes, and player conservation can be wrong for affected intervals. |
| Starter context (`S`) | 14 statistical rows in game 62452 have invalid/missing starter context. | Starter-filtered results can omit or misclassify these rows. |
| Team minutes (`T`) | Game 160 is 36.75 minutes for both teams versus 40 expected. | This is incomplete source coverage; canonical timing intentionally does not invent missing time. |
| Schedule/event score reconciliation (`V`) | 21 team-game rows do not match official schedule score. Affected games: 96, 139, 140, 141, 143, 211, 62452, 62500, 62504, 62584, 64893. | W/L and displayed final score come from schedule; derived ratings use event points. Small/massive gaps therefore coexist by design until source corrections are made. |
| Possession reconciliation (`W`) | Both team perspectives in game 62452 disagree between team offense and opponent defense possessions. | Team and opponent denominators are not symmetric for that game. |
| Player-minute conservation (`X`) | Four team-games fail: 62452/team 11, 178/teams 11 and 4, and 62479/team 7. | Sum of five player minutes does not equal five times team lineup minutes; do not use these games as minute-validation exemplars. |
| OT start coverage (`Y`) | Game 211 has no valid five-player state at the start of Q5 for either team; first valid clocks are late. | Early OT events/lineups are not reliably attributed. |
| OT event-player mismatch (`Z`) | Four events in games 62439, 62541, and 62587 name players absent from the attached lineup. | Player event and on-floor attribution disagree for these events. |
| Material clock order (`AA`) | 17 games contain period regression or >24-second reverse movement. | Raw/source-clock ordering is not a safe duration basis. |
| Clock jitter (`AB`) | 29 reversals across 20 games are 6-24 seconds. | Small source-order noise remains visible and can move action-subset boundaries. |
| Clutch exposure (`AD`) | Eight transitions in games 115, 127, 158, 62480, 62508, 62537, 62562, 62584 can move actions into/out of the five-minute scope. | Clutch outputs for those games can change because membership still uses the source-derived clock. |
| Canonical timing (`AH`) | Four tiny segment inconsistencies: games 62506 (both teams, 2 seconds) and 62511 (both teams, 5 seconds). | Team coverage is effectively conserved, but adjacent lineup allocation has small jitter; keep visible rather than rewriting raw actions. |
| Cold Parquet consistency (`AG`) | 53 issue rows / 19,395 affected cold rows, dominated by nonpositive old segment IDs and PWS mappings absent from old stint snapshots. | Offline audits/restores are affected; the live app tables are cleaner. A restore must be validated before republishing derivatives. |
| Raw free-throw trip metadata | Seven cold-source trips have conflicting `parameters_free_throws_awarded` values under one parent action (games 51, 69, 141, 192, 62463, 62527, and 64909). Game 192 action 1920557 says attempt 2 but awarded total 1. | Raw fields are deliberately retained for audit and must not be assumed internally consistent. `compute_possessions()` now uses the attempt number as the effective denominator only when it exceeds the stated total; published `pct_ft` remains 0-1 and DQ `AJ` passes. |

Checks for cleaned duplicates, publication coverage, downstream game rows,
aggregate duplicate keys, identity ambiguity, unresolved identity, missing
regulation quarters, duplicate app action-team keys, invalid persisted segment
IDs, invalid lineup metric values, mirrored team minutes, and the published
free-throw progress domain passed in that report.

### Resolved during the 2026-07-22 ETL audit

Detailed investigation and handoff:
`docs/lineup_anomalies_etl_memory_2026-07-22.md`.

- **Game 62461 / team 8:** the game roster omitted player 2543, Cody Demps,
  although 26 PBP actions identified him. The scoped reprocess recovered him
  from the same-season/team roster history, removed 18 invalid lineup states,
  restored 430 unmatched event-team rows, and cleared that team-game's player
  minute-conservation failure. The formerly terminal action 624610872 now has
  both offense and defense lineup rows. A full cold-history audit now finds
  zero action-player contexts without a roster row.
- The ETL now applies that recovery before lineup construction and then fails
  the individual game before writing if any nonzero PBP
  `(game_id, team_id, player_id)` is still absent from `full_rosters`. This
  prevents the same source omission from silently creating another
  four-player lineup.

### Reviewed exceptions, not generic fixes

- **Game 157:** period-boundary substitution declarations lack opponent starter
  context but have no statistical effect.
- **Game 184:** ended early; a short event timeline is expected.
- **Game 380:** ended early; schedule stores the official 20-1 result while
  played events reconstruct to 74-66.
- **Game 160:** incomplete retained timeline produces 36.75 minutes; do not pad
  it to 40 without external evidence.
- **Game 178:** invalid Q2 reset substitutions were removed, but the game still
  has 6/7-player lineup states, 362 unmatched persisted rows, and player-minute
  overcount. See `docs/game_178_invalid_q2_reset_2026-05-30.md`.
- **Game 381:** raw provider IDs 3810375-3810385 are duplicated across the
  Q2/Q3 boundary. Cleaning avoids a duplicate PK in `actions_clean`, but the raw
  duplicate check is still not automated. See
  `docs/game_381_json_quality_report_2026-06-10.md`.

### Query/clock limitations that remain

- Filtered/clutch minutes use canonical ordered endpoints, but selecting
  multiple non-contiguous action windows can bridge gaps within a segment. A
  rigorous fix would intersect segment intervals with each selected time
  window. This is not solved by the 2026-07-22 clock migration.
- Clutch time membership uses `end_game_seconds_remaining`, so canonical segment
  timing does not eliminate `AD` exposure.
- Official schedule points and reconstructed event points have different jobs.
  Do not “fix” a score mismatch by using schedule points as the numerator for
  event-filtered ratings.
- Four-factor TS and traditional TS intentionally use different denominators.
- Most opponent-rank filters use full-season ranks, while `onoff_compute`
  recomputes ranks for its date/year/game-type window. Cross-tab rank-filter
  parity should not be assumed without deciding which semantics are desired.

### Live catalog/repository drift found on 2026-07-22

1. **Only `basketball_test` is populated.** The `basketball` schema had no
   corresponding application relations in the catalog audit, even though ETL
   still models `APP_ENV=prod` as `basketball`.
2. **Twelve `__stage_*` tables are present.** One schedule stage and eleven
   empty stint stages look like failed/abandoned `upsert_by_like()` artifacts.
   They are not app dependencies. Verify no active ETL owns them before any
   cleanup.
3. **Obsolete lineup function overloads remain deployed.** There are three
   signatures each for `fetch_lineups_all` and `fetch_lineups_csv_v2` (23, 25,
   and 29-argument generations). The app passes all 29 arguments and resolves
   correctly, but shortened/manual calls can resolve an old return contract or
   become ambiguous. Drop old signatures explicitly during a controlled
   migration.
4. **Duplicate legacy RLS policies remain on many older tables.** Both
   `app_readonly_select_all` and `rls_read_all_app_readonly` exist on parts of
   the schema. Both grant the same read-all predicate to `app_readonly`; this is
   policy clutter, not broader browser-role access, but the apply script does
   not currently remove the legacy name.
5. **`games_ingest_status` is catalog-only legacy state.** No current app, ETL,
   SQL, or script reference was found.
6. **Logical uniqueness is unevenly enforced.** `team_ppp_ratings_mv` is
   logically one row per season/team but has only a nonunique index.
   `mv_lineup_totals_by_day` includes team/game/year columns but its unique key
   omits them. Current DQ checks pass; treat these as schema-design hazards when
   changing hash or schedule assumptions.
7. **The historical `_mv` naming is misleading.** Scripts that branch on name
   rather than `relkind` risk invalid refresh/drop operations.

### Cold-storage-specific known risk

The merge in `etl/cold_storage.R` gives current DB rows precedence for equal
keys, but a reprocessed game can leave obsolete old keys that are no longer
present in the live table. The 2026-07-22 handoff identifies this stale-key risk
as unresolved. Always run cold snapshot consistency checks after restoring or
rewriting historical games.

## 13. Rules for new database work

1. Identify the requested output grain before choosing a relation.
2. Start from the query decision table in section 1.
3. Preserve team perspective: join schedule/facts on both `game_id` and
   `team_id`.
4. Preserve season/team with player and lineup identities.
5. Sum raw counts before computing rates; never average displayed percentages.
6. Deduplicate canonical minutes at
   `(game_id, team_id, lineup_hash, segment_id)`.
7. Use pre-event margin for action/clutch filters.
8. Treat absent `shot_zones` rows as unknown.
9. Use DBI parameters for values and explicit casts for null arguments.
10. Check the deployed function catalog before changing signatures.
11. Test one known clean game first, then known anomalous games relevant to the
    change, then the full parity/quality suite.
12. For database writes, follow ETL publication and refresh order; never mark a
    game processed before downstream validation completes.
13. Run the security audit after creating relations/functions.
14. Refresh this document's “known current” snapshot when a migration or data
    repair materially changes it; keep durable semantics separate from counts.

## 14. Source file index

| Concern | Primary files |
|---|---|
| DB pool/reference queries | `app/R/global.R`, `app/app.R` |
| Player on/off | `sql/functions/onoff_compute.sql`, `sql/materialized_views/onoff_mv.sql`, `app/R/server_tab1.R` |
| Player four factors | `sql/functions/four_factors_compute.sql`, `sql/materialized_views/player_four_factors_by_game.sql`, `sql/materialized_views/player_advanced_stats_mv.sql` |
| Lineups | `sql/functions/fetch_lineups_all.sql`, `sql/functions/fetch_lineups_csv.sql`, `sql/functions/fetch_lineups_four_factors.sql`, `sql/functions/refresh_sub_lineups*.sql`, `app/R/server_tab2.R` |
| Team ratings/factors | `sql/functions/get_team_ratings_dynamic.sql`, `sql/functions/get_team_four_factors_dynamic.sql`, `sql/materialized_views/team_ppp_ratings_mv.sql`, `sql/materialized_views/team_four_factors_mv.sql` |
| Traditional players | `sql/functions/get_player_traditional_dynamic.sql`, `sql/materialized_views/player_traditional_stats_mv.sql`, `app/R/server_tab5_traditional.R` |
| Schedule | `sql/materialized_views/sched_long.sql`, `sql/materialized_views/final_schedule_mv.sql` |
| Central fact/timing | `sql/materialized_views/df_pts_poss_longer.sql`, `sql/functions/refresh_df_pts_poss_lineups_longer_for_games.sql`, `sql/functions/refresh_segment_clock_fields_for_games.sql`, `docs/canonical_clock_minutes.md` |
| Team game/rolling | `sql/materialized_views/team_metrics_by_game_mv.sql`, `sql/functions/refresh_team_metrics_by_game_for_games.sql`, `sql/materialized_views/team_metrics_rolling_mv.sql` |
| Player identity | `sql/migrations/2026-06-10_player_id_aliases.sql`, `sql/migrations/2026-06-18_player_identity_dictionary.sql`, `etl/player_id_aliases.R`, `etl/player_identity_dictionary.R` |
| Shot zones | `sql/migrations/2026-07-16_shot_zones.sql`, `scripts/backfill_shot_zones.R`, `etl/etl_full.R` |
| ETL/cold storage | `etl/etl_full.R`, `etl/etl_onoff.R`, `etl/etl_lineups.R`, `etl/cold_storage.R`, `scripts/restore_cold_storage.R` |
| Data quality | `etl/run_data_quality_report.R`, `etl/logs/data_quality/latest.md`, `docs/data_quality_player_identity_findings.md` |
| Security | `sql/security/enable_readonly_rls.sql`, `sql/security/audit_app_access.sql`, `scripts/apply_db_security.R` |

## 15. Complete live column dictionary

This dictionary was reconciled against `pg_attribute` in the deployed
`basketball_test` schema on 2026-07-22. It covers every non-staging application
relation. A type without `NOT NULL` is nullable in the live catalog. Views and
materialized views generally expose nullable catalog metadata even where their
source expression is normally populated.

Conventions used below:

- "Provider" means copied or lightly normalized from the Basket/Segev source.
- "Official" means schedule-feed result data, not a PBP reconstruction.
- `off_*` describes the row team's offense; `def_*` describes events produced
  by its opponent while the row team defends.
- A percentage is on a 0-100 scale unless the description explicitly says
  "fraction" or "percentile".
- Points per possession fields named `*_ppp` are on a per-100 scale unless
  explicitly labeled raw.
- Never use nullability alone as a quality guarantee. Most derived relations
  have no column-level `NOT NULL` constraints.

### 15.1 Schedule and operational columns

#### `schedule`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `id` | `integer` | Provider schedule-feed row identifier. It is not the application game key; join games on `game_id`. |
| `game_id` | `integer NOT NULL` | Provider external game identifier. Unique by itself live and also part of the `(game_id, game_year)` primary key. |
| `game_type` | `integer` | Provider competition/phase category consumed by the app's game-type filters. Values are provider-controlled; do not assign labels without the existing UI mapping. |
| `gn` | `integer` | Provider game/round number used by inclusive GN filters. It is not globally unique. |
| `team1` | `integer` | Team 1 ID in the schedule namespace. Map through `schedule_team_dict`; it is not necessarily the roster/PBP team ID. |
| `team2` | `integer` | Team 2 ID in the schedule namespace. Same namespace warning as `team1`. |
| `team_name_1` | `varchar(100)` | Provider local/display name for schedule team 1. |
| `team_name_2` | `varchar(100)` | Provider local/display name for schedule team 2. |
| `team_name_eng_1` | `varchar(100)` | English schedule name for team 1; becomes `team_name` in the team-1 schedule perspective. |
| `team_name_eng_2` | `varchar(100)` | English schedule name for team 2; becomes `team_name` in the team-2 schedule perspective. |
| `team_icon_1` | `varchar(130)` | Provider icon path/URL for team 1. Not used as an identity key. |
| `team_icon_2` | `varchar(130)` | Provider icon path/URL for team 2. |
| `game_date_txt` | `varchar(100)` | Original provider date text retained for audit. Use parsed `game_date` for filtering and ordering. |
| `game_year` | `integer NOT NULL` | Season-ending year, for example 2026 for 2025-26. |
| `score_team1` | `integer` | Official final score for schedule team 1. It can intentionally differ from reconstructed event points. |
| `score_team2` | `integer` | Official final score for schedule team 2. |
| `game_time` | `varchar(100)` | Provider tip-time text. It is not normalized to a PostgreSQL time or timestamp. |
| `live_channel` | `varchar(100)` | Provider broadcast/live-channel label. No analytical semantics. |
| `pbp_link` | `varchar(100)` | Provider play-by-play endpoint/link for the game. Operational, not a join key. |
| `is_atc` | `integer` | Raw provider status flag. No active analytical contract; inspect the upstream feed before assigning meaning. |
| `is_live` | `integer` | Raw provider live-status flag. It can become stale after ingestion and is not a completion guarantee. |
| `game_date` | `date` | Parsed date derived from `game_date_txt`; canonical schedule date for date filters and game order. |

#### `schedule_team_dict`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `game_year` | `integer NOT NULL` | Season scope of the mapping. Team IDs can change meaning across seasons. |
| `team_id_schedule` | `integer NOT NULL` | Team ID from `schedule.team1/team2`; with `game_year` forms the primary key. |
| `team_name_rosters` | `text` | Reference name observed in roster/PBP data. Informational aid for validating the mapping. |
| `team_id_rosters` | `integer` | Canonical roster/PBP team ID used by facts and app queries. This is the output needed when normalizing schedule IDs. |
| `team_name_schedule` | `text` | Reference name observed in the schedule feed. |

#### `sched_long`

This view converts each schedule game into two team-perspective rows after both
schedule team IDs map successfully.

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `game_id` | `integer` | Game key from `schedule`. |
| `game_year` | `integer` | Season-ending year from `schedule`. |
| `game_date` | `date` | Parsed schedule date. |
| `gn` | `integer` | Provider game/round number. |
| `game_type` | `integer` | Provider competition/phase category. |
| `team_slot` | `text` | Literal `team1` or `team2`, identifying which schedule side this perspective represents. |
| `team_id` | `integer` | Roster/PBP ID for the perspective team, mapped through `schedule_team_dict`. |
| `team_name` | `varchar(100)` | English schedule name for the perspective team. |
| `team_score` | `integer` | Official final score for the perspective team. Unlike the same-named PBP field, this is a final total. |
| `opp_team_id` | `integer` | Mapped roster/PBP ID for the other team. |
| `opp_team_name` | `varchar(100)` | English schedule name for the opponent. |
| `opp_score` | `integer` | Official final score for the opponent. |

#### `final_schedule_mv`

The first eleven fields preserve the corresponding `sched_long` values except
that `team_slot` is replaced by derived result/location fields.

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `game_id` | `integer` | Game key. Join team-perspective facts using both `game_id` and `team_id`. |
| `game_year` | `integer` | Season-ending year. |
| `game_date` | `date` | Canonical schedule date. |
| `gn` | `integer` | Provider game/round number. |
| `game_type` | `integer` | Provider competition/phase category. |
| `team_id` | `integer` | Perspective team's roster/PBP ID. |
| `team_name` | `varchar(100)` | Perspective team's English schedule name. |
| `opp_team_id` | `integer` | Opponent roster/PBP ID. |
| `opp_team_name` | `varchar(100)` | Opponent English schedule name. |
| `team_score` | `integer` | Perspective team's official final score. |
| `opp_score` | `integer` | Opponent official final score. |
| `margin` | `integer` | Official `team_score - opp_score`; positive is a win margin. |
| `has_won` | `boolean` | `team_score > opp_score`. A tie or missing score does not become a win. |
| `is_home` | `boolean` | True when `team_slot = 'team1'`. This relies on the provider's team-1-as-home convention. |

#### `etl_processed_games`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `game_id` | `integer NOT NULL` | Published game key and primary key. Presence means the full downstream pipeline completed, not merely that raw events were fetched. |
| `game_year` | `integer NOT NULL` | Season recorded when the game was published. |
| `processed_at` | `timestamptz`, default `now()` | Database timestamp at publication/upsert. Use for operations, not game chronology. |

#### `app_meta`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `key` | `text NOT NULL` | Metadata key and primary key. The active app key is `etl_full_last_success`. |
| `value` | `text NOT NULL` | Serialized metadata value. Parse according to the named key; it has no global type contract. |
| `updated_at` | `timestamptz NOT NULL`, default `now()` | Time the key was last inserted or replaced. |

#### `games_ingest_status`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `game_id` | `integer NOT NULL` | Legacy game key and primary key. This table is not part of the current ETL publication contract. |
| `source_hash` | `text NOT NULL` | Legacy digest of a game's source payload, intended for change detection. Current repository code does not maintain it. |
| `ingested_at` | `timestamptz NOT NULL`, default `now()` | Legacy ingestion timestamp. Do not use as current ETL freshness. |

### 15.2 Roster and identity columns

#### `full_rosters`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `player_id` | `integer NOT NULL` | Current canonical/provider player ID at this game/team context. Never join across season/team on this column alone. |
| `apiid` | `text` | Secondary raw provider identifier. It is not used by the active identity or app joins. |
| `firstname` | `text` | Normalized English/display first name. Whitespace and selected known provider defects are corrected during ETL. |
| `lastname` | `text` | Normalized English/display last name; dotted-initial spacing is canonicalized. |
| `firstnamelocal` | `text` | Provider local-language first name. |
| `lastnamelocal` | `text` | Provider local-language last name. |
| `inroster` | `integer` | Raw provider roster-membership flag, typically 0/1. It is not on-court state. |
| `jerseynumber` | `integer` | Jersey number for this provider roster snapshot. Not an identity key. |
| `team_id` | `integer NOT NULL` | Roster/PBP team ID; part of the primary key with `game_id` and `player_id`. |
| `game_id` | `integer NOT NULL` | Game context of the roster row; part of the primary key. |
| `team_name` | `text` | Normalized English team name, preferentially filled from the schedule-side name. |
| `team_name_local` | `text` | Provider local-language team name. |
| `is_on` | `boolean` | Raw roster payload state. Do not use for historical on-court membership; use `lineups_lookup.is_on_verdict`. |
| `game_year` | `integer NOT NULL` | Season-ending year attached from schedule. |
| `starter` | `boolean` | Box-score starter flag. It supplies the starter-count model but does not by itself prove an event lineup is valid. |

#### `player_id_aliases`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `game_year` | `integer NOT NULL` | Season scope of a legacy correction; part of the primary key. |
| `team_id` | `integer NOT NULL` | Team scope of the correction; part of the primary key. |
| `alias_player_id` | `integer NOT NULL` | Erroneous/split source ID to replace; part of the primary key and constrained to differ from the canonical ID. |
| `canonical_player_id` | `integer NOT NULL` | Basketball-table ID that the alias should resolve to. This is not the stable dictionary `identity_id`. |
| `player_name` | `text` | Human-readable evidence label for review, not the matching key. |
| `reason` | `text` | Human explanation/evidence for the mapping or retirement. |
| `active` | `boolean NOT NULL`, default `true` | Whether correction loading should currently apply the mapping. |
| `created_at` | `timestamptz NOT NULL`, default `now()` | Creation audit timestamp. |
| `updated_at` | `timestamptz NOT NULL`, default `now()` | Last manual/sync update timestamp. |
| `canonicalize_base` | `boolean NOT NULL`, default `true` | Whether ETL should rewrite base basketball rows to `canonical_player_id`; false permits identity linkage without base-row canonicalization. |

#### `player_id_game_overrides`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `game_id` | `integer NOT NULL` | Exact game scope; part of the primary key and more specific than a season alias. |
| `game_year` | `integer NOT NULL` | Season of the overridden game, retained for validation/lookups. |
| `team_id` | `integer NOT NULL` | Team scope; part of the primary key. |
| `alias_player_id` | `integer NOT NULL` | Misused/reused source ID in this game; part of the primary key. |
| `canonical_player_id` | `integer NOT NULL` | Correct basketball-table ID for this game/team only. |
| `player_name` | `text` | Human-readable evidence label. |
| `reason` | `text` | Explanation of the provider reuse/correction. |
| `active` | `boolean NOT NULL`, default `true` | Whether this override is currently applied. |
| `created_at` | `timestamptz NOT NULL`, default `now()` | Creation audit timestamp. |
| `updated_at` | `timestamptz NOT NULL`, default `now()` | Last update audit timestamp. |

#### `player_identities`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `identity_id` | `bigint NOT NULL` | Stable dictionary primary key. Generated dictionary IDs start at a high range so they do not collide with ordinary provider IDs. |
| `identity_key` | `text NOT NULL` | Unique stable key used by dictionary synchronization. Treat it as opaque; do not parse it to recover source IDs. |
| `display_name` | `text NOT NULL` | Preferred cross-context display name for the identity. |
| `first_name` | `text` | Preferred English/display first name. |
| `last_name` | `text` | Preferred English/display last name. |
| `first_name_local` | `text` | Preferred local-language first name. |
| `last_name_local` | `text` | Preferred local-language last name. |
| `notes` | `text` | Curator notes about ambiguity, evidence, or identity history. |
| `created_at` | `timestamptz NOT NULL`, default `now()` | Dictionary row creation time. |
| `updated_at` | `timestamptz NOT NULL`, default `now()` | Last dictionary update time. |

#### `player_identity_map`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `map_id` | `bigint NOT NULL` | Surrogate mapping-row primary key. |
| `provider` | `text NOT NULL`, default `segev` | Source system namespace. It is part of logical mapping uniqueness. |
| `game_year` | `integer NOT NULL` | Season scope/context. |
| `team_id` | `integer NOT NULL` | Team context because provider player IDs can be reused or split. |
| `game_id` | `integer` | Exact game for a `game` scope; must be null for `season` scope. |
| `source_player_id` | `integer NOT NULL` | ID as observed in the provider context before correction. |
| `identity_id` | `bigint NOT NULL` | Foreign key to the stable person in `player_identities`. |
| `canonical_player_id` | `integer NOT NULL` | ID that current basketball facts/rosters should expose for this context. |
| `scope` | `text NOT NULL` | Checked value `season` or `game`; controls whether `game_id` must be null or populated. |
| `active` | `boolean NOT NULL`, default `true` | Whether resolution views and correction compatibility output use this mapping. |
| `player_name` | `text` | Source/context name saved as evidence; not the join key. |
| `reason` | `text` | Human explanation for the mapping. |
| `evidence` | `text` | Additional provenance or supporting observations. |
| `mapping_origin` | `text NOT NULL`, default `manual` | How the mapping was created, such as manual curation or legacy synchronization. Treat values as provenance labels, not resolution priority. |
| `created_at` | `timestamptz NOT NULL`, default `now()` | Mapping creation time. |
| `updated_at` | `timestamptz NOT NULL`, default `now()` | Mapping last update time. |

#### `player_identity_aliases_v`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `game_id` | `integer` | Game scope for game mappings; null for season mappings. |
| `game_year` | `integer` | Mapping season. |
| `team_id` | `integer` | Mapping team. |
| `alias_player_id` | `integer` | `source_player_id` projected under the legacy correction name. |
| `canonical_player_id` | `integer` | Current basketball-table target ID. |
| `player_name` | `text` | Mapping name, falling back to the identity dictionary display name. |
| `reason` | `text` | Mapping explanation. |
| `correction_scope` | `text` | `season` or `game`, copied from `player_identity_map.scope`. Only active source-to-canonical differences appear. |

#### `resolved_player_identity_v`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `game_id` | `integer` | Concrete game being resolved. |
| `game_year` | `integer` | Season context. |
| `team_id` | `integer` | Team context. |
| `source_player_id` | `integer` | Provider/context ID before correction. |
| `identity_id` | `bigint` | Stable identity after game mapping, then season mapping, then source-ID fallback. A fallback can equal the source ID cast to bigint. |
| `canonical_player_id` | `integer` | Basketball-table ID after the same precedence; falls back to `source_player_id`. |
| `display_name` | `text` | Game identity name, season identity name, or roster name in that order. |
| `resolution_scope` | `text` | `game`, `season`, or `source`, showing which resolution branch won. |
| `reason` | `text` | Reason from the winning explicit mapping; null on source fallback. |

### 15.3 Event, possession, stint, and lineup columns

#### `actions_clean`: shared cleaned-action columns

These 36 columns are also the complete schema of `subs`. `possessions` and
`pws` inherit them and add the columns documented below.

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `quarter` | `integer` | Provider period number: 1-4 regulation and 5+ overtime. |
| `parameters_team` | `integer` | Team value nested inside the provider action parameters. Prefer normalized top-level `team_id` for joins. |
| `parameters_player` | `text` | Raw nested player value/label. Prefer normalized integer `player_id`. |
| `parameters_type` | `text` | Action subtype: examples include shot forms, `offensive`/`defensive` rebounds, and foul types. Its valid values depend on `type`. |
| `parameters_quarter` | `integer` | Raw period value carried inside provider parameters; `quarter` is the normalized event period. |
| `parameters_player_in` | `integer` | On substitution rows, identifies that the row's `player_id` is entering. ETL normalizes a populated provider flag to the integer actor ID. |
| `parameters_player_out` | `integer` | On substitution rows, identifies that the row's `player_id` is leaving. `possessions` and `pws` have historical `text` type drift for this column. |
| `parameters_current_quarter` | `integer` | Raw provider current-period annotation. It can disagree with the event `quarter`; do not use it for canonical ordering. |
| `parameters_current_quarter_time` | `text` | Raw provider current-clock annotation. Use `quarter_time`/derived seconds for current logic. |
| `parameters_coord_x` | `double precision` | Raw provider shot x coordinate. Coordinate scale/orientation is provider-specific. |
| `parameters_coord_y` | `double precision` | Raw provider shot y coordinate. Corner threes use `y <= 285` when a 3PA has coordinates. |
| `parameters_points` | `integer` | Shot value, normally 2 or 3; made free throws are normalized to 1. It is `numeric` in `possessions`/`pws` because of historical table typing. |
| `parameters_fast_break` | `boolean` | Provider fast-break tag for the action. Null is unknown, not false. |
| `parameters_second_chance_points` | `boolean` | Provider second-chance tag. The name says points, but it is a boolean event annotation. |
| `parameters_points_from_turnover` | `boolean` | Provider points-from-turnover tag. Null is unknown. |
| `parameters_made` | `text` | Outcome, principally `made`, `missed`, or `blocked`. Interpret only for shots/free throws. |
| `parameters_kind` | `text` | Additional provider action-kind label with type-specific meaning. No shared enum is enforced. |
| `parameters_fouled_on` | `text` | Raw provider reference to the fouled player/action context. Not normalized as an identity join. |
| `parameters_free_throws` | `text` | Raw provider free-throw-series descriptor. Use awarded/number fields for arithmetic. |
| `parameters_free_throws_awarded` | `integer` | Raw number of free throws awarded in the trip when supplied. Normally the denominator for `pct_ft`, but known provider rows disagree within a trip; if it is below the current attempt number, ETL uses the attempt number as the effective lower-bound denominator without rewriting this raw field. |
| `parameters_free_throw_number` | `integer` | Sequence number of this free throw within its trip. |
| `id` | `integer NOT NULL` | Provider action ID. Unique only with `game_id`; ordering logic assumes increasing IDs but validates clock reversals separately. |
| `parent_action_id` | `integer` | Related parent action, chiefly a foul for free throws. Provider zero is normalized to the action's own `id`; therefore self-reference can mean “no separate parent.” |
| `user_time` | `text` | Raw provider/log wall-clock string used only as a tie-breaker/audit field. Not game elapsed time. |
| `quarter_time` | `text` | Raw game-clock text, normally `MM:SS`, retained for audit and parsed into seconds. |
| `type` | `text` | Normalized top-level event category such as `shot`, `freeThrow`, `rebound`, `turnover`, `substitution`, `foul`, `assist`, `steal`, `block`, or `deflection`. |
| `player_id` | `integer` | Acting player ID after applicable canonical corrections. It can be null/non-player for administrative events. |
| `team_id` | `integer` | Acting/source event team's roster/PBP ID after corrections. It is not schedule namespace `team1/team2`. |
| `score` | `text` | Raw provider score text. It is unreliable and never the authoritative analytical score. |
| `total_player_points` | `text` | Raw provider running player-points annotation. Active totals are reconstructed from made actions. |
| `game_id` | `integer NOT NULL` | Game key; with `id` forms the primary key. |
| `end_quarter_seconds_remaining` | `double precision` | Seconds remaining in the event period, parsed from `quarter_time`. |
| `end_game_seconds_remaining` | `double precision` | Regulation: seconds remaining through the end of Q4. Overtime: seconds remaining in that OT period only. This mixed contract is why canonical elapsed time also needs `quarter`. |
| `parameters_is_coach_foul` | `integer` | Raw provider coach-foul flag, normally 0/1. It is not a boolean in the live schema. |
| `parameters_is_bench_foul` | `integer` | Raw provider bench-foul flag, normally 0/1. |
| `row_num` | `double precision` | Provider/order annotation retained from the flattened action payload. Do not substitute it for the `(game_id, id)` event key. |

#### `subs`

`subs` has exactly the 36 `actions_clean` columns above with the same live
types and meanings, but contains only substitution actions. It is a transient
performance input to lineup reconstruction, not a separate source of truth.

#### `possessions`

`possessions` contains all 36 shared action columns. Two inherited live types
differ: `parameters_player_out` is `text` and `parameters_points` is `numeric`.
It adds:

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `pct_ft` | `numeric` | Free-throw trip progress: `round(parameters_free_throw_number / effective_ft_awarded, 2)`. The effective denominator normally equals `parameters_free_throws_awarded`; when that raw total is impossibly below the current attempt number, the attempt number is used as the hard lower bound. A value of 1 identifies the final free throw, and published values are constrained by DQ to 0-1. |
| `q_bucket` | `integer` | Period join bucket: 0 for all regulation quarters, otherwise the OT quarter number. Prevents unrelated overtime periods from sharing stint bounds. |
| `team_score` | `numeric` | Current action's point increment: `parameters_points` for a made shot/free throw, otherwise null. It is not a cumulative score. |
| `end_poss` | `boolean` | Initial possession-ending flag: made shot, turnover, made last free throw, or the action before a defensive rebound. |
| `sum_poss_poss` | `numeric` | Count of initial possession-ending candidates in the action's `parent_action_id` group; used to suppress double endings. |
| `sum_block` | `numeric` | Count of blocked outcomes in the parent-action group; used to transfer the ending decision to the following event. |
| `sum_tech` | `numeric` | Count of technical subevents in the parent-action group; technical trips do not automatically end the possession. |
| `final_end_poss` | `boolean` | Corrected possession-ending flag after parent-group rules and end-of-quarter override. This is the flag summed by ratings. |

#### `stints`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `team_id` | `integer NOT NULL` | Team associated with `lineup_hash_offense`; part of the primary key context. |
| `game_id` | `integer NOT NULL` | Game scope. |
| `final_start_seg` | `integer` | Overlap start in source seconds-remaining space, using the smaller offense/defense start clock. Legacy boundary field; canonical durations use elapsed columns downstream. |
| `final_end_seg` | `numeric` | Overlap end in source seconds-remaining space, using the larger offense/defense end clock. |
| `segment_id` | `bigint` | Dense rank of `(final_start_id, final_end_id)` within a game. It repeats across games and can repeat across team perspectives. |
| `lineup_hash_offense` | `text` | Five-player hash for `team_id` during the overlapping interval. The name reflects construction perspective, not necessarily ownership of every joined event. |
| `lineup_hash_defense` | `text` | Five-player hash for `team_id_defense` during the same interval. |
| `team_id_defense` | `integer` | Opponent team paired to `team_id` for the stint. |
| `q_bucket` | `integer` | 0 for regulation, OT quarter number otherwise; used when joining events into stints. |
| `final_start_id` | `integer NOT NULL` | Later of the two teams' lineup interval start action IDs. Part of the primary key. |
| `final_end_id` | `integer NOT NULL` | Earlier of the two teams' lineup interval end action IDs and part of the primary key. It remains a real action ID for FK integrity. The PWS join is half-open `[start, end)` for ordinary intervals; only the final interval receives a temporary `end + 1` join bound so the maximum action is included. |

#### `pws`

`pws` contains all 44 `possessions` columns with the same live types and
meanings. It adds the stint overlap fields below. Historical dropped columns
make the catalog ordinals jump from 52 to 55; there are no unnamed active
columns in between.

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `final_start_seg` | `integer` | Source-clock overlap start inherited from the joined stint. |
| `final_end_seg` | `numeric` | Source-clock overlap end inherited from the joined stint. |
| `segment_id` | `bigint` | Joined game-local stint segment ID. Never use without game/team/lineup context. |
| `lineup_hash_offense` | `text` | Five-player hash for the original `team_id` perspective. |
| `lineup_hash_defense` | `text` | Five-player hash for `team_id_defense`. |
| `team_id_defense` | `integer` | Opposing roster/PBP team ID for the joined stint. |
| `final_start_id` | `integer` | Stint's inclusive starting action ID. |
| `final_end_id` | `integer` | Persisted source-stint end action ID. It is the exclusive boundary for ordinary intervals; the terminal action was included using a temporary `end + 1` bound during the PWS join. |
| `num_starters_offense` | `integer` | Count of box-score starters in `lineup_hash_offense`; valid range normally 0-5. |
| `num_starters_defense` | `integer` | Count of box-score starters in `lineup_hash_defense`. |

#### `lineups_lookup`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `id` | `integer NOT NULL` | Action/state ID at which membership is evaluated. Part of the composite primary key. |
| `game_id` | `integer NOT NULL` | Game scope and primary-key component. |
| `player_id` | `integer NOT NULL` | Roster player whose state is represented. Primary-key component. |
| `team_id` | `integer NOT NULL` | Player/team context. Primary-key component. |
| `quarter` | `integer NOT NULL` | Event period. Primary-key component. |
| `quarter_time` | `text` | Raw event clock associated with this lineup state. |
| `end_game_seconds_remaining` | `integer NOT NULL` | Derived source-clock seconds remaining. Primary-key component. |
| `end_quarter_seconds_remaining` | `double precision` | Seconds remaining in the current period. |
| `is_on_verdict` | `numeric` | Filled membership state: 1 on court, 0 off court, null if no state could be established. |
| `lineup_id` | `text` | Sorted underscore-separated IDs of players whose verdict is 1 at this state. |
| `n_on` | `bigint` | Count of on-court players for this game/team/action state. Valid lineups require exactly 5. |
| `lineup_hash` | `text` | PostgreSQL MD5 of `lineup_id`. Always retain team/season context; a hash is not a global team identity. |
| `game_year` | `integer NOT NULL` | Season-ending year. |
| `num_starters` | `integer` | Number of ON players whose `full_rosters.starter` flag is true. Normally 0-5. |

#### `lineups_lookup_on`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `player_id` | `integer NOT NULL` | One player belonging to the five-player lineup; primary-key component. |
| `lineup_hash` | `text NOT NULL` | Full lineup hash; primary-key component. |
| `team_id` | `integer NOT NULL` | Team scope; primary-key component. |
| `game_year` | `integer NOT NULL` | Season scope; primary-key component. |

#### `sub_lineups`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `team_id` | `integer NOT NULL` | Team scope of both full and subset lineups; primary-key component. |
| `lineup_hash` | `text NOT NULL` | Source five-player lineup containing this subset; primary-key component. |
| `sub_lineup_hash` | `text NOT NULL` | MD5 identity of the sorted subset `lineup_id`; primary-key component. |
| `lineup_id` | `text NOT NULL` | Sorted underscore-separated player IDs for this 2/3/4-player subset. |
| `num_lineup` | `smallint NOT NULL` | Subset cardinality, currently 2, 3, or 4 in this mapping table. Five-player rows are synthesized directly from full hashes elsewhere. |
| `player_ids` | `integer[]`, DB expression from `lineup_id` | Ordered integer array derived from splitting `lineup_id`. ETL omits it on insert. |
| `created_at` | `timestamptz NOT NULL`, default `now()` | Mapping creation timestamp. |
| `game_year` | `integer NOT NULL` | Season scope; primary-key component. |

#### `sub_lineups_stats`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `team_id` | `integer NOT NULL` | Team scope; primary-key component. |
| `sub_lineup_hash` | `text NOT NULL` | 2/3/4-player subset hash or the full lineup hash for five-player rows; primary-key component. |
| `num_lineup` | `smallint NOT NULL` | Number of players, 2-5. |
| `player_ids` | `integer[] NOT NULL` | Sorted player IDs represented by the row. |
| `player_names` | `text[] NOT NULL` | Names aligned to `player_ids`; missing roster names become `#<player_id>`. |
| `player_names_str` | `text NOT NULL` | Comma-separated display form of `player_names`. |
| `off_poss` | `integer NOT NULL` | Season offensive possessions with the player subset contained in the full lineup. |
| `off_pts` | `integer NOT NULL` | Season points scored on those offensive possessions. |
| `off_ppp` | `numeric(6,1)` | `100 * off_pts / off_poss`, rounded to one decimal; null when the implemented null/zero guard yields no rate. |
| `def_poss` | `integer NOT NULL` | Season defensive possessions with the subset on court. |
| `def_pts` | `integer NOT NULL` | Opponent points allowed on those defensive possessions. |
| `def_ppp` | `numeric(6,1)` | `100 * def_pts / def_poss`, rounded to one decimal. |
| `game_year` | `integer NOT NULL` | Season scope; primary-key component. |
| `minutes` | `numeric` | Canonical floor minutes from offense-side segment durations, counted once per mapped full lineup. |
| `off_fg2_made` | `integer`, default 0 | Made offensive 2PT shots with the subset on court. |
| `off_fg2_att` | `integer`, default 0 | Attempted offensive 2PT shots. |
| `off_fg3_made` | `integer`, default 0 | Made offensive 3PT shots. |
| `off_fg3_att` | `integer`, default 0 | Attempted offensive 3PT shots. |
| `def_fg2_made` | `integer`, default 0 | Opponent made 2PT shots while the subset defends. |
| `def_fg2_att` | `integer`, default 0 | Opponent attempted 2PT shots. |
| `def_fg3_made` | `integer`, default 0 | Opponent made 3PT shots. |
| `def_fg3_att` | `integer`, default 0 | Opponent attempted 3PT shots. |

#### `df_pts_poss_lineups_longer_mv`

This is a physical table. Each source event is mirrored into team perspectives;
therefore `player_id` remains the original actor even when the row's `team_id`
is the opponent.

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `quarter` | `integer` | Source event period. |
| `parameters_type` | `text` | Type-specific provider action subtype. |
| `parameters_points` | `numeric` | Shot/free-throw point value. |
| `parameters_made` | `text` | Outcome such as `made`, `missed`, or `blocked`. |
| `id` | `integer` | Source action ID; uniqueness check is `(game_id, team_id, id)` after mirroring. |
| `parent_action_id` | `integer` | Related parent action used especially to group foul/free-throw trips. |
| `type` | `text` | Top-level event category. |
| `player_id` | `integer` | Original acting player after canonical correction. On a mirrored defense row this player usually belongs to the opponent, not row `team_id`. |
| `team_id` | `integer` | Team perspective of this mirrored row. Join schedule using both game and team. |
| `game_id` | `integer` | Game key. |
| `end_game_seconds_remaining` | `double precision` | Source-derived clock used by current clutch membership; see the mixed regulation/OT contract above. |
| `pct_ft` | `numeric` | Free-throw trip progress/final-attempt indicator inherited from `possessions`. |
| `team_score` | `numeric` | Points on the current event. It is preserved on both perspectives: scored points on offense and points allowed on defense. |
| `final_end_poss` | `boolean` | Corrected possession-ending flag, preserved on both perspectives. |
| `segment_id` | `bigint` | Game-local stint segment. Full duration key is `(game_id, team_id, lineup_hash, segment_id)`. |
| `final_end_id` | `integer` | Persisted ending action ID of the source stint. It is normally exclusive; for the final interval the maximum action was attributed with a temporary join-only `end + 1` bound. |
| `own_team_score` | `numeric` | Cumulative event-derived score for row `team_id` through the current action. |
| `opp_team_score` | `numeric` | Cumulative event-derived opponent score through the current action. |
| `event_owner_side` | `text` | Natural event classification (`offense` for shots/assists/turnovers, `defense` for steals/blocks, rebound-dependent). It does not flip on the mirrored row. |
| `type_lineup` | `text` | Event relative to row `team_id`: `offense` or `defense`. Unlike `event_owner_side`, this flips for the mirrored opponent perspective. |
| `lineup_hash` | `text` | Row team's five-player lineup at the event. |
| `num_starters` | `integer` | Starter count for the row lineup; retained as a compatibility field. |
| `own_starters` | `integer` | Number of row-team starters on court. |
| `opp_starters` | `integer` | Number of opponent starters on court. |
| `event_elapsed_seconds` | `numeric` | Canonical seconds elapsed from game start: regulation `2400 - remaining`; OT adds 300 seconds per completed OT period. |
| `clock_regression_seconds` | `numeric` | Positive amount by which this event's canonical time is earlier than the preceding action ID for the same game/team; zero otherwise. Diagnostic, not a corrected clock. |
| `segment_start_elapsed_seconds` | `numeric` | Canonical elapsed time at the minimum action ID of the full segment key. Repeated on every event in the segment. |
| `segment_end_elapsed_seconds` | `numeric` | Next segment's canonical start, or the maximum retained event time for the final segment. |
| `segment_seconds` | `numeric` | Nonnegative `segment_end - segment_start`, repeated on segment events. Deduplicate at the full segment key before summing. |

### 15.4 Lineup and player fact columns

#### `mv_lineup_totals_by_day`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `team_id` | `integer` | Team perspective of the full lineup. |
| `lineup_hash` | `text` | Full five-player lineup hash. |
| `type_lineup` | `text` | `offense` for points/possessions scored or `defense` for opponent production allowed. |
| `g_date` | `date` | Schedule game date. Despite the historical “by day” name, `game_id` remains available. |
| `game_id` | `integer` | Game key. |
| `game_year` | `integer` | Season-ending year. |
| `total_poss` | `numeric` | Sum of corrected possession-ending flags for this lineup/game/side. |
| `total_pts` | `numeric` | Points scored (`offense`) or allowed (`defense`). |
| `fg2_made` | `numeric` | Made 2PT shots in this side/perspective. |
| `fg2_att` | `numeric` | Attempted 2PT shots. |
| `fg3_made` | `numeric` | Made 3PT shots. |
| `fg3_att` | `numeric` | Attempted 3PT shots. |
| `num_starters` | `integer` | Maximum row-lineup starter count across constituent segment events; normally 0-5. |
| `minutes` | `numeric` | Canonical segment minutes, deliberately populated from offense-side rows so a segment is counted once. Defense-side aggregate rows can therefore be null. |

#### `lineup_four_factors_by_game`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `lineup_hash` | `text` | Full five-player lineup hash. |
| `team_id` | `integer` | Team perspective. |
| `game_id` | `integer` | Game key. |
| `game_year` | `integer` | Season-ending year. |
| `type_lineup` | `text` | `offense` or `defense` perspective. |
| `num_starters` | `integer` | Starter count for the relevant row lineup. |
| `total_points` | `numeric` | Points scored/allowed in this lineup-side bucket. |
| `total_poss` | `bigint` | Corrected possession endings. Denominator for PPP and TOV%. |
| `ts_poss_count` | `bigint` | Shot attempts plus distinct personal-foul free-throw trips. Denominator component for four-factor TS. |
| `oreb_count` | `bigint` | Offensive rebounds observed in this perspective. On defense rows this is opponent OREB allowed. |
| `oreb_opportunities` | `bigint` | Missed/blocked shots plus missed final personal-foul free throws. Denominator for OREB%. |
| `tov_count` | `bigint` | Turnover events in the perspective. |
| `total_ft_attempts` | `bigint` | Free-throw action count. Numerator for FTR. |
| `total_fga` | `bigint` | Shot action count. Denominator for eFG and FTR. |
| `total_fgm` | `bigint` | Made field-goal action count. |
| `total_fg3_made` | `bigint` | Made 3PT field goals; the extra half-weight in eFG uses this field. |
| `minutes` | `numeric` | Canonical segment minutes counted on offense-side rows only to prevent duplicate floor time. |

#### `player_four_factors_by_game`

The row grain splits a player by game, ON/OFF membership, event perspective,
and starter context. Count columns describe the entire lineup/team production
while that player is ON or OFF; the two `player_*` usage fields are the
exception and describe actions attributed to that player.

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `player_id` | `integer` | Player being evaluated, not necessarily the actor of each aggregated event. |
| `team_id` | `integer` | Player's team and event perspective. |
| `game_id` | `integer` | Game key. |
| `game_year` | `integer` | Season-ending year. |
| `is_on_key` | `integer` | 1 when the player belongs to `lineup_hash`; 0 for the off-floor comparison bucket. |
| `type_lineup` | `text` | `offense` or `defense` perspective of the player's team. |
| `num_starters` | `integer` | Compatibility starter field; currently copied from `own_starters`. |
| `own_starters` | `integer` | Number of starters in the player's team lineup. |
| `opp_starters` | `integer` | Number of starters in the opponent lineup. |
| `total_points` | `numeric` | Team points scored/allowed in this split. |
| `total_poss` | `bigint` | Team corrected possession endings in this split. |
| `ts_poss_count` | `bigint` | Team shot attempts plus distinct personal-foul free-throw trips. |
| `oreb_count` | `bigint` | Offensive-rebound count in the row perspective. |
| `oreb_opportunities` | `bigint` | OREB opportunity denominator. |
| `tov_count` | `bigint` | Team turnover count in the row perspective. |
| `steal_count` | `bigint` | Steal event count. Used from defensive perspective for disruptions. |
| `deflection_count` | `bigint` | Deflection event count. Used with steals for disruptions. |
| `total_ft_attempts` | `bigint` | Team free-throw attempts. |
| `total_fga` | `bigint` | Team field-goal attempts. |
| `total_fgm` | `bigint` | Team made field goals. |
| `total_fg3_made` | `bigint` | Team made 3PT field goals. |
| `player_ts_poss_count` | `bigint` | This player's offensive shot attempts plus distinct personal-foul trips; usage numerator component. |
| `player_tov_count` | `bigint` | This player's offensive turnovers; usage numerator component. |
| `minutes` | `double precision` | Canonical segment minutes for this ON/OFF/starter split, populated on offense-side rows. Historical type differs from `onoff_minutes`. |
| `fg2_made` | `integer` | Team made 2PT shots in the ON/OFF/perspective split. |
| `fg2_att` | `integer` | Team attempted 2PT shots. |
| `fg3_made` | `integer` | Team made 3PT shots. |
| `fg3_att` | `integer` | Team attempted 3PT shots. |
| `layup_made` | `integer` | Made 2PT shots tagged exactly `lay-up`. |
| `layup_att` | `integer` | Attempted 2PT shots tagged exactly `lay-up`. |
| `dunk_made` | `integer` | Made 2PT shots tagged `dunk` or `allyhoop`. |
| `dunk_att` | `integer` | Attempted 2PT `dunk`/`allyhoop` shots. |
| `c3_made` | `integer` | Made known corner 3s (`shot_zones.is_corner3 = true`). |
| `c3_att` | `integer` | Attempted known corner 3s. |
| `c3_known_att` | `integer` | 3PA with any coordinate-derived zone row; denominator for corner share. |
| `onoff_minutes` | `numeric` | Canonical ON/OFF floor minutes assembled with shot-profile splits; meaningful on offense-side rows and retained separately for incremental summary refreshes. |

#### `onoff_default_mv`

This physical season-summary table uses quoted display columns. In all patterned
names, `off`/`def` first means team offense/defense and `on`/`off` second means
the player is on/off the floor.

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `Team` | `text` | Season/team roster display name. Quoted identifier; case and space matter in SQL. |
| `Year` | `integer` | Season-ending year. Quoted identifier. |
| `First Name` | `text` | Canonicalized roster first name for display. |
| `Last Name` | `text` | Canonicalized roster last name for display. |
| `Net RTG Diff` | `numeric` | `(Off ON PPP - Off OFF PPP) - (Def ON PPP - Def OFF PPP)`, equivalent to on-court net minus off-court net. Higher is better. |
| `Off ON Diff` | `numeric` | `Off ON PPP - Off OFF PPP`; offensive change when the player is on. |
| `Def ON Diff` | `numeric` | `Def ON PPP - Def OFF PPP`; positive means more points allowed and is therefore worse defense. |
| `Off ON PPP` | `numeric` | Team offensive points per 100 possessions with the player on. |
| `Def ON PPP` | `numeric` | Opponent points per 100 possessions with the player on. Lower is better. |
| `On Net RTG` | `numeric` | `Off ON PPP - Def ON PPP`. |
| `Off OFF PPP` | `numeric` | Team offensive points per 100 possessions with the player off. First `Off` means offense; second `OFF` means off-floor. |
| `Def OFF PPP` | `numeric` | Opponent points per 100 possessions with the player off. |
| `Off Net RTG` | `numeric` | Off-floor net rating: `Off OFF PPP - Def OFF PPP`. The label does not mean offensive net rating. |
| `ON Poss` | `bigint` | Maximum constituent ON possession count across offense/defense split rows; used as the summary ON sample size. |
| `OFF Poss` | `bigint` | Maximum constituent OFF possession count across offense/defense split rows. |
| `minutes` | `numeric` | Canonical on-court season minutes, summing offense-side segment duration once. |
| `pr_net` | `double precision` | 0-1 season percentile of `Net RTG Diff`; higher is better. |
| `pr_off_on` | `double precision` | 0-1 season percentile of `Off ON PPP`; higher offense is better. |
| `pr_off_off` | `double precision` | 0-1 season percentile of `Off OFF PPP`. |
| `pr_def_on_inv` | `double precision` | 0-1 inverted percentile of `Def ON PPP`; higher means fewer points allowed/better. |
| `pr_def_off_inv` | `double precision` | Inverted percentile of `Def OFF PPP`. |
| `pr_off_on_d` | `double precision` | Percentile of `Off ON Diff`; higher means a larger offensive improvement. |
| `pr_def_on_d` | `double precision` | Raw percentile of `Def ON Diff`; higher generally means a larger increase in points allowed and is worse. |
| `pr_def_on_d_inv` | `double precision` | Inverted defensive-diff percentile; higher is better-oriented. |
| `pr_on_net` | `double precision` | 0-1 season percentile of `On Net RTG`. |
| `pr_off_net` | `double precision` | 0-1 season percentile of off-floor net rating. |

Ordinary shooting counts:

| Column | Live type | Exact count |
|---|---|---|
| `off_on_fg2_made` | `bigint` | Team made 2PT, offense, player ON. |
| `off_on_fg2_att` | `bigint` | Team 2PA, offense, player ON. |
| `off_on_fg3_made` | `bigint` | Team made 3PT, offense, player ON. |
| `off_on_fg3_att` | `bigint` | Team 3PA, offense, player ON. |
| `off_off_fg2_made` | `bigint` | Team made 2PT, offense, player OFF. |
| `off_off_fg2_att` | `bigint` | Team 2PA, offense, player OFF. |
| `off_off_fg3_made` | `bigint` | Team made 3PT, offense, player OFF. |
| `off_off_fg3_att` | `bigint` | Team 3PA, offense, player OFF. |
| `def_on_fg2_made` | `bigint` | Opponent made 2PT, player ON defense. |
| `def_on_fg2_att` | `bigint` | Opponent 2PA, player ON defense. |
| `def_on_fg3_made` | `bigint` | Opponent made 3PT, player ON defense. |
| `def_on_fg3_att` | `bigint` | Opponent 3PA, player ON defense. |
| `def_off_fg2_made` | `bigint` | Opponent made 2PT, player OFF defense. |
| `def_off_fg2_att` | `bigint` | Opponent 2PA, player OFF defense. |
| `def_off_fg3_made` | `bigint` | Opponent made 3PT, player OFF defense. |
| `def_off_fg3_att` | `bigint` | Opponent 3PA, player OFF defense. |

Shot-profile counts use exact `lay-up`, combined `dunk`/`allyhoop`, and the
coordinate-known corner-three definitions from section 4:

| Column | Live type | Exact count |
|---|---|---|
| `off_on_layup_made` | `bigint` | Team made layups, offense, player ON. |
| `off_on_layup_att` | `bigint` | Team layup attempts, offense, player ON. |
| `off_on_dunk_made` | `bigint` | Team made dunks/allyhoops, offense, player ON. |
| `off_on_dunk_att` | `bigint` | Team dunk/allyhoop attempts, offense, player ON. |
| `off_on_c3_made` | `bigint` | Team made known corner 3s, offense, player ON. |
| `off_on_c3_att` | `bigint` | Team known corner 3PA, offense, player ON. |
| `off_on_c3_known_att` | `bigint` | Team 3PA with known zone, offense, player ON. |
| `off_off_layup_made` | `bigint` | Team made layups, offense, player OFF. |
| `off_off_layup_att` | `bigint` | Team layup attempts, offense, player OFF. |
| `off_off_dunk_made` | `bigint` | Team made dunks/allyhoops, offense, player OFF. |
| `off_off_dunk_att` | `bigint` | Team dunk/allyhoop attempts, offense, player OFF. |
| `off_off_c3_made` | `bigint` | Team made known corner 3s, offense, player OFF. |
| `off_off_c3_att` | `bigint` | Team known corner 3PA, offense, player OFF. |
| `off_off_c3_known_att` | `bigint` | Team 3PA with known zone, offense, player OFF. |
| `def_on_layup_made` | `bigint` | Opponent made layups, player ON defense. |
| `def_on_layup_att` | `bigint` | Opponent layup attempts, player ON defense. |
| `def_on_dunk_made` | `bigint` | Opponent made dunks/allyhoops, player ON defense. |
| `def_on_dunk_att` | `bigint` | Opponent dunk/allyhoop attempts, player ON defense. |
| `def_on_c3_made` | `bigint` | Opponent made known corner 3s, player ON defense. |
| `def_on_c3_att` | `bigint` | Opponent known corner 3PA, player ON defense. |
| `def_on_c3_known_att` | `bigint` | Opponent 3PA with known zone, player ON defense. |
| `def_off_layup_made` | `bigint` | Opponent made layups, player OFF defense. |
| `def_off_layup_att` | `bigint` | Opponent layup attempts, player OFF defense. |
| `def_off_dunk_made` | `bigint` | Opponent made dunks/allyhoops, player OFF defense. |
| `def_off_dunk_att` | `bigint` | Opponent dunk/allyhoop attempts, player OFF defense. |
| `def_off_c3_made` | `bigint` | Opponent made known corner 3s, player OFF defense. |
| `def_off_c3_att` | `bigint` | Opponent known corner 3PA, player OFF defense. |
| `def_off_c3_known_att` | `bigint` | Opponent 3PA with known zone, player OFF defense. |
| `player_id` | `integer` | Player identity used by current basketball facts. |
| `team_id` | `integer` | Team scope; with `Year` and `player_id` forms the unique key. |

#### `player_advanced_stats_mv`

The unquoted `off_on_*`, `off_off_*`, `def_on_*`, and `def_off_*` rate columns
are fractions on a 0-1 scale. The quoted `* Diff` columns multiply the direct
ON-minus-OFF fraction by 100 and are percentage-point differences.

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `player_id` | `integer` | Player being evaluated. |
| `team_id` | `integer` | Team scope. |
| `firstname` | `text` | Season/team roster first name. |
| `lastname` | `text` | Season/team roster last name. |
| `team_name` | `text` | Season/team roster display name. |
| `game_year` | `integer` | Season-ending year; with team/player forms the unique key. |
| `off_on_ts` | `numeric` | Team four-factor TS fraction on offense with player ON. |
| `off_off_ts` | `numeric` | Team TS fraction on offense with player OFF. |
| `def_on_ts` | `numeric` | Opponent TS fraction while player is ON defense; lower is better. |
| `def_off_ts` | `numeric` | Opponent TS fraction while player is OFF. |
| `off_on_efg` | `numeric` | Team eFG fraction with player ON offense. |
| `off_off_efg` | `numeric` | Team eFG fraction with player OFF offense. |
| `def_on_efg` | `numeric` | Opponent eFG fraction with player ON defense. |
| `def_off_efg` | `numeric` | Opponent eFG fraction with player OFF defense. |
| `off_on_oreb` | `numeric` | Team OREB opportunity conversion fraction with player ON offense. |
| `off_off_oreb` | `numeric` | Team OREB fraction with player OFF offense. |
| `def_on_oreb` | `numeric` | Opponent OREB fraction with player ON defense. |
| `def_off_oreb` | `numeric` | Opponent OREB fraction with player OFF defense. |
| `off_on_tov` | `numeric` | Team turnovers per possession with player ON offense. |
| `off_off_tov` | `numeric` | Team turnovers per possession with player OFF offense. |
| `def_on_tov` | `numeric` | Opponent turnovers per possession with player ON defense. Higher can be favorable defensively. |
| `def_on_disruptions` | `numeric` | Team steals plus deflections per defensive possession with player ON. |
| `def_off_tov` | `numeric` | Opponent turnovers per possession with player OFF defense. |
| `def_off_disruptions` | `numeric` | Team defensive disruptions per possession with player OFF. |
| `off_on_ftr` | `numeric` | Team FTA/FGA fraction with player ON offense. |
| `off_off_ftr` | `numeric` | Team FTA/FGA fraction with player OFF offense. |
| `def_on_ftr` | `numeric` | Opponent FTA/FGA fraction with player ON defense. |
| `def_off_ftr` | `numeric` | Opponent FTA/FGA fraction with player OFF defense. |
| `off_on_poss` | `bigint` | Offensive possessions with player ON. |
| `off_off_poss` | `bigint` | Offensive possessions with player OFF. |
| `def_on_poss` | `bigint` | Defensive possessions with player ON. |
| `def_off_poss` | `bigint` | Defensive possessions with player OFF. |
| `Off eFG% Diff` | `numeric` | `100 * (off_on_efg - off_off_efg)`. |
| `Off TS% Diff` | `numeric` | `100 * (off_on_ts - off_off_ts)`. |
| `Off OREB% Diff` | `numeric` | `100 * (off_on_oreb - off_off_oreb)`. |
| `Off TOV% Diff` | `numeric` | `100 * (off_on_tov - off_off_tov)`; a negative value is normally favorable. |
| `Off FTR Diff` | `numeric` | `100 * (off_on_ftr - off_off_ftr)`. |
| `Def eFG% Diff` | `numeric` | `100 * (def_on_efg - def_off_efg)`; negative is normally favorable. |
| `Def TS% Diff` | `numeric` | `100 * (def_on_ts - def_off_ts)`; negative is normally favorable. |
| `Def OREB% Diff` | `numeric` | `100 * (def_on_oreb - def_off_oreb)`; negative opponent OREB change is favorable. |
| `Def TOV% Diff` | `numeric` | `100 * (def_on_tov - def_off_tov)`; positive opponent turnover change can be favorable. |
| `Def Disruptions/100 Diff` | `numeric` | `100 * (def_on_disruptions - def_off_disruptions)`. |
| `Def FTR Diff` | `numeric` | `100 * (def_on_ftr - def_off_ftr)`; negative opponent FTR change is favorable. |
| `pr_diff_off_efg` | `double precision` | 0-1 season percentile of offensive eFG difference; higher is better. |
| `pr_diff_off_ts` | `double precision` | Percentile of offensive TS difference. |
| `pr_diff_off_oreb` | `double precision` | Percentile of offensive OREB difference. |
| `pr_diff_def_efg` | `double precision` | Better-oriented percentile of defensive eFG difference; SQL orders the raw difference descending so lower allowed-eFG changes score higher. |
| `pr_diff_def_ts` | `double precision` | Better-oriented percentile of defensive TS difference. |

#### `player_traditional_stats_mv`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `game_year` | `integer` | Season-ending year; part of the unique season/team/player key. |
| `team_id` | `integer` | Team scope. |
| `player_id` | `integer` | Player actor/on-floor identity. |
| `team_name` | `text` | Season roster team display name. |
| `firstname` | `text` | Canonicalized roster first name. |
| `lastname` | `text` | Canonicalized roster last name. |
| `player_name` | `text` | Trimmed concatenation of canonicalized first and last name. |
| `gp` | `integer` | Distinct games containing an offensive possession end while the player is mapped on floor. It is participation-derived, not simply roster appearances. |
| `poss_on_floor` | `integer` | Distinct team offensive possession endings while the player is on court. |
| `minutes` | `numeric(10,1)` | Canonical on-court segment minutes, rounded to one decimal. |
| `pts` | `integer` | Made field-goal point values plus made free throws attributed to the player on offense. |
| `reb` | `integer` | `oreb + dreb`. |
| `oreb` | `integer` | Player offensive rebounds from offense-perspective rebound events. |
| `dreb` | `integer` | Player defensive rebounds from defense-perspective rebound events. |
| `ast` | `integer` | Player assists on offense. |
| `stl` | `integer` | Player steals from defense-perspective events. |
| `blk` | `integer` | Player blocks from defense-perspective events. |
| `dfl` | `integer` | Player deflections from defense-perspective events. |
| `tov` | `integer` | Player turnovers on offense. |
| `fgm` | `integer` | Made field goals. |
| `fga` | `integer` | Field-goal attempts. |
| `3pm` | `integer` | Made 3PT field goals. Must be quoted in SQL because it begins with a digit. |
| `3pa` | `integer` | Attempted 3PT field goals; quoted identifier. |
| `ftm` | `integer` | Made free throws. |
| `fta` | `integer` | Free-throw attempts. |
| `fg_pct` | `numeric` | `100 * fgm / fga`, one decimal; null with zero FGA. |
| `tp_pct` | `numeric` | `100 * 3pm / 3pa`, one decimal; this is 3PT%, despite the `tp` abbreviation. |
| `ft_pct` | `numeric` | `100 * ftm / fta`, one decimal. |
| `efg` | `numeric` | `100 * (fgm + 0.5 * 3pm) / fga`, one decimal. |
| `ts` | `numeric` | Traditional `100 * pts / (2 * (fga + 0.44 * fta))`, one decimal. This differs from four-factor TS. |
| `usg_pct` | `numeric` | Team/on-floor-adjusted usage: player `(TS possessions + TOV)` share scaled by team possessions versus player on-floor possessions; one decimal. |

### 15.5 Team aggregate and shot-zone columns

#### `team_metrics_by_game_mv`

This physical table combines official schedule context, event-derived
ratings/factors, canonical minutes, and traditional team event totals. The
`*_raw` columns exist so filtered/multi-game queries can sum numerators and
denominators before dividing.

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `game_year` | `integer` | Season-ending year; part of the unique key. |
| `game_id` | `integer` | Game key; part of the unique key. |
| `game_date` | `date` | Official schedule date. |
| `gn` | `integer` | Provider game/round number. |
| `game_type` | `integer` | Provider competition/phase category. |
| `team_id` | `integer` | Team perspective; part of the unique key. |
| `team_name` | `varchar(100)` | Schedule English team name. |
| `opp_team_id` | `integer` | Opponent roster/PBP team ID. |
| `opp_team_name` | `varchar(100)` | Opponent schedule name. |
| `is_home` | `boolean` | True when this team is schedule team 1. |
| `has_won` | `boolean` | Official schedule result for this perspective. |
| `team_score` | `integer` | Official final team score, not event-derived `off_points_raw`. |
| `opp_score` | `integer` | Official final opponent score. |
| `margin` | `integer` | Official `team_score - opp_score`. |
| `off_ppp` | `numeric` | `100 * off_points_raw / off_poss_raw`, rounded to one decimal. |
| `def_ppp` | `numeric` | `100 * def_points_raw / def_poss_raw`, rounded to one decimal. |
| `net_rtg` | `numeric` | `off_ppp - def_ppp`, calculated from unrounded ratios and rounded to one decimal. |
| `off_poss` | `integer` | Integer offensive possession total for display/filtering. |
| `def_poss` | `integer` | Integer defensive possession total. |
| `off_points_raw` | `numeric` | Event-derived points scored; additive numerator for offensive rating/TS. |
| `def_points_raw` | `numeric` | Event-derived opponent points allowed. |
| `off_poss_raw` | `numeric` | Additive offensive corrected possession endings. |
| `def_poss_raw` | `numeric` | Additive defensive corrected possession endings. |
| `off_ts_poss_raw` | `numeric` | Offensive four-factor TS possession count: FGA plus distinct personal-foul trips. |
| `def_ts_poss_raw` | `numeric` | Opponent/defensive TS possession count. |
| `off_oreb_count_raw` | `numeric` | Team offensive rebounds. |
| `def_oreb_count_raw` | `numeric` | Opponent offensive rebounds allowed. |
| `off_oreb_opp_raw` | `numeric` | Team offensive-rebound opportunities. |
| `def_oreb_opp_raw` | `numeric` | Opponent offensive-rebound opportunities. |
| `off_tov_raw` | `numeric` | Team offensive turnovers. |
| `def_tov_raw` | `numeric` | Opponent turnovers while this team defends. |
| `off_fta_raw` | `numeric` | Team free-throw attempts. |
| `def_fta_raw` | `numeric` | Opponent free-throw attempts. |
| `off_fga_raw` | `numeric` | Team field-goal attempts. |
| `def_fga_raw` | `numeric` | Opponent field-goal attempts. |
| `off_fgm_raw` | `numeric` | Team made field goals. |
| `def_fgm_raw` | `numeric` | Opponent made field goals. |
| `off_fg3m_raw` | `numeric` | Team made 3PT field goals. |
| `def_fg3m_raw` | `numeric` | Opponent made 3PT field goals. |
| `off_ts` | `numeric` | Offensive four-factor TS%, `100 * points / (2 * ts_poss)`, one decimal. |
| `off_efg` | `numeric` | Offensive eFG%, one decimal. |
| `off_oreb` | `numeric` | Offensive OREB%, one decimal. |
| `off_tov` | `numeric` | Offensive turnovers per possession, percent scale. |
| `off_ftr` | `numeric` | Offensive FTA/FGA, percent scale. |
| `def_ts` | `numeric` | Opponent TS% allowed, four-factor definition. |
| `def_efg` | `numeric` | Opponent eFG% allowed. |
| `def_oreb` | `numeric` | Opponent OREB% allowed. |
| `def_tov` | `numeric` | Opponent turnovers per possession. Higher can be favorable. |
| `def_ftr` | `numeric` | Opponent FTA/FGA allowed. |
| `off_minutes` | `numeric(10,1)` | Canonical lineup minutes accumulated from offense-side rows. |
| `def_minutes` | `numeric(10,1)` | Defensive-perspective mirror of the same once-counted canonical team floor duration exposed as `off_minutes`. It intentionally equals `off_minutes`; it is not independently summed from defense fact rows. |
| `pts` | `integer` | Event-derived team points from made offensive field goals/free throws. |
| `reb` | `integer` | Team offensive rebounds plus team defensive rebounds selected through mirrored perspective logic. |
| `ast` | `integer` | Team assists. |
| `stl` | `integer` | Team steals. |
| `blk` | `integer` | Team blocks. |
| `dfl` | `integer` | Team deflections. |
| `tov` | `integer` | Team offensive turnovers. |
| `fgm` | `integer` | Team made field goals. |
| `fga` | `integer` | Team field-goal attempts. |
| `3pm` | `integer` | Team made 3PT field goals; quoted SQL identifier. |
| `3pa` | `integer` | Team 3PT attempts; quoted SQL identifier. |
| `ftm` | `integer` | Team made free throws. |
| `fta` | `integer` | Team free-throw attempts. |
| `fg_pct` | `numeric` | Traditional team FG%, one decimal. |
| `tp_pct` | `numeric` | Traditional team 3PT%, one decimal. |
| `ft_pct` | `numeric` | Traditional team FT%, one decimal. |
| `efg` | `numeric` | Traditional team eFG%, one decimal. Same formula as four-factor eFG but sourced from traditional event totals. |
| `ts` | `numeric` | Traditional team TS% using `FGA + 0.44 * FTA`, one decimal. |

#### `team_metrics_rolling_mv`

Columns 1-69 are the complete `team_metrics_by_game_mv` contract above with
identical names, live types, and meanings. The materialized view adds the
following columns. Rolling values are simple arithmetic means of already
calculated per-game rates, not possession-weighted recomputations. Early-season
rows use the available partial window.

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `team_game_seq` | `bigint` | 1-based chronological team-game number within `(game_year, team_id)`, ordered by date then game ID. |
| `off_ppp_r3` | `numeric` | Mean offensive rating over current and previous two team games. |
| `def_ppp_r3` | `numeric` | Mean defensive rating over the rolling three-game window. |
| `net_rtg_r3` | `numeric` | Mean per-game net rating over rolling three. |
| `off_ts_r3` | `numeric` | Mean offensive four-factor TS% over rolling three. |
| `off_efg_r3` | `numeric` | Mean offensive eFG% over rolling three. |
| `def_ts_r3` | `numeric` | Mean opponent TS% over rolling three. |
| `def_efg_r3` | `numeric` | Mean opponent eFG% over rolling three. |
| `off_ppp_r5` | `numeric` | Mean offensive rating over current and previous four games. |
| `def_ppp_r5` | `numeric` | Mean defensive rating over rolling five. |
| `net_rtg_r5` | `numeric` | Mean net rating over rolling five. |
| `off_ts_r5` | `numeric` | Mean offensive TS% over rolling five. |
| `off_efg_r5` | `numeric` | Mean offensive eFG% over rolling five. |
| `def_ts_r5` | `numeric` | Mean opponent TS% over rolling five. |
| `def_efg_r5` | `numeric` | Mean opponent eFG% over rolling five. |
| `off_ppp_r10` | `numeric` | Mean offensive rating over current and previous nine games. |
| `def_ppp_r10` | `numeric` | Mean defensive rating over rolling ten. |
| `net_rtg_r10` | `numeric` | Mean net rating over rolling ten. |
| `off_ts_r10` | `numeric` | Mean offensive TS% over rolling ten. |
| `off_efg_r10` | `numeric` | Mean offensive eFG% over rolling ten. |
| `def_ts_r10` | `numeric` | Mean opponent TS% over rolling ten. |
| `def_efg_r10` | `numeric` | Mean opponent eFG% over rolling ten. |
| `prev_net_rtg_r5` | `numeric` | Previous game row's rolling-five net rating for the same team-season. |
| `delta_net_rtg_r5` | `numeric` | Current `net_rtg_r5 - prev_net_rtg_r5`; momentum change, not current-game net rating. |

#### `team_ppp_ratings_mv`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `game_year` | `integer` | Season-ending year. |
| `team_id` | `integer` | Team scope; logically one row per season/team, though the live index is not unique. |
| `team_name` | `text` | Roster-derived team display name. |
| `off_ppp` | `numeric` | Season offensive points per 100 possessions, rounded through the historical `round(raw, 3) * 100` expression. |
| `def_ppp` | `numeric` | Season opponent points per 100 possessions. Lower is better. |
| `net_rtg` | `numeric` | Season `off_ppp - def_ppp`. |
| `games_played` | `bigint` | Distinct games present in central fact aggregation. |
| `wins` | `bigint` | Official schedule wins. |
| `losses` | `bigint` | Official schedule losses. |
| `off_poss` | `bigint` | Season offensive corrected possession endings. |
| `def_poss` | `bigint` | Season defensive corrected possession endings. |
| `rank_net_rtg` | `bigint` | Dense season rank, descending net rating; 1 is best. |
| `rank_off_ppp` | `bigint` | Dense season rank, descending offensive rating; 1 is best. |
| `rank_def_ppp` | `bigint` | Dense season rank, ascending defensive rating; 1 allows the fewest points per 100. |
| `off_fga` | `bigint` | Team field-goal attempts on offense. |
| `off_layup_att` | `bigint` | Team offensive 2PA tagged `lay-up`. |
| `off_dunk_att` | `bigint` | Team offensive 2PA tagged `dunk` or `allyhoop`. |
| `off_fg3_att` | `bigint` | Team offensive 3PA. |
| `off_c3_att` | `bigint` | Team known corner 3PA. |
| `off_c3_known_att` | `bigint` | Team 3PA with any known zone; denominator for offensive corner share. |
| `def_fga` | `bigint` | Opponent field-goal attempts while this team defends. |
| `def_layup_att` | `bigint` | Opponent layup attempts allowed. |
| `def_dunk_att` | `bigint` | Opponent dunk/allyhoop attempts allowed. |
| `def_fg3_att` | `bigint` | Opponent 3PA allowed. |
| `def_c3_att` | `bigint` | Opponent known corner 3PA allowed. |
| `def_c3_known_att` | `bigint` | Opponent 3PA with known zone. |

#### `team_four_factors_mv`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `team_id` | `integer` | Team scope; with season forms the unique key. |
| `game_year` | `integer` | Season-ending year. |
| `team_name` | `text` | Roster-derived team display name. |
| `off_ts` | `numeric` | Offensive four-factor TS%, 0-100. |
| `off_efg` | `numeric` | Offensive eFG%, 0-100. |
| `off_oreb` | `numeric` | Offensive OREB%, 0-100. |
| `off_tov` | `numeric` | Offensive turnovers per possession, 0-100. |
| `off_ftr` | `numeric` | Offensive FTA/FGA, percent scale. |
| `off_ppp` | `numeric` | Offensive points per 100 possessions. |
| `off_poss` | `integer` | Offensive possessions. |
| `off_pts` | `integer` | Event-derived offensive points. |
| `off_ts_poss` | `integer` | Offensive shot attempts plus distinct personal-foul trips. |
| `off_oreb_cnt` | `integer` | Offensive rebounds. |
| `off_oreb_opps` | `integer` | Offensive-rebound opportunities. |
| `off_tov_cnt` | `integer` | Offensive turnovers. |
| `off_fta` | `integer` | Offensive free-throw attempts. |
| `off_fga_cnt` | `integer` | Offensive field-goal attempts. |
| `off_fgm_cnt` | `integer` | Offensive made field goals. |
| `off_fg3m_cnt` | `integer` | Offensive made 3PT field goals. |
| `def_ts` | `numeric` | Opponent four-factor TS% allowed. |
| `def_efg` | `numeric` | Opponent eFG% allowed. |
| `def_oreb` | `numeric` | Opponent OREB% allowed. |
| `def_tov` | `numeric` | Opponent turnovers per possession. |
| `def_ftr` | `numeric` | Opponent FTA/FGA allowed. |
| `def_ppp` | `numeric` | Opponent points per 100 possessions. |
| `def_poss` | `integer` | Defensive possessions. |
| `def_pts` | `integer` | Opponent points allowed. |
| `def_ts_poss` | `integer` | Opponent shot attempts plus personal-foul trips. |
| `def_oreb_cnt` | `integer` | Opponent offensive rebounds. |
| `def_oreb_opps` | `integer` | Opponent offensive-rebound opportunities. |
| `def_tov_cnt` | `integer` | Opponent turnovers. |
| `def_fta` | `integer` | Opponent free-throw attempts. |
| `def_fga_cnt` | `integer` | Opponent field-goal attempts. |
| `def_fgm_cnt` | `integer` | Opponent made field goals. |
| `def_fg3m_cnt` | `integer` | Opponent made 3PT field goals. |
| `net_rtg` | `numeric` | `off_ppp - def_ppp`, rounded to one decimal. |

#### `shot_zones`

| Column | Live type | Meaning, source, and caveat |
|---|---|---|
| `game_id` | `integer NOT NULL` | Game key and primary-key component. |
| `id` | `integer NOT NULL` | Source shot action ID and primary-key component. |
| `is_corner3` | `boolean NOT NULL` | For coordinate-known 3PA, true when raw provider y is `<= 285`, otherwise false. Absence of a row means unknown zone, not false. |

### 15.6 Non-contract staging tables

The live catalog also contains one `__stage_schedule_*` table and eleven
`__stage_stints_*` tables. They were created with `LIKE` by `upsert_by_like()`
and therefore inherit the `schedule` or `stints` column contracts documented
above as of their creation. Their dynamic names, emptiness, and abandoned
transaction status make them anomalies rather than queryable application
relations. Do not add dependencies on them and do not drop them without the
active-session check in section 11.
