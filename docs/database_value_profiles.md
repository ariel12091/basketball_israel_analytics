# Database Live Value Profiles

Last reconciled: **2026-07-22** against the live `basketball_test` schema.

Use this file together with section 15 of `docs/database_context.md`:

- section 15 explains what every column means, where it comes from, its unit,
  and its join/quality caveats;
- this file records what is actually stored in each column now;
- domains with **10 or fewer non-null values** are listed exactly, with row
  counts;
- columns with more than 10 values are described in general terms, with an
  observed range/cardinality where that is useful.

No deployed column is intentionally omitted. When columns are grouped below,
the group names enumerate the covered columns or point to an explicitly named
inherited set in section 15.

## 1. How the values were checked

The audit combines four methods:

1. exact `GROUP BY` value/count queries for low-cardinality columns;
2. exact `MIN`/`MAX`, array-cardinality, null, and distinct checks for compact
   relations;
3. full-table aggregates plus PostgreSQL statistics for large facts;
4. base R `summary()` for compact numeric/date relations.

The reusable read-only profiler is `scripts/profile_database_values.R`.
Examples, using the R path configured in `PROJECT.md`:

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/profile_database_values.R --stats-coverage
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/profile_database_values.R schedule final_schedule_mv
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/profile_database_values.R --low-card-all
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/profile_database_values.R --r-summary team_ppp_ratings_mv
```

`--r-summary` deliberately refuses relations estimated above 5,000 rows. For
large facts, pulling every value into R is slower and less safe than executing
the equivalent aggregates in PostgreSQL.

### Interpretation rules

- Counts in exact domains include `NULL` when present.
- PostgreSQL `numeric` values can stringify at different scales (`1`, `1.0`,
  `1.000...`). Those are one numeric value and are combined below.
- Relation row counts below are exact where an exact low-domain count covered
  every row. PostgreSQL planner estimates were stale for several large facts.
- Min/max describes observed data, not a guaranteed constraint.
- `pg_stats` retains old samples after a table is emptied. Those samples are
  not treated as current values.

## 2. Snapshot row counts

| Family | Relation | Current rows |
|---|---|---:|
| Schedule/ops | `schedule` | 446 |
| Schedule/ops | `schedule_team_dict` | 28 |
| Schedule/ops | `sched_long` | 892 |
| Schedule/ops | `final_schedule_mv` | 892 |
| Schedule/ops | `etl_processed_games` | 439 |
| Schedule/ops | `app_meta` | 1 |
| Schedule/ops | `games_ingest_status` | 218 |
| Roster/identity | `full_rosters` | 14,113 |
| Roster/identity | `player_id_aliases` | 8 |
| Roster/identity | `player_id_game_overrides` | 28 |
| Roster/identity | `player_identities` | 635 |
| Roster/identity | `player_identity_map` | 688 |
| Roster/identity | `player_identity_aliases_v` | 34 |
| Roster/identity | `resolved_player_identity_v` | 20,692 |
| Hot ETL | `actions_clean` | 0 |
| Hot ETL | `subs` | 0 |
| Hot ETL | `possessions` | 0 |
| Hot ETL | `stints` | 0 |
| Hot ETL | `pws` | 0 |
| Lineups | `lineups_lookup` | 345,303 |
| Lineups | `lineups_lookup_on` | 43,368 |
| Lineups | `sub_lineups` | 217,178 |
| Lineups | `sub_lineups_stats` | 43,242 |
| Facts | `df_pts_poss_lineups_longer_mv` | 528,035 |
| Facts | `mv_lineup_totals_by_day` | 45,246 |
| Facts | `lineup_four_factors_by_game` | 45,246 |
| Facts | `player_four_factors_by_game` | 736,368 |
| Player summaries | `onoff_default_mv` | 657 |
| Player summaries | `player_advanced_stats_mv` | 657 |
| Player summaries | `player_traditional_stats_mv` | 568 |
| Team summaries | `team_metrics_by_game_mv` | 892 |
| Team summaries | `team_metrics_rolling_mv` | 892 |
| Team summaries | `team_ppp_ratings_mv` | 28 |
| Team summaries | `team_four_factors_mv` | 28 |
| Shot lookup | `shot_zones` | 23,588 |

## 3. Schedule and operational values

### `schedule`

Exact domains:

| Column | Exact current values |
|---|---|
| `game_type` | `5` (388), `16` (23), `17` (6), `26` (11), `33` (3), `34` (1), `35` (14) |
| `game_year` | `2025` (225), `2026` (221) |
| `live_channel` | `NULL` (225), `5GOLD` (22), `5LIVE` (30), `5LIVEּ-5SPORT` (1), `5PLUS` (64), `5SPORT` (88), `5SPORT-5STARS` (1), `5STARS` (13), `GOLD5 - SPORT5` (1), `sport5.co.il + LIVE5` (1) |
| `is_atc` | `NULL` (225), `0` (221) |
| `is_live` | `NULL` (225), `1` (221) |

General profiles for the remaining columns:

| Columns | Observed contents |
|---|---|
| `id`, `game_id` | Provider integers, not interchangeable. `game_id` spans 23-738725 and is the analytical game key. |
| `gn` | Round/game number 1-32; 3.14% null. |
| `team1`, `team2` | Schedule-namespace team IDs, observed in the 1092-2109 range; map through `schedule_team_dict`. |
| `team_name_1`, `team_name_2`, `team_name_eng_1`, `team_name_eng_2` | Provider team labels. More than 10 values and known spelling/case variants. |
| `team_icon_1`, `team_icon_2` | Provider icon paths/URLs; descriptive strings, not keys. |
| `game_date_txt`, `game_date` | Raw date text and parsed date. Parsed dates span 2024-10-06 through 2026-06-23. |
| `score_team1`, `score_team2` | Official final integer scores; observed overall score range 1-137. |
| `game_time` | Provider tip-time text rather than a typed time. |
| `pbp_link` | Per-game provider PBP endpoint/path. |

### `schedule_team_dict`

- `game_year`: `2025` (14), `2026` (14).
- `team_id_schedule` and `team_id_rosters`: 28 season-scoped mappings,
  covering 14 schedule teams per season and 20 distinct roster/PBP IDs overall.
- `team_name_rosters` and `team_name_schedule`: human-readable mapping evidence;
  more than 10 labels and affected by provider spelling variants.

### `sched_long`

Exact domains:

- `game_year`: `2025` (450), `2026` (442).
- `game_type`: `5` (776), `16` (46), `17` (12), `26` (22), `33` (6),
  `34` (2), `35` (28).
- `team_slot`: `team1` (446), `team2` (446).

`game_id`, `game_date`, `gn`, `team_id`, `team_name`, `team_score`,
`opp_team_id`, `opp_team_name`, and `opp_score` are the two team perspectives
of the schedule profiles above. Scores span 1-137; roster/PBP team IDs cover 20
IDs in the 2-246 numeric range; names have 28 observed spellings/variants.

### `final_schedule_mv`

The shared `game_id`, `game_year`, `game_date`, `gn`, `game_type`, `team_id`,
`team_name`, `opp_team_id`, `opp_team_name`, `team_score`, and `opp_score`
retain the `sched_long` profiles.

Exact derived domains:

- `has_won`: `false` (446), `true` (446).
- `is_home`: `false` (446), `true` (446).
- `margin`: more than 10 integer values, spanning -62 through 62.

### `etl_processed_games`

- `game_year`: `2025` (218), `2026` (221).
- `game_id`: 439 processed game keys, spanning 23-64942.
- `processed_at`: 54 processing timestamps in the current snapshot, spanning
  2026-03-17 through 2026-07-17.

There are 446 scheduled games but 439 published/processed games; do not use
schedule presence alone as proof that PBP-derived rows exist.

### `app_meta`

All values are exact because there is one row:

- `key`: `etl_full_last_success`.
- `value`: `2026-07-22 00:30:38` (application display value, IDT).
- `updated_at`: `2026-07-21 21:30:38.904806+00`.

### `games_ingest_status`

- `game_id`: 218 distinct legacy game keys.
- `source_hash`: 218 distinct source hashes.
- `ingested_at`: every row has exactly
  `2025-11-11 10:19:15.46567+00`.

The uniform timestamp and lack of current code references make this a legacy
snapshot, not a live publication clock.

## 4. Roster and identity values

### `full_rosters`

Exact domains:

- `inroster`: `0` (3,951), `2` (10,162); this is not encoded as 0/1.
- `is_on`: `NULL` (13,971), `false` (142); no current `true` rows.
- `game_year`: `2025` (6,833), `2026` (7,280).
- `starter`: `false` (9,739), `true` (4,374).

General profiles:

- `player_id`: provider player IDs, roughly 617 distinct, spanning 38-35495.
- `apiid`: external API text identifier; both null and empty string occur.
- `firstname`, `lastname`, `firstnamelocal`, `lastnamelocal`: populated provider
  name components with hundreds of values.
- `jerseynumber`: provider jersey integer, 64 observed values from 0-99.
- `team_id`: 20 roster/PBP team IDs in the 2-246 numeric range.
- `game_id`: game-scoped roster rows covering the 439 processed games.
- `team_name`, `team_name_local`: provider team labels; English labels have
  19 current normalized/legacy variants.

### `player_id_aliases`

This eight-row table is fully low-cardinality:

- `game_year`: `2025` (1), `2026` (7).
- `team_id`: `5`, `6`, `7`, `9`, `12`, `13`, `15` (team 13 occurs twice).
- `alias_player_id`: `1277`, `1982`, `2046`, `2052`, `2136`, `2143`, `2152`,
  `27817` (one each).
- `canonical_player_id`: `1110`, `1143`, `1165`, `1183`, `1251`, `1262`,
  `2060`, `3206` (one each).
- `player_name`: ALON DANIELI, AMIR DANON, BEN ALTSHULER, DJ BURNS,
  ITAY ZLOTOLOV, J'VON MCCORMICK, NOAM AVIVI, SAGIV DVIR.
- `reason`: `cross-team season re-mint: id 1982 (Bnei Herzliya) is the same person as canonical 1143 (Rishon Lezion)` (1),
  `cross-team season re-mint: id 2046 (Maccabi Raanana) is the same person as canonical 1165 (Galil Elion)` (1),
  `cross-team season re-mint: id 2052 (Bnei Herzliya) is the same person as canonical 1110 (Galil Elion)` (1),
  `retired: Holon 2060 is a reused provider id, not a season-wide duplicate of 2152` (1),
  `retired: Kiryat Ata 1183 is a reused provider id, not a season-wide duplicate of 1277` (1),
  `same team/season duplicate display name with split lineup identity` (3).
- `active`: `false` (2), `true` (6).
- `canonicalize_base`: `false` (3), `true` (5).
- `created_at`: `2026-06-10 20:18:25.764393+00`,
  `2026-06-10 20:18:26.016034+00`, `2026-06-10 20:18:26.26012+00`,
  `2026-06-10 20:18:26.504154+00`, `2026-06-10 20:18:26.746868+00`,
  `2026-06-24 07:39:20.925834+00`, `2026-06-24 07:39:21.173716+00`, and
  `2026-06-24 07:39:21.41698+00` (one row each).
- `updated_at`: `2026-07-17 06:14:39.574383+00`,
  `2026-07-17 06:14:39.967453+00`, `2026-07-17 06:14:40.308735+00`,
  `2026-07-17 06:14:40.715065+00`, `2026-07-17 06:14:41.113634+00`,
  `2026-07-17 06:14:41.411354+00`, `2026-07-17 06:14:41.969027+00`, and
  `2026-07-17 06:14:42.319543+00` (one row each).

### `player_id_game_overrides`

Exact domains:

- `game_year`: `2026` (28); `team_id`: `12` (21), `5` (7).
- `alias_player_id`: `1183` (21), `2060` (7).
- `canonical_player_id`: `1277` (21), `2152` (7).
- `player_name`: ITAY ZLOTOLOV (21), J'VON MCCORMICK (7).
- `reason`: `provider reused DeAndre Williams player_id for Itay Zlotolov in this game`
  (21), `provider reused Josh Hagins player_id for J'Von McCormick in this game`
  (7).
- `active`: `true` (28).

`game_id`, `created_at`, and `updated_at` have more than 10 values: 28 scoped
game keys and per-row audit timestamps.

### `player_identities`

- `identity_id`: 635 stable bigint identities, from 1000000000000 through
  1000000042834.
- `identity_key`: unique/stable identity-key strings.
- `display_name`, `first_name`, `last_name`, `first_name_local`,
  `last_name_local`: human names; 535 display names, with local/name components
  nullable in three records and `NEW NEW` present as a reviewed placeholder.
- `notes`: `NULL` (632) or
  `Contextual identity created because no unambiguous canonical roster self-map was available`
  (3).
- `created_at`: `2026-06-18 10:28:16.037468+00` (628),
  `2026-06-18 11:33:48.215322+00` (4),
  `2026-06-24 07:39:24.176447+00` (3).
- `updated_at`: `2026-06-18 11:00:58.17058+00` (2),
  `2026-07-16 22:15:04.498925+00` (2),
  `2026-07-21 21:30:33.843106+00` (631).

### `player_identity_map`

Exact domains:

- `provider`: `segev` (688).
- `game_year`: `2025` (296), `2026` (392).
- `scope`: `game` (28), `season` (660).
- `active`: `true` (688).
- `reason`: the three exact cross-team re-mint strings listed under
  `player_id_aliases` (1 each), the two exact provider-reused-ID strings listed
  under `player_id_game_overrides` (21 and 7),
  `same team/season duplicate display name with split lineup identity` (3),
  and `unambiguous full_rosters identity` (654).
- `evidence`: generated normalized roster name (654), legacy aliases row (6),
  legacy game override row (28).
- `mapping_origin`: `roster_self` (654), `legacy_season_alias` (6),
  `legacy_game_override` (28).
- `created_at`: `2026-06-18 10:28:16.037468+00` (684),
  `2026-06-18 11:33:48.215322+00` (2),
  `2026-07-16 22:18:33.489831+00` (2). `updated_at` is exactly
  `2026-07-21 21:30:33.843106+00` (688).

General profiles:

- `map_id`: 688 mapping IDs, 1-136115.
- `team_id`: 20 roster/PBP IDs, 2-246.
- `game_id`: null for 660 season mappings; 28 game mappings use keys 51-385.
- `source_player_id`: 620 source IDs, 38-35495.
- `identity_id`: 627 identity IDs, 1000000000000-1000000003083.
- `canonical_player_id`: 614 canonical provider IDs, 38-35495.
- `player_name`: 534 display labels.

### `player_identity_aliases_v`

Exact domains:

- `game_year`: `2025` (1), `2026` (33).
- `team_id`: `12` (21), `5` (7), `13` (2), and `6`, `7`, `9`, `15` (1 each).
- `alias_player_id`: `1183` (21), `2060` (7), plus `1982`, `2046`, `2052`,
  `2136`, `2143`, `27817` (1 each).
- `canonical_player_id`: `1277` (21), `2152` (7), plus `1110`, `1143`,
  `1165`, `1251`, `1262`, `3206` (1 each).
- `player_name`: eight names matching the two correction sources.
- `reason`: the three exact cross-team re-mint strings listed under
  `player_id_aliases` (1 each), the two exact provider-reused-ID strings listed
  under `player_id_game_overrides` (21 and 7), and
  `same team/season duplicate display name with split lineup identity` (3).
- `correction_scope`: `game` (28), `season` (6).

`game_id` contains 28 scoped keys from 51-385 and is null on the six season
rows.

### `resolved_player_identity_v`

Exact domains:

- `game_year`: `2025` (9,256), `2026` (11,436).
- `resolution_scope`: `game` (28), `season` (20,664).
- `reason`: the same seven identity-map strings: the 1982 re-mint (31), 2046
  re-mint (32), 2052 re-mint (32), Itay reused-ID correction (21), J'Von
  reused-ID correction (7), duplicate-name split identity (89), and
  `unambiguous full_rosters identity` (20,480).

General profiles: `game_id` has 439 values (23-64942), `team_id` has 20 values
(2-246), `source_player_id` has 620 values (38-35495), `identity_id` has 627
values (1000000000000-1000000003083), `canonical_player_id` has 614 values
(38-35495), and `display_name` has 534 values.

## 5. Hot ETL tables: current values are empty

`actions_clean`, `subs`, `possessions`, `stints`, and `pws` contain **zero live
rows** after cold-storage publication. Therefore every column in these five
relations currently has no value domain, min/max, or meaningful null rate.

Coverage is explicit:

- every `actions_clean` column listed in section 15.3 is empty;
- `subs` inherits all `actions_clean` columns and is empty;
- `possessions` inherits all `actions_clean` columns and its additional
  `pct_ft`, `q_bucket`, `team_score`, `end_poss`, `sum_poss_poss`, `sum_block`,
  `sum_tech`, and `final_end_poss` columns are empty;
- all `stints` columns (`team_id`, `game_id`, segment endpoints/IDs, lineup
  hashes, defensive team, and `q_bucket`) are empty;
- `pws` inherits all `possessions` columns and its additional segment, lineup,
  team-defense, event-ID, and starter-count columns are empty.

For literal column coverage, the empty `actions_clean` source/detail fields are
`parameters_team`, `parameters_player`, `parameters_quarter`,
`parameters_player_in`, `parameters_player_out`, `parameters_current_quarter`,
`parameters_current_quarter_time`, `parameters_coord_x`, `parameters_coord_y`,
`parameters_fast_break`, `parameters_second_chance_points`,
`parameters_points_from_turnover`, `parameters_kind`, `parameters_fouled_on`,
`parameters_free_throws`, `parameters_free_throws_awarded`,
`parameters_free_throw_number`, `user_time`, `score`, `total_player_points`,
`parameters_is_coach_foul`, `parameters_is_bench_foul`, and `row_num` (as well
as the shared columns already named elsewhere in this file). The empty `stints`
fields include `final_start_seg`, `final_end_seg`, `lineup_hash_offense`,
`lineup_hash_defense`, `team_id_defense`, and `final_start_id`. The empty `pws`
additions include `final_start_seg`, `final_end_seg`, `lineup_hash_offense`,
`lineup_hash_defense`, `team_id_defense`, `final_start_id`,
`num_starters_offense`, and `num_starters_defense`. Every one of these fields
has `<no rows>` as its current live profile.

Planner statistics still show old examples such as quarters 1-4, shot/rebound
event types, `made`/`missed`/`blocked`, and historical game IDs. Those are stale
statistics, not current table contents. Historical truth is in
`exports/cold/*.parquet`.

## 6. Lineup lookup and combination values

### `lineups_lookup`

Exact domains:

- `quarter`: `1` (76,575), `2` (100,273), `3` (79,151), `4` (86,933),
  `5` (2,087), `6` (284).
- `is_on_verdict`: `NULL` (157,372), `0` (80,488), `1` (107,443).
- `n_on`: `0` (472), `2` (14), `3` (42), `4` (13), `5` (344,399),
  `6` (291), `7` (72). There are no current `1` rows.
- `game_year`: `2025` (173,024), `2026` (172,279).
- `num_starters`: `0` (6,824), `1` (25,960), `2` (72,622), `3` (118,517),
  `4` (87,619), `5` (33,761).

General profiles:

- `id`: source action/substitution IDs.
- `game_id`: 439 processed game keys, 23-64942.
- `player_id`: roughly 617 provider IDs, including sentinel `0` in source
  artifacts.
- `team_id`: 20 roster/PBP team IDs, 2-246.
- `quarter_time`: 597 clock strings from `00:00` through `10:00`.
- `end_game_seconds_remaining`: numeric game clock 0-2400.
- `end_quarter_seconds_remaining`: numeric period clock 0-600.
- `lineup_id`, `lineup_hash`: about 7,936 lineup states; hashes are 32-character
  hexadecimal MD5-style strings.

### `lineups_lookup_on`

- `game_year`: `2025` (20,062), `2026` (23,306).
- `player_id`: roughly 528 provider player IDs.
- `lineup_hash`: lineup hash attached to the ON player; high-cardinality text.
- `team_id`: 20 roster/PBP team IDs.

### `sub_lineups`

Exact domains:

- `num_lineup`: `2` (86,765), `3` (86,857), `4` (43,556).
- `game_year`: `2025` (100,153), `2026` (117,025).

General profiles:

- `team_id`: 20 roster/PBP IDs.
- `lineup_hash`: about 8,537 five-player hashes.
- `sub_lineup_hash`: about 19,226 combination hashes.
- `lineup_id`: about 19,067 combination identifiers.
- `player_ids`: sorted integer arrays of cardinality 2-4.
- `created_at`: insertion timestamps across refreshes.

### `sub_lineups_stats`

Exact domains:

- `num_lineup`: `2` (4,154), `3` (12,509), `4` (17,930), `5` (8,649).
- `game_year`: `2025` (19,705), `2026` (23,537).

General profiles:

- `team_id`: 20 team IDs; `sub_lineup_hash` is high-cardinality combination
  text.
- `player_ids`, `player_names`: arrays of cardinality 2-5;
  `player_names_str` is the corresponding display string.
- `off_poss`, `def_poss`: nonnegative counts, observed up to 1,207 and 1,200;
  `off_pts`, `def_pts`: nonnegative points, observed up to 1,296 and 1,331.
- `off_ppp`, `def_ppp`: per-100 ratings, observed roughly 8.3-500; very high
  values come from tiny possession denominators.
- `minutes`: 0-628.4 combination minutes.
- `off_fg2_made`, `off_fg2_att`, `off_fg3_made`, `off_fg3_att`,
  `def_fg2_made`, `def_fg2_att`, `def_fg3_made`, `def_fg3_att`: nonnegative
  aggregate shot counts; attempts are not smaller than their corresponding
  makes at the intended grain.

## 7. Central and per-game fact values

### `df_pts_poss_lineups_longer_mv`

Exact domains:

| Column | Exact current values |
|---|---|
| `quarter` | `1` (114,578), `2` (141,366), `3` (131,573), `4` (137,610), `5` (2,578), `6` (330) |
| `parameters_points` | `NULL` (385,833), `1` (25,404), `2` (69,667), `3` (47,131) |
| `parameters_made` | `NULL` (376,475), `blocked` (4,508), `made` (79,042), `missed` (68,010) |
| `pct_ft` | `NULL` (493,273), `0` (7,055), `0.33` (288), `0.5` (7,842), `0.67` (288), `1` (19,289) |
| `team_score` | `NULL` (448,993), `1` (25,404), `2` (37,286), `3` (16,352) |
| `final_end_poss` | `NULL` (9,592), `false` (382,314), `true` (136,129) |
| `event_owner_side` | `NULL` (152,695), `defense` (109,703), `offense` (265,637) |
| `type_lineup` | `NULL` (152,695), `defense` (187,668), `offense` (187,672) |
| `num_starters`, `own_starters` | `0` (11,527), `1` (36,263), `2` (95,729), `3` (162,117), `4` (132,246), `5` (90,153) |
| `opp_starters` | `NULL` (48), `0` (11,618), `1` (36,240), `2` (95,704), `3` (162,087), `4` (132,201), `5` (90,137) |

General profiles:

- `parameters_type`: about 20 provider subtypes (shot, rebound, foul, turnover,
  substitution-related and related event labels).
- `id`, `parent_action_id`, `final_end_id`: source/action identifiers; high
  cardinality and game-scoped in practice.
- `type`: roughly 12 broad event types.
- `player_id`: about 500 source player IDs and possible sentinel `0`.
- `team_id`: 20 roster/PBP team IDs.
- `game_id`: 439 processed games, 23-64942.
- `end_game_seconds_remaining`: 0-2400.
- `segment_id`: ordered within-game segment number, observed 1-78.
- `own_team_score`, `opp_team_score`: cumulative PBP scores 0-137; about 0.97%
  null.
- `lineup_hash`: about 7,051 lineup hashes.
- `event_elapsed_seconds`: populated canonical elapsed time from 0-3000.
- `clock_regression_seconds`: 0-600; zero normally, positive when source order
  moves backward.
- `segment_start_elapsed_seconds`, `segment_end_elapsed_seconds`: populated
  canonical endpoints from 0-3000.
- `segment_seconds`: canonical interval duration 0-571 seconds.

`pct_ft` now has an enforced 0-1 domain; see the resolved source anomaly in
section 11.

### `mv_lineup_totals_by_day`

Exact domains:

- `type_lineup`: `NULL` (15,557), `defense` (14,987), `offense` (14,702).
- `game_year`: `2025` (22,878), `2026` (22,368).
- `num_starters`: `0` (903), `1` (3,781), `2` (10,808), `3` (16,411),
  `4` (10,726), `5` (2,617).

General profiles: `team_id` has 20 values; `lineup_hash` about 8,528;
`g_date` spans 2024-10-06 through 2026-06-23; `game_id` covers 439 games;
`total_poss` is 0-86; `total_pts` 0-115; `fg2_made`, `fg2_att`, `fg3_made`, and
`fg3_att` are nonnegative per-game lineup shot counts; `minutes` is 0-39.6167
and about 67% null because only the offense interval supplies duration.

### `lineup_four_factors_by_game`

Exact domains match the preceding table for `game_year`, `type_lineup`, and
`num_starters`.

General profiles: `lineup_hash`, `team_id`, and `game_id` retain the same
lineup/team/game domains; `total_points` is 1-115 when non-null;
`total_poss` 0-86; `ts_poss_count` 0-88; `oreb_count` 0-24;
`oreb_opportunities` 0-46; `tov_count` 0-14; `total_ft_attempts` 0-25;
`total_fga` 0-83; `total_fgm` 0-44; `total_fg3_made` 0-17; and `minutes`
0-39.6167 with the same offense-only null pattern.

### `player_four_factors_by_game`

Exact domains:

- `game_year`: `2025` (359,998), `2026` (376,370).
- `is_on_key`: `0` (507,461), `1` (228,907).
- `type_lineup`: `NULL` (267,468), `defense` (234,684), `offense` (234,216).
- `num_starters`, `own_starters`: `0` (20,363), `1` (70,294), `2` (152,539),
  `3` (204,877), `4` (174,832), `5` (113,463).
- `opp_starters`: `NULL` (144), `0` (21,188), `1` (70,909), `2` (151,653),
  `3` (197,458), `4` (175,128), `5` (119,888).
- `steal_count`: `0` (568,845), `1` (116,396), `2` (36,274), `3` (10,827),
  `4` (2,879), `5` (812), `6` (187), `7` (100), `8` (48).
- `deflection_count`: `0` (612,981), `1` (92,021), `2` (22,969),
  `3` (6,536), `4` (1,295), `5` (404), `6` (95), `7` (46), `8` (21).
- `player_tov_count`: `0` (725,781), `1` (9,287), `2` (1,139), `3` (147),
  `4` (13), `5` (1).
- `dunk_made`: `0` (675,147), `1` (53,732), `2` (6,430), `3` (824),
  `4` (207), `5` (28); `dunk_att`: `0` (669,928), `1` (57,435),
  `2` (7,601), `3` (1,095), `4` (281), `5` (28).
- `c3_made`: `0` (708,863), `1` (25,979), `2` (1,405), `3` (83), `4` (38);
  `c3_att`: `0` (672,028), `1` (55,610), `2` (7,594), `3` (986),
  `4` (52), `5` (27), `6` (71).

General profiles:

- `player_id`, `team_id`, `game_id`: provider player, roster/PBP team, and
  processed game identifiers.
- `total_points`: 1-100 when non-null; `total_poss`: 0-75;
  `ts_poss_count`: 0-76.
- `oreb_count`: 0-16; `oreb_opportunities`: 0-38; `tov_count`: 0-14.
- `total_ft_attempts`: 0-21; `total_fga`: 0-71; `total_fgm`: 0-38;
  `total_fg3_made`: 0-11.
- `player_ts_poss_count`: nonnegative player true-shooting opportunity count.
- `minutes`, `onoff_minutes`: 0-32.45; the generic `minutes` field is null on
  about 68% of offense/defense/null fact rows.
- `fg2_made`/`fg2_att`: 0-27/0-45;
  `fg3_made`/`fg3_att`: 0-11/0-30;
  `layup_made`/`layup_att`: 0-20/0-31;
  `c3_known_att`: 0-30.

## 8. Player summary values

### `onoff_default_mv`

- `Year`: `2025` (295), `2026` (362).
- `Team`: 19 team-name variants; `team_id`: 20 team IDs; `player_id`: provider
  player IDs; `First Name` and `Last Name`: 351 and 476 observed strings.
- `Net RTG Diff`, `Off ON Diff`, `Def ON Diff`, `On Net RTG`, `Off Net RTG`:
  signed per-100 rating/difference metrics. Small-exposure extremes occur;
  observed `Net RTG Diff` is -172.8 to 156.4.
- `Off ON PPP`, `Def ON PPP`, `Off OFF PPP`, `Def OFF PPP`: per-100 ratings.
  ON values can be extreme on tiny samples (roughly 14.3-266.7); OFF values
  are season aggregates (roughly 77.8-139.3).
- `ON Poss`, `OFF Poss`: nonnegative possession counts; observed 1-2,037 and
  59-2,870 respectively when populated. `minutes`: 0-994.6.
- Every `pr_*` column (`pr_net`, `pr_off_on`, `pr_off_off`,
  `pr_def_on_inv`, `pr_def_off_inv`, `pr_off_on_d`, `pr_def_on_d`,
  `pr_def_on_d_inv`, `pr_on_net`, `pr_off_net`) is a percentile-like fraction
  on 0-1, not a percentage on 0-100.
- Every `off_on_*`, `off_off_*`, `def_on_*`, and `def_off_*` shot column for
  `fg2_made`, `fg2_att`, `fg3_made`, `fg3_att`, `layup_made`, `layup_att`,
  `dunk_made`, `dunk_att`, `c3_made`, `c3_att`, and `c3_known_att` is a
  nonnegative aggregate count. ON columns are nullable when the player has no
  qualifying ON exposure; OFF columns are normally populated. Attempts should
  dominate makes within the same prefix/stat family.

The preceding shot-count family explicitly covers `off_on_fg2_made`,
`off_on_fg2_att`, `off_on_fg3_made`, `off_on_fg3_att`, `off_off_fg2_made`,
`off_off_fg2_att`, `off_off_fg3_made`, `off_off_fg3_att`, `def_on_fg2_made`,
`def_on_fg2_att`, `def_on_fg3_made`, `def_on_fg3_att`, `def_off_fg2_made`,
`def_off_fg2_att`, `def_off_fg3_made`, `def_off_fg3_att`,
`off_on_layup_made`, `off_on_layup_att`, `off_on_dunk_made`,
`off_on_dunk_att`, `off_on_c3_made`, `off_on_c3_att`,
`off_on_c3_known_att`, `off_off_layup_made`, `off_off_layup_att`,
`off_off_dunk_made`, `off_off_dunk_att`, `off_off_c3_made`, `off_off_c3_att`,
`off_off_c3_known_att`, `def_on_layup_made`, `def_on_layup_att`,
`def_on_dunk_made`, `def_on_dunk_att`, `def_on_c3_made`, `def_on_c3_att`,
`def_on_c3_known_att`, `def_off_layup_made`, `def_off_layup_att`,
`def_off_dunk_made`, `def_off_dunk_att`, `def_off_c3_made`, `def_off_c3_att`,
and `def_off_c3_known_att`.

### `player_advanced_stats_mv`

- `game_year`: `2025` (295), `2026` (362).
- `player_id`, `team_id`, `firstname`, `lastname`, `team_name`: the same
  player/team/name domains as `onoff_default_mv`.
- `off_on_ts`, `off_off_ts`, `def_on_ts`, `def_off_ts`, `off_on_efg`,
  `off_off_efg`, `def_on_efg`, `def_off_efg`: true/effective shooting
  fractions stored on 0-1 in this MV.
- `off_on_oreb`, `off_off_oreb`, `def_on_oreb`, `def_off_oreb`,
  `off_on_tov`, `off_off_tov`, `def_on_tov`, `def_off_tov`,
  `def_on_disruptions`, `def_off_disruptions`, `off_on_ftr`, `off_off_ftr`,
  `def_on_ftr`, `def_off_ftr`: rate fractions. ON small-sample values can reach
  1, and observed ON FTR outliers reach 2.5 offense / 1.8 defense.
- `off_on_poss`, `off_off_poss`, `def_on_poss`, `def_off_poss`: nonnegative
  possession counts, up to roughly 2,870.
- `Off eFG% Diff`, `Off TS% Diff`, `Off OREB% Diff`, `Off TOV% Diff`,
  `Off FTR Diff`, `Def eFG% Diff`, `Def TS% Diff`, `Def OREB% Diff`,
  `Def TOV% Diff`, `Def Disruptions/100 Diff`, `Def FTR Diff`: signed displayed
  percentage-point differences. Small samples cause large extremes.
- `pr_diff_off_efg`, `pr_diff_off_ts`, `pr_diff_off_oreb`,
  `pr_diff_def_efg`, `pr_diff_def_ts`: percentile fractions on 0-1.

### `player_traditional_stats_mv`

- `game_year`: `2025` (255), `2026` (313).
- `team_id`: 20 IDs, 2-246; `player_id`: 534 IDs, 38-35495.
- `team_name`, `firstname`, `lastname`, `player_name`: 19, 320, 412, and 461
  observed labels respectively.
- `gp`: season games played, 0-39; `poss_on_floor`: 0-2,037;
  `minutes`: 0.2-1,007.3.
- `pts`, `reb`, `oreb`, `dreb`, `ast`, `stl`, `blk`, `dfl`, `tov`: season
  totals, all nonnegative; observed maxima are 631, 286, 110, 183, 209, 65,
  50, 40, and 123.
- `fgm`, `fga`, `3pm`, `3pa`, `ftm`, `fta`: nonnegative season shooting
  totals; maxima 194, 437, 104, 271, 204, and 237.
- `fg_pct`, `tp_pct`, `ft_pct`: displayed 0-100 percentages; null when the
  denominator is zero. `efg` and `ts` are also 0-100 and can reach 150 on
  one-attempt three-point samples. `usg_pct` is 2.7-86.1 when populated.

## 9. Team summary values

### `team_metrics_by_game_mv`

Exact domains:

- `game_year`: `2025` (450), `2026` (442).
- `game_type`: `5` (776), `16` (46), `17` (12), `26` (22), `33` (6),
  `34` (2), `35` (28).
- `is_home`: `false` (446), `true` (446).
- `has_won`: `false` (446), `true` (446).

General profiles:

- `game_id`, `game_date`, `gn`: 446 games, dates 2024-10-06 through
  2026-06-23, round 1-32 with 3.14% null `gn`.
- `team_id`, `opp_team_id`: 20 IDs; `team_name`, `opp_team_name`: 28 spelling
  variants.
- `team_score`, `opp_score`: official values 1-137; `margin`: -62 to 62.
- `off_ppp`, `def_ppp`: 68.0-158.5 when populated; `net_rtg`: -76 to 76.
- `off_poss`, `def_poss`, `off_poss_raw`, `def_poss_raw`: 41-108;
  `off_points_raw`, `def_points_raw`: 51-137;
  `off_ts_poss_raw`, `def_ts_poss_raw`: 44-118.
- `off_oreb_count_raw`, `def_oreb_count_raw`: 1-28;
  `off_oreb_opp_raw`, `def_oreb_opp_raw`: 13-71;
  `off_tov_raw`, `def_tov_raw`: 0-29;
  `off_fta_raw`, `def_fta_raw`: 2-42;
  `off_fga_raw`, `def_fga_raw`: 31-109;
  `off_fgm_raw`, `def_fgm_raw`: 15-49;
  `off_fg3m_raw`, `def_fg3m_raw`: 1-23.
- `off_ts`, `def_ts`: 34.2-77.8;
  `off_efg`, `def_efg`: 28.8-75.9;
  `off_oreb`, `def_oreb`: 3.7-57.9;
  `off_tov`, `def_tov`: 0-36.1;
  `off_ftr`, `def_ftr`: 4.4-80.6. These are displayed percentages.
- `off_minutes`, `def_minutes`: identical mirrors of once-counted canonical
  team floor duration, 16.0-49.9 when lineup facts exist; both are null on the
  14 schedule rows without lineup facts.
- `pts`, `reb`, `ast`, `stl`, `blk`, `dfl`, `tov`, `fgm`, `fga`, `3pm`,
  `3pa`, `ftm`, `fta`: per-team-game traditional totals. They are nonnegative;
  their observed maxima are 137, 68, 36, 20, 13, 17, 29, 49, 109, 23, 56,
  35, and 42.
- `fg_pct`, `tp_pct`, `ft_pct`, `efg`, `ts`: displayed 0-100 percentages;
  observed ranges are 25.4-66.7, 4.5-63.3, 35.7-100, 28.8-75.9, and
  35.8-77.4.

### `team_metrics_rolling_mv`

All inherited `team_metrics_by_game_mv` columns have the same 892-row domains
above. Additional columns:

- `team_game_seq`: integer game sequence 1-39.
- `off_ppp_r3`, `def_ppp_r3`, `net_rtg_r3`: rolling 3-game values, observed
  84.03-144.17, 84.4-138.7, and -42.3-51.8.
- `off_ts_r3`, `off_efg_r3`, `def_ts_r3`, `def_efg_r3`: rolling displayed
  percentages, observed approximately 38.2-71.2 across the four fields.
- `off_ppp_r5`, `def_ppp_r5`, `net_rtg_r5`: rolling 5-game values, observed
  84.4-138.7 and -42.3-42.3.
- `off_ts_r5`, `off_efg_r5`, `def_ts_r5`, `def_efg_r5`: rolling displayed
  percentages, approximately 38.2-68.28.
- `off_ppp_r10`, `def_ppp_r10`, `net_rtg_r10`: rolling 10-game values,
  observed 84.4-138.7 and -42.3-42.3.
- `off_ts_r10`, `off_efg_r10`, `def_ts_r10`, `def_efg_r10`: rolling displayed
  percentages, approximately 38.2-67.4.
- `prev_net_rtg_r5`: previous five-game net rating, -42.3 to 42.3;
  `delta_net_rtg_r5`: current-minus-previous change, -31.7 to 25.45. Both are
  null on the first row for each team (3.14% overall).

### `team_ppp_ratings_mv`

- `game_year`: `2025` (14), `2026` (14).
- `team_id`, `team_name`: 20 IDs and 19 current display-name variants.
- `off_ppp`, `def_ppp`, `net_rtg`: 98.4-128.4, 103.4-118.3, and -13.3-20.9.
- `games_played`: 11 distinct totals from 26-39; `wins`: 4-35;
  `losses`: 4-28.
- `off_poss`, `def_poss`: 2,018-2,978 and 2,025-2,989.
- `rank_net_rtg`, `rank_off_ppp`, `rank_def_ppp`: integer season ranks 1-14.
- `off_fga`, `off_layup_att`, `off_dunk_att`, `off_fg3_att`, `off_c3_att`,
  `off_c3_known_att`: nonnegative season attempt totals, observed respectively
  1,714-2,737; 619-996; 18-182; 642-1,205; 42-157; 642-1,205.
- `def_fga`, `def_layup_att`, `def_dunk_att`, `def_fg3_att`, `def_c3_att`,
  `def_c3_known_att`: opponent attempt totals, observed respectively
  1,755-2,625; 678-968; 54-108; 680-1,186; 43-129; 680-1,186.

### `team_four_factors_mv`

- `game_year`: `2025` (14), `2026` (14).
- `team_id`, `team_name`: 20 IDs and 19 display-name variants.
- `off_ts`, `off_efg`, `off_oreb`, `off_tov`, `off_ftr`, `off_ppp`:
  displayed rates/ratings with observed ranges 50.1-60.2, 48.2-57.9,
  24.9-37.7, 14.6-23.6, 23.2-35.9, and 98.4-128.4.
- `off_poss`, `off_pts`, `off_ts_poss`, `off_oreb_cnt`, `off_oreb_opps`,
  `off_tov_cnt`, `off_fta`, `off_fga_cnt`, `off_fgm_cnt`, `off_fg3m_cnt`:
  nonnegative season totals; observed ranges 2,018-2,978; 2,078-3,823;
  1,973-3,174; 259-542; 992-1,442; 362-595; 471-824; 1,714-2,737;
  743-1,345; 220-471.
- `def_ts`, `def_efg`, `def_oreb`, `def_tov`, `def_ftr`, `def_ppp`:
  opponent rates/ratings with observed ranges 51.6-59.1, 49.0-57.3,
  28.1-35.4, 15.6-20.6, 22.0-38.2, and 103.4-118.3.
- `def_poss`, `def_pts`, `def_ts_poss`, `def_oreb_cnt`, `def_oreb_opps`,
  `def_tov_cnt`, `def_fta`, `def_fga_cnt`, `def_fgm_cnt`, `def_fg3m_cnt`:
  opponent season totals; observed ranges 2,025-2,989; 2,191-3,211;
  2,076-2,943; 287-449; 990-1,500; 328-536; 497-755; 1,755-2,625;
  785-1,188; 231-421.
- `net_rtg`: signed season rating, -13.2 to 21.0.

## 10. Shot-zone values

### `shot_zones`

- `game_id`: 439 processed game keys, 23-64942.
- `id`: 23,588 action IDs, 230030-649420980; the pair `(game_id, id)` is the
  lookup key.
- `is_corner3`: `false` (21,403), `true` (2,185).

## 11. Value-level anomalies and traps found by this audit

These supplement the broader quality findings in section 12 of
`docs/database_context.md`.

1. **Raw free-throw trip inconsistency:** seven cold-source parent groups have
   conflicting `parameters_free_throws_awarded` values (games 51, 69, 141,
   192, 62463, 62527, and 64909). Game 192 action 1920557 reports attempt 2
   with awarded total 1. The raw fields remain unchanged for audit, so consumers
   should use derived `pct_ft` for trip progress.
2. **Invalid lineup sizes:** `lineups_lookup.n_on` contains 904 rows outside
   the valid five-player state (`0`, `2`, `3`, `4`, `6`, or `7`). This is the
   row-level manifestation behind the lineup-state quality checks.
3. **Hot-table statistics are stale:** the five cold-purged ETL tables are
   empty while `pg_stats` still advertises historical values. Never infer live
   presence from those samples.
4. **Planner counts are stale:** exact domain totals show 528,035 central-fact
   rows versus a catalog estimate near 485,772; 736,368 player-factor rows
   versus about 608,471; 345,303 lineup lookup rows versus about 344,299; and
   14,113 roster rows versus about 13,593. Use `COUNT(*)` when an exact count is
   analytically important.
5. **Roster encodings are non-obvious:** `inroster` uses `0`/`2`, `is_on` has
   no current `true`, and `apiid` contains null and empty string. Do not coerce
   these into clean booleans without source-specific rules.
6. **Numeric display scale is not a category:** values such as `1`, `1.0`, and
   `1.000000...` are numerically equal. Do not group `numeric` columns after a
   text cast unless scale distinctions are intentional.
7. **Small denominators create legal extremes:** ON/OFF and combination PPP,
   eFG, TS, FTR, and rating differences can be far outside season-level ranges.
   Filter on possessions/minutes before interpreting them; do not clamp them as
   corrupt solely because they are extreme.
8. **Names are not keys:** schedule, roster, and summary relations contain
   casing/spelling variants such as BEER SHEVA/Be;er Sheva and ELIZUR/ELITZUR,
   plus reviewed `NEW NEW` placeholders. Use IDs and the identity layer.
9. **Coverage differs by surface:** 446 schedule games, 439 processed games,
    and 218 rows in the legacy ingest-status snapshot answer different
    questions. They must not be substituted for one another.

Resolved during this audit: `team_metrics_by_game_mv.def_minutes` now mirrors
the same once-counted canonical duration as `off_minutes`. All 878 rows with
lineup facts match exactly (16.0-49.9 minutes); 14 schedule-only rows remain
null. `team_metrics_rolling_mv` was refreshed and has zero minute mismatches.

Also resolved: the two mirrored published rows for game 192 action 1920557 no
longer expose `pct_ft = 2`. ETL interprets its raw attempt-2/awarded-1 pair as
`2 / 2 = 1`, correctly marks the final made free throw as a possession end,
and retains the contradictory raw denominator. Live and cold `pct_ft` ranges
are both 0-1, and DQ check `AJ_free_throw_progress_domain` passes with zero
issues.

Also resolved: game 62461/team 8 now includes Cody Demps (player 2543), who was
referenced by the PBP but omitted from the game roster source. The repaired
roster produces valid five-player lineups for that game; the full cold-history
action-to-roster audit has zero remaining gaps. Current DQ totals fell to 52
invalid lineup states, 486 unmatched event-team rows, and four player-minute
conservation failures, none involving game 62461.
