# Player Identity Data Quality Findings

Date: 2026-06-13

This note summarizes the ETL/player identity issues found while reviewing player ID aliases, with checks that should be added to a daily data quality report.

## Scope

The affected identity fields are not display-only. Canonicalized `player_id` values flow into:

- `actions_clean.player_id`
- action parameters such as `parameters_player_in`, `parameters_player_out`, `parameters_player`, `parameters_fouled_on`
- `subs`
- `full_rosters`
- `starters`
- `lineups_lookup`
- lineup hashes and sub-lineup hashes
- `lineups_lookup_on`
- `sub_lineups`
- `sub_lineups_stats`
- app-facing aggregates such as `onoff_default_mv` and `player_advanced_stats_mv`

Any incorrect alias therefore contaminates stats identity, lineup identity, filters, display names, and aggregate outputs.

## Bugs Found

### 1. Season-wide alias was too coarse for reused provider IDs

The original alias table used `(game_year, team_id, alias_player_id)` as the key. That means an alias applied to every game for that team and season.

That is correct for a true duplicate ID for the same real player, but wrong when the upstream provider reuses one ID for different people in different games.

Holon 2026 case:

- `player_id = 2060` is Josh Hagins in games `177, 183, 211`.
- `player_id = 2060` was also used upstream for J'Von McCormick in games `165, 199, 356, 361, 383, 384, 385`.
- The previous season-wide alias `2152 -> 2060` incorrectly collapsed McCormick into Hagins.
- Correct handling is game-scoped: `2060 -> 2152` only in McCormick games.

Fix implemented:

- Added `player_id_game_overrides`.
- Retired the Holon season-wide alias `2152 -> 2060`.
- Seeded game overrides for McCormick games only.
- Canonicalization now applies game overrides first, then season aliases.

### 2. Alias cleanup was deleting too broadly

`cleanup_player_alias_lineup_derivatives()` previously deleted `sub_lineups_stats` rows where `player_ids` contained any active alias ID across the schema/team/year.

Risk:

- A targeted backfill for one game could delete valid season aggregate rows unrelated to that game.
- This was especially risky because `sub_lineups_stats` is keyed by `(team_id, sub_lineup_hash, game_year)`, not by `game_id`.

Fix implemented:

- Cleanup now derives old lineup hashes from the target `game_ids`.
- It derives only sub-lineup identities tied to those old lineups.
- It deletes `sub_lineups_stats` only for matching old identities that still contain the relevant alias ID.

### 3. Backfill helper forced full `sub_lineups_stats` refresh

The alias backfill helper forced `force_full_sub_lineup_stats = TRUE`, causing long full-table refreshes even for small targeted fixes.

Fix implemented:

- Default is now `force_full_sub_lineup_stats = FALSE`.
- Targeted backfills use `refresh_sub_lineups_stats_for_games()` when available.
- The Holon fix ran with incremental refresh; Phase 5 touched `1,996` rows and took about `51s`.

### 4. Display names could become hybrid names

Some SQL aggregates chose `firstname` and `lastname` independently using `MIN()` or `MAX()`.

Failure mode:

- If two real players are collapsed under one `player_id`, the display layer can produce a name that never existed in the roster, such as `JOSH MCCORMICK`.
- This is a symptom of identity contamination, but it is also a display aggregation bug.

Recommended report behavior:

- Treat a display name as suspicious if `(firstname, lastname)` in an app aggregate does not appear as a full-name pair in `full_rosters` for the same `(game_year, team_id, player_id)`.
- Longer-term SQL should choose a full roster-name row atomically rather than aggregating first and last names separately.

### 5. Duplicate raw PBP action IDs can block base loading

Game `381` exposed a source-feed issue where the same PBP action IDs appeared more than once around the Q2 `00:00` boundary.

The raw duplicated ID block was `3810375` through `3810385`. The earlier block had `user_time` values around `18:37`, while a later source edit around `18:44` shifted the substitutions into a coherent sequence and moved the quarter marker to the expected row.

Failure mode:

- `actions_clean` is keyed by `(game_id, id)`.
- The cleaner previously deduped at `(game_id, team_id, id)`, so cross-team duplicate IDs survived.
- The staging table rejected game `381` with duplicate key `(game_id, id) = (381, 3810379)`.

Fix implemented:

- Added a narrow known-source correction for game `381`, dropping only the earlier Q2 `00:00` duplicated block.
- Added a guard that fails loudly if unknown duplicate `(game_id, id)` rows remain after known fixes.
- Did not make "latest duplicate wins" a global deterministic rule.

Future hypothesis to test:

- In game `381`, the later `user_time`/later source row appeared to be the source correction.
- For the next duplicate-action conflict, compare earliest vs latest source rows before deciding whether "latest source edit wins" should become a broader rule.

### 6. Partial ETL failures could suppress publication for successful games

The full ETL previously skipped `etl_processed_games` publication whenever the pipeline had failures, even if some games had fully completed base load and downstream refreshes.

Failure mode:

- A bad game could roll back correctly, but successful games from the same run would also remain unpublished.
- The next ETL run would continue treating those successful games as unprocessed.
- This is the class of issue that made games such as `368` and `369` need explicit verification after they were already present in base/downstream tables.

Fix implemented:

- Successful `processed_ids` are published when downstream phases complete, even if separate base-game loads rolled back.
- Failed base-game IDs still make the run return `success = FALSE`, so wrappers and schedulers can alert.
- `last_success` is not updated for partial runs with rolled-back base games.

### 7. Last-success markers could be misleading

There were two marker risks:

- The PowerShell wrapper wrote `etl/logs/last_success.txt` after dry-run-only executions.
- The R ETL could record `app_meta.etl_full_last_success` even when the run was only partially successful.

Failure mode:

- The app or operator could see a fresh "last updated" value even though no write run completed cleanly.
- A partial run could look successful while failed games still needed attention.

Fix implemented:

- The wrapper now checks `etl_full()`'s returned `success` flag and exits nonzero on failure or partial write runs.
- Dry-run-only wrapper executions no longer update `etl/logs/last_success.txt`.
- The ETL updates `app_meta.etl_full_last_success` only after a clean write run.

## Daily Report Checks

### Check A: Same team-season-player ID has multiple roster names

Purpose: this should be the first daily report check. It detects reused provider IDs or bad canonical merges by asking whether one `(game_year, team_id, player_id)` has more than one normalized full-name value.

The first CTE fixes the report grain to one row per `(game_year, team_id, player_id, game_id, normalized_name)`. The outer query then reports only identity keys with multiple names.

```sql
WITH roster_name_grain AS (
  SELECT
    game_year,
    team_id,
    player_id,
    game_id,
    upper(
      regexp_replace(
        btrim(concat_ws(' ', firstname, lastname)),
        '\s+',
        ' ',
        'g'
      )
    ) AS normalized_name
  FROM basketball_test.full_rosters
  WHERE player_id IS NOT NULL
    AND btrim(concat_ws(' ', firstname, lastname)) <> ''
  GROUP BY
    game_year,
    team_id,
    player_id,
    game_id,
    normalized_name
),
name_sets AS (
  SELECT
    game_year,
    team_id,
    player_id,
    normalized_name,
    string_agg(game_id::text, ',' ORDER BY game_id) AS games
  FROM roster_name_grain
  GROUP BY
    game_year,
    team_id,
    player_id,
    normalized_name
),
flagged AS (
  SELECT
    game_year,
    team_id,
    player_id,
    count(*) AS distinct_names,
    string_agg(normalized_name || ' [' || games || ']', ' | ' ORDER BY normalized_name) AS name_games
  FROM name_sets
  GROUP BY
    game_year,
    team_id,
    player_id
  HAVING count(*) > 1
)
SELECT *
FROM flagged
ORDER BY game_year DESC, team_id, player_id;
```

Expected handling:

- If names are the same player with spelling variants, add/adjust a season alias or name normalization rule.
- If names are different real players in different games, add a game-scoped override.

### Check B: Same roster name has multiple player IDs in one team-season

Purpose: detect true duplicate IDs for the same real player.

```sql
WITH roster_names AS (
  SELECT
    game_year,
    team_id,
    player_id,
    upper(regexp_replace(btrim(concat_ws(' ', firstname, lastname)), '\s+', ' ', 'g')) AS player_name,
    array_agg(DISTINCT game_id ORDER BY game_id) AS games
  FROM basketball_test.full_rosters
  WHERE player_id IS NOT NULL
  GROUP BY game_year, team_id, player_id, upper(regexp_replace(btrim(concat_ws(' ', firstname, lastname)), '\s+', ' ', 'g'))
)
SELECT
  game_year,
  team_id,
  player_name,
  count(DISTINCT player_id) AS distinct_player_ids,
  array_agg(DISTINCT player_id ORDER BY player_id) AS player_ids,
  array_agg(games) AS game_sets
FROM roster_names
GROUP BY game_year, team_id, player_name
HAVING count(DISTINCT player_id) > 1
ORDER BY game_year DESC, team_id, player_name;
```

Expected handling:

- If it is one real player split across IDs, add a season alias.
- If it is not verified as one real player, do not auto-canonicalize.

### Check C: Active correction residue remains after ETL

Purpose: verify aliases and overrides were actually applied in base and lineup tables.

Season aliases should not remain in `full_rosters`, `lineups_lookup`, `lineups_lookup_on`, or `sub_lineups_stats`.

Game overrides should not remain in `full_rosters` or `lineups_lookup` for their scoped games.

The R helper `player_alias_residue_summary(pg, schema, game_ids = NULL)` implements this report check.

Expected result after successful backfill:

- zero rows for processed games.

### Check D: Active game overrides have corresponding roster rows

Purpose: catch stale overrides, wrong game IDs, wrong team IDs, or upstream data changes.

```sql
SELECT
  o.game_id,
  o.game_year,
  o.team_id,
  o.alias_player_id,
  o.canonical_player_id,
  o.player_name,
  count(fr.*) AS matching_roster_rows
FROM basketball_test.player_id_game_overrides o
LEFT JOIN basketball_test.full_rosters fr
  ON fr.game_id = o.game_id
 AND fr.game_year = o.game_year
 AND fr.team_id = o.team_id
 AND fr.player_id = o.canonical_player_id
WHERE o.active
GROUP BY
  o.game_id,
  o.game_year,
  o.team_id,
  o.alias_player_id,
  o.canonical_player_id,
  o.player_name
HAVING count(fr.*) = 0
ORDER BY o.game_year DESC, o.team_id, o.game_id;
```

Expected result:

- zero rows after the corrected game has been backfilled.

### Check E: App aggregate names are roster-valid full-name pairs

Purpose: catch hybrid display names like `JOSH MCCORMICK`.

```sql
WITH roster_pairs AS (
  SELECT DISTINCT
    game_year,
    team_id,
    player_id,
    btrim(concat_ws(' ', firstname, lastname)) AS roster_name
  FROM basketball_test.full_rosters
),
onoff_names AS (
  SELECT
    "Year" AS game_year,
    team_id,
    player_id,
    btrim(concat_ws(' ', "First Name", "Last Name")) AS aggregate_name
  FROM basketball_test.onoff_default_mv
)
SELECT o.*
FROM onoff_names o
LEFT JOIN roster_pairs r
  ON r.game_year = o.game_year
 AND r.team_id = o.team_id
 AND r.player_id = o.player_id
 AND r.roster_name = o.aggregate_name
WHERE r.player_id IS NULL
ORDER BY o.game_year DESC, o.team_id, o.player_id;
```

Expected handling:

- If this returns rows, first check for identity contamination.
- If identity is correct, fix the aggregate SQL to choose full names atomically.

### Check F: Lineup derivative consistency after player corrections

Purpose: catch stale old lineup hashes after a correction backfill.

Report counts of active alias IDs inside:

- `lineups_lookup_on.player_id`
- `sub_lineups.player_ids`
- `sub_lineups_stats.player_ids`

Expected result:

- zero active season-alias residues.
- zero game-override residues in game-scoped base tables after those games are rebuilt.

### Check G: Raw PBP duplicate action IDs

Purpose: catch source-feed action ID conflicts before they break `actions_clean` staging.

This check has to run against the raw PBP payload before insertion because cold storage and `actions_clean` only contain cleaned rows.

Report fields:

- `game_id`
- duplicated action `id`
- raw duplicate row count
- teams involved
- players involved
- action types involved
- earliest and latest `user_time`
- whether a known source correction handled it
- which row the ETL kept or dropped, if any

Expected handling:

- If the duplicate is a known source defect, add a narrow source correction.
- If the later source row looks like a correction, record that evidence, but do not generalize without another confirmed case.
- If no known correction exists, fail the ETL for that game and surface it in the daily report.

### Check H: Base-loaded games missing `etl_processed_games`

Purpose: catch successful base/downstream loads that were not published as processed.

Expected result:

- zero rows.

If this returns rows, the ETL may reprocess games that already exist in base tables.

### Check I: Processed games missing non-purged base rows

Purpose: catch `etl_processed_games` markers that exist while non-purged base identity/lineup rows are absent.

Expected result:

- zero rows.

If this returns rows, ETL may skip a game that is not actually available in the app-facing base layer.

### Check J: Processed base-loaded games missing downstream game rows

Purpose: catch games marked processed before the game-grain refresh chain completed.

Expected result:

- zero rows.

This directly catches the "base tables are present but MVs/game-grain tables were not refreshed" failure mode.

### Check K: App aggregate duplicate keys

Purpose: keep app-facing player aggregates unique at their expected grain.

Expected result:

- zero duplicate groups for `onoff_default_mv` at `("Year", team_id, player_id)`.
- zero duplicate groups for `player_advanced_stats_mv` at `(game_year, team_id, player_id)`.

## Automated Report

Implemented first-pass report runner:

- `etl/run_data_quality_report.R`
- Default output directory: `etl/logs/data_quality`
- Writes timestamped Markdown, `latest.md`, summary CSV, and per-check detail CSVs.
- `DQ_FAIL_ON_ERROR=true` can be used by a scheduled data test to make the script exit nonzero when error checks return rows.
- The raw PBP duplicate check is recorded as not yet automated because it must run against provider payloads before `clean_actions()`.

## Known Verified Holon Result

After the targeted backfill on 2026-06-13:

- `basketball_test.player_id_aliases` has Holon `2152 -> 2060` inactive.
- `basketball_test.player_id_game_overrides` has active `2060 -> 2152` rows for games `165, 199, 356, 361, 383, 384, 385`.
- `full_rosters` shows:
  - `2060 = JOSH HAGINS`, games `{177,183,211}`
  - `2152 = J'VON MCCORMICK`, games `{165,199,356,361,383,384,385}`
- `player_alias_residue_summary()` returned zero rows for the targeted games.
- `onoff_default_mv` and `player_advanced_stats_mv` show separate rows for Hagins and McCormick with correct names.

## Report Severity Suggestions

- Error: active correction residue remains after a processed game.
- Error: app aggregate display name is not a roster-valid full-name pair.
- Error: one `(game_year, team_id, player_id)` has multiple real-looking names and no active correction.
- Warning: same normalized name has multiple player IDs and no active alias.
- Warning: active game override has no matching canonical roster row after backfill.
