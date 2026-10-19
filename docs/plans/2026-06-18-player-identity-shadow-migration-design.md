# Player Identity Shadow Migration Design and Implementation Plan

Date: 2026-06-18

Status: proposed; saved for implementation. This document does not authorize changing app grouping or user-visible results.

## Objective

Propagate the stable `identity_id` into the player data pipeline while preserving all existing app behavior and statistics.

During the shadow phase:

```text
player_id   = existing provider-compatible key used by the app
identity_id = stable person key carried alongside player_id
```

The app continues filtering, joining, grouping, and rendering by `player_id`. The new key is populated and validated in parallel so a later app migration can be based on measured parity rather than assumptions.

## Current State

The identity dictionary is implemented in:

- `basketball_test.player_identities`
- `basketball_test.player_identity_map`
- `basketball_test.resolved_player_identity_v`
- `basketball_test.player_identity_aliases_v`

ETL currently:

1. synchronizes the dictionary;
2. reads legacy-compatible corrections from `player_identity_aliases_v`;
3. canonicalizes provider IDs before writing base tables;
4. leaves app queries unchanged.

Test-schema snapshot on 2026-06-18:

| Object | Type | Approx. rows | Total size |
|---|---:|---:|---:|
| `full_rosters` | table | 13,934 | 3.0 MB |
| `lineups_lookup` | table | 341,225 | 113.6 MB |
| `player_onoff_by_game` | materialized view | 728,048 | 114.5 MB |
| `player_four_factors_by_game` | table | 726,994 | 329.8 MB |
| `player_traditional_stats_mv` | materialized view | 566 | 0.2 MB |
| `onoff_default_mv` | table | 655 | 0.5 MB |
| `player_advanced_stats_mv` | table | 655 | 0.6 MB |

The resolver currently reports zero unresolved source contexts.

## Important Compatibility Constraint

The app uses `SELECT *` against:

- `onoff_default_mv`
- `player_advanced_stats_mv`
- `player_traditional_stats_mv`

Adding `identity_id` directly to these objects could expose an unexpected column to R rendering and export paths. Therefore:

- base and game-grain objects may receive a physical `identity_id` column;
- app-facing season aggregates keep their current schemas during the shadow phase;
- companion identity views expose the app aggregates plus `identity_id`;
- the app will use those companion views only in a later, explicit migration.

## Scope

### Included

Physical `identity_id bigint` columns:

- `full_rosters`
- `lineups_lookup`
- `player_onoff_by_game`
- `player_four_factors_by_game`

Shadow companion views:

- `onoff_default_identity_v`
- `player_advanced_stats_identity_v`
- `player_traditional_stats_identity_v`

ETL support:

- backfill identity values for historical games;
- update identity values on every ETL iteration;
- fail publication when processed player rows cannot be resolved;
- preserve existing `player_id` keys, indexes, and grouping.

Validation:

- structural coverage;
- identity consistency;
- metric parity;
- app result-schema parity;
- known reused-ID cases.

### Deferred

- changing app filters or query parameters to `identity_id`;
- grouping statistics by `identity_id`;
- changing current primary or unique keys;
- replacing `player_id` in lineup hashes or `player_ids` arrays;
- cross-team/cross-season app aggregation;
- direct identity columns on `actions_clean`, `possessions`, `pws`, or cold-storage Parquet files.

## Why Raw Action Tables Are Deferred

An action can reference several players:

- `player_id`
- `parameters_player`
- `parameters_player_in`
- `parameters_player_out`
- `parameters_fouled_on`

A single generic `identity_id` column would represent only the action actor and would not cover substitution or foul participants. These tables are also purged to cold storage after ETL.

The first shadow migration should derive player identity from canonicalized player-grain rows instead. If action-level identity becomes necessary later, use explicitly named columns such as:

```text
actor_identity_id
player_in_identity_id
player_out_identity_id
fouled_on_identity_id
```

That should be a separate design.

## Data Model

### Base tables

Add nullable columns first:

```sql
ALTER TABLE basketball_test.full_rosters
  ADD COLUMN IF NOT EXISTS identity_id bigint;

ALTER TABLE basketball_test.lineups_lookup
  ADD COLUMN IF NOT EXISTS identity_id bigint;
```

Do not change the existing keys:

```text
full_rosters:
  (game_id, team_id, player_id)

lineups_lookup:
  (id, game_id, team_id, quarter, end_game_seconds_remaining, player_id)
```

`identity_id` is functionally dependent on the player occurrence but is not part of compatibility uniqueness during the shadow phase.

### Game-grain objects

Add and propagate `identity_id`:

```text
player_onoff_by_game
player_four_factors_by_game
```

Their current unique indexes remain based on `player_id`. This guarantees that merely adding the identity metadata cannot merge or split rows.

Do not add an identity index initially. Add one only when a real query consumes it; the game-grain objects are large and speculative indexes would increase storage and refresh cost.

### App-facing companion views

Expose identity without changing current object schemas:

```sql
CREATE VIEW basketball_test.onoff_default_identity_v AS
SELECT o.*, m.identity_id
FROM basketball_test.onoff_default_mv o
JOIN basketball_test.player_identity_map m
  ON m.active
 AND m.provider = 'segev'
 AND m.game_id IS NULL
 AND m.game_year = o."Year"
 AND m.team_id = o.team_id
 AND m.source_player_id = o.player_id;
```

Use the same pattern for:

- `player_advanced_stats_identity_v`
- `player_traditional_stats_identity_v`

Before creating these views, verify that each season-level compatibility key resolves to one identity:

```text
(game_year, team_id, player_id) -> exactly one identity_id
```

If this is not true, stop instead of choosing `MIN(identity_id)` or another arbitrary value.

## Resolution Contract

For already-canonicalized base rows, resolve using the complete occurrence key:

```text
game_id + game_year + team_id + player_id
```

The implementation may join `resolved_player_identity_v` using:

```sql
r.game_id = source.game_id
AND r.game_year = source.game_year
AND r.team_id = source.team_id
AND r.source_player_id = source.player_id
```

Because ETL canonicalization has already run, corrected aliases should resolve through the canonical player’s self-map. For example:

```text
game 165, team 5, canonical player_id 2152
    -> McCormick identity

game 177, team 5, canonical player_id 2060
    -> Hagins identity
```

Never resolve by name alone.

## ETL Design

### New Phase 2B: Identity Enrichment

Insert a small phase after base-game commits and dictionary synchronization, but before sub-lineup generation and downstream refreshes:

```text
Phase 2:  canonical base-table load
Dictionary sync
Phase 2B: identity enrichment for processed game_ids
Phase 3+: existing downstream pipeline
```

Phase 2B should:

1. resolve identities for `processed_ids`;
2. update `full_rosters.identity_id`;
3. update `lineups_lookup.identity_id`;
4. assert zero null identities for valid player rows;
5. assert one identity per `(game_id, team_id, player_id)`;
6. fail the pipeline before publication if an assertion fails.

Recommended helper:

```r
backfill_player_identity_for_games(
  pg,
  schema,
  game_ids,
  log_msg = message
)
```

The helper belongs in `etl/player_identity_dictionary.R`.

### Transaction behavior

Run the two base-table updates and their validation in one transaction per game batch.

If enrichment fails:

- roll back the enrichment transaction;
- mark the ETL pipeline failed;
- do not publish the affected games to `etl_processed_games`;
- leave existing app queries unaffected because they do not read `identity_id`.

### Historical backfill

Backfill existing games in bounded batches, recommended 25–50 game IDs per transaction.

Avoid one update over the complete `lineups_lookup` table because it has approximately 341,000 rows and its indexes are already operationally important.

Historical backfill process:

1. capture baseline row counts and checksums;
2. add nullable columns;
3. iterate scored game IDs in batches;
4. run the enrichment helper;
5. validate each batch;
6. run global validation;
7. optionally add foreign keys after coverage is complete.

### Foreign keys

Foreign keys are optional during initial rollout.

If added:

```sql
ALTER TABLE basketball_test.full_rosters
  ADD CONSTRAINT full_rosters_identity_fk
  FOREIGN KEY (identity_id)
  REFERENCES basketball_test.player_identities(identity_id)
  NOT VALID;
```

Use `NOT VALID`, then validate after the backfill. Apply the same pattern to `lineups_lookup`.

Do not set `NOT NULL` until at least one full automated ETL cycle has completed with zero unresolved rows. Keeping the column nullable is also acceptable long term if failure is enforced in ETL validation.

## Downstream Object Changes

### `player_onoff_by_game`

Update the build SQL so `base0` selects:

```sql
ll.player_id,
ll.identity_id,
ll.team_id,
ll.lineup_hash,
...
```

Carry `identity_id` through the final select and grouping.

Do not change:

- row grain;
- current metrics;
- existing unique index;
- `onoff_compute()` grouping or return schema.

Add a validation that one existing unique key never contains multiple identity IDs.

### `player_four_factors_by_game`

Update both:

- `sql/materialized_views/player_four_factors_by_game.sql`
- `sql/functions/refresh_player_four_factors_by_game_for_games.sql`

Carry `identity_id` from `lineups_lookup` through `base0`, `combined_data`, segment CTEs, and the final insert.

The incremental refresh insert column list must explicitly include `identity_id`.

Do not group by identity instead of player. Continue grouping by the existing player key and include the functionally dependent identity value.

### Traditional statistics

Do not add a column directly to `player_traditional_stats_mv` in the first rollout because Tab 5 queries it with `SELECT *`.

Create `player_traditional_stats_identity_v` as a companion view.

The dynamic function `get_player_traditional_dynamic()` must keep its current signature and return schema. A future identity-aware wrapper can join its result to the season identity map without changing the existing function.

### On/off and advanced season aggregates

Do not add columns directly to:

- `onoff_default_mv`
- `player_advanced_stats_mv`

Tab 1 queries both with `SELECT *`.

Create companion views only. Existing refresh functions and app queries remain unchanged.

## Validation Gates

### Gate 1: Structural coverage

For valid player rows:

```text
full_rosters.identity_id IS NOT NULL
lineups_lookup.identity_id IS NOT NULL
player_onoff_by_game.identity_id IS NOT NULL
player_four_factors_by_game.identity_id IS NOT NULL
```

Exclude only explicitly documented invalid or sentinel player IDs.

### Gate 2: One occurrence, one identity

Expected zero rows:

```sql
SELECT game_id, team_id, player_id
FROM basketball_test.lineups_lookup
GROUP BY game_id, team_id, player_id
HAVING count(DISTINCT identity_id) <> 1;
```

Run equivalent checks for each propagated table.

### Gate 3: Existing-key uniqueness

All current duplicate-key checks must remain zero:

- `onoff_default_mv`: `("Year", team_id, player_id)`
- `player_advanced_stats_mv`: `(game_year, team_id, player_id)`
- `player_onoff_by_game`: existing unique key
- `player_four_factors_by_game`: existing unique key
- `player_traditional_stats_mv`: `(game_year, team_id, player_id)`

### Gate 4: Metric parity

Capture before/after signatures using existing keys and metric columns.

At minimum compare:

- row count;
- existing-key checksum;
- sums of possessions, points, minutes, shot counts, and four-factor source counts;
- null counts for current metric columns.

Adding identity metadata must produce exact equality for all existing metrics.

### Gate 5: App schema parity

Verify that current app queries return the exact same columns in the same order.

Explicitly test:

- Tab 1 `onoff_default_mv`;
- Tab 1 `player_advanced_stats_mv`;
- Tab 5 `player_traditional_stats_mv`;
- `onoff_compute()`;
- `four_factors_compute()`;
- `get_player_traditional_dynamic()`.

The companion views may contain `identity_id`; current app sources must not.

### Gate 6: Known identity cases

Verify at least:

```text
McCormick game rows -> McCormick identity
Hagins game rows    -> Hagins identity
McCormick identity != Hagins identity
```

Also verify all active season aliases and game overrides.

### Gate 7: Identity consistency at aggregate grain

Expected zero rows:

```sql
SELECT game_year, team_id, player_id
FROM basketball_test.player_four_factors_by_game
GROUP BY game_year, team_id, player_id
HAVING count(DISTINCT identity_id) <> 1;
```

This gate is required before attaching one `identity_id` to a season aggregate companion view.

## Data Quality Report Additions

Extend `etl/run_data_quality_report.R` with:

- null identity values by propagated table;
- more than one identity per player occurrence;
- more than one identity per season aggregate compatibility key;
- identity IDs absent from `player_identities`;
- mismatch between base and game-grain identity IDs;
- known game-override identity mismatch;
- current app result-schema drift.

Severity:

- error: missing or conflicting identity in a processed game;
- error: metric or row-count parity failure;
- error: app result-schema drift;
- warning: same normalized name remains attached to multiple identities across teams/seasons and has not been reviewed.

## Deployment Plan

### Phase 0: Baseline and backup

1. Record object definitions, indexes, row counts, sizes, and signatures.
2. Record exact result schemas for app queries/functions.
3. Record identity coverage and known-case outputs.
4. Take targeted backups of the affected object definitions and small base tables.

Exit criterion: baseline artifacts are saved and reproducible.

### Phase 1: Base shadow columns

1. Add nullable `identity_id` to `full_rosters` and `lineups_lookup`.
2. Implement `backfill_player_identity_for_games()`.
3. Run historical backfill in batches.
4. Add Phase 2B to `etl_full.R`.
5. Add base-table DQ checks.

Exit criteria:

- zero missing valid identities;
- zero conflicting player occurrences;
- existing table counts and keys unchanged;
- targeted ETL rerun passes.

### Phase 2: Game-grain propagation

1. Update `player_onoff_by_game`.
2. Update `player_four_factors_by_game`.
3. Update incremental refresh SQL.
4. Rebuild/backfill affected objects.
5. Run exact metric parity checks.

Exit criteria:

- zero null/conflicting identities;
- exact metric parity;
- existing SQL function outputs unchanged.

### Phase 3: Aggregate companion views

1. Create `onoff_default_identity_v`.
2. Create `player_advanced_stats_identity_v`.
3. Create `player_traditional_stats_identity_v`.
4. Add season-grain identity consistency checks.
5. Keep app queries pointed at current objects.

Exit criteria:

- one identity per aggregate compatibility key;
- companion view row counts equal source object row counts;
- app result schemas remain unchanged.

### Phase 4: Observation period

Run at least one complete automated ETL cycle, preferably several daily cycles.

Monitor:

- unresolved identities;
- null coverage;
- dictionary mapping changes;
- refresh duration;
- database growth;
- app query and render tests.

Exit criterion: no identity or parity failures during the observation window.

### Phase 5: First app pilot

Recommended first consumer: the Compare tab’s player reference layer.

Reasons:

- it already has explicit player/team selection state;
- it benefits directly from stable cross-team identity;
- it can keep `player_id` as the query key while carrying `identity_id` as metadata;
- the change can be isolated from core Tab 1 metric tables.

Pilot behavior:

1. load `identity_id` in the player reference query;
2. keep current `player_id` values for existing SQL calls;
3. use `identity_id` only to recognize the same person across contexts;
4. add explicit tests before any cross-team aggregation.

Do not migrate every app tab together.

## Rollback Plan

The shadow design is intentionally reversible.

If a problem occurs:

1. disable Phase 2B enrichment;
2. restore previous game-grain object definitions;
3. drop companion identity views;
4. leave nullable base columns in place or drop them later;
5. keep the identity dictionary and compatibility correction view active.

Because existing app queries continue using `player_id`, disabling shadow propagation should not require an app rollback.

## Files Expected to Change During Implementation

New migration:

- `sql/migrations/2026-06-XX_player_identity_shadow_columns.sql`

ETL:

- `etl/player_identity_dictionary.R`
- `etl/etl_full.R`
- `etl/run_data_quality_report.R`

Game-grain SQL:

- `sql/materialized_views/player_onoff_by_game.sql`
- `sql/materialized_views/player_four_factors_by_game.sql`
- `sql/functions/refresh_player_four_factors_by_game_for_games.sql`

Companion views:

- `sql/views/player_identity_shadow_views.sql`

Tests:

- identity enrichment helper tests;
- DB shape and parity tests;
- app result-schema contract tests;
- targeted ETL dry-run and write-run tests.

## Implementation Order

1. Capture baselines.
2. Add base columns and the per-game enrichment helper.
3. Backfill `full_rosters`.
4. Backfill `lineups_lookup`.
5. Wire Phase 2B into ETL.
6. Validate a targeted game correction and one ordinary game.
7. Propagate identity into `player_onoff_by_game`.
8. Propagate identity into `player_four_factors_by_game`.
9. Run exact metric parity.
10. Add aggregate companion views.
11. Run the full test suite and one complete ETL cycle.
12. Observe before authorizing an app pilot.

## Definition of Done

The shadow migration is complete when:

- every valid persisted player occurrence has a stable `identity_id`;
- current `player_id` data and all existing metrics are unchanged;
- existing app queries return the same schemas and values;
- incremental ETL maintains identity coverage automatically;
- all ambiguity, coverage, and parity checks pass;
- companion views make identity available for future app work;
- no app surface groups by `identity_id` until separately approved.
