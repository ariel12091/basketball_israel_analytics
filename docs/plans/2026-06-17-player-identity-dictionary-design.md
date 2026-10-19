# Player Identity Dictionary Design

Date: 2026-06-17

Status: design saved for later implementation. Do not implement as part of the current ETL fix unless explicitly requested.

## Problem

The current player correction model maps one provider `player_id` to another provider `player_id`:

```text
alias_player_id -> canonical_player_id
```

That is useful as a tactical repair layer, but the "canonical" value is still a provider ID. This becomes awkward when:

- one real player appears under multiple provider IDs
- one provider ID is reused for different real players in different games
- game-level exceptions need to override season-level defaults
- app aggregates need a stable identity that is not tied to a provider's ID behavior

The McCormick/Hagins cases showed that provider IDs can be contextual. A real player identity should not be contextual; only the mapping from source ID to real identity should be contextual.

## Design Goal

Create one easy-to-query identity resolution layer:

```text
game_id + game_year + team_id + source_player_id -> identity_id
```

The `identity_id` is stable and represents a real person. The mapping can be season-scoped or game-scoped.

## Core Concepts

`identity_id`:

- internal stable key for a real person
- does not depend on `game_id`
- can be reused across seasons and teams when we are confident it is the same person

`source_player_id`:

- provider ID from PBP, box score, roster payloads, etc.
- can be wrong, duplicated, split, or reused

Mapping scope:

- `season`: default meaning of a source ID for a team-season
- `game`: exception for a specific game; wins over season mapping
- `source`: fallback when no explicit mapping exists

## Proposed Tables

### `player_identities`

One row per real player identity.

```sql
CREATE TABLE basketball_test.player_identities (
  identity_id bigserial PRIMARY KEY,
  display_name text NOT NULL,
  first_name text,
  last_name text,
  first_name_local text,
  last_name_local text,
  notes text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
```

### `player_identity_map`

Maps a provider/source ID in context to a stable identity.

```sql
CREATE TABLE basketball_test.player_identity_map (
  map_id bigserial PRIMARY KEY,
  provider text NOT NULL DEFAULT 'segev',
  game_year int NOT NULL,
  team_id int NOT NULL,
  game_id int,
  source_player_id int NOT NULL,
  identity_id bigint NOT NULL REFERENCES basketball_test.player_identities(identity_id),
  canonical_player_id int NOT NULL,
  scope text NOT NULL CHECK (scope IN ('season', 'game')),
  active boolean NOT NULL DEFAULT true,
  player_name text,
  reason text,
  evidence text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),

  CHECK (
    (scope = 'season' AND game_id IS NULL) OR
    (scope = 'game' AND game_id IS NOT NULL)
  )
);
```

Suggested uniqueness:

```sql
CREATE UNIQUE INDEX player_identity_map_one_active_season
  ON basketball_test.player_identity_map
  (provider, game_year, team_id, source_player_id)
  WHERE active AND game_id IS NULL;

CREATE UNIQUE INDEX player_identity_map_one_active_game
  ON basketball_test.player_identity_map
  (provider, game_id, team_id, source_player_id)
  WHERE active AND game_id IS NOT NULL;
```

These constraints make the resolver return at most one game mapping and at most one season mapping for a source player row.

## Resolution Rule

For a source row:

1. Use active game-level mapping for `provider + game_id + team_id + source_player_id`.
2. Else use active season-level mapping for `provider + game_year + team_id + source_player_id`.
3. Else fallback to source identity behavior and flag as `resolution_scope = 'source'`.

Game mapping must win over season mapping.

## Resolved View

Expose one queryable object for ETL, DQ, and investigation.

Expected view shape:

```text
game_id
game_year
team_id
source_player_id
identity_id
canonical_player_id
display_name
resolution_scope
reason
```

Conceptual query:

```sql
WITH source_players AS (
  SELECT DISTINCT
    ac.game_id,
    s.game_year,
    ac.team_id,
    ac.player_id AS source_player_id
  FROM basketball_test.actions_clean ac
  JOIN basketball_test.schedule s USING (game_id)
)
SELECT
  sp.game_id,
  sp.game_year,
  sp.team_id,
  sp.source_player_id,
  COALESCE(g.identity_id, y.identity_id, sp.source_player_id::bigint) AS identity_id,
  COALESCE(g.canonical_player_id, y.canonical_player_id, sp.source_player_id) AS canonical_player_id,
  COALESCE(pi_g.display_name, pi_y.display_name) AS display_name,
  CASE
    WHEN g.map_id IS NOT NULL THEN 'game'
    WHEN y.map_id IS NOT NULL THEN 'season'
    ELSE 'source'
  END AS resolution_scope,
  COALESCE(g.reason, y.reason) AS reason
FROM source_players sp
LEFT JOIN basketball_test.player_identity_map g
  ON g.active
 AND g.provider = 'segev'
 AND g.game_id = sp.game_id
 AND g.team_id = sp.team_id
 AND g.source_player_id = sp.source_player_id
LEFT JOIN basketball_test.player_identities pi_g
  ON pi_g.identity_id = g.identity_id
LEFT JOIN basketball_test.player_identity_map y
  ON y.active
 AND y.provider = 'segev'
 AND y.game_id IS NULL
 AND y.game_year = sp.game_year
 AND y.team_id = sp.team_id
 AND y.source_player_id = sp.source_player_id
LEFT JOIN basketball_test.player_identities pi_y
  ON pi_y.identity_id = y.identity_id;
```

Implementation should decide whether this is a plain view, materialized view, or SQL function. A view is easiest for investigation. A function that takes `game_ids int[]` may be better for ETL performance.

## Easy Queries

Resolve one source player row:

```sql
SELECT *
FROM basketball_test.resolved_player_identity_v
WHERE game_id = 62541
  AND team_id = 246
  AND source_player_id = 34404;
```

List all resolved identities used in one game:

```sql
SELECT *
FROM basketball_test.resolved_player_identity_v
WHERE game_id = 62541
ORDER BY team_id, display_name, source_player_id;
```

Find source IDs that map to the same identity:

```sql
SELECT
  game_year,
  team_id,
  identity_id,
  array_agg(DISTINCT source_player_id ORDER BY source_player_id) AS source_ids
FROM basketball_test.resolved_player_identity_v
GROUP BY game_year, team_id, identity_id
HAVING count(DISTINCT source_player_id) > 1;
```

Find game-specific overrides:

```sql
SELECT *
FROM basketball_test.resolved_player_identity_v
WHERE resolution_scope = 'game';
```

Find unresolved fallback rows:

```sql
SELECT *
FROM basketball_test.resolved_player_identity_v
WHERE resolution_scope = 'source';
```

The unresolved query is useful as a daily DQ signal.

## Migration Plan

1. Create `player_identities` and `player_identity_map`.
2. Populate identities from trusted existing `full_rosters` names.
3. Convert current `player_id_aliases` season rows into `player_identity_map` rows.
4. Convert current `player_id_game_overrides` rows into game-scoped `player_identity_map` rows.
5. Add `resolved_player_identity_v`.
6. Add a compatibility view that emits the current alias shape:

```text
game_id
game_year
team_id
alias_player_id
canonical_player_id
player_name
reason
correction_scope
```

7. Point `load_player_id_aliases()` to the compatibility view first, so current ETL keeps working.
8. Later add `identity_id` to `full_rosters`, `actions_clean`/cold-stage equivalents, `lineups_lookup`, and game-grain aggregate tables.
9. Eventually group app aggregates by `identity_id`, while keeping `canonical_player_id` only as a compatibility/display field.

## Risks

- Ambiguous mappings: the same source row must not resolve to more than one identity.
- Bad migration: existing aliases might encode a canonical provider ID but not a truly stable player identity.
- Mixed grouping: some reports could group by `player_id` while others group by `identity_id`.
- Fallback rows: if too many rows resolve as `source`, the dictionary is not actually controlling identity.
- Over-automation: automatic identity creation from names can merge distinct players with the same or malformed names.

## Guardrails

Add DQ checks for:

- more than one active game mapping for the same provider/game/team/source ID
- more than one active season mapping for the same provider/year/team/source ID
- game mapping whose `identity_id` conflicts with expected roster identity
- same `source_player_id` mapping to multiple identities inside the same game
- same normalized display name mapping to many identities without an explicit review note
- unresolved `source` rows in processed games above a small threshold

## Recommended First Implementation Scope

Do not rewrite app aggregates first.

First implementation should only:

1. Add tables and resolver view.
2. Backfill from existing correction tables.
3. Add DQ checks around mapping ambiguity.
4. Change `load_player_id_aliases()` to read from a dictionary-derived compatibility view.

This makes identity easier to query while preserving current ETL and app behavior.
