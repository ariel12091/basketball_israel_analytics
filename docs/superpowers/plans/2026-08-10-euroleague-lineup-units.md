# EuroLeague 2-5 Player Lineup Units Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give EuroLeague 2-, 3-, 4-, and 5-player lineup-unit statistics, from a new per-game fact through to a working league-scoped Shiny tab carrying the full EuroLeague filter set.

**Architecture:** Mimics the Israeli `sub_lineups` shape. A fact at five-player-lineup grain per game (`lineup_totals_by_game`), a metric-free season mapping that expands each lineup into its 26 sub-units (`sub_lineups`), and a cached season roll-up (`sub_lineups_stats_mv`). The 26× expansion therefore multiplies *distinct lineups per season*, not team-game-segments. Nothing is written back to `action_team_context_actions` or `matchup_segments_actions`.

**Tech Stack:** PostgreSQL 15 (Supabase, schema `euroleague`), Python 3.11 (`euroleague/.venv`), R 4.4.2 + Shiny/bslib, `psycopg`.

**Spec:** `docs/superpowers/specs/2026-08-10-euroleague-013-lineup-units-design.md`

## Global Constraints

- **Every database-touching step requires explicit user approval before it runs.** Do not create, alter, or refresh any object without it. Ask, wait, then act.
- Never create, alter, load, truncate, refresh, or otherwise modify objects in `basketball` or `basketball_test`. Every statement is `euroleague.`-qualified.
- Do not load new games. The corpus is the recorded `E/2025/1-84` under `load_run_id=4`.
- Do not push, merge, or deploy the branch.
- Every write transaction opens with `BEGIN;` then `SET LOCAL search_path TO euroleague, public;`.
- DDL connects on **port 5432** (direct); the app pool uses 6543.
- Store additive numerators, denominators, and seconds only. No stored ratio, no stored rank. Rates are derived after aggregation.
- `game_year` is the PROVIDER season: `schedule.season`, where 2025 means 2025-26.
- `DROP FUNCTION` wipes EXECUTE grants and `DROP`+`CREATE` on a materialized view wipes its SELECT grants. Re-grant everything a migration touches, not only what it creates.
- **Never retype verified SQL into new code.** Where this plan says "copy verbatim from `<file>:<lines>`", open that file and copy the bytes. Retyping is how defects entered migration 009.
- Migration order is `001 → 002 → 004 → 005 → 006 → 007 → 008 → 009 → 010 → 011 → 012 → 013 → 014`. Migration 003 is superseded and must never be applied.
- Python: `& .venv/Scripts/python.exe` from `euroleague/`. R: `/c/Program\ Files/R/R-4.4.2/bin/Rscript.exe`.
- `app/app.R` and `app/www/app.js` have mixed/LF line endings. Run `git diff --stat` after editing either; a 5-line change producing a 400-line diff means the editor rewrote the file — fix by re-applying on bytes with `git -c core.autocrlf=false add`.
- Commit after every task. Never `--no-verify`.

## File Structure

**Created:**

| Path | Responsibility |
|---|---|
| `euroleague/sql/013_lineup_units.sql` | Two tables, two refresh functions, indexes, grants |
| `euroleague/sql/014_lineup_units_read_layer.sql` | Season MV, `fetch_lineups_dynamic`, refresh registration, grants |
| `euroleague/scripts/measure_lineup_unit_fanout.py` | Read-only sizing evidence (Task 1) |
| `euroleague/scripts/verify_lineup_units.py` | Validation gates G1-G9 |
| `app/R/ui_tab10_euro_lineups.R` | Tab 10 UI |
| `app/R/server_tab10_euro_lineups.R` | Tab 10 server: fetch, Summary/FF render, modal |
| `tests/testthat/test-euro-lineup-units.R` | R unit tests for the new pure helpers |

**Modified:**

| Path | Change |
|---|---|
| `euroleague/src/euroleague_possessions/postgres_backend.py:374-393` | Add both tables to the schema guard's `expected` set |
| `euroleague/src/euroleague_possessions/postgres_backend.py:916-946` | Two refresh calls in `validate_game()` |
| `euroleague/tests/test_postgres_backend.py` | Extend `ActionTeamContextWiringTest` |
| `app/R/helpers.R` | Promote `auto_minposs_from_df()` out of `server_tab2.R` |
| `app/R/server_tab2.R:61-73` | Delete the local copy, use the shared helper |
| `app/R/global_euro.R` | Add `euro_fetch_players_basic()` |
| `app/app.R` | Source and wire tab 10 |
| `app/www/app.js:763-771` | Add tab 10's view-mode entry to `CFG` |
| `euroleague/PROJECT.md` | Rewrite the superseded "Next deliverable" section |

---

# Phase 1 — the fact and the mapping

### Task 1: Measure the fan-out

Evidence before DDL. Read-only; touches nothing.

**Files:**
- Create: `euroleague/scripts/measure_lineup_unit_fanout.py`

**Interfaces:**
- Consumes: nothing.
- Produces: printed counts only. No later task imports from this file.

- [ ] **Step 1: Write the measurement script**

```python
#!/usr/bin/env python
"""Read-only sizing evidence for the lineup-unit fact. Creates nothing."""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402


QUERIES = {
    "distinct (game, team, own_lineup, opp_starters)": """
        SELECT count(*) FROM (
          SELECT DISTINCT game_id, team_id, own_lineup, opp_starters
            FROM euroleague.matchup_segments_actions
        ) s
    """,
    "projected lineup_totals_by_game rows (x2 contexts)": """
        SELECT count(*) * 2 FROM (
          SELECT DISTINCT game_id, team_id, own_lineup, opp_starters
            FROM euroleague.matchup_segments_actions
        ) s
    """,
    "distinct (season, team, own_lineup)": """
        SELECT count(*) FROM (
          SELECT DISTINCT sch.season, ms.team_id, ms.own_lineup
            FROM euroleague.matchup_segments_actions ms
            JOIN euroleague.schedule sch ON sch.game_id = ms.game_id
        ) s
    """,
    "projected sub_lineups rows (x26 masks)": """
        SELECT count(*) * 26 FROM (
          SELECT DISTINCT sch.season, ms.team_id, ms.own_lineup
            FROM euroleague.matchup_segments_actions ms
            JOIN euroleague.schedule sch ON sch.game_id = ms.game_id
        ) s
    """,
    "euroleague schema size (MB)": """
        SELECT round(sum(pg_total_relation_size(c.oid)) / 1048576.0, 1)
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'euroleague'
    """,
}


def main() -> int:
    connection = connect_from_env_file(REPO.parent / "etl" / ".Renviron")
    cursor = connection.cursor()
    try:
        for label, sql in QUERIES.items():
            cursor.execute(sql)
            print(f"{label}: {cursor.fetchone()[0]}")
    finally:
        cursor.close()
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Ask the user for approval to connect to the database**

State plainly: this script only runs `SELECT`s and creates nothing. Wait for approval.

- [ ] **Step 3: Run it**

```bash
cd euroleague && ./.venv/Scripts/python.exe scripts/measure_lineup_unit_fanout.py
```

Expected: five numbers. The spec estimates ≤ 23k projected fact rows and ~65k projected mapping rows for 84 games.

- [ ] **Step 4: Compare against the spec's estimates**

If either projection exceeds its spec estimate by more than 2×, **stop and report to the user before writing any DDL.** The architecture assumes these are small; a large surprise means the assumption is wrong, not that the plan should continue.

- [ ] **Step 5: Commit**

```bash
git add euroleague/scripts/measure_lineup_unit_fanout.py
git commit -m "Add read-only sizing script for the lineup-unit fact"
```

---

### Task 2: Migration 013 — tables and refresh functions

**Files:**
- Create: `euroleague/sql/013_lineup_units.sql`

**Interfaces:**
- Consumes: `euroleague.matchup_segments_actions`, `euroleague.action_team_context_actions`, `euroleague.full_rosters`, `euroleague.players`, `euroleague.schedule`, `euroleague.actions`.
- Produces:
  - table `euroleague.lineup_totals_by_game`
  - table `euroleague.sub_lineups`
  - `euroleague.refresh_lineup_totals_by_game(game_ids bigint[]) RETURNS bigint` — returns rows inserted
  - `euroleague.refresh_sub_lineups(game_ids bigint[]) RETURNS bigint` — returns rows inserted

- [ ] **Step 1: Write the migration file**

Create `euroleague/sql/013_lineup_units.sql` with exactly this content:

```sql
-- EuroLeague shadow schema -- migration 013: 2-5 player lineup units.
--
-- Mimics the Israeli sub_lineups shape: a fact at five-player-lineup grain per
-- game, plus a metric-free season mapping that expands each lineup into its 26
-- sub-units. The 26x expansion multiplies distinct lineups per season, never
-- team-game-segments.
--
-- Nothing is written back to action_team_context_actions or
-- matchup_segments_actions. They keep their provider-name arrays untouched.

BEGIN;

SET LOCAL search_path TO euroleague, public;

CREATE TABLE IF NOT EXISTS euroleague.lineup_totals_by_game (
  game_id            bigint   NOT NULL REFERENCES euroleague.schedule(game_id)
                                ON DELETE CASCADE,
  team_id            bigint   NOT NULL REFERENCES euroleague.teams(team_id),
  lineup_key         text     NOT NULL,
  type_lineup        text     NOT NULL CHECK (type_lineup IN ('offense', 'defense')),
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
  -- Floor time lives on offense rows only, so a naive SUM across both
  -- contexts cannot double-count minutes. This makes that a schema
  -- guarantee rather than a convention a later query might forget.
  CHECK ((type_lineup = 'offense') = (seconds IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS euroleague_lineup_totals_by_game_season_idx
  ON euroleague.lineup_totals_by_game
     (competition, game_year, team_id, lineup_key, type_lineup);

-- Identity and mapping only. A unit maps to MANY lineups; that many-to-many is
-- what makes a 2-player unit answerable at all.
--
-- Grain difference from the Israeli relation of the same name: that one holds
-- sizes 2-4 and synthesizes size 5 from the full lineup hash elsewhere. This
-- one holds 2-5 uniformly, and because unit_key and lineup_key use the same
-- md5 construction, unit_key = lineup_key at size 5 automatically.
CREATE TABLE IF NOT EXISTS euroleague.sub_lineups (
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

-- The join direction the read layer uses: unit_key -> its lineups.
CREATE INDEX IF NOT EXISTS euroleague_sub_lineups_unit_idx
  ON euroleague.sub_lineups
     (competition, game_year, team_id, unit_key, unit_size);

-- ---------------------------------------------------------------------------
-- refresh_lineup_totals_by_game
-- ---------------------------------------------------------------------------
--
-- Two aggregations plus one identity join. Both aggregations group on
-- own_lineup, which is already present and already sorted on every source row,
-- so name resolution runs once per distinct lineup per game instead of once
-- per event.
--
-- The row set is driven from SEGMENTS, not from events, with the event counts
-- left-joined on. The Israeli original builds from its event fact and
-- therefore loses a segment's minutes when every event in it has a NULL
-- context. Here a lineup that was on court but recorded no offensive or
-- defensive event still gets its row and its seconds, with zero counts.

CREATE OR REPLACE FUNCTION euroleague.refresh_lineup_totals_by_game(
  game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.lineup_totals_by_game;
  ELSE
    DELETE FROM euroleague.lineup_totals_by_game WHERE game_id = ANY(game_ids);
  END IF;

  INSERT INTO euroleague.lineup_totals_by_game (
    game_id, team_id, lineup_key, type_lineup, opp_starters,
    competition, game_year, own_starters, own_lineup, player_ids,
    possessions, points, fg2_made, fg2_att, fg3_made, fg3_att,
    ts_possessions, fgm, fga, ft_attempts,
    orebounds, oreb_opportunities, turnovers, steals, seconds,
    load_run_id, derivation_version
  )
  WITH real_roster AS (
    SELECT fr.game_id, fr.team_id, fr.player_id, fr.source_player_name
      FROM euroleague.full_rosters fr
      JOIN euroleague.players p ON p.player_id = fr.player_id
     WHERE (game_ids IS NULL OR fr.game_id = ANY(game_ids))
       AND lower(p.provider_player_id) NOT IN ('team', 'total')
       AND lower(btrim(p.display_name)) NOT IN ('team', 'total')
  ),
  seg AS (
    SELECT
      ms.game_id,
      ms.team_id,
      ms.own_lineup,
      ms.opp_starters,
      max(ms.own_starters)   AS own_starters,
      sum(ms.segment_seconds) AS seconds
    FROM euroleague.matchup_segments_actions ms
    WHERE game_ids IS NULL OR ms.game_id = ANY(game_ids)
    GROUP BY ms.game_id, ms.team_id, ms.own_lineup, ms.opp_starters
  ),
  distinct_lineups AS (
    SELECT DISTINCT s.game_id, s.team_id, s.own_lineup FROM seg s
  ),
  keyed AS (
    SELECT
      d.game_id,
      d.team_id,
      d.own_lineup,
      ids.player_ids,
      md5(array_to_string(ids.player_ids, '_')) AS lineup_key
    FROM distinct_lineups d
    CROSS JOIN LATERAL (
      SELECT ARRAY(
        SELECT rr.player_id
          FROM real_roster rr
         WHERE rr.game_id = d.game_id
           AND rr.team_id = d.team_id
           AND rr.source_player_name = ANY(d.own_lineup)
         ORDER BY rr.player_id
      ) AS player_ids
    ) ids
  ),
  counts AS (
    SELECT
      atc.game_id,
      atc.team_id,
      atc.own_lineup,
      atc.type_lineup,
      atc.opp_starters,
      sum(atc.possession_flag)::integer   AS possessions,
      sum(atc.points)::integer            AS points,
      sum(atc.fg2_made)::integer          AS fg2_made,
      sum(atc.fg2_att)::integer           AS fg2_att,
      sum(atc.fg3_made)::integer          AS fg3_made,
      sum(atc.fg3_att)::integer           AS fg3_att,
      sum(atc.ts_possessions)::integer    AS ts_possessions,
      sum(atc.fgm)::integer               AS fgm,
      sum(atc.fga)::integer               AS fga,
      sum(atc.ft_attempts)::integer       AS ft_attempts,
      sum(atc.orebounds)::integer         AS orebounds,
      sum(atc.oreb_opportunities)::integer AS oreb_opportunities,
      sum(atc.turnovers)::integer         AS turnovers,
      sum(atc.steals)::integer            AS steals
    FROM euroleague.action_team_context_actions atc
    WHERE (game_ids IS NULL OR atc.game_id = ANY(game_ids))
      AND atc.type_lineup IS NOT NULL
    GROUP BY atc.game_id, atc.team_id, atc.own_lineup,
             atc.type_lineup, atc.opp_starters
  ),
  game_run AS (
    SELECT a.game_id, max(a.load_run_id) AS load_run_id
      FROM euroleague.actions a
     WHERE game_ids IS NULL OR a.game_id = ANY(game_ids)
     GROUP BY a.game_id
  )
  SELECT
    seg.game_id,
    seg.team_id,
    k.lineup_key,
    side.type_lineup,
    seg.opp_starters,
    sch.competition,
    sch.season,
    seg.own_starters,
    seg.own_lineup,
    k.player_ids,
    coalesce(c.possessions, 0),
    coalesce(c.points, 0),
    coalesce(c.fg2_made, 0),
    coalesce(c.fg2_att, 0),
    coalesce(c.fg3_made, 0),
    coalesce(c.fg3_att, 0),
    coalesce(c.ts_possessions, 0),
    coalesce(c.fgm, 0),
    coalesce(c.fga, 0),
    coalesce(c.ft_attempts, 0),
    coalesce(c.orebounds, 0),
    coalesce(c.oreb_opportunities, 0),
    coalesce(c.turnovers, 0),
    coalesce(c.steals, 0),
    CASE WHEN side.type_lineup = 'offense' THEN seg.seconds END,
    gr.load_run_id,
    'units-v1'
  FROM seg
  JOIN keyed k
    ON k.game_id = seg.game_id
   AND k.team_id = seg.team_id
   AND k.own_lineup = seg.own_lineup
  JOIN euroleague.schedule sch ON sch.game_id = seg.game_id
  LEFT JOIN game_run gr ON gr.game_id = seg.game_id
  CROSS JOIN (VALUES ('offense'), ('defense')) AS side(type_lineup)
  LEFT JOIN counts c
    ON c.game_id = seg.game_id
   AND c.team_id = seg.team_id
   AND c.own_lineup = seg.own_lineup
   AND c.opp_starters = seg.opp_starters
   AND c.type_lineup = side.type_lineup;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

-- ---------------------------------------------------------------------------
-- refresh_sub_lineups
-- ---------------------------------------------------------------------------
--
-- A deterministic index expansion over the five sorted resolved IDs, driven by
-- a static 26-row VALUES list: 10 pairs, 10 triples, 5 quads, 1 quintet. Never
-- a cross join against a roster.
--
-- Reads lineup_totals_by_game, so this performs no name resolution at all --
-- resolution happened once, upstream. Rows are additive identity and are never
-- deleted per game: a lineup observed in an earlier game of the same season
-- stays mapped.

CREATE OR REPLACE FUNCTION euroleague.refresh_sub_lineups(
  game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  INSERT INTO euroleague.sub_lineups (
    competition, game_year, team_id, lineup_key, unit_key, unit_size, player_ids
  )
  WITH masks(unit_size, idxs) AS (
    VALUES
      (2::smallint, ARRAY[1,2]), (2::smallint, ARRAY[1,3]),
      (2::smallint, ARRAY[1,4]), (2::smallint, ARRAY[1,5]),
      (2::smallint, ARRAY[2,3]), (2::smallint, ARRAY[2,4]),
      (2::smallint, ARRAY[2,5]), (2::smallint, ARRAY[3,4]),
      (2::smallint, ARRAY[3,5]), (2::smallint, ARRAY[4,5]),
      (3::smallint, ARRAY[1,2,3]), (3::smallint, ARRAY[1,2,4]),
      (3::smallint, ARRAY[1,2,5]), (3::smallint, ARRAY[1,3,4]),
      (3::smallint, ARRAY[1,3,5]), (3::smallint, ARRAY[1,4,5]),
      (3::smallint, ARRAY[2,3,4]), (3::smallint, ARRAY[2,3,5]),
      (3::smallint, ARRAY[2,4,5]), (3::smallint, ARRAY[3,4,5]),
      (4::smallint, ARRAY[1,2,3,4]), (4::smallint, ARRAY[1,2,3,5]),
      (4::smallint, ARRAY[1,2,4,5]), (4::smallint, ARRAY[1,3,4,5]),
      (4::smallint, ARRAY[2,3,4,5]),
      (5::smallint, ARRAY[1,2,3,4,5])
  )
  SELECT DISTINCT
    l.competition,
    l.game_year,
    l.team_id,
    l.lineup_key,
    md5(array_to_string(u.ids, '_')) AS unit_key,
    m.unit_size,
    u.ids
  FROM euroleague.lineup_totals_by_game l
  CROSS JOIN masks m
  CROSS JOIN LATERAL (
    SELECT ARRAY(
      SELECT l.player_ids[i] FROM unnest(m.idxs) AS i ORDER BY 1
    ) AS ids
  ) u
  WHERE game_ids IS NULL OR l.game_id = ANY(game_ids)
  ON CONFLICT DO NOTHING;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

GRANT SELECT ON
  euroleague.lineup_totals_by_game,
  euroleague.sub_lineups
TO app_readonly;

COMMIT;
```

- [ ] **Step 2: Verify the mask list is complete before applying anything**

The 26 masks are the one place a silent arithmetic error would survive review. Count them by hand from the file: 10 rows of size 2, 10 of size 3, 5 of size 4, 1 of size 5. Confirm `C(5,2)=10`, `C(5,3)=10`, `C(5,4)=5`, `C(5,5)=1`, and that no index pair repeats.

- [ ] **Step 3: Ask the user for approval to apply migration 013**

State: two new tables, two new functions, two indexes, one GRANT. Nothing dropped. Nothing outside `euroleague`. Wait for approval.

- [ ] **Step 4: Apply the migration**

Apply through the project's existing DDL path on port 5432, the same way migration 012 was applied (see `euroleague/RUNBOOK.md`).

- [ ] **Step 5: Confirm the objects exist and are tables, not views**

```sql
SELECT relname, relkind
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'euroleague'
   AND relname IN ('lineup_totals_by_game', 'sub_lineups');
```

Expected: both present with `relkind = 'r'`. Check `relkind`, never the name suffix — this schema's history is full of `_mv` names that are physical tables.

- [ ] **Step 6: Commit**

```bash
git add euroleague/sql/013_lineup_units.sql
git commit -m "Add migration 013: lineup-unit fact and season mapping"
```

---

### Task 3: Wire the refreshes into publication

The schema guard refuses to publish against a schema containing tables it does not know, so **publication is broken from the moment Task 2 lands until this task ships.** That is the intended fail-closed behaviour; it is also why these two tasks belong back to back.

**Files:**
- Modify: `euroleague/src/euroleague_possessions/postgres_backend.py:374-393` and `:916-946`
- Test: `euroleague/tests/test_postgres_backend.py`

**Interfaces:**
- Consumes: `euroleague.refresh_lineup_totals_by_game(bigint[])` and `euroleague.refresh_sub_lineups(bigint[])` from Task 2.
- Produces: no new Python symbols. `validate_game()` gains two statements.

- [ ] **Step 1: Write the failing tests**

Append to `euroleague/tests/test_postgres_backend.py`, immediately after the existing `ActionTeamContextWiringTest` class. These mirror that class's fakes exactly — `_SchemaConnection` and `LoadRunConnection` already exist in this file.

```python
class LineupUnitWiringTest(unittest.TestCase):
    """The unit relations must be known to the guard and refreshed on publish."""

    BASE_TABLES = {
        "load_runs", "teams", "players", "schedule", "source_artifacts",
        "player_four_factors_by_game", "team_four_factors_by_game",
        "matchup_segments_actions", "action_team_context_actions",
        "lineup_totals_by_game", "sub_lineups",
    }

    def test_schema_allowlist_accepts_the_unit_relations(self) -> None:
        """Publication cannot start until the guard knows both new tables.

        Asserted through behaviour rather than a source text match: a text
        match passes when the names appear only in a comment.
        """
        existing = set(TABLE_COLUMNS) | self.BASE_TABLES
        assert_shadow_schema_compatible(_SchemaConnection(existing))

    def test_schema_allowlist_still_rejects_an_unknown_table(self) -> None:
        """Widening the allowlist must not turn the guard off."""
        existing = set(TABLE_COLUMNS) | self.BASE_TABLES | {"rogue_table"}
        with self.assertRaises(RuntimeError) as caught:
            assert_shadow_schema_compatible(_SchemaConnection(existing))
        self.assertIn("rogue_table", str(caught.exception))

    def test_validate_game_refreshes_units_after_the_fact(self) -> None:
        """Order matters: the units read lineup_totals, which reads the fact.

        LoadRunConnection, not RecordingConnection: RecordingCursor has no
        fetchone(), so the first refresh that reads a result would abort the
        mock and leave every later statement unrecorded.
        """
        connection = LoadRunConnection()
        backend = PostgresTransactionBackend(connection, load_run_id=17)
        try:
            backend.validate_game(game_id=23)
        except Exception:
            pass  # the 1-tuple cannot satisfy the later count checks

        executed = [sql for sql, _ in connection.statements]

        def index_of(needle: str) -> int:
            return next(i for i, s in enumerate(executed) if needle in s)

        fact = index_of("refresh_actions_consumer_candidates")
        totals = index_of("refresh_lineup_totals_by_game")
        units = index_of("refresh_sub_lineups")

        self.assertLess(fact, totals, "the event fact must be refreshed first")
        self.assertLess(totals, units, "units are expanded from lineup totals")
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd euroleague && ./.venv/Scripts/python.exe -m unittest tests.test_postgres_backend -v
```

Expected: `test_schema_allowlist_accepts_the_unit_relations` fails with `RuntimeError: existing euroleague schema has unknown tables: ['lineup_totals_by_game', 'sub_lineups']`, and `test_validate_game_refreshes_units_after_the_fact` fails with `StopIteration`.

- [ ] **Step 3: Add both tables to the schema guard**

In `postgres_backend.py`, inside the `expected = {...}` set (around line 374), after the `"action_team_context_actions",` entry:

```python
        "action_team_context_actions",
        # Lineup-unit relations (migration 013). Like the two above, these are
        # rebuilt by their own refresh_*() functions rather than written by the
        # loader, but they live in the schema, so the guard has to know them or
        # it refuses to publish.
        "lineup_totals_by_game",
        "sub_lineups",
```

- [ ] **Step 4: Add both refreshes to `validate_game()`**

In `validate_game()`, immediately after the `refresh_team_four_factors_by_game_for_games` block and before `actual_counts = self._count_all_rows(cursor, game_id)`:

```python
            # Lineup-unit facts (migration 013). Ordered after the event fact
            # because refresh_lineup_totals_by_game reads
            # action_team_context_actions, and refresh_sub_lineups reads
            # lineup_totals_by_game. Without these a newly published game has
            # player and team analytics but no lineup units, and the lineup
            # surface silently omits it.
            cursor.execute(
                "SELECT euroleague.refresh_lineup_totals_by_game("
                "ARRAY[%s]::bigint[])",
                (game_id,),
            )
            cursor.fetchone()
            cursor.execute(
                "SELECT euroleague.refresh_sub_lineups(ARRAY[%s]::bigint[])",
                (game_id,),
            )
            cursor.fetchone()
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cd euroleague && ./.venv/Scripts/python.exe -m unittest tests.test_postgres_backend -v
```

Expected: PASS, including the pre-existing `ActionTeamContextWiringTest` tests.

- [ ] **Step 6: Run the full Python suite**

```bash
cd euroleague && ./.venv/Scripts/python.exe -m unittest discover -s tests -v
```

Expected: all pass. Report any failure rather than working around it.

- [ ] **Step 7: Commit**

```bash
git add euroleague/src/euroleague_possessions/postgres_backend.py euroleague/tests/test_postgres_backend.py
git commit -m "Refresh lineup units during EuroLeague publication"
```

---

### Task 4: Backfill and validation gates G1-G4, G7-G9

**Files:**
- Create: `euroleague/scripts/verify_lineup_units.py`

**Interfaces:**
- Consumes: both tables and both refresh functions.
- Produces: `main() -> int`, exit 0 when every gate passes. Task 7 extends this same file with G5 and G6.

- [ ] **Step 1: Write the gate script**

Create `euroleague/scripts/verify_lineup_units.py`:

```python
#!/usr/bin/env python
"""Validation gates for the EuroLeague lineup-unit relations.

Each gate is a query that must return zero rows. Three of the gates the design
brief originally proposed -- containment, duplicate units, and five-player
identity -- are tautologies under this architecture: units are generated FROM
observed lineups, the primary key forbids duplicates, and unit_key = lineup_key
at size 5 by construction. Asserting those would repeat migration 009's mistake
of checking what the schema already guarantees. These are the checks that can
actually fail.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402


# Each entry: (label, sql). The query must return zero rows to pass; any row it
# returns is printed as evidence of the failure.
GATES: list[tuple[str, str]] = [
    (
        "G1 every lineup resolves to exactly 5 internal player_ids",
        """
        SELECT game_id, team_id, own_lineup, cardinality(player_ids) AS resolved
          FROM euroleague.lineup_totals_by_game
         WHERE cardinality(player_ids) <> 5
         LIMIT 20
        """,
    ),
    (
        "G2 distinct lineups in a game map to distinct lineup_keys",
        """
        SELECT game_id, team_id, lineup_key, count(DISTINCT own_lineup) AS arrays
          FROM euroleague.lineup_totals_by_game
         GROUP BY game_id, team_id, lineup_key
        HAVING count(DISTINCT own_lineup) > 1
         LIMIT 20
        """,
    ),
    (
        "G3 lineup totals reconcile with team_four_factors_by_game",
        """
        WITH lineup_side AS (
          SELECT game_id, team_id,
                 sum(possessions) FILTER (WHERE type_lineup = 'offense') AS off_poss,
                 sum(points)      FILTER (WHERE type_lineup = 'offense') AS off_pts,
                 sum(possessions) FILTER (WHERE type_lineup = 'defense') AS def_poss,
                 sum(points)      FILTER (WHERE type_lineup = 'defense') AS def_pts
            FROM euroleague.lineup_totals_by_game
           GROUP BY game_id, team_id
        ),
        team_side AS (
          SELECT game_id, team_id,
                 sum(off_poss) AS off_poss, sum(off_pts) AS off_pts,
                 sum(def_poss) AS def_poss, sum(def_pts) AS def_pts
            FROM euroleague.team_four_factors_by_game
           GROUP BY game_id, team_id
        )
        SELECT l.game_id, l.team_id,
               l.off_poss, t.off_poss, l.off_pts, t.off_pts,
               l.def_poss, t.def_poss, l.def_pts, t.def_pts
          FROM lineup_side l
          FULL JOIN team_side t USING (game_id, team_id)
         WHERE l.off_poss IS DISTINCT FROM t.off_poss
            OR l.off_pts  IS DISTINCT FROM t.off_pts
            OR l.def_poss IS DISTINCT FROM t.def_poss
            OR l.def_pts  IS DISTINCT FROM t.def_pts
         LIMIT 20
        """,
    ),
    (
        "G4 lineup seconds equal segment seconds and canonical game length",
        """
        WITH lineup_seconds AS (
          SELECT game_id, team_id, sum(seconds) AS seconds
            FROM euroleague.lineup_totals_by_game
           WHERE type_lineup = 'offense'
           GROUP BY game_id, team_id
        ),
        segment_seconds AS (
          SELECT game_id, team_id, sum(segment_seconds) AS seconds
            FROM euroleague.matchup_segments_actions
           GROUP BY game_id, team_id
        ),
        game_length AS (
          SELECT game_id,
                 2400 + 300 * greatest(max(period) - 4, 0) AS seconds
            FROM euroleague.actions
           GROUP BY game_id
        )
        SELECT l.game_id, l.team_id, l.seconds, s.seconds, g.seconds
          FROM lineup_seconds l
          JOIN segment_seconds s USING (game_id, team_id)
          JOIN game_length g ON g.game_id = l.game_id
         WHERE l.seconds IS DISTINCT FROM s.seconds
            OR round(l.seconds) IS DISTINCT FROM round(g.seconds::numeric)
         LIMIT 20
        """,
    ),
    (
        "G7 a unit's possessions are never below a larger unit containing it",
        """
        WITH unit_poss AS (
          SELECT sl.competition, sl.game_year, sl.team_id, sl.unit_key,
                 sl.unit_size, sl.player_ids,
                 sum(l.possessions) AS poss
            FROM euroleague.sub_lineups sl
            JOIN euroleague.lineup_totals_by_game l
              ON l.competition = sl.competition AND l.game_year = sl.game_year
             AND l.team_id = sl.team_id AND l.lineup_key = sl.lineup_key
           WHERE l.type_lineup = 'offense'
           GROUP BY 1, 2, 3, 4, 5, 6
        )
        SELECT small.unit_key, small.poss, big.unit_key, big.poss
          FROM unit_poss small
          JOIN unit_poss big
            ON big.competition = small.competition
           AND big.game_year = small.game_year
           AND big.team_id = small.team_id
           AND big.unit_size > small.unit_size
           AND small.player_ids <@ big.player_ids
         WHERE small.poss < big.poss
         LIMIT 20
        """,
    ),
    (
        "G8 exactly 26 mapping rows per lineup, split 10/10/5/1",
        """
        SELECT competition, game_year, team_id, lineup_key,
               count(*) AS total,
               count(*) FILTER (WHERE unit_size = 2) AS pairs,
               count(*) FILTER (WHERE unit_size = 3) AS triples,
               count(*) FILTER (WHERE unit_size = 4) AS quads,
               count(*) FILTER (WHERE unit_size = 5) AS quints
          FROM euroleague.sub_lineups
         GROUP BY 1, 2, 3, 4
        HAVING count(*) <> 26
            OR count(*) FILTER (WHERE unit_size = 2) <> 10
            OR count(*) FILTER (WHERE unit_size = 3) <> 10
            OR count(*) FILTER (WHERE unit_size = 4) <> 5
            OR count(*) FILTER (WHERE unit_size = 5) <> 1
         LIMIT 20
        """,
    ),
]


def run_gates(cursor, gates: list[tuple[str, str]]) -> list[str]:
    failures: list[str] = []
    for label, sql in gates:
        cursor.execute(sql)
        rows = cursor.fetchall()
        if rows:
            failures.append(f"{label}: {len(rows)} offending row(s), e.g. {rows[0]}")
            print(f"FAIL {label}")
        else:
            print(f"ok   {label}")
    return failures


def check_idempotence(cursor) -> list[str]:
    """G9: refreshing one game twice must produce byte-identical rows."""
    cursor.execute(
        "SELECT game_id FROM euroleague.lineup_totals_by_game "
        "ORDER BY game_id LIMIT 1"
    )
    row = cursor.fetchone()
    if row is None:
        return ["G9 idempotence: no rows to test"]
    game_id = int(row[0])

    columns = (
        "game_id, team_id, lineup_key, type_lineup, opp_starters, "
        "competition, game_year, own_starters, own_lineup, player_ids, "
        "possessions, points, fg2_made, fg2_att, fg3_made, fg3_att, "
        "ts_possessions, fgm, fga, ft_attempts, orebounds, "
        "oreb_opportunities, turnovers, steals, seconds"
    )
    order = "game_id, team_id, lineup_key, type_lineup, opp_starters"

    cursor.execute(
        f"SELECT {columns} FROM euroleague.lineup_totals_by_game "
        f"WHERE game_id = %s ORDER BY {order}",
        (game_id,),
    )
    before = cursor.fetchall()

    cursor.execute(
        "SELECT euroleague.refresh_lineup_totals_by_game(ARRAY[%s]::bigint[])",
        (game_id,),
    )
    cursor.fetchone()

    cursor.execute(
        f"SELECT {columns} FROM euroleague.lineup_totals_by_game "
        f"WHERE game_id = %s ORDER BY {order}",
        (game_id,),
    )
    after = cursor.fetchall()

    if before != after:
        print("FAIL G9 refreshing a game twice is not idempotent")
        return [f"G9 idempotence: game {game_id} changed on re-refresh"]
    print("ok   G9 refreshing a game twice is idempotent")
    return []


def main() -> int:
    connection = connect_from_env_file(REPO.parent / "etl" / ".Renviron")
    cursor = connection.cursor()
    try:
        failures = run_gates(cursor, GATES)
        failures.extend(check_idempotence(cursor))
        connection.commit()
    finally:
        cursor.close()
        connection.close()

    if failures:
        print("\nFAILURES:")
        for failure in failures:
            print(f"  {failure}")
        return 1
    print("\nAll gates passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Run the gates before backfilling, to verify they fail**

```bash
cd euroleague && ./.venv/Scripts/python.exe scripts/verify_lineup_units.py
```

Expected: **G3 fails**, because `lineup_totals_by_game` has no rows while `team_four_factors_by_game` does, so the `FULL JOIN` yields NULL-versus-value mismatches.

Note that G1, G2, G7 and G8 will *pass* against the empty tables — they are `GROUP BY`/`HAVING` gates and a table with no rows produces no offending rows. That is expected and is exactly why G3 is the one to watch here: it is the only gate whose failure proves the harness is actually connected and reading real data. **If G3 passes at this point, the script is not seeing the database — stop and fix the harness before backfilling**, or the post-backfill run proves nothing.

- [ ] **Step 3: Ask the user for approval to backfill all 84 games**

State: two `INSERT`s across 84 games into the two new tables. Nothing else is touched. Wait for approval.

- [ ] **Step 4: Backfill**

```sql
BEGIN;
SET LOCAL search_path TO euroleague, public;
SELECT euroleague.refresh_lineup_totals_by_game(NULL);
SELECT euroleague.refresh_sub_lineups(NULL);
COMMIT;
```

Record both returned row counts and compare them against Task 1's projections. A count more than 2× the projection means the `seg`/`counts` join is fanning out — stop and investigate rather than proceeding.

- [ ] **Step 5: Run the gates**

```bash
cd euroleague && ./.venv/Scripts/python.exe scripts/verify_lineup_units.py
```

Expected: every gate `ok`, exit 0. **If G3 or G4 fails, stop.** Those two are load-bearing; a failure means the fact does not reconcile with already-verified relations, and no later task should be started on top of it.

- [ ] **Step 6: Commit**

```bash
git add euroleague/scripts/verify_lineup_units.py
git commit -m "Add validation gates for the lineup-unit relations"
```

---

# Phase 2 — read path and Shiny tab

### Task 5: Migration 014 — season roll-up

**Files:**
- Create: `euroleague/sql/014_lineup_units_read_layer.sql` (the MV half; Task 6 appends the function)

**Interfaces:**
- Consumes: `euroleague.sub_lineups`, `euroleague.lineup_totals_by_game`, `euroleague.players`.
- Produces: materialized view `euroleague.sub_lineups_stats_mv` with columns
  `competition, game_year, team_id, unit_key, unit_size, player_ids, player_names, player_names_str, off_poss, off_pts, off_fg2_made, off_fg2_att, off_fg3_made, off_fg3_att, off_ts_poss, off_fgm, off_fga, off_fta, off_oreb, off_oreb_opp, off_tov, off_steals, def_*` (same 14 suffixes), `minutes`.

- [ ] **Step 1: Write the migration file**

Create `euroleague/sql/014_lineup_units_read_layer.sql`:

```sql
-- EuroLeague shadow schema -- migration 014: lineup-unit read layer.
--
-- Season roll-up plus the filtered dynamic function, matching the
-- default-fast-path / filtered-path split the player and team surfaces use.
--
-- No stored ratios. AGENTS.md requires additive counts and seconds only; the
-- app derives PPP and the four factors after aggregation. This is a deliberate
-- deviation from the Israeli sub_lineups_stats, which stores rounded PPP.

BEGIN;

SET LOCAL search_path TO euroleague, public;

DROP MATERIALIZED VIEW IF EXISTS euroleague.sub_lineups_stats_mv;

CREATE MATERIALIZED VIEW euroleague.sub_lineups_stats_mv AS
WITH unit_totals AS (
  SELECT
    sl.competition,
    sl.game_year,
    sl.team_id,
    sl.unit_key,
    sl.unit_size,
    sl.player_ids,
    sum(l.possessions)        FILTER (WHERE l.type_lineup = 'offense') AS off_poss,
    sum(l.points)             FILTER (WHERE l.type_lineup = 'offense') AS off_pts,
    sum(l.fg2_made)           FILTER (WHERE l.type_lineup = 'offense') AS off_fg2_made,
    sum(l.fg2_att)            FILTER (WHERE l.type_lineup = 'offense') AS off_fg2_att,
    sum(l.fg3_made)           FILTER (WHERE l.type_lineup = 'offense') AS off_fg3_made,
    sum(l.fg3_att)            FILTER (WHERE l.type_lineup = 'offense') AS off_fg3_att,
    sum(l.ts_possessions)     FILTER (WHERE l.type_lineup = 'offense') AS off_ts_poss,
    sum(l.fgm)                FILTER (WHERE l.type_lineup = 'offense') AS off_fgm,
    sum(l.fga)                FILTER (WHERE l.type_lineup = 'offense') AS off_fga,
    sum(l.ft_attempts)        FILTER (WHERE l.type_lineup = 'offense') AS off_fta,
    sum(l.orebounds)          FILTER (WHERE l.type_lineup = 'offense') AS off_oreb,
    sum(l.oreb_opportunities) FILTER (WHERE l.type_lineup = 'offense') AS off_oreb_opp,
    sum(l.turnovers)          FILTER (WHERE l.type_lineup = 'offense') AS off_tov,
    sum(l.steals)             FILTER (WHERE l.type_lineup = 'offense') AS off_steals,
    sum(l.possessions)        FILTER (WHERE l.type_lineup = 'defense') AS def_poss,
    sum(l.points)             FILTER (WHERE l.type_lineup = 'defense') AS def_pts,
    sum(l.fg2_made)           FILTER (WHERE l.type_lineup = 'defense') AS def_fg2_made,
    sum(l.fg2_att)            FILTER (WHERE l.type_lineup = 'defense') AS def_fg2_att,
    sum(l.fg3_made)           FILTER (WHERE l.type_lineup = 'defense') AS def_fg3_made,
    sum(l.fg3_att)            FILTER (WHERE l.type_lineup = 'defense') AS def_fg3_att,
    sum(l.ts_possessions)     FILTER (WHERE l.type_lineup = 'defense') AS def_ts_poss,
    sum(l.fgm)                FILTER (WHERE l.type_lineup = 'defense') AS def_fgm,
    sum(l.fga)                FILTER (WHERE l.type_lineup = 'defense') AS def_fga,
    sum(l.ft_attempts)        FILTER (WHERE l.type_lineup = 'defense') AS def_fta,
    sum(l.orebounds)          FILTER (WHERE l.type_lineup = 'defense') AS def_oreb,
    sum(l.oreb_opportunities) FILTER (WHERE l.type_lineup = 'defense') AS def_oreb_opp,
    sum(l.turnovers)          FILTER (WHERE l.type_lineup = 'defense') AS def_tov,
    sum(l.steals)             FILTER (WHERE l.type_lineup = 'defense') AS def_steals,
    -- seconds live on offense rows only, so this cannot double-count
    sum(l.seconds)            FILTER (WHERE l.type_lineup = 'offense') AS seconds
  FROM euroleague.sub_lineups sl
  JOIN euroleague.lineup_totals_by_game l
    ON l.competition = sl.competition
   AND l.game_year   = sl.game_year
   AND l.team_id     = sl.team_id
   AND l.lineup_key  = sl.lineup_key
  GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
  ut.competition, ut.game_year, ut.team_id, ut.unit_key, ut.unit_size,
  ut.player_ids,
  names.player_names,
  names.player_names_str,
  ut.off_poss, ut.off_pts, ut.off_fg2_made, ut.off_fg2_att,
  ut.off_fg3_made, ut.off_fg3_att, ut.off_ts_poss, ut.off_fgm, ut.off_fga,
  ut.off_fta, ut.off_oreb, ut.off_oreb_opp, ut.off_tov, ut.off_steals,
  ut.def_poss, ut.def_pts, ut.def_fg2_made, ut.def_fg2_att,
  ut.def_fg3_made, ut.def_fg3_att, ut.def_ts_poss, ut.def_fgm, ut.def_fga,
  ut.def_fta, ut.def_oreb, ut.def_oreb_opp, ut.def_tov, ut.def_steals,
  round(coalesce(ut.seconds, 0) / 60.0, 1) AS minutes
FROM unit_totals ut
CROSS JOIN LATERAL (
  SELECT
    array_agg(coalesce(p.display_name, '#' || u.pid::text) ORDER BY u.ord)
      AS player_names,
    string_agg(coalesce(p.display_name, '#' || u.pid::text), ', ' ORDER BY u.ord)
      AS player_names_str
  FROM unnest(ut.player_ids) WITH ORDINALITY AS u(pid, ord)
  LEFT JOIN euroleague.players p ON p.player_id = u.pid
) names
WITH NO DATA;

CREATE UNIQUE INDEX euroleague_sub_lineups_stats_mv_pk
  ON euroleague.sub_lineups_stats_mv
     (competition, game_year, team_id, unit_key);

CREATE INDEX euroleague_sub_lineups_stats_mv_size_idx
  ON euroleague.sub_lineups_stats_mv
     (competition, game_year, unit_size, team_id);

-- ---------------------------------------------------------------------------
-- Refresh entry point.
-- ---------------------------------------------------------------------------
--
-- NOT concurrent. refresh_app_materialized_views() runs inside the publication
-- transaction so a load cannot be marked completed with a stale snapshot, and
-- REFRESH ... CONCURRENTLY cannot run in a transaction block. Fail-closed
-- publication and concurrent refresh are mutually exclusive; this project has
-- already chosen fail-closed.

CREATE OR REPLACE FUNCTION euroleague.refresh_app_materialized_views()
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  REFRESH MATERIALIZED VIEW euroleague.final_schedule_mv;
  REFRESH MATERIALIZED VIEW euroleague.team_game_ratings_mv;
  REFRESH MATERIALIZED VIEW euroleague.team_ppp_ratings_mv;
  REFRESH MATERIALIZED VIEW euroleague.team_four_factors_mv;
  REFRESH MATERIALIZED VIEW euroleague.player_onoff_default_mv;
  REFRESH MATERIALIZED VIEW euroleague.player_advanced_stats_mv;
  REFRESH MATERIALIZED VIEW euroleague.sub_lineups_stats_mv;
END;
$function$;

GRANT SELECT ON euroleague.sub_lineups_stats_mv TO app_readonly;

COMMIT;
```

- [ ] **Step 2: Confirm no grant is silently dropped**

`refresh_app_materialized_views()` is `CREATE OR REPLACE`, not `DROP`+`CREATE`, so its EXECUTE grants survive. `sub_lineups_stats_mv` is newly created, so its grant is in this file. Confirm by listing what this migration touches and checking each against a `GRANT` line: `sub_lineups_stats_mv` (SELECT, present), `refresh_app_materialized_views` (replaced, grants retained). No other object is touched.

- [ ] **Step 3: Ask the user for approval to apply migration 014's MV half**

State: one new materialized view created `WITH NO DATA`, two indexes, one function replaced, one GRANT. Wait for approval.

- [ ] **Step 4: Apply, then populate**

```sql
REFRESH MATERIALIZED VIEW euroleague.sub_lineups_stats_mv;
```

Time this statement and record the duration. The spec's open item is whether the MV stays an MV; that decision needs this number.

- [ ] **Step 5: Sanity-check the shape**

```sql
SELECT unit_size, count(*) FROM euroleague.sub_lineups_stats_mv
 GROUP BY unit_size ORDER BY unit_size;
```

Expected: four rows, sizes 2-5, with counts decreasing as size increases.

- [ ] **Step 6: Commit**

```bash
git add euroleague/sql/014_lineup_units_read_layer.sql
git commit -m "Add migration 014: lineup-unit season roll-up"
```

---

### Task 6: `fetch_lineups_dynamic`

**Files:**
- Modify: `euroleague/sql/014_lineup_units_read_layer.sql` (append before the final `COMMIT;`)

**Interfaces:**
- Consumes: `euroleague.sub_lineups`, `euroleague.lineup_totals_by_game`, `euroleague.team_game_ratings_mv`, `euroleague.team_ppp_ratings_mv`.
- Produces: `euroleague.fetch_lineups_dynamic(text, int4, date, date, text, text, text, text, text, text, int4, text, int4, int4, int4, int4, int4, int4, int4, int4, text, text, int4)` returning one row per `(team_id, unit_key)`.

**Do not retype the schedule filter.** The `games` CTE and its parameter-normalisation preamble already exist, verified, in `euroleague/sql/006_team_four_factors.sql`. Copy those bytes.

- [ ] **Step 1: Copy the verified filter scaffolding**

Open `euroleague/sql/006_team_four_factors.sql` and copy two regions verbatim:

- **Lines 474-538** — the `get_team_four_factors_dynamic` signature preamble: the `DECLARE` block and every `v_*` normalisation (`v_competition`, `v_phases`, `v_team_ids`, `v_opp_ids`, `v_home_away`, `v_outcome`, `v_rank_side`, `v_rank_metric`). Keep the parameter names identical.
- **Lines 539-583** — the `schedule_ranked`, `team_ranked`, and `games` CTEs, unchanged.

Only the `RETURNS TABLE` declaration, the four extra parameters, and the aggregation after `games` differ.

- [ ] **Step 2: Append the function to migration 014**

Insert before the final `COMMIT;`. The `<copied preamble>` and `<copied CTEs>` markers below are the regions from Step 1 — paste the real bytes there.

```sql
-- ---------------------------------------------------------------------------
-- fetch_lineups_dynamic -- filtered path for 2-5 player units.
-- ---------------------------------------------------------------------------
--
-- sub_lineups -> lineup_totals_by_game -> the verified schedule filter.
--
-- sub_lineups's primary key means one unit_key has at most one row per
-- lineup_key, so this join can never duplicate a lineup_totals_by_game row
-- into a unit's sum. Without that key the whole surface would double-count.
--
-- p_players_on_csv / p_players_off_csv read sub_lineups.player_ids, not the
-- hash: "these two players together" is an array question.

CREATE OR REPLACE FUNCTION euroleague.fetch_lineups_dynamic(
    p_competition          TEXT,
    p_game_year            INTEGER,
    p_start_date           DATE    DEFAULT NULL,
    p_end_date             DATE    DEFAULT NULL,
    p_team_ids_csv         TEXT    DEFAULT NULL,
    p_phase_csv            TEXT    DEFAULT NULL,
    p_opp_ids_csv          TEXT    DEFAULT NULL,
    p_home_away            TEXT    DEFAULT 'all',
    p_outcome              TEXT    DEFAULT 'all',
    p_opp_rank_side        TEXT    DEFAULT NULL,
    p_opp_rank_n           INTEGER DEFAULT NULL,
    p_opp_rank_metric      TEXT    DEFAULT NULL,
    p_min_gn               INTEGER DEFAULT NULL,
    p_max_gn               INTEGER DEFAULT NULL,
    p_last_n_games         INTEGER DEFAULT NULL,
    p_num_starters_off_min INTEGER DEFAULT NULL,
    p_num_starters_off_max INTEGER DEFAULT NULL,
    p_num_starters_def_min INTEGER DEFAULT NULL,
    p_num_starters_def_max INTEGER DEFAULT NULL,
    p_unit_size            INTEGER DEFAULT 5,
    p_players_on_csv       TEXT    DEFAULT NULL,
    p_players_off_csv      TEXT    DEFAULT NULL,
    p_min_poss             INTEGER DEFAULT 0
)
RETURNS TABLE (
    team_id          BIGINT,
    unit_key         TEXT,
    unit_size        SMALLINT,
    player_ids       BIGINT[],
    player_names     TEXT[],
    player_names_str TEXT,
    off_poss         BIGINT, off_pts       BIGINT,
    off_fg2_made     BIGINT, off_fg2_att   BIGINT,
    off_fg3_made     BIGINT, off_fg3_att   BIGINT,
    off_ts_poss      BIGINT, off_fgm       BIGINT, off_fga BIGINT,
    off_fta          BIGINT, off_oreb      BIGINT, off_oreb_opp BIGINT,
    off_tov          BIGINT, off_steals    BIGINT,
    def_poss         BIGINT, def_pts       BIGINT,
    def_fg2_made     BIGINT, def_fg2_att   BIGINT,
    def_fg3_made     BIGINT, def_fg3_att   BIGINT,
    def_ts_poss      BIGINT, def_fgm       BIGINT, def_fga BIGINT,
    def_fta          BIGINT, def_oreb      BIGINT, def_oreb_opp BIGINT,
    def_tov          BIGINT, def_steals    BIGINT,
    minutes          NUMERIC
)
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = force_custom_plan
AS $function$
DECLARE
  v_players_on  bigint[];
  v_players_off bigint[];
  -- <copied preamble: every v_* DECLARE from 006:474-538>
BEGIN
  -- <copied preamble: every v_* assignment from 006:474-538>

  v_players_on := CASE
    WHEN p_players_on_csv IS NULL OR btrim(p_players_on_csv) = '' THEN NULL
    ELSE string_to_array(btrim(p_players_on_csv), ',')::bigint[]
  END;
  v_players_off := CASE
    WHEN p_players_off_csv IS NULL OR btrim(p_players_off_csv) = '' THEN NULL
    ELSE string_to_array(btrim(p_players_off_csv), ',')::bigint[]
  END;

  RETURN QUERY
  -- <copied CTEs: schedule_ranked, team_ranked, games from 006:539-583>
  ,
  unit_rows AS (
    SELECT
      sl.team_id,
      sl.unit_key,
      sl.unit_size,
      sl.player_ids,
      l.type_lineup,
      l.possessions, l.points, l.fg2_made, l.fg2_att, l.fg3_made, l.fg3_att,
      l.ts_possessions, l.fgm, l.fga, l.ft_attempts,
      l.orebounds, l.oreb_opportunities, l.turnovers, l.steals, l.seconds
    FROM euroleague.sub_lineups sl
    JOIN euroleague.lineup_totals_by_game l
      ON l.competition = sl.competition
     AND l.game_year   = sl.game_year
     AND l.team_id     = sl.team_id
     AND l.lineup_key  = sl.lineup_key
    JOIN games g ON g.game_id = l.game_id AND g.team_id = l.team_id
    WHERE sl.competition = v_competition
      AND sl.game_year   = p_game_year
      AND sl.unit_size   = p_unit_size::smallint
      AND (v_players_on  IS NULL OR sl.player_ids @> v_players_on)
      AND (v_players_off IS NULL OR NOT (sl.player_ids && v_players_off))
      AND (p_num_starters_off_min IS NULL OR l.own_starters >= p_num_starters_off_min)
      AND (p_num_starters_off_max IS NULL OR l.own_starters <= p_num_starters_off_max)
      AND (p_num_starters_def_min IS NULL OR l.opp_starters >= p_num_starters_def_min)
      AND (p_num_starters_def_max IS NULL OR l.opp_starters <= p_num_starters_def_max)
  ),
  agg AS (
    SELECT
      u.team_id, u.unit_key, u.unit_size, u.player_ids,
      sum(u.possessions)        FILTER (WHERE u.type_lineup = 'offense') AS off_poss,
      sum(u.points)             FILTER (WHERE u.type_lineup = 'offense') AS off_pts,
      sum(u.fg2_made)           FILTER (WHERE u.type_lineup = 'offense') AS off_fg2_made,
      sum(u.fg2_att)            FILTER (WHERE u.type_lineup = 'offense') AS off_fg2_att,
      sum(u.fg3_made)           FILTER (WHERE u.type_lineup = 'offense') AS off_fg3_made,
      sum(u.fg3_att)            FILTER (WHERE u.type_lineup = 'offense') AS off_fg3_att,
      sum(u.ts_possessions)     FILTER (WHERE u.type_lineup = 'offense') AS off_ts_poss,
      sum(u.fgm)                FILTER (WHERE u.type_lineup = 'offense') AS off_fgm,
      sum(u.fga)                FILTER (WHERE u.type_lineup = 'offense') AS off_fga,
      sum(u.ft_attempts)        FILTER (WHERE u.type_lineup = 'offense') AS off_fta,
      sum(u.orebounds)          FILTER (WHERE u.type_lineup = 'offense') AS off_oreb,
      sum(u.oreb_opportunities) FILTER (WHERE u.type_lineup = 'offense') AS off_oreb_opp,
      sum(u.turnovers)          FILTER (WHERE u.type_lineup = 'offense') AS off_tov,
      sum(u.steals)             FILTER (WHERE u.type_lineup = 'offense') AS off_steals,
      sum(u.possessions)        FILTER (WHERE u.type_lineup = 'defense') AS def_poss,
      sum(u.points)             FILTER (WHERE u.type_lineup = 'defense') AS def_pts,
      sum(u.fg2_made)           FILTER (WHERE u.type_lineup = 'defense') AS def_fg2_made,
      sum(u.fg2_att)            FILTER (WHERE u.type_lineup = 'defense') AS def_fg2_att,
      sum(u.fg3_made)           FILTER (WHERE u.type_lineup = 'defense') AS def_fg3_made,
      sum(u.fg3_att)            FILTER (WHERE u.type_lineup = 'defense') AS def_fg3_att,
      sum(u.ts_possessions)     FILTER (WHERE u.type_lineup = 'defense') AS def_ts_poss,
      sum(u.fgm)                FILTER (WHERE u.type_lineup = 'defense') AS def_fgm,
      sum(u.fga)                FILTER (WHERE u.type_lineup = 'defense') AS def_fga,
      sum(u.ft_attempts)        FILTER (WHERE u.type_lineup = 'defense') AS def_fta,
      sum(u.orebounds)          FILTER (WHERE u.type_lineup = 'defense') AS def_oreb,
      sum(u.oreb_opportunities) FILTER (WHERE u.type_lineup = 'defense') AS def_oreb_opp,
      sum(u.turnovers)          FILTER (WHERE u.type_lineup = 'defense') AS def_tov,
      sum(u.steals)             FILTER (WHERE u.type_lineup = 'defense') AS def_steals,
      sum(u.seconds)            FILTER (WHERE u.type_lineup = 'offense') AS seconds
    FROM unit_rows u
    GROUP BY u.team_id, u.unit_key, u.unit_size, u.player_ids
  )
  SELECT
    a.team_id, a.unit_key, a.unit_size, a.player_ids,
    names.player_names, names.player_names_str,
    a.off_poss, a.off_pts, a.off_fg2_made, a.off_fg2_att,
    a.off_fg3_made, a.off_fg3_att, a.off_ts_poss, a.off_fgm, a.off_fga,
    a.off_fta, a.off_oreb, a.off_oreb_opp, a.off_tov, a.off_steals,
    a.def_poss, a.def_pts, a.def_fg2_made, a.def_fg2_att,
    a.def_fg3_made, a.def_fg3_att, a.def_ts_poss, a.def_fgm, a.def_fga,
    a.def_fta, a.def_oreb, a.def_oreb_opp, a.def_tov, a.def_steals,
    round(coalesce(a.seconds, 0) / 60.0, 1)
  FROM agg a
  CROSS JOIN LATERAL (
    SELECT
      array_agg(coalesce(p.display_name, '#' || x.pid::text) ORDER BY x.ord)
        AS player_names,
      string_agg(coalesce(p.display_name, '#' || x.pid::text), ', ' ORDER BY x.ord)
        AS player_names_str
    FROM unnest(a.player_ids) WITH ORDINALITY AS x(pid, ord)
    LEFT JOIN euroleague.players p ON p.player_id = x.pid
  ) names
  WHERE coalesce(a.off_poss, 0) + coalesce(a.def_poss, 0) >= coalesce(p_min_poss, 0);
END;
$function$;

GRANT EXECUTE ON FUNCTION
  euroleague.fetch_lineups_dynamic(
    text, int4, date, date, text, text, text, text, text, text, int4, text,
    int4, int4, int4, int4, int4, int4, int4, int4, text, text, int4)
TO app_readonly;
```

- [ ] **Step 3: Ask for approval, then apply**

State: one new function plus its EXECUTE grant. Wait for approval, then apply.

- [ ] **Step 4: Verify the filtered path agrees with the fast path**

The unfiltered call must reproduce the MV exactly. This is the check that proves the copied filter CTE was wired correctly.

```sql
WITH fn AS (
  SELECT team_id, unit_key, off_poss, off_pts, def_poss, def_pts, minutes
    FROM euroleague.fetch_lineups_dynamic('E', 2025, p_unit_size => 5)
),
mv AS (
  SELECT team_id, unit_key, off_poss, off_pts, def_poss, def_pts, minutes
    FROM euroleague.sub_lineups_stats_mv
   WHERE competition = 'E' AND game_year = 2025 AND unit_size = 5
)
SELECT count(*) AS mismatches
  FROM fn FULL JOIN mv USING (team_id, unit_key)
 WHERE fn.off_poss IS DISTINCT FROM mv.off_poss
    OR fn.off_pts  IS DISTINCT FROM mv.off_pts
    OR fn.def_poss IS DISTINCT FROM mv.def_poss
    OR fn.def_pts  IS DISTINCT FROM mv.def_pts
    OR fn.minutes  IS DISTINCT FROM mv.minutes;
```

Expected: `0`. A non-zero result means the copied `games` CTE is filtering when it should not — investigate before continuing.

- [ ] **Step 5: Re-run the security apply**

```bash
CONFIRM_DB_SECURITY_APPLY=1 "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/apply_db_security.R
```

Run from the repository root. This is required because function deployment can wipe `app_readonly` EXECUTE grants.

- [ ] **Step 6: Commit**

```bash
git add euroleague/sql/014_lineup_units_read_layer.sql
git commit -m "Add fetch_lineups_dynamic filtered path for lineup units"
```

---

### Task 7: Gates G5 and G6

G5 is the only gate that exercises the mapping through a different code path than the one under test. Write it from the source semantics — do **not** copy the MV's SQL and change the join.

**Files:**
- Modify: `euroleague/scripts/verify_lineup_units.py`

**Interfaces:**
- Consumes: `euroleague.sub_lineups_stats_mv`, `euroleague.team_ppp_ratings_mv`.
- Produces: two more entries in `GATES`.

- [ ] **Step 1: Write the failing gates**

Append these two tuples to the `GATES` list in `verify_lineup_units.py`, before the closing `]`:

```python
    (
        "G5 the MV agrees with an independent name-membership recomputation",
        """
        WITH unit_names AS (
          -- The unit's provider names in a given game, from the roster --
          -- deliberately NOT from lineup_key. This is the second, independent
          -- derivation path.
          SELECT DISTINCT
                 sl.competition, sl.game_year, sl.team_id, sl.unit_key,
                 l.game_id,
                 ARRAY(
                   SELECT fr.source_player_name
                     FROM euroleague.full_rosters fr
                    WHERE fr.game_id = l.game_id
                      AND fr.team_id = sl.team_id
                      AND fr.player_id = ANY(sl.player_ids)
                    ORDER BY fr.source_player_name
                 ) AS names
            FROM euroleague.sub_lineups sl
            JOIN euroleague.lineup_totals_by_game l
              ON l.competition = sl.competition AND l.game_year = sl.game_year
             AND l.team_id = sl.team_id AND l.lineup_key = sl.lineup_key
        ),
        recomputed AS (
          SELECT
            un.competition, un.game_year, un.team_id, un.unit_key,
            sum(atc.possession_flag) FILTER (WHERE atc.type_lineup = 'offense')
              AS off_poss,
            sum(atc.points) FILTER (WHERE atc.type_lineup = 'offense')
              AS off_pts,
            sum(atc.possession_flag) FILTER (WHERE atc.type_lineup = 'defense')
              AS def_poss,
            sum(atc.points) FILTER (WHERE atc.type_lineup = 'defense')
              AS def_pts
          FROM unit_names un
          JOIN euroleague.action_team_context_actions atc
            ON atc.game_id = un.game_id
           AND atc.team_id = un.team_id
           AND atc.own_lineup @> un.names
          WHERE atc.type_lineup IS NOT NULL
          GROUP BY 1, 2, 3, 4
        )
        SELECT m.unit_key, m.off_poss, r.off_poss, m.off_pts, r.off_pts,
               m.def_poss, r.def_poss, m.def_pts, r.def_pts
          FROM euroleague.sub_lineups_stats_mv m
          FULL JOIN recomputed r
            USING (competition, game_year, team_id, unit_key)
         WHERE m.off_poss IS DISTINCT FROM r.off_poss
            OR m.off_pts  IS DISTINCT FROM r.off_pts
            OR m.def_poss IS DISTINCT FROM r.def_poss
            OR m.def_pts  IS DISTINCT FROM r.def_pts
         LIMIT 20
        """,
    ),
    (
        "G6 size-5 units reproduce team season totals",
        """
        WITH unit_side AS (
          SELECT competition, game_year, team_id,
                 sum(off_poss) AS off_poss, sum(off_pts) AS off_pts,
                 sum(def_poss) AS def_poss, sum(def_pts) AS def_pts
            FROM euroleague.sub_lineups_stats_mv
           WHERE unit_size = 5
           GROUP BY 1, 2, 3
        )
        SELECT u.team_id, u.off_poss, t.off_poss, u.off_pts, t.off_pts,
               u.def_poss, t.def_poss, u.def_pts, t.def_pts
          FROM unit_side u
          FULL JOIN euroleague.team_ppp_ratings_mv t
            USING (competition, game_year, team_id)
         WHERE u.off_poss IS DISTINCT FROM t.off_poss
            OR u.off_pts  IS DISTINCT FROM t.off_pts
            OR u.def_poss IS DISTINCT FROM t.def_poss
            OR u.def_pts  IS DISTINCT FROM t.def_pts
         LIMIT 20
        """,
    ),
```

- [ ] **Step 2: Confirm `team_ppp_ratings_mv`'s column names before running**

```sql
SELECT column_name FROM information_schema.columns
 WHERE table_schema = 'euroleague' AND table_name = 'team_ppp_ratings_mv'
 ORDER BY ordinal_position;
```

If the season totals are not named `off_poss`/`off_pts`/`def_poss`/`def_pts`, adjust G6's column references to the real names. Do not assume.

- [ ] **Step 3: Run all gates**

```bash
cd euroleague && ./.venv/Scripts/python.exe scripts/verify_lineup_units.py
```

Expected: all nine `ok`, exit 0.

**If G5 fails, stop and report.** It is the only independent check of the mapping; a failure there means the unit numbers are wrong even though every other gate passes.

- [ ] **Step 4: Commit**

```bash
git add euroleague/scripts/verify_lineup_units.py
git commit -m "Add independent-recomputation gates for lineup units"
```

---

### Task 8: Shared R helper and the EuroLeague player lookup

`auto_minposs_from_df()` currently lives inside `server_tab2.R`'s server function, so tab 10 cannot reach it. Copying it would violate the standing rule that helpers live in `helpers.R` and are never duplicated into mocks. Promote it, with a characterization test proving the move is output-identical.

**Files:**
- Modify: `app/R/helpers.R`, `app/R/server_tab2.R:61-73`, `app/R/global_euro.R`
- Test: `tests/testthat/test-euro-lineup-units.R`

**Interfaces:**
- Produces:
  - `auto_minposs_from_df(df, usage_col = "total_poss", step = 10L, target_rows = 150L)` in `helpers.R` — returns `integer`, or `NA_integer_` on empty/missing input.
  - `euro_fetch_players_basic(competition, season)` in `global_euro.R` — returns a data frame with `player_id`, `player_name`, `team_id`.

- [ ] **Step 1: Write the characterization test**

Create `tests/testthat/test-euro-lineup-units.R`:

```r
test_that("auto_minposs_from_df reproduces the Tab 2 behaviour it replaces", {
  # Fewer rows than the target: no threshold is needed.
  small <- data.frame(total_poss = c(500, 400, 300))
  expect_identical(auto_minposs_from_df(small, target_rows = 150L), 0L)

  # More rows than the target: the kth largest value, rounded up to the step.
  many <- data.frame(total_poss = seq(1000, 1, by = -1))
  expect_identical(
    auto_minposs_from_df(many, target_rows = 10L, step = 10L),
    as.integer(ceiling(991 / 10) * 10)
  )

  # Empty, NULL, and missing-column inputs are NA, never an error.
  expect_true(is.na(auto_minposs_from_df(NULL)))
  expect_true(is.na(auto_minposs_from_df(data.frame())))
  expect_true(is.na(auto_minposs_from_df(data.frame(other = 1:3))))

  # Non-finite values are dropped before ranking.
  mixed <- data.frame(total_poss = c(100, NA, Inf, 50, 25))
  expect_identical(auto_minposs_from_df(mixed, target_rows = 2L, step = 10L), 50L)
})
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-euro-lineup-units.R')"
```

Expected: FAIL with `could not find function "auto_minposs_from_df"`.

- [ ] **Step 3: Move the helper into `helpers.R`**

Append to `app/R/helpers.R`, preserving the implementation byte-for-byte from `server_tab2.R:61-73` and only changing the `target_rows` default from the local `AUTO_TARGET_ROWS` constant to its literal value:

```r
# Auto minimum-possessions threshold: the kth largest usage value, rounded up
# to `step`, where k is the row-count target. Shared by Tab 2 (Israeli lineups)
# and Tab 10 (EuroLeague lineups). Returns 0 when the population already fits
# under the target, and NA when there is nothing to rank.
auto_minposs_from_df <- function(df, usage_col = "total_poss", step = 10L,
                                 target_rows = 150L) {
  if (is.null(df) || !NROW(df)) return(NA_integer_)
  if (!usage_col %in% names(df)) return(NA_integer_)
  vals <- suppressWarnings(as.numeric(df[[usage_col]]))
  vals <- vals[is.finite(vals)]
  if (!length(vals)) return(NA_integer_)
  vals <- sort(vals, decreasing = TRUE)
  n <- length(vals)
  if (n <= target_rows) return(0L)
  kth <- vals[target_rows]
  if (!is.finite(kth)) return(NA_integer_)
  as.integer(ceiling(kth / step) * step)
}
```

- [ ] **Step 4: Delete the local copy from `server_tab2.R`**

Remove lines 61-73 of `app/R/server_tab2.R` (the `auto_minposs_from_df <- function(...) {...}` block). Keep `AUTO_TARGET_ROWS <- 150L` — call sites at `:428` pass it explicitly, so they continue to work unchanged.

- [ ] **Step 5: Run the test to verify it passes**

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-euro-lineup-units.R')"
```

Expected: PASS.

- [ ] **Step 6: Run the whole R suite to prove Tab 2 is unchanged**

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_dir('tests/testthat')"
```

Expected: all pass. This move must be output-identical; a Tab 2 test failure means it was not.

- [ ] **Step 7: Add the EuroLeague player lookup**

Append to `app/R/global_euro.R`, following the exact `cached_ref_query` shape of `euro_fetch_teams()` immediately above it:

```r
# Players for the Tab 10 players-on/off pool. Its own cache key: sharing the
# Israeli fetch_players_basic() key would serve one league's roster to the
# other.
euro_fetch_players_basic <- function(competition, season) {
  competition <- as.character(competition)
  season <- as.integer(season)
  cached_ref_query(
    key = sprintf("euro_players_%s_%d", competition, season),
    query_fun = function() db_get_query(
      pg_pool,
      "SELECT DISTINCT fr.player_id, p.display_name AS player_name, fr.team_id
         FROM euroleague.full_rosters fr
         JOIN euroleague.players p ON p.player_id = fr.player_id
         JOIN euroleague.schedule s ON s.game_id = fr.game_id
        WHERE s.competition = $1::text AND s.season = $2::int4
          AND lower(p.provider_player_id) NOT IN ('team', 'total')
        ORDER BY p.display_name",
      params = list(competition, season)
    )
  )
}
```

- [ ] **Step 8: Commit**

```bash
git add app/R/helpers.R app/R/server_tab2.R app/R/global_euro.R tests/testthat/test-euro-lineup-units.R
git commit -m "Share the auto min-poss helper and add the EuroLeague player lookup"
```

---

### Task 9: Tab 10 UI

**Files:**
- Create: `app/R/ui_tab10_euro_lineups.R`

**Interfaces:**
- Consumes: `shared_head_tags()`, `tt()`, `lineup_player_filter_ui(id, layout, ...)` from `app/R/mod_lineup_player_filter.R`, `EURO_DEFAULT_START`, `EURO_DEFAULT_END`.
- Produces: `ui_tab10_euro_lineups()` returning a `tabPanel` with `value = "euro_lineups"`. Every input ID is prefixed `euro_ld_`.

- [ ] **Step 1: Read the file being mirrored**

Open `app/R/ui_tab8_euro.R` in full. Tab 10 copies its sidebar structure exactly — the mobile collapse button, the reset button, the date range, the starter-mode `fluidRow` pairs, the accordion toggle. Reproduce that structure rather than inventing a layout.

- [ ] **Step 2: Write the UI file**

Create `app/R/ui_tab10_euro_lineups.R`:

```r
# ui_tab10_euro_lineups.R - Tab 10: EuroLeague 2-5 player lineup units.
#
# Mirrors Tab 8's sidebar so the shared euro filter vocabulary behaves
# identically, plus the lineup-specific controls Tab 2 has:
#   * group size (2/3/4/5)
#   * team + players-on / players-off, via the shared filter module
#   * minimum possessions, auto by default
#
# Clutch controls are deliberately absent. They arrive with the query path that
# backs them; a disabled control that silently does nothing is worse than no
# control at all.

ui_tab10_euro_lineups <- function() tabPanel(
  title = tags$span(tags$i(class = "bi bi-people"), "EL Lineups"),
  value = "euro_lineups",
  fluidPage(
    shared_head_tags(),

    sidebarLayout(
      sidebarPanel(
        width = 3,
        div(
          class = "view-mode-container",
          radioButtons("euro_ld_view_mode", label = "Select View:",
                       choices = c("Summary", "Four Factors"),
                       selected = "Summary",
                       inline = TRUE)
        ),
        tags$hr(),
        tags$button(class = "btn btn-outline-secondary d-md-none w-100 mb-2",
                    `data-bs-toggle` = "collapse",
                    `data-bs-target` = "#euro-ld-filters",
                    "Show Filters"),
        div(
          id = "euro-ld-filters", class = "collapse d-md-block",
          actionButton("euro_ld_reset", "Reset to defaults"),
          tags$hr(),

          selectInput("euro_ld_group_size", "Group size",
                      choices = c("2" = "2", "3" = "3", "4" = "4", "5" = "5"),
                      selected = "5"),

          lineup_player_filter_ui(
            "euro_ld_lineup_filter",
            layout = "stacked",
            team_label = "Team",
            team_placeholder = "All teams"
          ),

          sliderInput("euro_ld_minposs", "Minimum possessions",
                      min = 0, max = 500, value = 0, step = 10),

          tags$hr(),

          dateRangeInput("euro_ld_date_range", "Game Date Range",
                         start = EURO_DEFAULT_START, end = EURO_DEFAULT_END,
                         min = EURO_DEFAULT_START, max = EURO_DEFAULT_END,
                         format = "yyyy-mm-dd"),
          selectizeInput("euro_ld_opponents", "Opponents", choices = NULL,
                         multiple = TRUE,
                         options = list(placeholder = "All opponents")),
          selectizeInput("euro_ld_phase", "Game type (phase)", choices = NULL,
                         multiple = TRUE,
                         options = list(placeholder = "All phases")),
          fluidRow(
            column(6, selectInput("euro_ld_gn_min", "Round from",
                                  choices = c("—" = ""), selected = "")),
            column(6, selectInput("euro_ld_gn_max", "Round to",
                                  choices = c("—" = ""), selected = ""))
          ),
          selectInput("euro_ld_last_n", "Last N games",
                      choices = c("All" = ""), selected = ""),
          selectInput("euro_ld_home_away", "Home / Away",
                      choices = c("All" = "all", "Home" = "home",
                                  "Away" = "away"),
                      selected = "all"),
          selectInput("euro_ld_outcome", "Outcome",
                      choices = c("All" = "all", "Wins" = "win",
                                  "Losses" = "loss"),
                      selected = "all"),
          fluidRow(
            column(4, selectInput("euro_ld_opp_rank_side", "Opp rank",
                                  choices = c("All" = "", "Top" = "top",
                                              "Bottom" = "bottom"),
                                  selected = "")),
            column(4, selectInput("euro_ld_opp_rank_n", "N",
                                  choices = c("—" = "", as.character(1:20)),
                                  selected = "")),
            column(4, selectInput("euro_ld_opp_rank_metric", "By",
                                  choices = c("Net" = "net", "Off" = "off",
                                              "Def" = "def"),
                                  selected = "net"))
          ),
          fluidRow(
            column(6, selectInput("euro_ld_num_starters_off_mode",
                                  tt("Own lineup starters", "own_starters"),
                                  choices = c("ALL" = "",
                                              "At least (>=)" = "gte",
                                              "At most (<=)" = "lte"),
                                  selected = "")),
            column(6, selectInput("euro_ld_num_starters_off", "Own value",
                                  choices = c("—" = "", as.character(0:5)),
                                  selected = ""))
          ),
          fluidRow(
            column(6, selectInput("euro_ld_num_starters_def_mode",
                                  tt("Opponent lineup starters", "opp_starters"),
                                  choices = c("ALL" = "",
                                              "At least (>=)" = "gte",
                                              "At most (<=)" = "lte"),
                                  selected = "")),
            column(6, selectInput("euro_ld_num_starters_def", "Opp value",
                                  choices = c("—" = "", as.character(0:5)),
                                  selected = ""))
          )
        )
      ),
      mainPanel(
        width = 9,
        uiOutput("euro_ld_filter_chips"),
        DT::dataTableOutput("euro_ld_table")
      )
    )
  )
)
```

- [ ] **Step 3: Confirm the file parses**

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "invisible(parse('app/R/ui_tab10_euro_lineups.R')); cat('parsed OK\n')"
```

Expected: `parsed OK`.

- [ ] **Step 4: Commit**

```bash
git add app/R/ui_tab10_euro_lineups.R
git commit -m "Add EuroLeague lineup units tab UI"
```

---

### Task 10: Tab 10 server

**Files:**
- Create: `app/R/server_tab10_euro_lineups.R`

**Interfaces:**
- Consumes: `euro_selected_competition(input)`, `euro_selected_game_year(input)`, `euro_fetch_teams()`, `euro_fetch_round_values()`, `euro_fetch_phases()`, `euro_fetch_players_basic()`, `euro_data_version()`, `lineup_player_filter_server(id, players_ref)`, `auto_minposs_from_df()`, `guard_heavy_request()`, `db_get_query()`, `pg_pool`.
- Produces: `server_tab10_euro_lineups(input, output, session, shared)`.

- [ ] **Step 1: Read the two files being combined**

Open `app/R/server_tab8_euro.R` (euro filter plumbing, chips, reset, `guard_heavy_request` usage, DT render conventions) and `app/R/server_tab2.R` (group size, players-on/off wiring, auto min-poss, TOTAL row). Tab 10 is Tab 8's plumbing around Tab 2's shape.

- [ ] **Step 2: Write the server file**

Create `app/R/server_tab10_euro_lineups.R`. Key requirements, each of which must appear in the implementation:

```r
# server_tab10_euro_lineups.R - Tab 10 server.
#
# Tab 8's euro filter plumbing around Tab 2's lineup shape.

server_tab10_euro_lineups <- function(input, output, session, shared) {

  # 1. Reference data. Own cache keys -- never an Israeli lookup's key.
  euro_ld_ref <- reactiveValues(teams = NULL, players = NULL)

  # 2. The shared players-on/off module, fed the EuroLeague player pool.
  ld_filter <- lineup_player_filter_server(
    "euro_ld_lineup_filter",
    players_ref = reactive(euro_ld_ref$players)
  )

  # 3. Populate every choice-driven control when competition/season changes.
  #    Follow server_tab8_euro.R's observeEvent shape exactly, including its
  #    updateDateRangeInput guard: updateDateRangeInput() with a start outside
  #    min yields NA. Check is.na() before using the bounds.

  # 4. The fetch. Always p_min_poss = 0 so ranks are computed over the whole
  #    comparison population; the displayed threshold is applied afterwards.
  #    Wrap in guard_heavy_request().
  euro_ld_data <- reactive({
    guard_heavy_request(session)
    db_get_query(
      pg_pool,
      "SELECT * FROM euroleague.fetch_lineups_dynamic(
         $1::text, $2::int4, $3::date, $4::date, $5::text, $6::text, $7::text,
         $8::text, $9::text, $10::text, $11::int4, $12::text,
         $13::int4, $14::int4, $15::int4,
         $16::int4, $17::int4, $18::int4, $19::int4,
         $20::int4, $21::text, $22::text, $23::int4)",
      params = list(
        euro_selected_competition(input),          # $1  p_competition
        euro_selected_game_year(input),            # $2  p_game_year
        input$euro_ld_date_range[[1]],             # $3  p_start_date
        input$euro_ld_date_range[[2]],             # $4  p_end_date
        csv_or_null(ld_filter$team()),             # $5  p_team_ids_csv
        csv_or_null(input$euro_ld_phase),          # $6  p_phase_csv
        csv_or_null(input$euro_ld_opponents),      # $7  p_opp_ids_csv
        input$euro_ld_home_away %||% "all",        # $8  p_home_away
        input$euro_ld_outcome %||% "all",          # $9  p_outcome
        blank_to_null(input$euro_ld_opp_rank_side),   # $10 p_opp_rank_side
        int_or_null(input$euro_ld_opp_rank_n),        # $11 p_opp_rank_n
        blank_to_null(input$euro_ld_opp_rank_metric), # $12 p_opp_rank_metric
        int_or_null(input$euro_ld_gn_min),         # $13 p_min_gn
        int_or_null(input$euro_ld_gn_max),         # $14 p_max_gn
        int_or_null(input$euro_ld_last_n),         # $15 p_last_n_games
        starters_min("off"),                       # $16 p_num_starters_off_min
        starters_max("off"),                       # $17 p_num_starters_off_max
        starters_min("def"),                       # $18 p_num_starters_def_min
        starters_max("def"),                       # $19 p_num_starters_def_max
        as.integer(input$euro_ld_group_size %||% 5),  # $20 p_unit_size
        csv_or_null(ld_filter$players_on()),       # $21 p_players_on_csv
        csv_or_null(ld_filter$players_off()),      # $22 p_players_off_csv
        0L                                         # $23 p_min_poss -- always 0;
                                                   # ranks need the full
                                                   # population, and the
                                                   # displayed threshold is
                                                   # applied afterwards
      )
    )
  }) |>
    bindCache(
      euro_selected_competition(input),
      euro_selected_game_year(input),
      input$euro_ld_date_range,
      ld_filter$team(), ld_filter$players_on(), ld_filter$players_off(),
      input$euro_ld_phase, input$euro_ld_opponents,
      input$euro_ld_home_away, input$euro_ld_outcome,
      input$euro_ld_opp_rank_side, input$euro_ld_opp_rank_n,
      input$euro_ld_opp_rank_metric,
      input$euro_ld_gn_min, input$euro_ld_gn_max, input$euro_ld_last_n,
      input$euro_ld_num_starters_off_mode, input$euro_ld_num_starters_off,
      input$euro_ld_num_starters_def_mode, input$euro_ld_num_starters_def,
      input$euro_ld_group_size,
      euro_data_version()   # invalidates every cache after a EuroLeague load
    )

  # csv_or_null / blank_to_null / int_or_null / starters_min / starters_max are
  # the same small normalisers server_tab8_euro.R already defines. Reuse that
  # file's versions rather than writing new ones; if they are local to tab 8,
  # promote them to helpers.R the way Task 8 promoted auto_minposs_from_df.

  # 5. Derive rates AFTER aggregation. Never read a stored ratio.
  #    off_ppp  = 100 * off_pts / off_poss
  #    def_ppp  = 100 * def_pts / def_poss
  #    net      = off_ppp - def_ppp
  #    ts_pct   = 100 * off_pts / (2 * off_ts_poss)
  #    tov_pct  = 100 * off_tov / off_poss
  #    oreb_pct = 100 * off_oreb / off_oreb_opp
  #    ftr      = 100 * off_fta / off_fga
  #    and the def_* equivalents from the def_ columns.
  #    Guard every denominator with NULLIF-equivalent R logic; a zero
  #    denominator is NA, never 0 and never 50.

  # 6. Auto min-poss on the team/player-filtered population, BEFORE the
  #    min_poss filter, using the shared helper:
  #      auto_minposs_from_df(df, usage_col = "total_poss", step = 10L)
  #    where total_poss = off_poss + def_poss. Manual slider use switches to
  #    manual; a filter change returns to auto. Use an `autoUpdating` flag so
  #    an auto-triggered slider update is not read as a manual one.

  # 7. TOTAL row: sum the raw counts, derive the rates from those sums, pin at
  #    top, rank fields NULL, not clickable.

  # 8. Filter chips and reset, mirroring server_tab8_euro.R.
}
```

Write the file in full following that outline, in the style of `server_tab8_euro.R`. Drive repetitive work from vectors and maps in one pass rather than long if-chains, and use base `lapply`/`vapply`/`Filter` rather than purrr, matching the surrounding code.

- [ ] **Step 3: Verify the four-factor denominators against the fact**

For one unit, compare the tab's computed TS% against a direct query:

```sql
SELECT 100.0 * off_pts / (2 * NULLIF(off_ts_poss, 0)) AS ts_pct
  FROM euroleague.sub_lineups_stats_mv
 WHERE competition = 'E' AND game_year = 2025 AND unit_size = 5
 ORDER BY off_poss DESC LIMIT 5;
```

The tab must show the same numbers. `ts_possessions` is FGA plus the last free throw of a committed-foul trip — **not** `FGA + 0.44 * FTA`. Do not substitute the conventional formula.

- [ ] **Step 4: Confirm the file parses**

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "invisible(parse('app/R/server_tab10_euro_lineups.R')); cat('parsed OK\n')"
```

- [ ] **Step 5: Commit**

```bash
git add app/R/server_tab10_euro_lineups.R
git commit -m "Add EuroLeague lineup units tab server"
```

---

### Task 11: Wire tab 10 into the app

**Files:**
- Modify: `app/app.R` (source lines ~24-28, UI list ~119-120, server calls ~462-463)
- Modify: `app/www/app.js:763-771`

**Interfaces:**
- Consumes: `ui_tab10_euro_lineups()`, `server_tab10_euro_lineups()`.
- Produces: a reachable tab with `value = "euro_lineups"`.

- [ ] **Step 1: Record the pre-edit diff baseline**

```bash
git diff --stat app/app.R app/www/app.js
```

Expected: empty. Both files have mixed line endings; this baseline is how you will detect an editor rewrite in Step 5.

- [ ] **Step 2: Source and wire in `app.R`**

After line 28 (`source("R/server_tab9_euro_team.R", local = TRUE)`):

```r
source("R/ui_tab10_euro_lineups.R", local = TRUE)
source("R/server_tab10_euro_lineups.R", local = TRUE)
```

After line 120 (`ui_tab9_euro_team()`), adding the required comma to the preceding line:

```r
  ui_tab9_euro_team(),
  ui_tab10_euro_lineups()
```

After line 463 (`server_tab9_euro_team(input, output, session, shared)`):

```r
  server_tab10_euro_lineups(input, output, session, shared)
```

- [ ] **Step 3: Add the view-mode entry in `app.js`**

The user-facing view selector is the navbar hover menu built from the hardcoded `CFG` array, not the sidebar radio. Both must be updated or the menu will not offer tab 10's views.

In `app/www/app.js`, inside `var CFG = [` (line 763), after the `euro_team` entry on line 771 — add a comma to that line first:

```javascript
    { tab: "euro_team", inputId: "euroteam_view_mode", items: ["Summary", "Four Factors"], def: "Summary" },
    { tab: "euro_lineups", inputId: "euro_ld_view_mode", items: ["Summary", "Four Factors"], def: "Summary" }
```

Then, in the league map at line 896, add `euro_lineups` alongside the other EuroLeague tabs:

```javascript
    euro: "el", euro_team: "el", euro_lineups: "el"
```

- [ ] **Step 4: Verify the app parses**

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "invisible(parse('app/app.R')); cat('parsed OK\n')"
```

- [ ] **Step 5: Check the diff is the size it should be**

```bash
git diff --stat app/app.R app/www/app.js
```

Expected: roughly 6 inserted lines in `app.R` and 3 changed lines in `app.js`. **If either file shows hundreds of changed lines, the editor rewrote its line endings.** Revert and re-apply on bytes:

```bash
git checkout -- app/app.R app/www/app.js
# re-apply with a byte-preserving edit, then:
git -c core.autocrlf=false add app/app.R app/www/app.js
```

- [ ] **Step 6: Launch the app and confirm the tab appears**

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app')"
```

Open the app, switch to the EuroLeague section, confirm "EL Lineups" is present and renders a table. Check the browser console (F12) for JavaScript errors from the `CFG` change.

- [ ] **Step 7: Commit**

```bash
git add app/app.R app/www/app.js
git commit -m "Wire the EuroLeague lineup units tab into the app"
```

---

### Task 12: Lineup game-log modal

This is the payoff for keeping `game_id` in the base fact's key: the per-game rows already exist, so the modal needs no new relation.

**Files:**
- Modify: `app/R/server_tab10_euro_lineups.R`

**Interfaces:**
- Consumes: `euroleague.sub_lineups`, `euroleague.lineup_totals_by_game`, `euroleague.final_schedule_mv`.
- Produces: an `observeEvent` on `input$euro_ld_clicked_unit` opening a `modalDialog`.

- [ ] **Step 1: Add the click handler to the table's JS render**

In the DT column definition for the player-names column, wrap the cell so a click sets a Shiny input carrying the `unit_key`. Guard the render function, per the standing DT rule:

```r
"function(data, type, row) {",
"  if (type !== 'display' || !row) return data;",
"  return '<a href=\"#\" class=\"euro-ld-unit\" data-unit=\"' + row[UNIT_KEY_COL] +",
"         '\" onclick=\"Shiny.setInputValue(\\'euro_ld_clicked_unit\\', this.dataset.unit, {priority: \\'event\\'}); return false;\">' +",
"         data + '</a>';",
"}"
```

Replace `UNIT_KEY_COL` with the zero-based index of the hidden `unit_key` column in the DT data.

- [ ] **Step 2: Add the modal server logic**

```r
  observeEvent(input$euro_ld_clicked_unit, ignoreInit = TRUE, {
    unit <- as.character(input$euro_ld_clicked_unit %||% "")
    if (!nzchar(unit)) return(NULL)

    comp <- euro_selected_competition(input)
    season <- euro_selected_game_year(input)

    # One row per game for this unit. The join through sub_lineups cannot
    # duplicate a game: its primary key gives one row per (lineup_key,
    # unit_key), so each lineup contributes each of its games exactly once.
    rows <- db_get_query(
      pg_pool,
      "SELECT f.game_date, f.round_number, f.opp_team_name, f.is_home,
              sum(l.possessions) FILTER (WHERE l.type_lineup = 'offense') AS off_poss,
              sum(l.points)      FILTER (WHERE l.type_lineup = 'offense') AS off_pts,
              sum(l.possessions) FILTER (WHERE l.type_lineup = 'defense') AS def_poss,
              sum(l.points)      FILTER (WHERE l.type_lineup = 'defense') AS def_pts,
              round(sum(l.seconds) FILTER (WHERE l.type_lineup = 'offense') / 60.0, 1) AS minutes
         FROM euroleague.sub_lineups sl
         JOIN euroleague.lineup_totals_by_game l
           ON l.competition = sl.competition AND l.game_year = sl.game_year
          AND l.team_id = sl.team_id AND l.lineup_key = sl.lineup_key
         JOIN euroleague.final_schedule_mv f
           ON f.game_id = l.game_id AND f.team_id = l.team_id
        WHERE sl.competition = $1::text AND sl.game_year = $2::int4
          AND sl.unit_key = $3::text
        GROUP BY f.game_date, f.round_number, f.opp_team_name, f.is_home
        ORDER BY f.game_date",
      params = list(comp, season, unit)
    )

    showModal(modalDialog(
      title = "Lineup game log",
      size = "l",
      easyClose = TRUE,
      DT::renderDataTable(rows, options = list(pageLength = 25, dom = "t"))
    ))
  })
```

- [ ] **Step 3: Verify `final_schedule_mv`'s column names**

```sql
SELECT column_name FROM information_schema.columns
 WHERE table_schema = 'euroleague' AND table_name = 'final_schedule_mv'
 ORDER BY ordinal_position;
```

If `opp_team_name` or `is_home` are named differently, adjust the query. Do not assume.

- [ ] **Step 4: Verify in the running app**

Launch the app, open the tab, click a lineup. Confirm the modal opens, shows one row per game, and that the summed `off_poss` across those rows equals the unit's season `off_poss` in the table behind the modal.

- [ ] **Step 5: Commit**

```bash
git add app/R/server_tab10_euro_lineups.R
git commit -m "Add lineup game-log modal to the EuroLeague lineups tab"
```

---

### Task 13: End-to-end verification and documentation

**Files:**
- Modify: `euroleague/PROJECT.md`

**Interfaces:**
- Consumes: everything above.
- Produces: an accurate handoff document.

- [ ] **Step 1: Re-run every automated check**

```bash
cd euroleague && ./.venv/Scripts/python.exe -m unittest discover -s tests -v
cd euroleague && ./.venv/Scripts/python.exe scripts/verify_lineup_units.py
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_dir('tests/testthat')"
```

Expected: all pass, gates exit 0. Report any failure with its output rather than working around it.

- [ ] **Step 2: Run the three R regression tests**

```bash
cd etl/tests
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" test_euroleague_event_grouping_fixtures.R
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" test_euroleague_group_events.R
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" test_euroleague_count_possessions.R
```

Expected: unchanged output. The parser was not touched; a change here means something unexpected happened.

- [ ] **Step 3: Reconcile the tab against the database by hand**

Pick three units — one pair, one quad, one full lineup — and for each:

1. Read the tab's season Summary numbers with no filters applied.
2. Query `sub_lineups_stats_mv` directly for the same `unit_key`.
3. Confirm off/def possessions, points, and minutes match exactly.
4. Apply a date filter in the tab, then run `fetch_lineups_dynamic` with the same dates and confirm they match.

Record the three `unit_key`s and the numbers in the commit message.

- [ ] **Step 4: Rewrite `PROJECT.md`**

Three edits:

1. Replace the whole "Next deliverable: 2-5 player lineup units" section with a short statement that the unit fact is implemented, pointing at this plan and the design spec. The unit-grain design it currently describes was not built.
2. In the current-schema tables, add `lineup_totals_by_game`, `sub_lineups`, and `sub_lineups_stats_mv` with their grains. Note explicitly that `sub_lineups` reuses an Israeli relation name with a different grain: mapping only, sizes 2-5 uniformly, no metrics.
3. In "Known gaps and risks", remove gap 1 (the unit fact is no longer missing) and add: clutch filtering for lineup units is not implemented, and unit identity is season-scoped because `players` is not yet a cross-season person dictionary.

Also update the migration order line to `... → 012 → 013 → 014`.

- [ ] **Step 5: Commit**

```bash
git add euroleague/PROJECT.md
git commit -m "Record the lineup-unit deliverable in the EuroLeague handoff"
```

---

## Deferred to Phase 3

Clutch filtering for lineup units. It cannot read the pre-aggregated fact: the margin test is per-event, needing the pre-shot margin derived from `action_team_context_actions.own_team_score` / `opp_team_score` minus the current event's points. That is a third query path and needs its own design pass covering the four clutch parameters, minutes attribution when a clutch window covers only part of a segment, and whether the OT bypass convention should match Israel's.

Do not add disabled clutch controls to the tab in the meantime.
