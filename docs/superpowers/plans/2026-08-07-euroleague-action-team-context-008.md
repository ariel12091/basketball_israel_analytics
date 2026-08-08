# EuroLeague `action_team_context` — Migration 008 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create `euroleague.action_team_context` — a persisted event × team-perspective fact — populate it for all 84 loaded games, and prove it can reproduce `player_four_factors_by_game` exactly, without anything reading it yet.

**Architecture:** A physical table at one row per (action, perspective team), maintained per game by `refresh_action_team_context_for_games(bigint[])` in the same DELETE-by-game/INSERT shape as every other `refresh_*_for_games` function in this schema. The refresh is called inside `PostgresTransactionBackend.validate_game()`, before the player and team four-factor refreshes, in the same per-game transaction. Nothing reads the fact in 008; migration 009 switches consumers.

**Tech Stack:** PostgreSQL 15 on Supabase (direct port 5432), Python 3.10 with pinned `psycopg==3.2.9`, project venv at `euroleague/.venv`.

**Spec:** `docs/superpowers/specs/2026-08-07-euroleague-action-team-context-design.md`

## Global Constraints

- Work on branch `shiny/euro-tab1`. Do **not** merge or push to `main`.
- `euroleague` schema only. No Israeli (`basketball`, `basketball_test`) object is read or written. `apply_shadow_schema()` rejects DDL containing `BASKETBALL.` or `BASKETBALL_TEST.` — this includes SQL comments.
- Every migration file must contain the literal marker `EuroLeague shadow schema` or `apply_shadow_schema()` refuses it.
- No migration in this plan may contain the string `DROP ` (with trailing space). `apply_shadow_schema()` rejects it. Use `CREATE TABLE IF NOT EXISTS` and `CREATE OR REPLACE FUNCTION`.
- Migration 003 is superseded and must never be applied. Applied order is `001 → 002 → 004 → 005 → 006 → 007 → 008`.
- All DB work uses the direct port 5432 via `connect_from_env_file(Path("../etl/.Renviron"), direct_port=5432)`, run from the `euroleague/` directory.
- Run Python as `./.venv/Scripts/python.exe` from `euroleague/`.
- The session default `statement_timeout` is `2min`. Any statement expected to exceed it must `SET LOCAL statement_timeout` inside its transaction.
- Output-identical gate: `derived_at` is excluded from every comparison. Comparing two fresh runs inside one transaction will **not** reveal a `derived_at` difference, because `now()` is the transaction timestamp — always compare against stored rows.
- Commit after each task. Commit messages end with:
  ```
  Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01VoYwMt63fpfNoeKjfgWmdL
  ```

## File Structure

| File | Responsibility |
|---|---|
| `euroleague/sql/008_action_team_context.sql` | Create (new) — `matchup_segments` and `action_team_context`, their constraints and indexes, and `refresh_action_team_context_for_games()` which maintains both. |
| `euroleague/scripts/verify_action_team_context.py` | Create (new) — the acceptance gate. Reproduces `player_four_factors_by_game` from the fact and diffs it against stored rows; also checks coverage and possession-side consistency. Standalone, read-only. |
| `euroleague/src/euroleague_possessions/postgres_backend.py` | Modify — add both tables to the `assert_shadow_schema_compatible()` allowlist; call the new refresh in `validate_game()`. |
| `euroleague/tests/test_postgres_backend.py` | Modify — cover the allowlist entries and the refresh call order. |
| `euroleague/scripts/probe_batched_publish.py` | Modify — add both relations to `PROJECTIONS`. |
| `euroleague/scripts/load_games.py` | Modify — add a coverage check to `verify()`. |
| `euroleague/RUNBOOK.md`, `euroleague/PROJECT.md` | Modify — migration order and status. |

---

## The side-assignment rule

This is the one piece of genuinely new logic. Every task below depends on it, so it is stated once here in full.

For a row `(event, perspective team T)`, `type_lineup` is:

| `play_type` | acting team is T | acting team is the opponent |
|---|---|---|
| `2FGM`, `2FGA`, `3FGM`, `3FGA`, `FTM`, `FTA`, `TO`, `O` (offensive rebound), `AS` (assist), `RV` (foul drawn) | `offense` | `defense` |
| `ST` (steal), `FV` (block), `CM` (foul committed), `D` (defensive rebound) | `defense` | `offense` |
| anything else, or `event_team_id IS NULL` | `NULL` | `NULL` |

**Only the first row group is verified by the 008 gate.** The measures that exist today are carried by `2FGM/2FGA/3FGM/3FGA/FTM/FTA/TO/O` (all offense-side) and `ST` (defense-side). `AS`, `RV`, `FV`, `CM` and `D` carry no measure column today, so their side is assigned by rule but **cannot** be proven by reproducing `player_four_factors_by_game`. Record that in the migration header; a future consumer that starts counting assists or blocks is the first thing that will test it.

`ST` is the one flip. The current code encodes it as `def_steals = steals WHEN event_team = side.team_id`, opposite to every other metric pair — that is not a bug to fix, it is the rule.

---

## Task 1: Acceptance gate that fails first

**Files:**
- Create: `euroleague/scripts/verify_action_team_context.py`

**Interfaces:**
- Produces: `verify(conn, game_ids: list[int] | None) -> int` returning the count of failed checks; `main()` exits 1 on any failure. Tasks 3, 4 and 5 run this script unchanged.

- [ ] **Step 1: Write the verification script**

Eight checks: coverage, two rows per action, possession side consistency, the unmodelled columns still being zero, segment durations tiling each team-game, and the three-way diff of the rebuilt player grain against the stored rows.

```python
#!/usr/bin/env python
"""Acceptance gate for euroleague.action_team_context (migration 008).

Read-only. Proves the fact can reproduce player_four_factors_by_game exactly
before anything is allowed to read it. Exit code 1 if any check fails.

    .venv/Scripts/python.exe scripts/verify_action_team_context.py
    .venv/Scripts/python.exe scripts/verify_action_team_context.py --games 1-3
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))
sys.path.insert(0, str(REPO / "scripts"))

from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402
from load_games import parse_games  # noqa: E402

# Rebuilds the player_four_factors_by_game grain from the fact alone. This is
# the prototype migration 009 promotes into the function body, so it must not
# reference the tables the current function derives from.
PLAYER_GRAIN_FROM_FACT = """
WITH real_roster AS (
  SELECT fr.game_id, fr.team_id, fr.player_id
    FROM euroleague.full_rosters fr
    JOIN euroleague.players p ON p.player_id = fr.player_id
   WHERE (%(game_ids)s IS NULL OR fr.game_id = ANY(%(game_ids)s))
     AND lower(p.provider_player_id) NOT IN ('team', 'total')
     AND lower(btrim(p.display_name)) NOT IN ('team', 'total')
),
exposure AS (
  SELECT atc.*,
         rr.player_id,
         CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END::smallint AS is_on_key
    FROM euroleague.action_team_context atc
    JOIN real_roster rr
      ON rr.game_id = atc.game_id AND rr.team_id = atc.team_id
    LEFT JOIN euroleague.lineup_players lp
      ON lp.lineup_id = atc.own_lineup_id AND lp.player_id = rr.player_id
   WHERE %(game_ids)s IS NULL OR atc.game_id = ANY(%(game_ids)s)
)
-- Minutes come from matchup_segments, which holds each segment's duration
-- exactly once. No DISTINCT and no MAX-per-segment convention: the duration
-- cannot be double-counted because it is not repeated. This mirrors the
-- current player_minutes CTE in 002 (lines 571-586), which joins the derived
-- joint_segments to the roster the same way. Credited to offense only.
minutes AS (
  SELECT ms.game_id, ms.team_id, rr.player_id,
         CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END::smallint AS is_on_key,
         ms.own_starters, ms.opp_starters,
         round(sum(ms.segment_seconds) / 60.0, 3) AS minutes
    FROM euroleague.matchup_segments ms
    JOIN real_roster rr
      ON rr.game_id = ms.game_id AND rr.team_id = ms.team_id
    LEFT JOIN euroleague.lineup_players lp
      ON lp.lineup_id = ms.own_lineup_id AND lp.player_id = rr.player_id
   WHERE %(game_ids)s IS NULL OR ms.game_id = ANY(%(game_ids)s)
   GROUP BY ms.game_id, ms.team_id, rr.player_id,
            CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END,
            ms.own_starters, ms.opp_starters
)
SELECT e.game_id, e.team_id, e.player_id, e.is_on_key, e.type_lineup,
       s.season                          AS game_year,
       e.own_starters                    AS num_starters,
       e.own_starters, e.opp_starters,
       sum(e.points)::numeric            AS total_points,
       sum(e.possession_flag)::bigint    AS total_poss,
       sum(e.ts_possessions)::bigint     AS ts_poss_count,
       sum(e.orebounds)::bigint          AS oreb_count,
       sum(e.oreb_opportunities)::bigint AS oreb_opportunities,
       sum(e.turnovers)::bigint          AS tov_count,
       sum(e.steals)::bigint             AS steal_count,
       sum(e.ft_attempts)::bigint        AS total_ft_attempts,
       sum(e.fga)::bigint                AS total_fga,
       sum(e.fgm)::bigint                AS total_fgm,
       sum(e.fg3_made)::bigint           AS total_fg3_made,
       -- Player-attributed variants: only when this player took the action,
       -- and only on offense. Mirrors off_player_ts_possessions in 002.
       sum(CASE WHEN e.type_lineup = 'offense'
                 AND e.action_player_id = e.player_id
                THEN e.ts_possessions ELSE 0 END)::bigint AS player_ts_poss_count,
       sum(CASE WHEN e.type_lineup = 'offense'
                 AND e.action_player_id = e.player_id
                THEN e.turnovers ELSE 0 END)::bigint      AS player_tov_count,
       CASE WHEN e.type_lineup = 'offense'
            THEN coalesce(max(m.minutes), 0) ELSE 0 END::numeric AS minutes,
       sum(e.fg2_made)::integer          AS fg2_made,
       sum(e.fg2_att)::integer           AS fg2_att,
       sum(e.fg3_made)::integer          AS fg3_made,
       sum(e.fg3_att)::integer           AS fg3_att,
       sum(e.layup_made)::integer        AS layup_made,
       sum(e.layup_att)::integer         AS layup_att,
       sum(e.dunk_made)::integer         AS dunk_made,
       sum(e.dunk_att)::integer          AS dunk_att,
       CASE WHEN e.type_lineup = 'offense'
            THEN coalesce(max(m.minutes), 0) ELSE 0 END::numeric AS onoff_minutes
  FROM exposure e
  JOIN euroleague.schedule s ON s.game_id = e.game_id
  LEFT JOIN minutes m
    ON m.game_id = e.game_id AND m.team_id = e.team_id
   AND m.player_id = e.player_id AND m.is_on_key = e.is_on_key
   AND m.own_starters = e.own_starters AND m.opp_starters = e.opp_starters
 WHERE e.type_lineup IS NOT NULL
 GROUP BY e.game_id, e.team_id, e.player_id, e.is_on_key, e.type_lineup,
          s.season, e.own_starters, e.opp_starters
"""

# Same column list, same order. Anything present in the stored table and absent
# here is a column the gate does not prove -- keep the two lists in lockstep.
#
# player_four_factors_by_game has 39 columns. The gate compares 32. The seven
# it does not compare, and why:
#   deflection_count, c3_made, c3_att, c3_known_att -- hardcoded 0 in the
#     current refresh; EuroLeague has no shots endpoint and no deflection
#     event. Asserted to still be all-zero by a separate check below, so the
#     day someone starts populating them the gate fails loudly instead of
#     silently ignoring them.
#   load_run_id, derivation_version, derived_at -- lineage, expected to differ.
STORED_PLAYER_GRAIN = """
SELECT game_id, team_id, player_id, is_on_key, type_lineup,
       game_year, num_starters, own_starters, opp_starters,
       total_points, total_poss, ts_poss_count, oreb_count,
       oreb_opportunities, tov_count, steal_count, total_ft_attempts,
       total_fga, total_fgm, total_fg3_made,
       player_ts_poss_count, player_tov_count, minutes,
       fg2_made, fg2_att, fg3_made, fg3_att,
       layup_made, layup_att, dunk_made, dunk_att, onoff_minutes
  FROM euroleague.player_four_factors_by_game
 WHERE %(game_ids)s IS NULL OR game_id = ANY(%(game_ids)s)
"""


def verify(conn: Any, game_ids: list[int] | None) -> int:
    cur = conn.cursor()
    cur.execute("SET LOCAL statement_timeout = '15min'")
    failures = 0
    params = {"game_ids": game_ids}

    def check(name: str, ok: bool, detail: str = "") -> None:
        nonlocal failures
        if not ok:
            failures += 1
        print(f"  {'PASS' if ok else 'FAIL'}  {name:46s} {detail}")

    # 1. Coverage: every published game has fact rows.
    cur.execute(
        "SELECT count(*) FROM euroleague.schedule s "
        " WHERE (%(game_ids)s IS NULL OR s.game_id = ANY(%(game_ids)s)) "
        "   AND NOT EXISTS (SELECT 1 FROM euroleague.action_team_context a "
        "                    WHERE a.game_id = s.game_id)",
        params,
    )
    missing = cur.fetchone()[0]
    check("every published game has fact rows", missing == 0, f"{missing} missing")

    # 2. Two rows per action, one per perspective team.
    cur.execute(
        "SELECT (SELECT count(*) FROM euroleague.action_team_context a "
        "         WHERE %(game_ids)s IS NULL OR a.game_id = ANY(%(game_ids)s)), "
        "       (SELECT count(*) FROM euroleague.actions_raw r "
        "         WHERE %(game_ids)s IS NULL OR r.game_id = ANY(%(game_ids)s))",
        params,
    )
    fact_rows, action_rows = cur.fetchone()
    check(
        "exactly two fact rows per action",
        fact_rows == 2 * action_rows,
        f"{fact_rows} vs 2 x {action_rows}",
    )

    # 3. No endpoint row contradicts its possession side.
    cur.execute(
        "SELECT count(*) FROM euroleague.action_team_context a "
        "  JOIN euroleague.possessions p "
        "    ON p.game_id = a.game_id "
        "   AND p.endpoint_source_event_order = a.source_event_order "
        " WHERE a.possession_flag = 1 "
        "   AND ((a.team_id = p.offense_team_id AND a.type_lineup <> 'offense') "
        "     OR (a.team_id <> p.offense_team_id AND a.type_lineup <> 'defense')) "
        "   AND (%(game_ids)s IS NULL OR a.game_id = ANY(%(game_ids)s))",
        params,
    )
    contradictions = cur.fetchone()[0]
    check(
        "possession side agrees with type_lineup",
        contradictions == 0,
        f"{contradictions} contradicting rows",
    )

    # 4. The four columns the gate cannot compare must still be all zero.
    cur.execute(
        "SELECT count(*) FROM euroleague.player_four_factors_by_game "
        " WHERE (%(game_ids)s IS NULL OR game_id = ANY(%(game_ids)s)) "
        "   AND (deflection_count <> 0 OR c3_made <> 0 "
        "     OR c3_att <> 0 OR c3_known_att <> 0)",
        params,
    )
    nonzero = cur.fetchone()[0]
    check(
        "unmodelled columns still all zero",
        nonzero == 0,
        f"{nonzero} rows -- gate must be widened",
    )

    # 5. Segment durations must tile the game exactly, per team. This is the
    #    invariant the Israeli pipeline can only check by asserting its
    #    duplicated copies agree; here it is a straight sum.
    cur.execute(
        "WITH per_team AS ("
        "  SELECT m.game_id, m.team_id, sum(m.segment_seconds) AS seconds "
        "    FROM euroleague.matchup_segments m "
        "   WHERE %(game_ids)s IS NULL OR m.game_id = ANY(%(game_ids)s) "
        "   GROUP BY m.game_id, m.team_id"
        "), game_length AS ("
        "  SELECT game_id, "
        "         (2400 + greatest(max(period) - 4, 0) * 300)::numeric AS seconds "
        "    FROM euroleague.actions_raw "
        "   WHERE %(game_ids)s IS NULL OR game_id = ANY(%(game_ids)s) "
        "   GROUP BY game_id"
        ") SELECT count(*) FROM per_team p JOIN game_length g USING (game_id) "
        "   WHERE p.seconds IS DISTINCT FROM g.seconds",
        params,
    )
    bad_tiling = cur.fetchone()[0]
    check(
        "segment seconds tile the game per team",
        bad_tiling == 0,
        f"{bad_tiling} team-games off",
    )

    # 6. The gate: rebuild the player grain from the fact and diff both ways.
    cur.execute(f"CREATE TEMP TABLE gate_from_fact AS {PLAYER_GRAIN_FROM_FACT}", params)
    cur.execute(f"CREATE TEMP TABLE gate_stored AS {STORED_PLAYER_GRAIN}", params)
    cur.execute("SELECT count(*) FROM gate_stored")
    stored_n = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM gate_from_fact")
    fact_n = cur.fetchone()[0]
    cur.execute(
        "SELECT count(*) FROM (SELECT * FROM gate_stored "
        "EXCEPT ALL SELECT * FROM gate_from_fact) x"
    )
    stored_only = cur.fetchone()[0]
    cur.execute(
        "SELECT count(*) FROM (SELECT * FROM gate_from_fact "
        "EXCEPT ALL SELECT * FROM gate_stored) x"
    )
    fact_only = cur.fetchone()[0]
    check("player grain row count matches", stored_n == fact_n, f"{stored_n} vs {fact_n}")
    check("stored rows all reproduced", stored_only == 0, f"{stored_only} unreproduced")
    check("no rows invented", fact_only == 0, f"{fact_only} extra")
    cur.close()
    return failures


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--games", default=None, help="gamecodes, e.g. '1-3'; default all")
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--competition", default="E")
    ap.add_argument("--env-file", type=Path, default=REPO.parent / "etl" / ".Renviron")
    args = ap.parse_args()

    conn = connect_from_env_file(args.env_file, direct_port=5432)
    game_ids = None
    if args.games:
        cur = conn.cursor()
        cur.execute(
            "SELECT game_id FROM euroleague.schedule "
            " WHERE competition = %s AND season = %s AND gamecode = ANY(%s) "
            " ORDER BY gamecode",
            (args.competition, args.season, parse_games(args.games)),
        )
        game_ids = [int(r[0]) for r in cur.fetchall()]
        cur.close()
    print(f"=== action_team_context gate ({'all games' if game_ids is None else game_ids}) ===")
    try:
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.close()
        failures = verify(conn, game_ids)
    finally:
        cur = conn.cursor()
        cur.execute("ROLLBACK")   # temp tables only; nothing is written
        cur.close()
        conn.close()
    print(f"\n{'GATE PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it to verify it fails for the right reason**

Run from `euroleague/`:
```bash
./.venv/Scripts/python.exe scripts/verify_action_team_context.py --games 1-3
```
Expected: `psycopg.errors.UndefinedTable: relation "euroleague.action_team_context" does not exist`.

If it fails with anything else — an import error, a bad `--games` parse — fix that before continuing. The gate must fail *only* because the table is absent.

- [ ] **Step 3: Commit**

```bash
git add euroleague/scripts/verify_action_team_context.py
git commit -m "Add the acceptance gate for action_team_context"
```

---

## Task 2: The table

**Files:**
- Create: `euroleague/sql/008_action_team_context.sql`

**Interfaces:**
- Produces: tables `euroleague.matchup_segments` (one row per joint segment, holding its duration once) and `euroleague.action_team_context` (one row per action × perspective team, referencing a segment). Column names and types are fixed here and used verbatim by Tasks 3-6.

- [ ] **Step 1: Write the DDL**

Create `euroleague/sql/008_action_team_context.sql`. The `EuroLeague shadow schema` marker is mandatory; no `DROP `, no `BASKETBALL.`/`BASKETBALL_TEST.` anywhere including comments.

```sql
-- EuroLeague shadow schema -- migration 008.
-- Persisted event x team-perspective fact.
--
-- One row per (action, perspective team): two rows per action, the long form
-- the Israeli pipeline settled on. Consumers filter and sum; no consumer
-- re-implements the perspective CASE.
--
-- Nothing reads this table in 008. Migration 009 switches the four-factor
-- refreshes onto it, gated on reproducing every stored row.
--
-- Side assignment is per event type RELATIVE to the perspective team. Steals,
-- blocks, committed fouls and defensive rebounds sit on the acting team's
-- DEFENSE; shots, free throws, turnovers, offensive rebounds, assists and
-- fouls drawn sit on its OFFENSE. Only the measure-carrying types are proven
-- by the 008 gate: 2FGM/2FGA/3FGM/3FGA/FTM/FTA/TO/O on offense and ST on
-- defense. AS, RV, FV, CM and D carry no measure column today, so their side
-- is assigned by rule and unverified until something counts them.

BEGIN;

SET LOCAL search_path TO euroleague, public;

-- The joint segment is an entity; its duration is an attribute of that entity,
-- not of every event inside it. Storing the duration here rather than on each
-- event row is a deliberate deviation from the Israeli backbone, which
-- denormalises it and consequently needs a fill-in ETL pass, a MAX-per-segment
-- convention repeated at four call sites, and a standing validator asserting
-- the repeated copies have not drifted (count(DISTINCT segment_seconds) = 1).
-- One row per segment makes all three unnecessary. Durations are identical;
-- only the storage grain differs.
CREATE TABLE IF NOT EXISTS euroleague.matchup_segments (
  game_id                bigint   NOT NULL,
  team_id                bigint   NOT NULL,
  segment_id             integer  NOT NULL,
  own_lineup_id          bigint   NOT NULL,
  opp_lineup_id          bigint   NOT NULL,
  own_starters           smallint,
  opp_starters           smallint,
  start_event_order          integer NOT NULL,
  end_event_order_exclusive  integer NOT NULL,
  start_elapsed_seconds  numeric,
  end_elapsed_seconds    numeric,
  segment_seconds        numeric  NOT NULL,
  load_run_id            bigint,
  derivation_version     text     NOT NULL,
  derived_at             timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (game_id, team_id, segment_id),
  FOREIGN KEY (game_id, own_lineup_id)
    REFERENCES euroleague.lineups (game_id, lineup_id),
  FOREIGN KEY (game_id, opp_lineup_id)
    REFERENCES euroleague.lineups (game_id, lineup_id),
  FOREIGN KEY (team_id) REFERENCES euroleague.teams (team_id),
  CONSTRAINT matchup_segments_seconds_nonnegative
    CHECK (segment_seconds >= 0),
  CONSTRAINT matchup_segments_segment_id_positive
    CHECK (segment_id >= 0),
  -- Half-open, matching the stints convention already in this schema. This is
  -- what lets the fact resolve its segment_id by range join instead of the
  -- two INSERTs having to share a staging table.
  CONSTRAINT matchup_segments_half_open
    CHECK (end_event_order_exclusive > start_event_order)
);

CREATE TABLE IF NOT EXISTS euroleague.action_team_context (
  game_id                bigint   NOT NULL,
  source_event_order     integer  NOT NULL,
  team_id                bigint   NOT NULL,
  opponent_team_id       bigint   NOT NULL,
  period                 smallint,

  type_lineup            text,
  own_lineup_id          bigint   NOT NULL,
  opp_lineup_id          bigint   NOT NULL,
  own_stint_id           bigint,
  opp_stint_id           bigint,
  own_starters           smallint,
  opp_starters           smallint,

  event_team_id          bigint,
  action_player_id       bigint,
  play_type              text,
  play_info              text,
  synthetic_ft_trip_id   text,
  parent_play_type       text,
  ft_reverse_order       integer,

  points                 integer  NOT NULL DEFAULT 0,
  ts_possessions         integer  NOT NULL DEFAULT 0,
  orebounds              integer  NOT NULL DEFAULT 0,
  oreb_opportunities     integer  NOT NULL DEFAULT 0,
  turnovers              integer  NOT NULL DEFAULT 0,
  steals                 integer  NOT NULL DEFAULT 0,
  ft_attempts            integer  NOT NULL DEFAULT 0,
  fga                    integer  NOT NULL DEFAULT 0,
  fgm                    integer  NOT NULL DEFAULT 0,
  fg2_made               integer  NOT NULL DEFAULT 0,
  fg2_att                integer  NOT NULL DEFAULT 0,
  fg3_made               integer  NOT NULL DEFAULT 0,
  fg3_att                integer  NOT NULL DEFAULT 0,
  layup_made             integer  NOT NULL DEFAULT 0,
  layup_att              integer  NOT NULL DEFAULT 0,
  dunk_made              integer  NOT NULL DEFAULT 0,
  dunk_att               integer  NOT NULL DEFAULT 0,

  possession_flag        smallint NOT NULL DEFAULT 0,
  final_end_poss         boolean  NOT NULL DEFAULT false,
  endpoint_reason        text,

  event_elapsed_seconds  numeric,
  segment_id             integer,

  own_team_score         integer  NOT NULL DEFAULT 0,
  opp_team_score         integer  NOT NULL DEFAULT 0,

  load_run_id            bigint,
  derivation_version     text     NOT NULL,
  derived_at             timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (game_id, source_event_order, team_id),
  FOREIGN KEY (game_id, source_event_order)
    REFERENCES euroleague.actions_raw (game_id, source_event_order)
    ON DELETE CASCADE,
  FOREIGN KEY (game_id, team_id, segment_id)
    REFERENCES euroleague.matchup_segments (game_id, team_id, segment_id),
  FOREIGN KEY (game_id, own_lineup_id)
    REFERENCES euroleague.lineups (game_id, lineup_id),
  FOREIGN KEY (game_id, opp_lineup_id)
    REFERENCES euroleague.lineups (game_id, lineup_id),
  FOREIGN KEY (team_id) REFERENCES euroleague.teams (team_id),
  FOREIGN KEY (opponent_team_id) REFERENCES euroleague.teams (team_id),
  CONSTRAINT action_team_context_side_check
    CHECK (type_lineup IS NULL OR type_lineup IN ('offense', 'defense')),
  CONSTRAINT action_team_context_distinct_teams
    CHECK (team_id <> opponent_team_id),
  CONSTRAINT action_team_context_possession_flag_check
    CHECK (possession_flag IN (0, 1))
);

CREATE INDEX IF NOT EXISTS action_team_context_agg_idx
  ON euroleague.action_team_context (game_id, team_id, type_lineup);

CREATE INDEX IF NOT EXISTS action_team_context_lineup_idx
  ON euroleague.action_team_context (own_lineup_id);

COMMIT;
```

- [ ] **Step 2: Apply it**

```bash
./.venv/Scripts/python.exe -c "
import sys; from pathlib import Path; sys.path.insert(0,'src')
from euroleague_possessions.postgres_backend import connect_from_env_file, apply_shadow_schema, inspect_target
conn = connect_from_env_file(Path('../etl/.Renviron'), direct_port=5432)
t = inspect_target(conn); assert int(t['server_port']) == 5432, t
apply_shadow_schema(conn, Path('sql/008_action_team_context.sql'))
print('008 table applied'); conn.close()"
```

Expected: `008 table applied`. If it raises `shadow DDL safety marker is missing`, the header lost the `EuroLeague shadow schema` string.

- [ ] **Step 3: Run the gate to confirm it now fails on emptiness, not absence**

```bash
./.venv/Scripts/python.exe scripts/verify_action_team_context.py --games 1-3
```
Expected: the script runs to completion and reports `FAIL` on coverage, row count, and both diff directions. No exception.

- [ ] **Step 4: Commit**

```bash
git add euroleague/sql/008_action_team_context.sql
git commit -m "Add the action_team_context table (migration 008)"
```

---

## Task 3: The refresh function

**Files:**
- Modify: `euroleague/sql/008_action_team_context.sql` (append the function)

**Interfaces:**
- Produces: `euroleague.refresh_action_team_context_for_games(bigint[]) RETURNS bigint`, maintaining both `matchup_segments` and `action_team_context` and returning the number of **fact** rows inserted. Task 5 calls it from `validate_game()`.

**Consumes:** the measure expressions already in `euroleague/sql/002_existing_analytics_compatibility.sql`, CTE `event_metrics`, lines 317-351. Lift that block **verbatim** — twenty `CASE` expressions defining `points`, `ts_possessions`, `orebounds`, `oreb_opportunities`, `turnovers`, `steals`, `ft_attempts`, `fga`, `fgm`, `fg3_made`, `fg2_made`, `fg2_att`, `fg3_att`, `layup_made`, `layup_att`, `dunk_made`, `dunk_att`. Do not retype them; copy them. They are known-correct and the gate cannot distinguish a transcription slip in a rarely-hit branch from a design error.

- [ ] **Step 1: Append the function to the migration**

Insert before the final `COMMIT;`. The CTEs `target_games`, `real_roster`, `clock_parts`, `raw_elapsed`, `event_clock`, `game_ends`, `event_base`, `event_metrics` and the `joint_*` chain are lifted from 002 with migration 007's pushdown predicates retained. What is new is `cum_scores`, the `side` expansion, `side_assignment`, and the stint range join.

```sql
CREATE OR REPLACE FUNCTION euroleague.refresh_action_team_context_for_games(
  game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  PERFORM euroleague.refresh_stint_timing_for_games(game_ids);

  -- Child first: action_team_context references matchup_segments.
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.action_team_context;
    DELETE FROM euroleague.matchup_segments;
  ELSE
    DELETE FROM euroleague.action_team_context WHERE game_id = ANY(game_ids);
    DELETE FROM euroleague.matchup_segments WHERE game_id = ANY(game_ids);
  END IF;

  -- Parent first. This INSERT needs only the clock and the two lineups per
  -- event -- NOT event_metrics -- so it shares just the cheap part of the
  -- chain with the fact below. That is deliberate: a temp table would have
  -- let both statements share one derivation, but `ON COMMIT DROP` contains
  -- the literal 'DROP ' that apply_shadow_schema() refuses, and the guard is
  -- worth more than the saved scan.
  INSERT INTO euroleague.matchup_segments (
    game_id, team_id, segment_id, own_lineup_id, opp_lineup_id,
    own_starters, opp_starters,
    start_event_order, end_event_order_exclusive,
    start_elapsed_seconds, end_elapsed_seconds, segment_seconds,
    load_run_id, derivation_version
  )
  WITH target_games AS (
    SELECT s.* FROM euroleague.schedule s
     WHERE game_ids IS NULL OR s.game_id = ANY(game_ids)
  ),
  -- ... clock_parts, raw_elapsed, event_clock, game_ends: lifted verbatim
  -- ... from migration 002 lines 233-282, keeping 007's pushdown predicates.
  game_bounds AS (
    SELECT ar.game_id, max(ar.source_event_order) + 1 AS end_event_order_exclusive
      FROM euroleague.actions_raw ar
      JOIN target_games tg ON tg.game_id = ar.game_id
     WHERE game_ids IS NULL OR ar.game_id = ANY(game_ids)
     GROUP BY ar.game_id
  ),
  lineup_sided AS MATERIALIZED (
    SELECT
      al.game_id, al.source_event_order,
      ec.event_elapsed_seconds, ge.game_end_elapsed_seconds,
      side.team_id, side.own_lineup_id, side.opp_lineup_id,
      own_lineup.starter_count AS own_starters,
      opp_lineup.starter_count AS opp_starters,
      tg.last_seen_load_run_id
    FROM euroleague.action_lineups al
    JOIN target_games tg ON tg.game_id = al.game_id
    JOIN event_clock ec
      ON ec.game_id = al.game_id AND ec.source_event_order = al.source_event_order
    JOIN game_ends ge ON ge.game_id = al.game_id
    CROSS JOIN LATERAL (
      VALUES
        (tg.home_team_id, al.home_lineup_id, al.away_lineup_id),
        (tg.away_team_id, al.away_lineup_id, al.home_lineup_id)
    ) AS side(team_id, own_lineup_id, opp_lineup_id)
    JOIN euroleague.lineups own_lineup ON own_lineup.lineup_id = side.own_lineup_id
    JOIN euroleague.lineups opp_lineup ON opp_lineup.lineup_id = side.opp_lineup_id
    WHERE game_ids IS NULL OR al.game_id = ANY(game_ids)
  ),
  numbered AS (
    SELECT ls.*,
      sum(
        CASE WHEN lag(ls.own_lineup_id) OVER w IS DISTINCT FROM ls.own_lineup_id
               OR lag(ls.opp_lineup_id) OVER w IS DISTINCT FROM ls.opp_lineup_id
             THEN 1 ELSE 0 END
      ) OVER (PARTITION BY ls.game_id, ls.team_id
              ORDER BY ls.source_event_order
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS segment_number
    FROM lineup_sided ls
    WINDOW w AS (PARTITION BY ls.game_id, ls.team_id ORDER BY ls.source_event_order)
  ),
  starts AS (
    SELECT game_id, team_id, segment_number,
           own_lineup_id, opp_lineup_id, own_starters, opp_starters,
           min(source_event_order)       AS start_event_order,
           min(event_elapsed_seconds)    AS start_elapsed_seconds,
           max(game_end_elapsed_seconds) AS game_end_elapsed_seconds,
           max(last_seen_load_run_id)    AS load_run_id
      FROM numbered
     GROUP BY game_id, team_id, segment_number,
              own_lineup_id, opp_lineup_id, own_starters, opp_starters
  ),
  ordered AS (
    SELECT s.*,
           lead(s.start_event_order) OVER w    AS next_start_event_order,
           lead(s.start_elapsed_seconds) OVER w AS next_start_elapsed_seconds
      FROM starts s
    WINDOW w AS (PARTITION BY s.game_id, s.team_id ORDER BY s.segment_number)
  )
  SELECT
    o.game_id, o.team_id, o.segment_number,
    o.own_lineup_id, o.opp_lineup_id, o.own_starters, o.opp_starters,
    o.start_event_order,
    coalesce(o.next_start_event_order, gb.end_event_order_exclusive),
    o.start_elapsed_seconds,
    coalesce(o.next_start_elapsed_seconds, o.game_end_elapsed_seconds),
    greatest(
      coalesce(o.next_start_elapsed_seconds, o.game_end_elapsed_seconds)
      - o.start_elapsed_seconds, 0
    )::numeric,
    o.load_run_id,
    'action-team-context-v1'
  FROM ordered o
  JOIN game_bounds gb ON gb.game_id = o.game_id;

  INSERT INTO euroleague.action_team_context (
    game_id, source_event_order, team_id, opponent_team_id, period,
    type_lineup, own_lineup_id, opp_lineup_id, own_stint_id, opp_stint_id,
    own_starters, opp_starters,
    event_team_id, action_player_id, play_type, play_info,
    synthetic_ft_trip_id, parent_play_type, ft_reverse_order,
    points, ts_possessions, orebounds, oreb_opportunities, turnovers,
    steals, ft_attempts, fga, fgm, fg2_made, fg2_att, fg3_made, fg3_att,
    layup_made, layup_att, dunk_made, dunk_att,
    possession_flag, final_end_poss, endpoint_reason,
    event_elapsed_seconds, segment_id,
    own_team_score, opp_team_score,
    load_run_id, derivation_version
  )
  WITH target_games AS (
    SELECT s.* FROM euroleague.schedule s
     WHERE game_ids IS NULL OR s.game_id = ANY(game_ids)
  ),
  -- ... clock_parts, raw_elapsed, event_clock, game_ends, event_base,
  -- ... event_metrics: lifted verbatim from migration 002 lines 233-351,
  -- ... keeping migration 007's pushdown predicates.
  cum_scores AS MATERIALIZED (
    -- Cumulative score per team through each event, for clutch filtering.
    SELECT em.game_id, em.source_event_order, em.event_team_id,
           sum(em.points) OVER (
             PARTITION BY em.game_id, em.event_team_id
             ORDER BY em.source_event_order
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           )::integer AS team_running_score
      FROM event_metrics em
  ),
  sided AS MATERIALIZED (
    SELECT
      em.*,
      ec.event_elapsed_seconds,
      side.team_id, side.opponent_team_id,
      side.own_lineup_id, side.opp_lineup_id,
      own_lineup.starter_count AS own_starters,
      opp_lineup.starter_count AS opp_starters,
      CASE
        WHEN em.event_team_id IS NULL THEN NULL
        WHEN em.play_type IN ('2FGM','2FGA','3FGM','3FGA','FTM','FTA',
                              'TO','O','AS','RV')
          THEN CASE WHEN em.event_team_id = side.team_id
                    THEN 'offense' ELSE 'defense' END
        WHEN em.play_type IN ('ST','FV','CM','D')
          THEN CASE WHEN em.event_team_id = side.team_id
                    THEN 'defense' ELSE 'offense' END
        ELSE NULL
      END AS type_lineup,
      CASE WHEN em.endpoint_offense_team_id IS NULL THEN 0 ELSE 1 END::smallint
        AS possession_flag
    FROM event_metrics em
    JOIN event_clock ec
      ON ec.game_id = em.game_id
     AND ec.source_event_order = em.source_event_order
    CROSS JOIN LATERAL (
      VALUES
        (em.home_team_id, em.away_team_id, em.home_lineup_id, em.away_lineup_id),
        (em.away_team_id, em.home_team_id, em.away_lineup_id, em.home_lineup_id)
    ) AS side(team_id, opponent_team_id, own_lineup_id, opp_lineup_id)
    JOIN euroleague.lineups own_lineup ON own_lineup.lineup_id = side.own_lineup_id
    JOIN euroleague.lineups opp_lineup ON opp_lineup.lineup_id = side.opp_lineup_id
  )
  SELECT
    sd.game_id, sd.source_event_order, sd.team_id, sd.opponent_team_id, sd.period,
    sd.type_lineup, sd.own_lineup_id, sd.opp_lineup_id,
    own_stint.stint_id, opp_stint.stint_id,
    sd.own_starters, sd.opp_starters,
    sd.event_team_id, sd.action_player_id, sd.play_type, sd.play_info,
    sd.synthetic_ft_trip_id, sd.parent_play_type, sd.ft_reverse_order,
    sd.points, sd.ts_possessions, sd.orebounds, sd.oreb_opportunities,
    sd.turnovers, sd.steals, sd.ft_attempts, sd.fga, sd.fgm,
    sd.fg2_made, sd.fg2_att, sd.fg3_made, sd.fg3_att,
    sd.layup_made, sd.layup_att, sd.dunk_made, sd.dunk_att,
    sd.possession_flag,
    coalesce(ac.final_end_possession, false),
    ac.endpoint_reason,
    sd.event_elapsed_seconds,
    ms.segment_id,
    coalesce(own_score.team_running_score, 0),
    coalesce(opp_score.team_running_score, 0),
    tg.last_seen_load_run_id,
    'action-team-context-v1'
  FROM sided sd
  JOIN target_games tg ON tg.game_id = sd.game_id
  JOIN euroleague.actions_clean ac
    ON ac.game_id = sd.game_id AND ac.source_event_order = sd.source_event_order
  -- The segments written immediately above. Half-open, so exactly one matches.
  JOIN euroleague.matchup_segments ms
    ON ms.game_id = sd.game_id
   AND ms.team_id = sd.team_id
   AND sd.source_event_order >= ms.start_event_order
   AND sd.source_event_order <  ms.end_event_order_exclusive
  LEFT JOIN euroleague.stints own_stint
    ON own_stint.game_id = sd.game_id
   AND own_stint.team_id = sd.team_id
   AND sd.source_event_order >= own_stint.start_event_order
   AND sd.source_event_order <  own_stint.end_event_order_exclusive
  LEFT JOIN euroleague.stints opp_stint
    ON opp_stint.game_id = sd.game_id
   AND opp_stint.team_id = sd.opponent_team_id
   AND sd.source_event_order >= opp_stint.start_event_order
   AND sd.source_event_order <  opp_stint.end_event_order_exclusive
  LEFT JOIN cum_scores own_score
    ON own_score.game_id = sd.game_id
   AND own_score.source_event_order = sd.source_event_order
   AND own_score.event_team_id = sd.team_id
  LEFT JOIN cum_scores opp_score
    ON opp_score.game_id = sd.game_id
   AND opp_score.source_event_order = sd.source_event_order
   AND opp_score.event_team_id = sd.opponent_team_id;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;
```

Five notes the implementer will otherwise get wrong:

1. **`sided`, `cum_scores` and `lineup_sided` must stay `MATERIALIZED`,** for the reason migration 007 exists: PostgreSQL 12+ inlines a CTE referenced once, the estimate collapses to `rows=1`, and the planner picks a nested loop that re-runs the aggregate per output row. After applying, run `EXPLAIN (ANALYZE, BUFFERS)` on both INSERTs and confirm no node reports `loops=` in the thousands.
2. **Order matters and is enforced.** `matchup_segments` must be written before the fact, because the fact resolves `segment_id` by range join against it and carries a foreign key to it. Writing them the other way round fails loudly on the FK rather than silently producing NULL segments.
3. **The two INSERTs both derive the clock chain** (`clock_parts` → `raw_elapsed` → `event_clock` → `game_ends`). That is accepted duplication: a shared temp table would need `ON COMMIT DROP`, whose literal `DROP ` string `apply_shadow_schema()` refuses. Only the *cheap* part is duplicated — the twenty measure `CASE` expressions in `event_metrics` are derived once, in the fact's INSERT only.
4. **`inserted_count` counts fact rows only,** not `matchup_segments` rows. That matches the function's contract and the gate's expectation of ~1,134 per game.
5. **`segment_id` is 0-based** — `sum(...)` over a window whose first row has a NULL `lag()` starts the count at 1, so verify with `SELECT min(segment_id), max(segment_id) FROM euroleague.matchup_segments` after the backfill rather than assuming. The `CHECK (segment_id >= 0)` accommodates either; what matters is that the fact and the segment table agree, which the foreign key enforces.

- [ ] **Step 2: Apply and backfill three games**

```bash
./.venv/Scripts/python.exe -c "
import sys, time; from pathlib import Path; sys.path.insert(0,'src')
from euroleague_possessions.postgres_backend import connect_from_env_file, apply_shadow_schema
conn = connect_from_env_file(Path('../etl/.Renviron'), direct_port=5432)
apply_shadow_schema(conn, Path('sql/008_action_team_context.sql'))
cur = conn.cursor()
cur.execute(\"SELECT game_id FROM euroleague.schedule WHERE competition='E' AND season=2025 AND gamecode <= 3 ORDER BY gamecode\")
ids = [int(r[0]) for r in cur.fetchall()]
cur.execute('BEGIN'); cur.execute(\"SET LOCAL statement_timeout = '15min'\")
t = time.perf_counter()
cur.execute('SELECT euroleague.refresh_action_team_context_for_games(%s::bigint[])', (ids,))
print('rows:', cur.fetchone()[0], f'in {time.perf_counter()-t:.2f}s')
cur.execute('COMMIT'); conn.close()"
```

Expected: roughly 3,400 fact rows (three games × ~1,134) and a few seconds. Confirm `matchup_segments` also filled — about 413 rows (three games × ~137.6):

```bash
./.venv/Scripts/python.exe -c "
import sys; from pathlib import Path; sys.path.insert(0,'src')
from euroleague_possessions.postgres_backend import connect_from_env_file
c=connect_from_env_file(Path('../etl/.Renviron'), direct_port=5432); cur=c.cursor()
cur.execute('SELECT count(*), min(segment_id), max(segment_id) FROM euroleague.matchup_segments')
print('segments, min id, max id:', cur.fetchone()); c.close()"
```

If it exceeds a minute, the `MATERIALIZED` fences in note 1 above are missing.

- [ ] **Step 3: Run the gate on those three games**

```bash
./.venv/Scripts/python.exe scripts/verify_action_team_context.py --games 1-3
```
Expected: `GATE PASSED`, all eight checks.

This is the moment the design is proven or disproven. If `stored rows all reproduced` fails, dump a sample and compare column by column:

```sql
SELECT * FROM gate_stored EXCEPT ALL SELECT * FROM gate_from_fact LIMIT 5;
```
The likely causes, in order: a side-assignment mistake for one `play_type`; `possession_flag` not matching `total_poss`; or `own_starters`/`opp_starters` taken from the wrong lineup.

- [ ] **Step 4: Commit**

```bash
git add euroleague/sql/008_action_team_context.sql
git commit -m "Add the action_team_context refresh function"
```

---

## Task 4: Backfill all 84 games

**Files:** none changed — this is an operational step with a gate.

- [ ] **Step 1: Backfill**

```bash
./.venv/Scripts/python.exe -c "
import sys, time; from pathlib import Path; sys.path.insert(0,'src')
from euroleague_possessions.postgres_backend import connect_from_env_file
conn = connect_from_env_file(Path('../etl/.Renviron'), direct_port=5432)
cur = conn.cursor()
cur.execute('BEGIN'); cur.execute(\"SET LOCAL statement_timeout = '30min'\")
t = time.perf_counter()
cur.execute('SELECT euroleague.refresh_action_team_context_for_games(NULL::bigint[])')
print('rows:', cur.fetchone()[0], f'in {time.perf_counter()-t:.2f}s')
cur.execute('COMMIT'); conn.close()"
```

Expected: 95,216 fact rows (2 × 47,608) and 11,554 `matchup_segments` rows — both measured on this data, so treat a deviation as a defect, not as noise.

`NULL` means all games, per the function's own branch. If this exceeds 30 minutes, kill it and backfill in batches of 20 gamecodes instead — the refresh is per-game and idempotent, so batching is safe.

- [ ] **Step 2: Run the full gate**

```bash
./.venv/Scripts/python.exe scripts/verify_action_team_context.py
```
Expected: `GATE PASSED`, with the two sides of `player grain row count matches`
equal to each other.

**Do not assert a literal stored row count here.** An earlier draft of this plan
expected `182868 vs 182868`. That figure is obsolete: migration 007's refresh was
corrected on 2026-08-08 (Task 3b) to stop generating rows for
`(player, is_on_key, own_starters, opp_starters)` combinations that never
occurred, which removed roughly 37% of the table — 6,240 → 3,910 rows on the
three pilot games. After this backfill the population is on the order of 109,000,
but the exact figure depends on the corrected grain and must not be hardcoded.

The gate's value is the **bidirectional zero-diff**, not the count. A row count
is an artifact that legitimately moves whenever the grain is corrected, so
asserting it converts a valid improvement into a spurious failure. Record the
count as an output; test only that both directions of the `EXCEPT ALL` return
zero rows.

- [ ] **Step 3: Record the storage cost**

```bash
./.venv/Scripts/python.exe -c "
import sys; from pathlib import Path; sys.path.insert(0,'src')
from euroleague_possessions.postgres_backend import connect_from_env_file
conn = connect_from_env_file(Path('../etl/.Renviron'), direct_port=5432); cur = conn.cursor()
cur.execute(\"SELECT pg_size_pretty(pg_total_relation_size('euroleague.action_team_context'))\")
print('action_team_context:', cur.fetchone()[0])
cur.execute(\"SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='euroleague'\")
print('euroleague total:', cur.fetchone()[0]); conn.close()"
```

Note both numbers in the Task 6 doc update. The spec projected 70-115 MB for a full season; if the 84-game figure implies materially more than that, say so rather than quietly moving on.

- [ ] **Step 4: Commit** (nothing to stage; skip if the tree is clean)

---

## Task 5: Wire the refresh into publication

**Files:**
- Modify: `euroleague/src/euroleague_possessions/postgres_backend.py`
- Modify: `euroleague/tests/test_postgres_backend.py`

**Interfaces:**
- Consumes: `euroleague.refresh_action_team_context_for_games(bigint[])` from Task 3.
- Produces: publication that populates the fact for every newly loaded game.

- [ ] **Step 1: Write the failing tests**

Append to `euroleague/tests/test_postgres_backend.py`:

```python
class ActionTeamContextWiringTest(unittest.TestCase):
    """The derived fact must be known to the guard and refreshed on publish."""

    def test_schema_allowlist_knows_the_derived_fact(self) -> None:
        import inspect
        from euroleague_possessions import postgres_backend

        source = inspect.getsource(postgres_backend.assert_shadow_schema_compatible)
        self.assertIn('"action_team_context"', source)
        self.assertIn('"matchup_segments"', source)

    def test_validate_game_refreshes_the_fact_before_four_factors(self) -> None:
        connection = RecordingConnection()
        backend = PostgresTransactionBackend(connection, load_run_id=17)
        try:
            backend.validate_game(game_id=23)
        except Exception:
            pass  # RecordingConnection cannot satisfy the later count checks

        executed = [sql for sql, _ in connection.statements]
        fact = next(
            i for i, s in enumerate(executed)
            if "refresh_action_team_context_for_games" in s
        )
        player = next(
            i for i, s in enumerate(executed)
            if "refresh_player_four_factors_by_game_for_games" in s
        )
        self.assertLess(fact, player, "the fact must be refreshed first")
```

- [ ] **Step 2: Run them to verify they fail**

```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
./euroleague/.venv/Scripts/python.exe -m unittest discover -s euroleague/tests -k ActionTeamContext -v
```
Expected: both FAIL — `'"action_team_context"' not found` and `StopIteration`.

- [ ] **Step 3: Add the allowlist entry**

In `postgres_backend.py`, `assert_shadow_schema_compatible()`, the `expected` set already lists `player_four_factors_by_game` and `team_four_factors_by_game` under a comment about derived analytics facts. Add one line beside them:

```python
        "team_four_factors_by_game",
        "matchup_segments",
        "action_team_context",
```

- [ ] **Step 4: Call the refresh in `validate_game()`**

In `validate_game()`, immediately before the existing `refresh_player_four_factors_by_game_for_games` call, insert:

```python
            # The event x team-perspective fact every other analytic reads.
            # It must be refreshed first: the four-factor refreshes below are
            # derived from it, and refresh_stint_timing runs inside it.
            cursor.execute(
                "SELECT euroleague.refresh_action_team_context_for_games("
                "ARRAY[%s]::bigint[])",
                (game_id,),
            )
            cursor.fetchone()
```

- [ ] **Step 5: Run the tests**

```bash
./euroleague/.venv/Scripts/python.exe -m unittest discover -s euroleague/tests
```
Expected: OK, 64 tests.

**Expected duplication, leave it alone.** `refresh_stint_timing_for_games()` now runs twice per game — once inside the fact's refresh, once at the top of `refresh_player_four_factors_by_game_for_games()`, which 008 does not modify. It is an idempotent `UPDATE` over ~70 stint rows per game, so the cost is negligible and the result identical. Migration 009 removes the second call when it rewrites that function. Do not remove it now: doing so would change a function this plan is not verifying.

- [ ] **Step 6: Prove publication still works and still produces identical wiring**

```bash
cd euroleague
./.venv/Scripts/python.exe scripts/probe_batched_publish.py --games 1-3
./.venv/Scripts/python.exe scripts/load_games.py --games 1-84 --verify-only
./.venv/Scripts/python.exe scripts/verify_action_team_context.py
```
Expected: `ALL PROBES PASSED`, `ALL CHECKS PASSED`, `GATE PASSED`.

The probe republishes and rolls back, so it now exercises the fact's refresh inside the transaction too. Note the new `validate` timing — it should rise by roughly the fact's refresh cost per game.

- [ ] **Step 7: Commit**

```bash
git add euroleague/src/euroleague_possessions/postgres_backend.py euroleague/tests/test_postgres_backend.py
git commit -m "Refresh action_team_context during publication"
```

---

## Task 6: Extend the standing checks and update the docs

**Files:**
- Modify: `euroleague/scripts/probe_batched_publish.py`
- Modify: `euroleague/scripts/load_games.py`
- Modify: `euroleague/RUNBOOK.md`
- Modify: `euroleague/PROJECT.md`

- [ ] **Step 1: Add the fact to the rollback probe's projections**

In `probe_batched_publish.py`, add to the `PROJECTIONS` dict. Keyed by the natural key, so it stays comparable across regenerated surrogate ids:

```python
    "action_team_context": """
        SELECT (a.source_event_order, t.provider_team_code)::text,
               a.type_lineup, a.points, a.possession_flag,
               a.ts_possessions, a.orebounds, a.turnovers, a.steals,
               a.own_team_score, a.opp_team_score, a.segment_id,
               (ol.team_id, ol.lineup_hash)::text
          FROM euroleague.action_team_context a
          JOIN euroleague.teams t ON t.team_id = a.team_id
          JOIN euroleague.lineups ol ON ol.lineup_id = a.own_lineup_id
         WHERE a.game_id = %(game_id)s
    """,
    "matchup_segments": """
        SELECT (t.provider_team_code, m.segment_id)::text,
               m.own_starters, m.opp_starters, m.start_event_order,
               m.start_elapsed_seconds, m.end_elapsed_seconds,
               m.segment_seconds,
               (ol.team_id, ol.lineup_hash)::text,
               (pl.team_id, pl.lineup_hash)::text
          FROM euroleague.matchup_segments m
          JOIN euroleague.teams t ON t.team_id = m.team_id
          JOIN euroleague.lineups ol ON ol.lineup_id = m.own_lineup_id
          JOIN euroleague.lineups pl ON pl.lineup_id = m.opp_lineup_id
         WHERE m.game_id = %(game_id)s
    """,
```

- [ ] **Step 2: Add a coverage check to `load_games.py`**

In `verify()`, after the existing "all games have team analytics" check:

```python
    orphan_fact = q(
        "SELECT count(*) FROM euroleague.schedule s "
        "WHERE NOT EXISTS (SELECT 1 FROM euroleague.action_team_context a "
        "                   WHERE a.game_id = s.game_id)"
    )[0][0]
    check("all games have the event fact", orphan_fact == 0, f"{orphan_fact} games missing")
```

- [ ] **Step 3: Run both**

```bash
./.venv/Scripts/python.exe scripts/probe_batched_publish.py --games 1-3
./.venv/Scripts/python.exe scripts/load_games.py --games 1-84 --verify-only
```
Expected: `ALL PROBES PASSED` (now including `action_team_context` in the compared projections) and 11 passing checks.

- [ ] **Step 4: Update the docs**

`euroleague/RUNBOOK.md`: change the migration order line to `001 → 002 → 004 → 005 → 006 → 007 → 008`, and add the per-game refresh cost measured in Task 5 Step 6 to the publication phase table.

`euroleague/PROJECT.md`: add `| Event x team-perspective fact | Migration 008 | Applied |` to the repository/live state table, update the apply-order line, and rewrite the known-issue bullet that currently reads "The player four-factor refresh still re-derives the event fact" to say the fact now exists and is populated, that nothing reads it yet, and that migration 009 switches consumers.

- [ ] **Step 5: Commit**

```bash
git add euroleague/scripts/probe_batched_publish.py euroleague/scripts/load_games.py euroleague/RUNBOOK.md euroleague/PROJECT.md
git commit -m "Check the event fact in the standing verifications"
```

---

## Done when

- `verify_action_team_context.py` passes all eight checks: zero rows differing either way across every stored player four-factor row, and segment durations tiling every team-game exactly. The completion criterion is the zero-diff in both directions, not a particular row count — see Task 4 Step 2 for why no literal count is asserted.
- `probe_batched_publish.py --games 1-3` passes with the fact in its projections.
- `load_games.py --games 1-84 --verify-only` passes 11 checks.
- The Python suite passes (64 tests).
- `main` is untouched; all work is on `shiny/euro-tab1`.

## Explicitly not in this plan

- Rewriting `refresh_player_four_factors_by_game_for_games()` to read the fact — that is migration 009. In 008 the fact is written and proven but unread, so a failure at any point is revertible by leaving it unread.
- Dropping `pws` — migration 010, and blocked on moving its integrity assertion into `game_qa` and on the `apply_shadow_schema()` `DROP ` guard.
- Correcting the `play_info` layup/dunk matching, corner-3 columns, season-scoped lineup identity, the player identity layer, and any new app surface.
