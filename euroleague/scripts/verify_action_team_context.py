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
#
# Population and floor time come from matchup_segments; measures come from
# action_team_context. That split is the contract, not a convenience:
# docs/database_context.md section 4 defines a segment's duration as an
# attribute of the (game, team, lineup, segment) entity, and says floor time is
# counted once per segment. A player can be on court for a whole segment in
# which his team recorded no event on one side; that combination has real
# minutes and no event, so an event-sourced grain cannot produce it.
#
# The shape mirrors the refresh function in migration 007 after its revision 2:
#   player_minutes (segment-derived, the observed population)
#     CROSS JOIN type_lineup {offense, defense}      -> complete_grid
#     LEFT JOIN counts (event-derived measures)      -> zero where no event
# The two sides are independent implementations of that shape -- 007 re-derives
# segments and per-event sides from actions_raw on every call, this reads the
# persisted matchup_segments and action_team_context -- so agreement is
# evidence, not a tautology. Mutation tests (task 3c, change 3) confirm the
# comparison still fails on a corrupted measure and on a flipped side.
PLAYER_GRAIN_FROM_FACT = """
WITH real_roster AS (
  SELECT fr.game_id, fr.team_id, fr.player_id
    FROM euroleague.full_rosters fr
    JOIN euroleague.players p ON p.player_id = fr.player_id
   WHERE (%(game_ids)s::bigint[] IS NULL OR fr.game_id = ANY(%(game_ids)s::bigint[]))
     AND lower(p.provider_player_id) NOT IN ('team', 'total')
     AND lower(btrim(p.display_name)) NOT IN ('team', 'total')
),
-- Minutes come from matchup_segments, which holds each segment's duration
-- exactly once. No DISTINCT and no MAX-per-segment convention: the duration
-- cannot be double-counted because it is not repeated. is_on_key is a plain
-- membership test of the roster player against the segment's own lineup --
-- EuroLeague lineups are first-class, so no lineup derivation is needed here.
player_minutes AS (
  SELECT ms.game_id, ms.team_id, rr.player_id,
         CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END::smallint AS is_on_key,
         ms.own_starters, ms.opp_starters,
         round(sum(ms.segment_seconds) / 60.0, 3) AS minutes
    FROM euroleague.matchup_segments ms
    JOIN real_roster rr
      ON rr.game_id = ms.game_id AND rr.team_id = ms.team_id
    LEFT JOIN euroleague.lineup_players lp
      ON lp.lineup_id = ms.own_lineup_id AND lp.player_id = rr.player_id
   WHERE %(game_ids)s::bigint[] IS NULL OR ms.game_id = ANY(%(game_ids)s::bigint[])
   GROUP BY ms.game_id, ms.team_id, rr.player_id,
            CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END,
            ms.own_starters, ms.opp_starters
),
-- The output grain: every observed (game, team, player, is_on_key,
-- own_starters, opp_starters) combination, on both sides.
complete_grid AS (
  SELECT pm.game_id, pm.team_id, pm.player_id, pm.is_on_key,
         pm.own_starters, pm.opp_starters, pm.minutes,
         side.type_lineup
    FROM player_minutes pm
    CROSS JOIN (VALUES ('offense'::text), ('defense'::text)) AS side(type_lineup)
),
-- Measures, straight off the fact. Rows the contract leaves unsided
-- (substitutions, timeouts, period markers) have type_lineup NULL, carry no
-- measure, and are excluded here; they cost no floor time because floor time
-- does not come from this CTE.
counts AS (
  SELECT atc.game_id, atc.team_id, rr.player_id,
         CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END::smallint AS is_on_key,
         atc.type_lineup, atc.own_starters, atc.opp_starters,
         sum(atc.points)::numeric            AS total_points,
         sum(atc.possession_flag)::bigint    AS total_poss,
         sum(atc.ts_possessions)::bigint     AS ts_poss_count,
         sum(atc.orebounds)::bigint          AS oreb_count,
         sum(atc.oreb_opportunities)::bigint AS oreb_opportunities,
         sum(atc.turnovers)::bigint          AS tov_count,
         sum(atc.steals)::bigint             AS steal_count,
         sum(atc.ft_attempts)::bigint        AS total_ft_attempts,
         sum(atc.fga)::bigint                AS total_fga,
         sum(atc.fgm)::bigint                AS total_fgm,
         sum(atc.fg3_made)::bigint           AS total_fg3_made,
         -- Player-attributed variants: only when this player took the action,
         -- and only on offense. Mirrors off_player_ts_possessions in 002.
         sum(CASE WHEN atc.type_lineup = 'offense'
                   AND atc.action_player_id = rr.player_id
                  THEN atc.ts_possessions ELSE 0 END)::bigint
           AS player_ts_poss_count,
         sum(CASE WHEN atc.type_lineup = 'offense'
                   AND atc.action_player_id = rr.player_id
                  THEN atc.turnovers ELSE 0 END)::bigint
           AS player_tov_count,
         sum(atc.fg2_made)::integer          AS fg2_made,
         sum(atc.fg2_att)::integer           AS fg2_att,
         sum(atc.fg3_made)::integer          AS fg3_made,
         sum(atc.fg3_att)::integer           AS fg3_att,
         sum(atc.layup_made)::integer        AS layup_made,
         sum(atc.layup_att)::integer         AS layup_att,
         sum(atc.dunk_made)::integer         AS dunk_made,
         sum(atc.dunk_att)::integer          AS dunk_att
    FROM euroleague.action_team_context atc
    JOIN real_roster rr
      ON rr.game_id = atc.game_id AND rr.team_id = atc.team_id
    LEFT JOIN euroleague.lineup_players lp
      ON lp.lineup_id = atc.own_lineup_id AND lp.player_id = rr.player_id
   WHERE (%(game_ids)s::bigint[] IS NULL OR atc.game_id = ANY(%(game_ids)s::bigint[]))
     AND atc.type_lineup IS NOT NULL
   GROUP BY atc.game_id, atc.team_id, rr.player_id,
            CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END,
            atc.type_lineup, atc.own_starters, atc.opp_starters
)
SELECT cg.game_id, cg.team_id, cg.player_id, cg.is_on_key, cg.type_lineup,
       s.season                              AS game_year,
       cg.own_starters                       AS num_starters,
       cg.own_starters, cg.opp_starters,
       coalesce(c.total_points, 0)::numeric  AS total_points,
       coalesce(c.total_poss, 0)::bigint     AS total_poss,
       coalesce(c.ts_poss_count, 0)::bigint  AS ts_poss_count,
       coalesce(c.oreb_count, 0)::bigint     AS oreb_count,
       coalesce(c.oreb_opportunities, 0)::bigint AS oreb_opportunities,
       coalesce(c.tov_count, 0)::bigint      AS tov_count,
       coalesce(c.steal_count, 0)::bigint    AS steal_count,
       coalesce(c.total_ft_attempts, 0)::bigint AS total_ft_attempts,
       coalesce(c.total_fga, 0)::bigint      AS total_fga,
       coalesce(c.total_fgm, 0)::bigint      AS total_fgm,
       coalesce(c.total_fg3_made, 0)::bigint AS total_fg3_made,
       coalesce(c.player_ts_poss_count, 0)::bigint AS player_ts_poss_count,
       coalesce(c.player_tov_count, 0)::bigint AS player_tov_count,
       CASE WHEN cg.type_lineup = 'offense'
            THEN coalesce(cg.minutes, 0) ELSE 0 END::numeric AS minutes,
       coalesce(c.fg2_made, 0)::integer      AS fg2_made,
       coalesce(c.fg2_att, 0)::integer       AS fg2_att,
       coalesce(c.fg3_made, 0)::integer      AS fg3_made,
       coalesce(c.fg3_att, 0)::integer       AS fg3_att,
       coalesce(c.layup_made, 0)::integer    AS layup_made,
       coalesce(c.layup_att, 0)::integer     AS layup_att,
       coalesce(c.dunk_made, 0)::integer     AS dunk_made,
       coalesce(c.dunk_att, 0)::integer      AS dunk_att,
       CASE WHEN cg.type_lineup = 'offense'
            THEN coalesce(cg.minutes, 0) ELSE 0 END::numeric AS onoff_minutes
  FROM complete_grid cg
  JOIN euroleague.schedule s ON s.game_id = cg.game_id
  LEFT JOIN counts c
    ON c.game_id = cg.game_id
   AND c.team_id = cg.team_id
   AND c.player_id = cg.player_id
   AND c.is_on_key = cg.is_on_key
   AND c.type_lineup = cg.type_lineup
   AND c.own_starters = cg.own_starters
   AND c.opp_starters = cg.opp_starters
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
 WHERE %(game_ids)s::bigint[] IS NULL OR game_id = ANY(%(game_ids)s::bigint[])
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
        " WHERE (%(game_ids)s::bigint[] IS NULL OR s.game_id = ANY(%(game_ids)s::bigint[])) "
        "   AND NOT EXISTS (SELECT 1 FROM euroleague.action_team_context a "
        "                    WHERE a.game_id = s.game_id)",
        params,
    )
    missing = cur.fetchone()[0]
    check("every published game has fact rows", missing == 0, f"{missing} missing")

    # 2. Two rows per action, one per perspective team.
    cur.execute(
        "SELECT (SELECT count(*) FROM euroleague.action_team_context a "
        "         WHERE %(game_ids)s::bigint[] IS NULL OR a.game_id = ANY(%(game_ids)s::bigint[])), "
        "       (SELECT count(*) FROM euroleague.actions_raw r "
        "         WHERE %(game_ids)s::bigint[] IS NULL OR r.game_id = ANY(%(game_ids)s::bigint[]))",
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
        # IS DISTINCT FROM, not <>: a NULL type_lineup must count as a
        # contradiction, not vanish. possession_flag comes from
        # possessions.offense_team_id while the side comes from the play-type
        # enumeration, and this check is the only thing holding the two
        # together. If an endpoint ever lands on an unenumerated play type,
        # both perspective rows get possession_flag = 1 with a NULL side;
        # every consumer filters on type_lineup, so those possessions would
        # drop out of the denominators and inflate every rate -- while a
        # <>-based check still printed zero.
        " WHERE a.possession_flag = 1 "
        "   AND ((a.team_id = p.offense_team_id "
        "         AND a.type_lineup IS DISTINCT FROM 'offense') "
        "     OR (a.team_id <> p.offense_team_id "
        "         AND a.type_lineup IS DISTINCT FROM 'defense')) "
        "   AND (%(game_ids)s::bigint[] IS NULL OR a.game_id = ANY(%(game_ids)s::bigint[]))",
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
        " WHERE (%(game_ids)s::bigint[] IS NULL OR game_id = ANY(%(game_ids)s::bigint[])) "
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
        "   WHERE %(game_ids)s::bigint[] IS NULL OR m.game_id = ANY(%(game_ids)s::bigint[]) "
        "   GROUP BY m.game_id, m.team_id"
        "), game_length AS ("
        "  SELECT game_id, "
        "         (2400 + greatest(max(period) - 4, 0) * 300)::numeric AS seconds "
        "    FROM euroleague.actions_raw "
        "   WHERE %(game_ids)s::bigint[] IS NULL OR game_id = ANY(%(game_ids)s::bigint[]) "
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
