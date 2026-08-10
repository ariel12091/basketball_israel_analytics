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
