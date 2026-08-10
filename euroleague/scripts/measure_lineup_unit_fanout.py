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
