"""List EuroLeague objects whose definitions mention legacy relations."""

from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402


LEGACY_RELATIONS = (
    "actions_clean",
    "possessions",
    "lineups",
    "lineup_players",
    "action_lineups",
    "stints",
    "pws",
    "action_team_context",
    "matchup_segments",
)


def main() -> int:
    connection = connect_from_env_file(REPO.parent / "etl" / ".Renviron")
    cursor = connection.cursor()
    try:
        cursor.execute(
            """
            WITH names(name) AS (
              SELECT unnest(%s::text[])
            )
            SELECT 'routine', p.proname, n.name
              FROM pg_proc p
              JOIN pg_namespace s ON s.oid = p.pronamespace
              CROSS JOIN names n
             WHERE s.nspname = 'euroleague'
               AND p.prosrc ~* (
                 'euroleague\\.' || n.name || '([^a-z0-9_]|$)'
               )
            UNION ALL
            SELECT 'view', v.viewname, n.name
              FROM pg_views v
              CROSS JOIN names n
             WHERE v.schemaname = 'euroleague'
               AND v.definition ~* (
                 'euroleague\\.' || n.name || '([^a-z0-9_]|$)'
               )
            UNION ALL
            SELECT 'materialized view', v.matviewname, n.name
              FROM pg_matviews v
              CROSS JOIN names n
             WHERE v.schemaname = 'euroleague'
               AND v.definition ~* (
                 'euroleague\\.' || n.name || '([^a-z0-9_]|$)'
               )
            ORDER BY 1, 2, 3
            """,
            (list(LEGACY_RELATIONS),),
        )
        for row in cursor.fetchall():
            print("\t".join(str(value) for value in row))
    finally:
        cursor.close()
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
