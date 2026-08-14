#!/usr/bin/env python
"""Apply and verify EuroLeague migration 016."""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    apply_shadow_schema,
    connect_from_env_file,
)

DDL_PATH = REPO / "sql" / "016_matchup_segment_join_order.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"


def main() -> int:
    connection = connect_from_env_file(ENV_PATH)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT pg_get_functiondef("
                "  'euroleague.refresh_actions_consumer_candidates(bigint[])'::regprocedure"
                ") LIKE '%%event_sides AS MATERIALIZED%%'"
            )
            already_applied = bool(cursor.fetchone()[0])
        finally:
            cursor.close()

        if already_applied:
            print(f"already applied {DDL_PATH.name}")
        else:
            apply_shadow_schema(connection, DDL_PATH)
            print(f"applied {DDL_PATH.name}")

        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT "
                "  pg_get_functiondef(p.oid) LIKE '%%event_lineups AS MATERIALIZED%%', "
                "  pg_get_functiondef(p.oid) LIKE '%%event_sides AS MATERIALIZED%%', "
                "  (length(pg_get_functiondef(p.oid)) - length(replace("
                "     pg_get_functiondef(p.oid), "
                "     'side(team_id, opponent_team_id, own_lineup, opp_lineup)', ''"
                "   ))) / length('side(team_id, opponent_team_id, own_lineup, opp_lineup)') = 1 "
                "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
                "WHERE n.nspname='euroleague' "
                "AND p.proname='refresh_actions_consumer_candidates'"
            )
            checks = tuple(bool(value) for value in cursor.fetchone())
        finally:
            cursor.close()
    finally:
        connection.close()

    print(f"  exact matchup materialized: {checks[0]}")
    print(f"  two perspectives materialized: {checks[1]}")
    print(f"  only analytical-fact lateral expansion remains: {checks[2]}")
    return 0 if checks == (True, True, True) else 1


if __name__ == "__main__":
    raise SystemExit(main())
