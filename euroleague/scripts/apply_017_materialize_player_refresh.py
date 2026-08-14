#!/usr/bin/env python
"""Apply and verify EuroLeague migration 017."""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    apply_shadow_schema,
    connect_from_env_file,
)

DDL_PATH = REPO / "sql" / "017_materialize_player_refresh.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"


def main() -> int:
    connection = connect_from_env_file(ENV_PATH)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT pg_get_functiondef("
                "  'euroleague.refresh_player_four_factors_by_game_for_games(bigint[])'::regprocedure"
                ") LIKE '%%player_minutes AS MATERIALIZED%%' "
                "AND pg_get_functiondef("
                "  'euroleague.refresh_player_four_factors_by_game_for_games(bigint[])'::regprocedure"
                ") LIKE '%%counts AS MATERIALIZED%%'"
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
                "SELECT pg_get_functiondef("
                "  'euroleague.refresh_player_four_factors_by_game_for_games(bigint[])'::regprocedure"
                ") LIKE '%%player_minutes AS MATERIALIZED%%' "
                "AND pg_get_functiondef("
                "  'euroleague.refresh_player_four_factors_by_game_for_games(bigint[])'::regprocedure"
                ") LIKE '%%counts AS MATERIALIZED%%'"
            )
            ok = bool(cursor.fetchone()[0])
        finally:
            cursor.close()
    finally:
        connection.close()
    print(f"  both heavy CTEs materialized: {ok}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
