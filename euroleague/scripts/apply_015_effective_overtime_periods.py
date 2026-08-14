#!/usr/bin/env python
"""Apply and verify EuroLeague migration 015."""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    apply_shadow_schema,
    connect_from_env_file,
)

DDL_PATH = REPO / "sql" / "015_effective_overtime_periods.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"


def main() -> int:
    connection = connect_from_env_file(ENV_PATH)
    try:
        preflight = connection.cursor()
        try:
            preflight.execute(
                "SELECT to_regprocedure("
                "         'euroleague.effective_period(smallint,integer,text)'"
                "       ) IS NOT NULL "
                "   AND pg_get_functiondef("
                "         'euroleague.refresh_actions_consumer_candidates(bigint[])'::regprocedure"
                "       ) LIKE '%%actions-v2%%'"
            )
            already_applied = bool(preflight.fetchone()[0])
        finally:
            preflight.close()
        if already_applied:
            print(f"already applied {DDL_PATH.name}")
        else:
            apply_shadow_schema(connection, DDL_PATH)
            print(f"applied {DDL_PATH.name}")
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT euroleague.effective_period(5::smallint, 46, 'EP'), "
                "       euroleague.effective_period(5::smallint, 46, 'BP'), "
                "       euroleague.effective_period(5::smallint, 51, 'BP'), "
                "       euroleague.effective_period(5::smallint, 56, 'EP'), "
                "       pg_get_functiondef(" 
                "         'euroleague.refresh_actions_consumer_candidates(bigint[])'::regprocedure"
                "       ) LIKE '%%actions-v2%%'"
            )
            end_ot1, start_ot2, start_ot3, end_ot3, version_ok = cursor.fetchone()
            print(f"  OT1 end period: {end_ot1}")
            print(f"  OT2 start period: {start_ot2}")
            print(f"  OT3 start period: {start_ot3}")
            print(f"  OT3 end period: {end_ot3}")
            print(f"  consumer derivation actions-v2: {bool(version_ok)}")
        finally:
            cursor.close()
    finally:
        connection.close()

    if (
        int(end_ot1), int(start_ot2), int(start_ot3), int(end_ot3), bool(version_ok)
    ) != (5, 6, 7, 7, True):
        print("FAILED: effective overtime verification did not match")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
