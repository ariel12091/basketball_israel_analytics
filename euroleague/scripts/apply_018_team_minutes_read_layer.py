#!/usr/bin/env python
"""Apply and verify EuroLeague migration 018."""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    apply_shadow_schema,
    connect_from_env_file,
)

DDL_PATH = REPO / "sql" / "018_team_minutes_read_layer.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"
FUNCTION_SIGNATURE = (
    "euroleague.get_team_minutes_dynamic("
    "text,integer,date,date,text,text,text,text,text,text,integer,text,"
    "integer,integer,integer,integer,integer,integer,integer)"
)


def main() -> int:
    connection = connect_from_env_file(ENV_PATH)
    try:
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT COALESCE((SELECT p.prosecdef FROM pg_proc p "
                "WHERE p.oid = to_regprocedure(%s)), FALSE)",
                (FUNCTION_SIGNATURE,),
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
                "SELECT team_id, minutes "
                "FROM euroleague.get_team_minutes_dynamic("
                "%s, %s, NULL, NULL, NULL, NULL, NULL, NULL, NULL, "
                "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL) "
                "ORDER BY team_id",
                ("E", 2025),
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()
    finally:
        connection.close()

    invalid = [row for row in rows if row[1] is None or float(row[1]) <= 0]
    print(f"  teams returned: {len(rows)}")
    print(f"  invalid minutes: {len(invalid)}")
    if rows:
        values = [float(row[1]) for row in rows]
        print(f"  minutes range: {min(values):.1f} - {max(values):.1f}")
    return 0 if rows and not invalid else 1


if __name__ == "__main__":
    raise SystemExit(main())
