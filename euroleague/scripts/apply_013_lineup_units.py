#!/usr/bin/env python
"""Apply the reviewed EuroLeague-only lineup-unit migration.

Fixed to migration 013. Delegates the safety checks to apply_shadow_schema(),
which refuses a missing safety marker, any DROP statement, and any Israeli
schema reference. Reports the resulting relkinds so a caller can confirm the
new objects are physical tables rather than views.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    apply_shadow_schema,
    connect_from_env_file,
)

DDL_PATH = REPO / "sql" / "013_lineup_units.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"
NEW_RELATIONS = ("lineup_totals_by_game", "sub_lineups")


def main() -> int:
    connection = connect_from_env_file(ENV_PATH)
    try:
        apply_shadow_schema(connection, DDL_PATH)
        print(f"applied {DDL_PATH.name}")

        cursor = connection.cursor()
        try:
            # relkind, never the name suffix: this schema's history contains
            # _mv names that are physical tables.
            cursor.execute(
                "SELECT c.relname, c.relkind "
                "  FROM pg_class c "
                "  JOIN pg_namespace n ON n.oid = c.relnamespace "
                " WHERE n.nspname = 'euroleague' AND c.relname = ANY(%s) "
                " ORDER BY c.relname",
                (list(NEW_RELATIONS),),
            )
            found = {str(name): str(kind) for name, kind in cursor.fetchall()}
            for relation in NEW_RELATIONS:
                print(f"  {relation}: relkind={found.get(relation, 'MISSING')}")

            cursor.execute(
                "SELECT p.proname "
                "  FROM pg_proc p "
                "  JOIN pg_namespace n ON n.oid = p.pronamespace "
                " WHERE n.nspname = 'euroleague' "
                "   AND p.proname IN ('refresh_lineup_totals_by_game', "
                "                     'refresh_sub_lineups') "
                " ORDER BY p.proname"
            )
            for (name,) in cursor.fetchall():
                print(f"  function {name}: present")
        finally:
            cursor.close()
    finally:
        connection.close()

    missing = [r for r in NEW_RELATIONS if found.get(r) != "r"]
    if missing:
        print(f"FAILED: not ordinary tables: {missing}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
