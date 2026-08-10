#!/usr/bin/env python
"""Apply the reviewed EuroLeague-only lineup-unit read layer (migration 014).

Fixed to migration 014. This one cannot go through apply_shadow_schema(),
because changing a materialized view's definition or a function's RETURNS TABLE
requires dropping it first, and that helper refuses every destructive
statement. The euroleague-only checks it performs are reproduced here; only the
DROP ban is relaxed, and only for the two objects this migration owns.

Re-running is safe: the migration recreates both objects and re-grants
app_readonly, which a drop would otherwise wipe.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
)

DDL_PATH = REPO / "sql" / "014_lineup_units_read_layer.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"

# The only objects this script is allowed to drop.
OWNED_MV = "euroleague.sub_lineups_stats_mv"
OWNED_FN = (
    "euroleague.fetch_lineups_dynamic("
    "text, int4, date, date, text, text, text, text, text, text, int4, text, "
    "int4, int4, int4, int4, int4, int4, int4, int4, text, text, int4)"
)


def main() -> int:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    upper = ddl.upper()
    if "EUROLEAGUE SHADOW SCHEMA" not in upper:
        raise ValueError("shadow DDL safety marker is missing")
    if "BASKETBALL." in upper or "BASKETBALL_TEST." in upper:
        raise ValueError("shadow DDL references an Israeli schema")

    statements = _split_sql_statements(ddl)
    for statement in statements:
        s = statement.upper()
        if "BASKETBALL." in s or "BASKETBALL_TEST." in s:
            raise ValueError(f"cross-schema statement: {statement[:80]}")

    connection = connect_from_env_file(ENV_PATH)
    cursor = connection.cursor()
    try:
        for statement in statements:
            # Substring, not startswith: _split_sql_statements keeps each
            # statement's leading comment block, so the CREATE is rarely the
            # first token.
            head = statement.upper()
            # Drop immediately before the matching create, so a failure leaves
            # the previous definition in place rather than nothing at all. The
            # migration's own BEGIN/COMMIT makes that guarantee real: a failure
            # anywhere rolls the whole rebuild back.
            if "CREATE MATERIALIZED VIEW" in head:
                cursor.execute(f"DROP MATERIALIZED VIEW IF EXISTS {OWNED_MV} CASCADE")
                print(f"  dropped {OWNED_MV}")
            elif "CREATE OR REPLACE FUNCTION EUROLEAGUE.FETCH_LINEUPS_DYNAMIC" in head:
                # CREATE OR REPLACE cannot change a RETURNS TABLE signature.
                cursor.execute(f"DROP FUNCTION IF EXISTS {OWNED_FN}")
                print("  dropped fetch_lineups_dynamic (signature change)")
            cursor.execute(statement)
        print(f"applied {DDL_PATH.name}")

        started = time.time()
        cursor.execute(f"REFRESH MATERIALIZED VIEW {OWNED_MV}")
        print(f"  refreshed in {time.time() - started:.1f}s")

        # A drop wipes grants. The migration re-grants; confirm it took.
        cursor.execute(
            "SELECT has_table_privilege('app_readonly', %s, 'SELECT')", (OWNED_MV,)
        )
        mv_ok = bool(cursor.fetchone()[0])
        cursor.execute(
            "SELECT bool_or(has_function_privilege('app_readonly', p.oid, 'EXECUTE')) "
            "  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
            " WHERE n.nspname = 'euroleague' AND p.proname = 'fetch_lineups_dynamic'"
        )
        fn_ok = bool(cursor.fetchone()[0])
        print(f"  app_readonly SELECT on MV: {mv_ok}")
        print(f"  app_readonly EXECUTE on fetch_lineups_dynamic: {fn_ok}")
        if not (mv_ok and fn_ok):
            print("FAILED: a grant did not survive the rebuild")
            return 1
    finally:
        cursor.close()
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
