#!/usr/bin/env python
"""Apply the reviewed EuroLeague-only actions consumer cutover.

This entry point is intentionally fixed to migration 012. It refuses any
unexpected destructive statement, any cross-schema target, or a live schema
that is not in the expected pre-cutover state.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
    inspect_target,
)


DDL_PATH = REPO / "sql" / "012_actions_consumer_cutover.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"

EXPECTED_OLD_TABLES = {
    "action_team_context",
    "matchup_segments",
    "pws",
    "stints",
    "action_lineups",
    "lineup_players",
    "lineups",
    "possessions",
    "actions_clean",
}
EXPECTED_NEW_TABLES = {
    "actions",
    "action_team_context_actions",
    "matchup_segments_actions",
}
ALLOWED_DROP_LINES = {
    "DROP MATERIALIZED VIEW EUROLEAGUE.TEAM_PPP_RATINGS_MV;",
    "DROP MATERIALIZED VIEW EUROLEAGUE.TEAM_GAME_RATINGS_MV;",
    "DROP FUNCTION EUROLEAGUE.REFRESH_ACTION_TEAM_CONTEXT_FOR_GAMES(BIGINT[]);",
    "DROP FUNCTION EUROLEAGUE.REFRESH_STINT_TIMING_FOR_GAMES(BIGINT[]);",
    *(f"DROP TABLE EUROLEAGUE.{name.upper()};" for name in EXPECTED_OLD_TABLES),
}


def validate_ddl(ddl: str) -> None:
    upper = ddl.upper()
    if "EUROLEAGUE SHADOW SCHEMA" not in upper:
        raise ValueError("EuroLeague migration safety marker is missing")
    if re.search(r"\bBASKETBALL(?:_TEST)?\s*\.", upper):
        raise ValueError("migration references a forbidden Israeli schema")
    if re.search(r"\bCASCADE\b", upper):
        raise ValueError("migration contains CASCADE")
    drop_lines = {
        " ".join(line.strip().upper().split())
        for line in ddl.splitlines()
        if line.lstrip().upper().startswith("DROP ")
    }
    if drop_lines != ALLOWED_DROP_LINES:
        missing = sorted(ALLOWED_DROP_LINES - drop_lines)
        unexpected = sorted(drop_lines - ALLOWED_DROP_LINES)
        raise ValueError(
            f"unexpected DROP scope; missing={missing}, unexpected={unexpected}"
        )


def euroleague_tables(connection: object) -> set[str]:
    cursor = connection.cursor()  # type: ignore[attr-defined]
    cursor.execute(
        "SELECT tablename FROM pg_catalog.pg_tables "
        "WHERE schemaname = 'euroleague' ORDER BY tablename"
    )
    tables = {str(row[0]) for row in cursor.fetchall()}
    cursor.close()
    return tables


def main() -> int:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    validate_ddl(ddl)
    statements = _split_sql_statements(ddl)

    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError(f"EuroLeague schema is unavailable: {target}")
        tables_before = euroleague_tables(connection)
        missing_old = EXPECTED_OLD_TABLES - tables_before
        missing_new = EXPECTED_NEW_TABLES - tables_before
        if missing_old or missing_new:
            raise RuntimeError(
                "database is not in the expected pre-cutover state; "
                f"missing_old={sorted(missing_old)}, missing_new={sorted(missing_new)}"
            )

        print(
            "target "
            f"database={target['database']} user={target['user']} "
            f"server={target['server_address']}:{target['server_port']}"
        )
        print(f"applying {DDL_PATH.name}: {len(statements)} reviewed statements")
        for index, statement in enumerate(statements, start=1):
            cursor.execute(statement)
            print(f"  [{index}/{len(statements)}] {statement.splitlines()[-1][:72]}")

        tables_after = euroleague_tables(connection)
        remaining_old = EXPECTED_OLD_TABLES & tables_after
        missing_after = EXPECTED_NEW_TABLES - tables_after
        if remaining_old or missing_after:
            raise RuntimeError(
                "post-cutover table contract failed; "
                f"remaining_old={sorted(remaining_old)}, "
                f"missing_new={sorted(missing_after)}"
            )
        print("migration 012 committed; EuroLeague table contract is correct")
        return 0
    except Exception:
        try:
            cursor.execute("ROLLBACK")
        finally:
            raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
