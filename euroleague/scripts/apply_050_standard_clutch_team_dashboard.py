#!/usr/bin/env python
"""Guard, parity-check, and optionally apply migration 050."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
    inspect_target,
)
from gate_050_combined_team_dashboard_readers import (  # noqa: E402
    COMBINED_COLUMNS,
    ROUTES,
    arguments,
    candidate,
    legacy,
)


DDL = ROOT / "sql" / "050_standard_clutch_team_dashboard.sql"
ENV = ROOT.parent / "etl" / ".Renviron"
DYNAMIC_ROUTES = tuple(route for route in ROUTES if route.kind == "dynamic")
SIGNATURE = (
    "text,integer,date,date,text,text,text,text,text,text,integer,text,integer,"
    "text,integer,boolean,integer,integer,integer,integer,integer,integer,integer"
)


def options():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def validated_statements() -> list[str]:
    source = DDL.read_text(encoding="utf-8")
    upper = source.upper()
    if "EUROLEAGUE SHADOW SCHEMA" not in upper:
        raise ValueError("EuroLeague migration safety marker is missing")
    if re.search(r"\bBASKETBALL(?:_TEST)?\s*\.", upper):
        raise ValueError("migration references an Israeli schema")
    if "CASCADE" in upper or re.search(r"\bDROP\b", upper):
        raise ValueError("migration 050 must remain additive")
    if source.count("CREATE OR REPLACE FUNCTION") != 1:
        raise ValueError("migration 050 must define exactly one function")
    expected_name = "CREATE OR REPLACE FUNCTION euroleague.get_team_dashboard_dynamic"
    if expected_name not in source:
        raise ValueError("migration 050 defines an unexpected function")
    for privilege in (
        f"REVOKE ALL ON FUNCTION euroleague.get_team_dashboard_dynamic({SIGNATURE}) FROM PUBLIC",
        f"GRANT EXECUTE ON FUNCTION euroleague.get_team_dashboard_dynamic({SIGNATURE}) TO app_readonly",
    ):
        if privilege not in source:
            raise ValueError(f"missing privilege statement: {privilege}")
    return [
        statement
        for statement in _split_sql_statements(source)
        if statement.strip().upper() not in {"BEGIN", "COMMIT"}
    ]


def main() -> int:
    args = options()
    statements = validated_statements()
    connection = connect_from_env_file(ENV, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague" or target["server_port"] != 5432:
            raise RuntimeError(f"unexpected target: {target}")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        cursor.execute("SET LOCAL statement_timeout='90s'")
        for statement in statements:
            cursor.execute(statement)
        print("migration 050 created transactionally", flush=True)

        for route in DYNAMIC_ROUTES:
            call_args = arguments(route.extra)
            expected, _ = legacy(cursor, route, call_args)
            actual, _ = candidate(cursor, route, call_args)
            if not actual:
                raise RuntimeError(f"{route.label}: parity check returned no rows")
            if expected != actual:
                keys = sorted(set(expected) | set(actual))
                team_id = next(key for key in keys if expected.get(key) != actual.get(key))
                left, right = expected.get(team_id), actual.get(team_id)
                if left is None or right is None:
                    detail = f"team_id={team_id} expected={left!r} actual={right!r}"
                else:
                    index = next(i for i, values in enumerate(zip(left, right)) if values[0] != values[1])
                    detail = (
                        f"team_id={team_id} column={COMBINED_COLUMNS[index]} "
                        f"expected={left[index]!r} actual={right[index]!r}"
                    )
                raise RuntimeError(f"{route.label}: {detail}")
            print(f"  OK {route.label:<24} rows={len(actual)}", flush=True)

        cursor.execute(
            "SELECT NOT has_function_privilege('public', %s, 'EXECUTE'), "
            "has_function_privilege('app_readonly', %s, 'EXECUTE')",
            (f"euroleague.get_team_dashboard_dynamic({SIGNATURE})",) * 2,
        )
        public_revoked, app_granted = cursor.fetchone()
        if not public_revoked or not app_granted:
            raise RuntimeError("migration 050 function privileges are incorrect")

        if args.apply:
            connection.commit()
            print("COMMITTED migration 050")
        else:
            connection.rollback()
            print("ROLLBACK gate passed; no persistent database change")
        return 0
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
