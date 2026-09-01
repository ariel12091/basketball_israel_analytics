#!/usr/bin/env python
"""Guard, parity-check, and optionally apply the Israeli FF+Minutes reader."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from benchmark_051_israeli_four_factors_minutes import (  # noqa: E402
    ENV, PRESETS, assert_parity, legacy,
)
from benchmark_051_israeli_standard_clutch_dashboard import args_for, fetch  # noqa: E402
from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements, connect_from_env_file, inspect_target,
)


DDL = ROOT.parent / "sql" / "functions" / "get_team_four_factors_dashboard_dynamic.sql"
FUNCTION = "get_team_four_factors_dashboard_dynamic"
SIGNATURE = (
    "integer,date,date,text,text,text,text,text,integer,text,integer,text,integer,"
    "boolean,integer,integer,integer,integer,integer,integer,integer,integer,integer"
)


def options():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def validated_statements():
    source = DDL.read_text(encoding="utf-8")
    upper = source.upper()
    if source.count("CREATE OR REPLACE FUNCTION") != 1:
        raise ValueError("migration must define exactly one function")
    if f"CREATE OR REPLACE FUNCTION basketball_test.{FUNCTION}" not in source:
        raise ValueError("migration defines an unexpected function")
    if "CASCADE" in upper or re.search(r"\bDROP\b", upper):
        raise ValueError("migration must remain additive")
    if "EUROLEAGUE." in upper or re.search(r"\bBASKETBALL\s*\.", upper):
        raise ValueError("migration references a schema outside basketball_test")
    for privilege in (
        f"REVOKE ALL ON FUNCTION basketball_test.{FUNCTION}({SIGNATURE}) FROM PUBLIC",
        f"GRANT EXECUTE ON FUNCTION basketball_test.{FUNCTION}({SIGNATURE}) TO app_readonly",
    ):
        if privilege not in source:
            raise ValueError(f"missing privilege statement: {privilege}")
    return [
        statement for statement in _split_sql_statements(source)
        if statement.strip().upper() not in {"BEGIN", "COMMIT"}
    ]


def main() -> int:
    args = options()
    connection = connect_from_env_file(ENV, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        cursor.execute("SELECT to_regnamespace('basketball_test')::text")
        if target["server_port"] != 5432 or cursor.fetchone()[0] != "basketball_test":
            raise RuntimeError(f"unexpected target: {target}")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        cursor.execute("SET LOCAL statement_timeout='90s'")
        for statement in validated_statements():
            cursor.execute(statement)
        print("Israeli FF+Minutes reader created transactionally", flush=True)
        for preset in PRESETS:
            old = legacy(cursor, preset)
            new = fetch(cursor, f"basketball_test.{FUNCTION}", args_for(preset))
            assert_parity(preset, old, new)
            print(f"  parity OK {preset.label:<18} rows={len(new[1])}", flush=True)
        cursor.execute(
            "SELECT NOT has_function_privilege('public', %s, 'EXECUTE'), "
            "has_function_privilege('app_readonly', %s, 'EXECUTE')",
            (f"basketball_test.{FUNCTION}({SIGNATURE})",) * 2,
        )
        public_revoked, app_granted = cursor.fetchone()
        if not public_revoked or not app_granted:
            raise RuntimeError("function privileges are incorrect")
        if args.apply:
            connection.commit()
            print("COMMITTED Israeli FF+Minutes reader")
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
