#!/usr/bin/env python
"""Audit every app-executable EuroLeague function for a declared consumer.

The audit is read-only. A function is covered when it is a declared direct app
reader, is referenced by another database function/view, or is explicitly
pending removal in migration 047.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from euroleague_function_contract import (  # noqa: E402
    DIRECT_APP_READERS,
    PENDING_REMOVAL_FUNCTIONS,
)
from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402


ENV = ROOT.parent / "etl" / ".Renviron"


def referenced_names(cursor) -> set[str]:
    cursor.execute(
        """
        SELECT p.proname, p.prosrc
          FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
         WHERE n.nspname='euroleague'
        """
    )
    functions = cursor.fetchall()
    cursor.execute(
        """
        SELECT c.relname, pg_get_viewdef(c.oid, true)
          FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
         WHERE n.nspname='euroleague' AND c.relkind IN ('v','m')
        """
    )
    definitions = functions + cursor.fetchall()
    names = {name for name, _ in functions}
    referenced = set()
    for owner, definition in definitions:
        for name in names:
            if owner != name and re.search(rf"\b{re.escape(name)}\b", definition or ""):
                referenced.add(name)
    return referenced


def main() -> int:
    connection = connect_from_env_file(ENV, direct_port=5432)
    cursor = connection.cursor()
    try:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute(
            """
            SELECT p.proname, pg_get_function_identity_arguments(p.oid)
              FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
             WHERE n.nspname='euroleague'
               AND has_function_privilege('app_readonly', p.oid, 'EXECUTE')
             ORDER BY p.proname
            """
        )
        executable_rows = cursor.fetchall()
        executable = {row[0] for row in executable_rows}
        overloaded = sorted(
            name for name in executable
            if sum(row[0] == name for row in executable_rows) > 1
        )
        referenced = referenced_names(cursor)

        missing = sorted(DIRECT_APP_READERS - executable)
        uncovered = sorted(
            executable - DIRECT_APP_READERS - referenced - PENDING_REMOVAL_FUNCTIONS
        )
        pending_live = sorted(executable & PENDING_REMOVAL_FUNCTIONS)

        print(f"app-executable functions: {len(executable)}")
        print(f"declared direct app readers: {len(DIRECT_APP_READERS)}")
        print(f"pending migration 047 removal: {pending_live or 'none'}")
        print(f"missing declared readers: {missing or 'none'}")
        print(f"overloaded executable names: {overloaded or 'none'}")
        print(f"uncovered executable functions: {uncovered or 'none'}")

        if missing or overloaded or uncovered:
            return 1
        return 0
    finally:
        connection.rollback()
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
