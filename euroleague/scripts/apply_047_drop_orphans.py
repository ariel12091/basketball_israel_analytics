#!/usr/bin/env python
"""Apply migration 047: drop orphaned EuroLeague functions and views.

Destructive, so the gate is stricter than the additive migrations':

1. Re-verify in the live catalog that every target has zero referrers among
   euroleague views, materialized views and function bodies. Refuse otherwise.
2. Re-verify that player_game_context is NOT among the targets -- an earlier
   audit draft wrongly listed it as orphaned and scripts/load_games.py reads it.
3. Drop inside one transaction.
4. Smoke every app-facing reader the EuroLeague tabs actually call, so a wrong
   drop shows up here rather than in the app.
5. Roll back unless --apply is passed.

apply_shadow_schema() refuses DDL containing DROP by design, so this applies the
statements directly, exactly as the 045 and 046 applicators do.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))
from euroleague_function_contract import (  # noqa: E402
    APP_READER_SMOKE,
    PENDING_REMOVAL_FUNCTIONS,
    PENDING_REMOVAL_VIEWS,
    PROTECTED_RELATIONS,
)
from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
)

DDL = ROOT / "sql" / "047_drop_orphaned_objects.sql"
ENV = ROOT.parent / "etl" / ".Renviron"

TARGET_FUNCTIONS = tuple(sorted(PENDING_REMOVAL_FUNCTIONS))
TARGET_VIEWS = tuple(sorted(PENDING_REMOVAL_VIEWS))

# Must never be dropped: the loader's published-game QA check reads it.
PROTECTED = tuple(sorted(PROTECTED_RELATIONS))

# Every reader the EuroLeague tabs can name, including the ones Tab 9 assembles
# from fragments (base + "_" + kind), which appear nowhere in the R source.
#
# The point is to prove each reader still resolves and executes, not to
# benchmark it. The lineup readers are narrowed to two-player units over the
# last two games -- an unfiltered five-player expansion takes minutes and
# exceeded the statement timeout on the first run.
SMOKE = APP_READER_SMOKE


def options():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def referrers(cursor, name: str) -> list[str]:
    """Every euroleague view or function whose definition mentions `name`."""
    cursor.execute(
        """
        SELECT c.relname, pg_get_viewdef(c.oid, true)
          FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = 'euroleague' AND c.relkind IN ('v', 'm')
        """
    )
    found = [r[0] for r in cursor.fetchall() if r[0] != name and re.search(rf"\b{name}\b", r[1] or "")]
    cursor.execute(
        """
        SELECT p.proname, p.prosrc
          FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = 'euroleague'
        """
    )
    found += [r[0] for r in cursor.fetchall() if r[0] != name and re.search(rf"\b{name}\b", r[1] or "")]
    return found


def main() -> int:
    args = options()
    statements = [
        s for s in _split_sql_statements(DDL.read_text(encoding="utf-8"))
        if s.strip().upper() not in {"BEGIN", "COMMIT"}
    ]

    ddl_text = DDL.read_text(encoding="utf-8")
    for guarded in PROTECTED:
        if re.search(rf"DROP\s+(VIEW|MATERIALIZED VIEW)[^;]*\b{guarded}\b", ddl_text):
            raise RuntimeError(f"refusing: migration drops protected object {guarded}")
    print(f"protected objects absent from DROP list: {', '.join(PROTECTED)}")

    connection = connect_from_env_file(ENV, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        cursor.execute("SET LOCAL statement_timeout='300s'")

        print("\n-- pre-flight: referrer check --")
        blocked = False
        for name in TARGET_FUNCTIONS + TARGET_VIEWS:
            refs = referrers(cursor, name)
            print(f"  {name:<42} referrers={refs or 'none'}")
            if refs:
                blocked = True
        if blocked:
            raise RuntimeError("a target still has an in-database referrer; nothing dropped")

        before = {}
        for fn, sig in SMOKE:
            cursor.execute(f"SELECT count(*) FROM euroleague.{fn}({sig})")
            before[fn] = cursor.fetchone()[0]

        print("\n-- applying --")
        for statement in statements:
            cursor.execute(statement)
        print(f"  {len(statements)} statement(s) executed")

        cursor.execute(
            """
            SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE n.nspname = 'euroleague' AND p.proname = ANY(%s)
            """,
            (list(TARGET_FUNCTIONS),),
        )
        still = [r[0] for r in cursor.fetchall()]
        if still:
            raise RuntimeError(f"targets survived the drop: {still}")

        cursor.execute(
            """
            SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = 'euroleague' AND c.relname = ANY(%s)
            """,
            (list(TARGET_VIEWS + PROTECTED),),
        )
        remaining = {r[0] for r in cursor.fetchall()}
        if remaining & set(TARGET_VIEWS):
            raise RuntimeError(f"views survived the drop: {remaining & set(TARGET_VIEWS)}")
        for guarded in PROTECTED:
            if guarded not in remaining:
                raise RuntimeError(f"protected object {guarded} was removed")
        print(f"  protected objects intact: {', '.join(PROTECTED)}")

        print("\n-- post-drop smoke of every app-reachable reader --")
        for fn, sig in SMOKE:
            cursor.execute(f"SELECT count(*) FROM euroleague.{fn}({sig})")
            after = cursor.fetchone()[0]
            same = "ok" if after == before[fn] else f"*** CHANGED {before[fn]} -> {after} ***"
            print(f"  {fn:<42} rows={after:<7} {same}")
            if after != before[fn]:
                raise RuntimeError(f"{fn} changed after the drop")

        if args.apply:
            connection.commit()
            print("\nCOMMITTED migration 047")
        else:
            connection.rollback()
            print("\nROLLBACK gate passed; no persistent database change")
        return 0
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
