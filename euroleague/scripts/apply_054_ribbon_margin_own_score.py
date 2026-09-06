#!/usr/bin/env python
"""Apply and verify EuroLeague migration 054 (own_team_score on ribbon_margin_v).

Fixed to migration 054. Purely additive -- one view, `CREATE OR REPLACE VIEW`
only, with the new column APPENDED at the end of the select list, which is the
one shape Postgres permits without dropping the view. A DROP would wipe
app_readonly's SELECT grant on the ribbon read layer, so this refuses any DROP
statement, any CASCADE, and any reference to an Israeli schema, same as the
053/037/038 applicators.

The gate:
1. Refuse DROP / CASCADE / an Israeli-schema reference in the DDL, and require
   the EuroLeague safety marker.
2. Refuse the migration unless it actually appends the column: own_team_score
   must be the LAST entry of the select list, after source_event_order.
3. Apply (CREATE OR REPLACE VIEW, idempotent).
4. Verify the column is now present on the view, that the pre-existing columns
   are unchanged (a pure append), and that the column is readable.
5. Verify BOTH directions of the grant, which is the point of the read layer:
   app_readonly can still SELECT euroleague.ribbon_margin_v, and is STILL
   DENIED on euroleague.action_team_context_actions (plus the other two raw
   evidence relations 053 closed).
6. Sanity-check the data: a non-trivial row-count floor so no check passes
   vacuously, zero NULL margins (the regression 053 fixed), and zero NULL
   own_team_score -- the new column is the one this migration exists for, and
   a NULL there would blank points for/against exactly as NULL scores once
   blanked the margin curve.
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

DDL_PATH = REPO / "sql" / "054_ribbon_margin_own_score.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"

VIEW = "ribbon_margin_v"
NEW_COLUMN = "own_team_score"
DENIED_RELATIONS = ("actions", "matchup_segments_actions", "action_team_context_actions")

# Row-count floor so the sanity checks below cannot pass on an empty view.
# Reflects the 109,184 scoring rows across 589 games measured for 053.
MIN_ROWS = 50000


def validate_ddl(ddl: str) -> list[str]:
    upper = ddl.upper()
    if "EUROLEAGUE SHADOW SCHEMA" not in upper:
        raise ValueError("EuroLeague migration safety marker is missing")
    if re.search(r"\bBASKETBALL(?:_TEST)?\s*\.", upper):
        raise ValueError("migration references a forbidden Israeli schema")
    if re.search(r"\bCASCADE\b", upper):
        raise ValueError("migration contains CASCADE")

    statements = _split_sql_statements(ddl)
    for statement in statements:
        normalized = " ".join(statement.strip().upper().split())
        if normalized.startswith("DROP "):
            raise ValueError(
                f"migration 054 is additive and must not DROP: {statement[:80]!r}"
            )

    # The column must be APPENDED, not inserted mid-list: CREATE OR REPLACE
    # VIEW rejects any other shape, and rejecting it here names the reason
    # rather than letting Postgres fail with a column-mismatch error.
    body = None
    for statement in statements:
        if re.search(
            r"CREATE\s+OR\s+REPLACE\s+VIEW\s+euroleague\." + VIEW + r"\b",
            statement,
            re.IGNORECASE,
        ):
            body = statement
            break
    if body is None:
        raise ValueError("migration does not CREATE OR REPLACE euroleague." + VIEW)
    select_list = body[: body.upper().rfind("FROM")]
    last_entry = select_list.rstrip().rstrip(",").split(",")[-1].strip()
    if last_entry != NEW_COLUMN:
        raise ValueError(
            NEW_COLUMN + " is not the last entry of the select list (found "
            + repr(last_entry) + ") -- CREATE OR REPLACE VIEW can only append"
        )
    return statements


def view_columns(connection, view: str) -> list[str]:
    cursor = connection.cursor()
    cursor.execute(
        "SELECT a.attname FROM pg_attribute a "
        "JOIN pg_class c ON c.oid = a.attrelid "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'euroleague' AND c.relname = %s "
        "AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
        (view,),
    )
    names = [str(row[0]) for row in cursor.fetchall()]
    cursor.close()
    return names


def main() -> int:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    statements = validate_ddl(ddl)
    print(
        "safety gate passed: no DROP, no CASCADE, no Israeli schema; "
        + NEW_COLUMN
        + " is appended last"
    )

    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError(f"EuroLeague schema is unavailable: {target}")
        print(
            "target "
            f"database={target['database']} user={target['user']} "
            f"server={target['server_address']}:{target['server_port']}"
        )

        before = view_columns(connection, VIEW)
        if not before:
            raise RuntimeError(
                "euroleague." + VIEW + " does not exist -- apply 053 first"
            )
        print("before: euroleague." + VIEW + " columns = " + str(before))

        print(f"applying {DDL_PATH.name}: {len(statements)} reviewed statements")
        for index, statement in enumerate(statements, start=1):
            cursor.execute(statement)
            last_line = statement.strip().splitlines()[-1][:72]
            print(f"  [{index}/{len(statements)}] {last_line}")

        after = view_columns(connection, VIEW)
        print("after:  euroleague." + VIEW + " columns = " + str(after))
        if NEW_COLUMN not in after:
            raise RuntimeError(
                VIEW + " still has no " + NEW_COLUMN + " column after applying"
            )
        if after[: len(before)] != before:
            raise RuntimeError(
                VIEW + "'s existing columns changed -- expected a pure append: "
                + str(before) + " -> " + str(after)
            )

        print("\n-- verify 1: the column is there and readable --")
        cursor.execute("SELECT " + NEW_COLUMN + " FROM euroleague." + VIEW + " LIMIT 1")
        row = cursor.fetchone()
        print(
            "  SELECT " + NEW_COLUMN + " FROM euroleague." + VIEW
            + " LIMIT 1 -> " + str(row)
        )

        print("\n-- verify 2: app_readonly's SELECT grant survived the replace --")
        cursor.execute(
            "SELECT has_table_privilege('app_readonly', %s, 'SELECT')",
            ("euroleague." + VIEW,),
        )
        granted = bool(cursor.fetchone()[0])
        print(f"  {VIEW:<28} app_readonly SELECT={granted}")
        if not granted:
            raise RuntimeError(
                VIEW + ": app_readonly lost SELECT -- re-run "
                "scripts/apply_db_security.R with CONFIRM_DB_SECURITY_APPLY=1"
            )

        print("\n-- verify 3: app_readonly still DENIED on the raw evidence tables --")
        for relation in DENIED_RELATIONS:
            cursor.execute(
                "SELECT has_table_privilege('app_readonly', %s, 'SELECT')",
                ("euroleague." + relation,),
            )
            has = bool(cursor.fetchone()[0])
            print(f"  {relation:<28} app_readonly SELECT={has} (denied={not has})")
            if has:
                raise RuntimeError(
                    relation + ": app_readonly can read this directly -- "
                    "the read layer no longer narrows access"
                )

        print("\n-- sanity: euroleague." + VIEW + " --")
        cursor.execute(
            "SELECT count(*), count(*) FILTER (WHERE margin IS NULL), "
            "count(*) FILTER (WHERE " + NEW_COLUMN + " IS NULL) "
            "FROM euroleague." + VIEW
        )
        total, null_margin, null_own = cursor.fetchone()
        print(
            f"  rows={total} null_margin={null_margin} "
            f"null_{NEW_COLUMN}={null_own}"
        )
        if total < MIN_ROWS:
            raise RuntimeError(f"{VIEW} row floor not met: {total}")
        if null_margin:
            raise RuntimeError(f"{VIEW} has {null_margin} NULL-margin rows")
        if null_own:
            raise RuntimeError(
                f"{VIEW} has {null_own} NULL {NEW_COLUMN} rows -- points "
                "for/against would blank out exactly as the margin curve once did"
            )

        print("\nmigration 054 verified")
        return 0
    except Exception:
        try:
            cursor.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
