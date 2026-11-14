#!/usr/bin/env python
"""Apply and verify EuroLeague migration 063 (sub_lineups_stats_mv as a table).

Dry run by default: the migration runs inside a transaction, every gate runs,
and the transaction rolls back. Set CONFIRM_EL_063_APPLY=1 to commit.

The migration drops exactly one object, the materialized view it replaces, so
this refuses any other DROP, TRUNCATE, CASCADE or Israeli-schema reference.

Gates, all before COMMIT:
1. Row floor on the view before the swap.
2. Before any lock: the function's own SELECT (read from the migration file,
   every season) returns exactly the view's rows, read-only. This is the
   slow gate (~1-2 min), so it runs outside the swap transaction, whose DROP
   holds an exclusive lock that would block the app's Lineups reads.
3. The table holds the view's rows (EXCEPT both ways, every column).
4. A one-season refresh (the latest EuroLeague and EuroCup game) leaves the
   table unchanged, and is timed: the per-load cost this migration buys.
5. app_readonly can SELECT it (grant, row-level security and policy), and
   refresh_app_materialized_views() no longer names it.
"""

from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
    inspect_target,
)

DDL_PATH = REPO / "sql" / "063_sub_lineups_stats_per_season.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"
TABLE = "euroleague.sub_lineups_stats_mv"
ALLOWED_DROP = "DROP MATERIALIZED VIEW EUROLEAGUE.SUB_LINEUPS_STATS_MV"
MIN_ROWS = 50000
MAX_SEASON_SECONDS = 30.0


def executable(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines()
                     if not line.strip().startswith("--"))


def validate_ddl(ddl: str) -> list[str]:
    if "EUROLEAGUE SHADOW SCHEMA" not in ddl.upper():
        raise ValueError("missing EuroLeague safety marker")
    body = executable(ddl).upper()
    if re.search(r"\bBASKETBALL(?:_TEST)?\s*\.", body):
        raise ValueError("forbidden Israeli schema reference")
    if re.search(r"\b(?:TRUNCATE|CASCADE)\b", body):
        raise ValueError("TRUNCATE / CASCADE not allowed")
    kept = []
    for statement in _split_sql_statements(ddl):
        normalized = " ".join(executable(statement).upper().split()).rstrip(";")
        if normalized in {"BEGIN", "COMMIT", ""}:
            continue
        if re.search(r"\bDROP\b", normalized) and normalized != ALLOWED_DROP:
            raise ValueError("unexpected DROP: " + normalized[:120])
        kept.append(statement)
    if sum(1 for s in kept if " ".join(executable(s).upper().split()).rstrip(";") == ALLOWED_DROP) != 1:
        raise ValueError("expected exactly one DROP of the view being replaced")
    return kept


def one(cursor, sql: str, params: tuple = ()):
    cursor.execute(sql, params)
    return cursor.fetchone()


def differs(cursor, a: str, b: str) -> tuple[int, int]:
    (ab,) = one(cursor, f"SELECT count(*) FROM (SELECT * FROM {a} EXCEPT ALL SELECT * FROM {b}) x")
    (ba,) = one(cursor, f"SELECT count(*) FROM (SELECT * FROM {b} EXCEPT ALL SELECT * FROM {a}) x")
    return int(ab), int(ba)


def function_select(ddl: str) -> str:
    """The INSERT's SELECT inside refresh_sub_lineups_stats(), for every season."""
    m = re.search(r"INSERT INTO euroleague\.sub_lineups_stats_mv\n(.*?);\n\n  GET DIAGNOSTICS", ddl, re.S)
    if not m:
        raise ValueError("refresh_sub_lineups_stats() SELECT not found in the migration")
    return m.group(1).replace("p_game_ids", "NULL::bigint[]")


def temp_bytes(cursor) -> int:
    (n,) = one(cursor, "SELECT temp_bytes FROM pg_stat_database WHERE datname = current_database()")
    return int(n)


def main() -> int:
    apply = os.environ.get("CONFIRM_EL_063_APPLY") == "1"
    statements = validate_ddl(DDL_PATH.read_text(encoding="utf-8"))
    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError("EuroLeague schema unavailable: " + str(target))
        print("mode=" + ("EXECUTE" if apply else "DRY RUN (CONFIRM_EL_063_APPLY=1 to commit)"))

        # Gate 2, without the swap's lock: it only reads the view (a temp
        # table is not allowed in a READ ONLY transaction) and rolls back.
        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL statement_timeout='600s'")
        cursor.execute("SET LOCAL work_mem='64MB'")
        query = function_select(DDL_PATH.read_text(encoding="utf-8"))
        t0 = time.perf_counter()
        cursor.execute(f"CREATE TEMP TABLE slsm_function ON COMMIT DROP AS {query}")
        gap = differs(cursor, "slsm_function", TABLE)
        print(f"function SELECT, every season: {time.perf_counter() - t0:.1f}s; "
              f"differs from the view (function-view, view-function)={gap}")
        cursor.execute("ROLLBACK")
        if gap != (0, 0):
            raise RuntimeError("the function's query does not reproduce the view")

        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        cursor.execute("SET LOCAL statement_timeout='600s'")
        (kind,) = one(cursor, "SELECT relkind FROM pg_class WHERE oid = %s::regclass", (TABLE,))
        if kind != "m":
            raise RuntimeError(f"{TABLE} is relkind {kind!r}, expected the materialized view")
        cursor.execute(f"CREATE TEMP TABLE slsm_before ON COMMIT DROP AS SELECT * FROM {TABLE}")
        (before,) = one(cursor, "SELECT count(*) FROM slsm_before")
        print(f"view rows={before}")
        if before < MIN_ROWS:
            raise RuntimeError(f"row floor not met: {before} < {MIN_ROWS}")

        for statement in statements:
            cursor.execute(statement)

        (kind,) = one(cursor, "SELECT relkind FROM pg_class WHERE oid = %s::regclass", (TABLE,))
        gap = differs(cursor, TABLE, "slsm_before")
        print(f"table relkind={kind} differs from view (table-view, view-table)={gap}")
        if kind != "r" or gap != (0, 0):
            raise RuntimeError("swap gate failed")

        for comp in ("E", "U"):
            (gid,) = one(cursor, """
                SELECT game_id FROM euroleague.schedule WHERE competition = %s
                ORDER BY season DESC, scheduled_at DESC NULLS LAST, game_id DESC LIMIT 1""", (comp,))
            tb0 = temp_bytes(cursor)
            t0 = time.perf_counter()
            (n,) = one(cursor, "SELECT euroleague.refresh_sub_lineups_stats(ARRAY[%s]::bigint[])", (gid,))
            secs = time.perf_counter() - t0
            temp_mb = (temp_bytes(cursor) - tb0) / 1048576
            gap = differs(cursor, TABLE, "slsm_before")
            print(f"refresh {comp} season of game {gid}: {n} rows in {secs:.1f}s, "
                  f"~{temp_mb:.0f} MB temp (database-wide counter); differs={gap}")
            if gap != (0, 0):
                raise RuntimeError(f"one-season refresh changed rows ({comp})")
            if secs > MAX_SEASON_SECONDS:
                raise RuntimeError(f"one-season refresh took {secs:.1f}s")

        (granted,) = one(cursor, "SELECT has_table_privilege('app_readonly', %s, 'SELECT')", (TABLE,))
        (rls,) = one(cursor, "SELECT relrowsecurity FROM pg_class WHERE oid = %s::regclass", (TABLE,))
        (policy,) = one(cursor, """
            SELECT count(*) FROM pg_policies WHERE schemaname = 'euroleague'
            AND tablename = 'sub_lineups_stats_mv' AND policyname = 'app_readonly_select_all'
            AND 'app_readonly' = ANY (roles) AND cmd = 'SELECT'""")
        (named,) = one(cursor, """
            SELECT position('sub_lineups_stats_mv' IN prosrc) > 0 FROM pg_proc
            WHERE oid = 'euroleague.refresh_app_materialized_views()'::regprocedure""")
        print(f"app_readonly SELECT={granted} rls={rls} policy={policy} "
              f"refresh_app_materialized_views names it={named}")
        if not granted or not rls or policy != 1 or named:
            raise RuntimeError("access / refresh gate failed")

        if apply:
            cursor.execute("COMMIT")
            print("migration 063 applied")
        else:
            cursor.execute("ROLLBACK")
            print("DRY RUN passed and rolled back")
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
