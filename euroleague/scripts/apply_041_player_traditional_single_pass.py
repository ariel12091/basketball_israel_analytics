#!/usr/bin/env python
"""Apply EuroLeague migration 041 (single-pass player exposure/usage).

Fixed to migration 041. Unlike the additive migrations, this one MUST drop a
relation: REFRESH re-runs the stored definition, so only DROP+CREATE can change
it. The guard is therefore narrowed rather than absolute -- the ONLY DROP
permitted is the bare `player_traditional_stats_mv`, and CASCADE and Israeli
schema references are still refused outright.

The gate is output parity. The MV's full contents are snapshotted before the
drop and compared row-for-row afterwards; any difference fails the run. Timing
is reported for information only.

--dry-run applies and compares inside a transaction that is then rolled back,
so the live MV is left exactly as it was.
"""

from __future__ import annotations

import argparse
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

DDL_PATH = REPO / "sql" / "041_player_traditional_single_pass.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"

MV = "euroleague.player_traditional_stats_mv"
ALLOWED_DROP = "DROP MATERIALIZED VIEW EUROLEAGUE.PLAYER_TRADITIONAL_STATS_MV"
ORDER_BY = "competition, game_year, team_id, player_id"

EXPECTED_INDEXES = {
    "euroleague_player_traditional_stats_mv_pk",
    "euroleague_player_traditional_stats_mv_team_idx",
}


def strip_comments(sql: str) -> str:
    """Executable text only. The keyword guards below must judge what the
    database will run, not what the migration says about itself -- the header
    documents why no CASCADE is used, which a naive scan reads as a CASCADE."""
    return "\n".join(
        line for line in sql.splitlines() if not line.strip().startswith("--")
    )


def validate_ddl(ddl: str) -> list[str]:
    if "EUROLEAGUE SHADOW SCHEMA" not in ddl.upper():
        raise ValueError("EuroLeague migration safety marker is missing")

    executable = strip_comments(ddl).upper()
    if re.search(r"\bBASKETBALL(?:_TEST)?\s*\.", executable):
        raise ValueError("migration references a forbidden Israeli schema")
    if re.search(r"\bCASCADE\b", executable):
        raise ValueError("migration contains CASCADE")

    statements = _split_sql_statements(ddl)
    for statement in statements:
        body = "\n".join(
            line for line in statement.splitlines()
            if not line.strip().startswith("--")
        )
        normalized = " ".join(body.upper().split()).rstrip(";")
        if normalized.startswith("DROP ") and normalized != ALLOWED_DROP:
            raise ValueError(f"migration 041 may only drop {MV}: {normalized[:80]!r}")

    def is_txn_control(statement: str) -> bool:
        body = "\n".join(
            line for line in statement.splitlines()
            if not line.strip().startswith("--")
        )
        return " ".join(body.upper().split()).rstrip(";") in ("BEGIN", "COMMIT")

    return [statement for statement in statements if not is_txn_control(statement)]


def snapshot(cursor) -> list[tuple]:
    cursor.execute(f"SELECT * FROM {MV} ORDER BY {ORDER_BY}")
    return [tuple(row) for row in cursor.fetchall()]


def column_names(cursor) -> list[str]:
    cursor.execute(f"SELECT * FROM {MV} LIMIT 0")
    return [d[0] for d in cursor.description]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="apply + compare, then roll back; the live MV is untouched")
    args = ap.parse_args()

    ddl = DDL_PATH.read_text(encoding="utf-8")
    statements = validate_ddl(ddl)

    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError("EuroLeague schema is unavailable: " + str(target))
        print("target database={} user={} server={}:{}".format(
            target["database"], target["user"],
            target["server_address"], target["server_port"]))
        print("mode  : " + ("DRY RUN (rolled back)" if args.dry_run else "EXECUTE"))

        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL statement_timeout = 0")
        cursor.execute("SET LOCAL search_path TO euroleague, public")

        before_cols = column_names(cursor)
        before = snapshot(cursor)
        print("\nbefore: {} rows, {} columns".format(len(before), len(before_cols)))

        print("\napplying {}: {} reviewed statements".format(DDL_PATH.name, len(statements)))
        for index, statement in enumerate(statements, start=1):
            cursor.execute(statement)
            print("  [{}/{}] {}".format(index, len(statements),
                                        statement.strip().splitlines()[-1][:72]))

        started = time.perf_counter()
        cursor.execute(f"REFRESH MATERIALIZED VIEW {MV}")
        refresh_s = time.perf_counter() - started
        print("\nrefresh: {:.1f}s".format(refresh_s))

        after_cols = column_names(cursor)
        after = snapshot(cursor)
        print("after : {} rows, {} columns".format(len(after), len(after_cols)))

        failures = []
        if before_cols != after_cols:
            failures.append("column list changed:\n  before {}\n  after  {}".format(
                before_cols, after_cols))
        if len(before) != len(after):
            failures.append("row count {} -> {}".format(len(before), len(after)))
        else:
            differing = [i for i, (a, b) in enumerate(zip(before, after)) if a != b]
            if differing:
                failures.append("{} differing row(s)".format(len(differing)))
                for i in differing[:3]:
                    print("  first differences at row {}:".format(i))
                    for name, a, b in zip(before_cols, before[i], after[i]):
                        if a != b:
                            print("    {}: {!r} -> {!r}".format(name, a, b))

        cursor.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname='euroleague' "
            "AND tablename='player_traditional_stats_mv'")
        indexes = {r[0] for r in cursor.fetchall()}
        if indexes != EXPECTED_INDEXES:
            failures.append("indexes not restored: {}".format(indexes))

        cursor.execute(
            "SELECT pg_catalog.array_to_string(c.relacl, ' ') FROM pg_class c "
            "JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname='euroleague' AND c.relname='player_traditional_stats_mv'")
        acl = cursor.fetchone()[0] or ""
        if "app_readonly=r" not in acl:
            failures.append("app_readonly SELECT grant not restored: {!r}".format(acl))

        if failures:
            for f in failures:
                print("  FAIL " + f)
            cursor.execute("ROLLBACK")
            raise RuntimeError("{} gate(s) failed -- rolled back".format(len(failures)))

        print("\n  OK  identical rows and columns, indexes restored, grant restored")

        if args.dry_run:
            cursor.execute("ROLLBACK")
            print("\nDRY RUN: rolled back. The live MV is unchanged.")
            return 0

        cursor.execute("COMMIT")
        print("migration 041 applied")
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
