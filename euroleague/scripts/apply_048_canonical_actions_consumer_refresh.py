#!/usr/bin/env python
"""Apply migration 048's literal canonical refresh-function definition.

The migration is intentionally behavior-neutral. It may commit only when the
live body has the reviewed hash and CREATE OR REPLACE preserves the body,
owner, security mode, settings, and ACL exactly. Default mode rolls back.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
)


DDL = ROOT / "sql" / "048_canonical_actions_consumer_refresh.sql"
ENV = ROOT.parent / "etl" / ".Renviron"
SIGNATURE = "euroleague.refresh_actions_consumer_candidates(bigint[])"
EXPECTED_BODY_MD5 = "18b7329c289960f0825f0035f98a6bd8"


def options():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    return parser.parse_args()


def metadata(cursor) -> tuple:
    cursor.execute(
        """
        SELECT md5(p.prosrc), pg_get_userbyid(p.proowner), p.prosecdef,
               p.proconfig, coalesce(p.proacl::text, '')
          FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
         WHERE n.nspname='euroleague'
           AND p.proname='refresh_actions_consumer_candidates'
           AND pg_get_function_identity_arguments(p.oid)='game_ids bigint[]'
        """
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"missing required function {SIGNATURE}")
    return row


def main() -> int:
    args = options()
    source = DDL.read_text(encoding="utf-8")
    upper = source.upper()
    for forbidden in ("PG_GET_FUNCTIONDEF", "SELECT P.PROSRC", "EXECUTE DEFINITION"):
        if forbidden in upper:
            raise RuntimeError(f"literal migration contains forbidden catalog patch token: {forbidden}")
    if source.count("CREATE OR REPLACE FUNCTION euroleague.refresh_actions_consumer_candidates") != 1:
        raise RuntimeError("migration must contain exactly one literal canonical function")

    statements = [
        statement
        for statement in _split_sql_statements(source)
        if statement.strip().upper() not in {"BEGIN", "COMMIT"}
    ]
    connection = connect_from_env_file(ENV, direct_port=5432)
    connection.autocommit = False
    cursor = connection.cursor()
    try:
        cursor.execute("SET LOCAL statement_timeout='120s'")
        before = metadata(cursor)
        if before[0] != EXPECTED_BODY_MD5:
            raise RuntimeError(
                f"live body drifted: expected {EXPECTED_BODY_MD5}, found {before[0]}"
            )
        if before[1:] != ("postgres", False, None, "{postgres=X/postgres}"):
            raise RuntimeError(f"unexpected live function metadata: {before[1:]!r}")

        for statement in statements:
            cursor.execute(statement)
        after = metadata(cursor)
        if after != before:
            raise RuntimeError(f"canonical replay changed function metadata: {before!r} -> {after!r}")

        if args.apply:
            connection.commit()
            cursor.execute("BEGIN READ ONLY")
            committed = metadata(cursor)
            connection.rollback()
            if committed != before:
                raise RuntimeError(f"committed catalog differs from gate: {committed!r}")
            print(f"COMMITTED migration 048; body md5={committed[0]}")
        else:
            connection.rollback()
            print(f"ROLLBACK gate passed; body md5={after[0]}; no persistent change")
        return 0
    except Exception:
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
