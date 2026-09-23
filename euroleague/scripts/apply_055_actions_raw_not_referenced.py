#!/usr/bin/env python
"""Apply and verify EuroLeague migration 055 (nothing references actions_raw).

Dry run by default: prints the current foreign keys into actions_raw and what
would change. ``--apply`` runs the three ALTERs in one transaction and then
verifies that no constraint references actions_raw, that qa_incidents now
references actions, and that no row count moved.

This migration must be applied BEFORE any actions_raw row is deleted: the old
actions -> actions_raw key was ON DELETE CASCADE and would take canonical
actions with it.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
)

DDL_PATH = REPO / "sql" / "055_actions_raw_not_referenced.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"
COUNTED = ("actions_raw", "actions", "qa_incidents")


def validate_ddl(ddl: str) -> list[str]:
    upper = ddl.upper()
    if "EUROLEAGUE SHADOW SCHEMA" not in upper:
        raise ValueError("EuroLeague migration safety marker is missing")
    if re.search(r"\bBASKETBALL(?:_TEST)?\s*\.", upper):
        raise ValueError("migration references a forbidden Israeli schema")
    if re.search(r"\bCASCADE\b", re.sub(r"--[^\n]*", "", upper)):
        raise ValueError("migration contains CASCADE")
    statements = _split_sql_statements(ddl)
    for statement in statements:
        normalized = " ".join(re.sub(r"--[^\n]*", "", statement).split()).upper()
        if not normalized.startswith("ALTER TABLE EUROLEAGUE."):
            raise ValueError(f"only ALTER TABLE euroleague.* is allowed: {statement[:80]!r}")
    if len(statements) != 3:
        raise ValueError(f"expected 3 statements, found {len(statements)}")
    return statements


def references(cursor) -> list[tuple[str, str, str]]:
    cursor.execute(
        "SELECT conname, conrelid::regclass::text, confrelid::regclass::text "
        "FROM pg_constraint WHERE contype = 'f' "
        "AND conname IN ('actions_game_id_source_event_order_fkey', "
        "                'qa_incidents_game_id_source_event_order_fkey') "
        "   OR (contype = 'f' AND confrelid = 'euroleague.actions_raw'::regclass) "
        "ORDER BY 1"
    )
    return [tuple(r) for r in cursor.fetchall()]


def counts(cursor) -> dict[str, int]:
    out = {}
    for table in COUNTED:
        cursor.execute(f"SELECT count(*) FROM euroleague.{table}")
        out[table] = cursor.fetchone()[0]
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    statements = validate_ddl(DDL_PATH.read_text(encoding="utf-8"))
    conn = connect_from_env_file(ENV_PATH)
    cur = conn.cursor()
    before_refs, before_counts = references(cur), counts(cur)
    print("before:", *before_refs, sep="\n  ")
    print("row counts:", before_counts)

    if not args.apply:
        print("\nDRY RUN: re-run with --apply to run the three ALTERs.")
        return 0

    conn.autocommit = False
    try:
        cur.execute("SET LOCAL lock_timeout = '5s'")
        cur.execute("SET LOCAL statement_timeout = '300s'")
        for statement in statements:
            cur.execute(statement)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    conn.autocommit = True

    after_refs, after_counts = references(cur), counts(cur)
    print("after:", *after_refs, sep="\n  ")
    failures = []
    if any(target == "euroleague.actions_raw" for _, _, target in after_refs):
        failures.append("a constraint still references actions_raw")
    if ("qa_incidents_game_id_source_event_order_fkey",
            "euroleague.qa_incidents", "euroleague.actions") not in after_refs:
        failures.append("qa_incidents does not reference actions")
    if after_counts != before_counts:
        failures.append(f"row counts moved: {before_counts} -> {after_counts}")
    for failure in failures:
        print("FAIL", failure)
    print("055 VERIFIED" if not failures else "055 NOT VERIFIED")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
