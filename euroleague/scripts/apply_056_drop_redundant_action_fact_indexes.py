#!/usr/bin/env python
"""Apply EuroLeague migration 056 (drop two redundant covering indexes).

Dry run by default: prints the table's indexes and size. ``--apply`` runs each
DROP INDEX CONCURRENTLY on its own (no transaction, so app reads are never
blocked) and verifies that exactly the two indexes are gone and
euroleague_player_stats_actions_lineups_idx and _minutes_idx remain.
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

DDL_PATH = REPO / "sql" / "056_drop_redundant_action_fact_indexes.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"
DROPPED = {"euroleague_player_stats_actions_team_idx",
           "euroleague_player_stats_actions_filter_idx"}
KEPT = {"euroleague_player_stats_actions_lineups_idx",
        "euroleague_player_stats_actions_minutes_idx"}


def statements() -> list[str]:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    if "EUROLEAGUE SHADOW SCHEMA" not in ddl.upper():
        raise ValueError("EuroLeague migration safety marker is missing")
    out = []
    for statement in _split_sql_statements(ddl):
        sql = " ".join(re.sub(r"--[^\n]*", "", statement).split())
        match = re.fullmatch(r"DROP INDEX CONCURRENTLY IF EXISTS euroleague\.(\w+);?", sql)
        if not match or match.group(1) not in DROPPED:
            raise ValueError(f"unexpected statement: {sql[:100]!r}")
        out.append(sql.rstrip(";"))
    if len(out) != len(DROPPED):
        raise ValueError(f"expected {len(DROPPED)} statements, found {len(out)}")
    return out


def state(cursor) -> tuple[set[str], int]:
    cursor.execute(
        "SELECT indexrelname FROM pg_stat_user_indexes "
        "WHERE schemaname = 'euroleague' AND relname = 'player_stats_actions_by_game'"
    )
    names = {r[0] for r in cursor.fetchall()}
    cursor.execute("SELECT pg_total_relation_size('euroleague.player_stats_actions_by_game')")
    return names, cursor.fetchone()[0] // 1048576


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    sqls = statements()
    conn = connect_from_env_file(ENV_PATH)  # autocommit: CONCURRENTLY needs it
    cur = conn.cursor()
    names, size = state(cur)
    print(f"before: {size} MB, indexes: {sorted(names)}")
    if not args.apply:
        print("\nDRY RUN: re-run with --apply to drop:", *sorted(DROPPED), sep="\n  ")
        return 0

    cur.execute("SET lock_timeout = '10s'")
    for sql in sqls:
        cur.execute(sql)
        print("ok:", sql)
    names, size = state(cur)
    print(f"after: {size} MB, indexes: {sorted(names)}")
    failures = [f"still present: {n}" for n in DROPPED & names]
    failures += [f"missing: {n}" for n in KEPT - names]
    for failure in failures:
        print("FAIL", failure)
    print("056 VERIFIED" if not failures else "056 NOT VERIFIED")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
