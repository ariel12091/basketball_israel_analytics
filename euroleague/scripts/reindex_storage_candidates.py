#!/usr/bin/env python
"""Rebuild reviewed EuroLeague indexes concurrently and report space reclaimed.

This is a physical compaction only: each index keeps the same name, columns,
predicate, and access method.  PostgreSQL builds the replacement alongside the
live index, then swaps it atomically.  The script refuses to run during an
active EuroLeague load and verifies the definition after every rebuild.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    connect_from_env_file,
    inspect_target,
)

ENV_PATH = REPO.parent / "etl" / ".Renviron"
REVIEWED_INDEXES = (
    "euroleague_player_stats_actions_lineups_idx",
    "euroleague_player_stats_actions_filter_idx",
    "euroleague_player_stats_actions_minutes_idx",
    "player_stats_actions_by_game_pkey",
    "player_four_factors_by_game_pkey",
    "sub_lineups_pkey",
    "action_team_context_actions_pkey",
    "euroleague_actions_raw_period_order_idx",
    "actions_raw_pkey",
    "actions_pkey",
    "lineup_totals_by_game_pkey",
    "euroleague_sub_lineups_unit_idx",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument(
        "--index",
        action="append",
        choices=REVIEWED_INDEXES,
        dest="indexes",
        help="Reviewed index to rebuild; repeat for multiple. Defaults to all.",
    )
    return parser.parse_args()


def index_state(cursor, name: str) -> tuple[int, str, bool, bool]:
    cursor.execute(
        """
        SELECT pg_relation_size(i.oid), pg_get_indexdef(i.oid),
               x.indisvalid, x.indisready
        FROM pg_class i
        JOIN pg_namespace n ON n.oid=i.relnamespace
        JOIN pg_index x ON x.indexrelid=i.oid
        WHERE n.nspname='euroleague' AND i.relname=%s
        """,
        (name,),
    )
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"reviewed index is missing: {name}")
    return int(row[0]), str(row[1]), bool(row[2]), bool(row[3])


def assert_no_active_load(cursor) -> None:
    cursor.execute(
        """
        SELECT load_run_id, status, started_at
        FROM euroleague.load_runs
        WHERE status = 'running'
          AND started_at >= now() - interval '24 hours'
        ORDER BY load_run_id
        """
    )
    active = cursor.fetchall()
    if active:
        raise RuntimeError(f"EuroLeague publication/load is active: {active}")
    cursor.execute(
        """
        SELECT DISTINCT l.pid, a.usename, a.application_name
        FROM pg_locks l
        JOIN pg_class c ON c.oid=l.relation
        JOIN pg_namespace n ON n.oid=c.relnamespace
        JOIN pg_stat_activity a ON a.pid=l.pid
        WHERE n.nspname='euroleague'
          AND l.pid <> pg_backend_pid()
          AND l.granted
          AND l.mode IN (
            'RowExclusiveLock', 'ShareRowExclusiveLock',
            'ExclusiveLock', 'AccessExclusiveLock'
          )
        """
    )
    writers = cursor.fetchall()
    if writers:
        raise RuntimeError(f"EuroLeague writer/DDL session is active: {writers}")


def main() -> int:
    options = parse_args()
    selected = tuple(options.indexes or REVIEWED_INDEXES)
    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError(f"EuroLeague schema is unavailable: {target}")
        assert_no_active_load(cursor)

        before: dict[str, tuple[int, str]] = {}
        for name in selected:
            size, definition, valid, ready = index_state(cursor, name)
            if not valid or not ready:
                raise RuntimeError(f"index is not valid and ready: {name}")
            before[name] = (size, definition)
            print(f"{name:<55} {size / 1048576:8.1f} MiB")

        if not options.apply:
            print("DRY RUN; no indexes rebuilt")
            return 0

        cursor.execute("SET lock_timeout = '3s'")
        cursor.execute("SET statement_timeout = '15min'")
        total_before = 0
        total_after = 0
        for name in selected:
            old_size, old_definition = before[name]
            print(f"reindexing {name} concurrently ...", flush=True)
            cursor.execute(f"REINDEX INDEX CONCURRENTLY euroleague.{name}")
            new_size, new_definition, valid, ready = index_state(cursor, name)
            if new_definition != old_definition or not valid or not ready:
                raise RuntimeError(f"post-reindex definition/state changed: {name}")
            total_before += old_size
            total_after += new_size
            print(
                f"  {old_size / 1048576:.1f} -> {new_size / 1048576:.1f} MiB "
                f"(saved {(old_size - new_size) / 1048576:.1f} MiB)"
            )
        print(
            f"total {total_before / 1048576:.1f} -> {total_after / 1048576:.1f} MiB; "
            f"saved {(total_before - total_after) / 1048576:.1f} MiB"
        )
        return 0
    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
