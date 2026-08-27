#!/usr/bin/env python
"""Read-only verification of the simplified EuroLeague actions schema."""

from __future__ import annotations

import sys
from pathlib import Path


REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402


OLD_TABLES = {
    "action_team_context",
    "matchup_segments",
    "pws",
    "stints",
    "action_lineups",
    "lineup_players",
    "lineups",
    "possessions",
    "actions_clean",
}
NEW_TABLES = {
    "actions",
    "action_team_context_actions",
    "matchup_segments_actions",
}


def main() -> int:
    connection = connect_from_env_file(REPO.parent / "etl" / ".Renviron")
    cursor = connection.cursor()
    failures: list[str] = []
    try:
        cursor.execute(
            "SELECT tablename FROM pg_catalog.pg_tables "
            "WHERE schemaname = 'euroleague' ORDER BY tablename"
        )
        tables = {str(row[0]) for row in cursor.fetchall()}
        remaining_old = sorted(OLD_TABLES & tables)
        missing_new = sorted(NEW_TABLES - tables)
        print(f"obsolete tables present: {remaining_old}")
        print(f"required actions tables missing: {missing_new}")
        if remaining_old:
            failures.append("obsolete tables remain")
        if missing_new:
            failures.append("required actions tables are missing")

        cursor.execute(
            "SELECT (SELECT count(*) FROM euroleague.actions_raw), "
            "       (SELECT count(*) FROM euroleague.actions), "
            "       (SELECT count(*) FROM euroleague.action_team_context_actions), "
            "       (SELECT count(*) FROM euroleague.matchup_segments_actions)"
        )
        raw_count, action_count, fact_count, segment_count = cursor.fetchone()
        print(
            f"rows: actions_raw={raw_count:,}, actions={action_count:,}, "
            f"event_fact={fact_count:,}, segments={segment_count:,}"
        )
        if raw_count != action_count:
            failures.append("raw and canonical action counts differ")

        cursor.execute(
            "SELECT count(*) FROM euroleague.actions_raw ar "
            "FULL JOIN euroleague.actions a "
            "  ON a.game_id = ar.game_id "
            " AND a.source_event_order = ar.source_event_order "
            "WHERE ar.game_id IS NULL OR a.game_id IS NULL"
        )
        key_mismatches = int(cursor.fetchone()[0])
        print(f"raw/canonical event-key mismatches: {key_mismatches}")
        if key_mismatches:
            failures.append("raw/canonical event keys differ")

        cursor.execute(
            "SELECT count(*) FROM euroleague.actions "
            "WHERE cardinality(lineup_a) <> 5 "
            "   OR cardinality(lineup_b) <> 5"
        )
        bad_lineups = int(cursor.fetchone()[0])
        print(f"actions with non-five-player package lineup: {bad_lineups}")
        if bad_lineups:
            failures.append("invalid package lineup sizes")

        cursor.execute(
            "SELECT count(*) FROM euroleague.actions a "
            "WHERE a.end_possession "
            "  AND (a.game_possession_number IS NULL "
            "       OR a.possession_offense_team_id IS NULL "
            "       OR a.team_possession_number IS NULL)"
        )
        bad_endpoints = int(cursor.fetchone()[0])
        print(f"incomplete possession endpoints: {bad_endpoints}")
        if bad_endpoints:
            failures.append("incomplete possession endpoint fields")

        cursor.execute(
            "SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))), "
            "       sum(pg_total_relation_size(c.oid)) "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'euroleague' "
            "  AND c.relkind IN ('r', 'm')"
        )
        pretty_size, size_bytes = cursor.fetchone()
        print(f"EuroLeague schema size: {pretty_size} ({size_bytes:,} bytes)")
    finally:
        cursor.close()
        connection.close()

    if failures:
        print("FAILED: " + "; ".join(failures))
        return 1
    print("PASS: simplified EuroLeague actions schema is live")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
