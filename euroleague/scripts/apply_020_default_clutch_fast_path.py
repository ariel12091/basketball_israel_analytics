#!/usr/bin/env python
"""Apply and verify migration 020 (incremental default-clutch fast path).

The script is fixed to one migration, direct PostgreSQL, and the isolated
EuroLeague schema. It verifies the cached additive rows bidirectionally against
the exact migration-019 calculator and prints cached-versus-dynamic timings.
"""

from __future__ import annotations

import re
import statistics
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

DDL_PATH = REPO / "sql" / "020_default_clutch_fast_path.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"
TABLE_NAME = "default_clutch_lineup_totals_by_game"

FACT_COLUMNS = (
    "game_id, team_id, own_lineup, own_starters, opp_starters, type_lineup, "
    "possessions, points, fg2_made, fg2_att, fg3_made, fg3_att, "
    "ts_possessions, fgm, fga, ft_attempts, orebounds, "
    "oreb_opportunities, turnovers, steals, seconds"
)


def validate_ddl(ddl: str) -> list[str]:
    upper = ddl.upper()
    if "EUROLEAGUE SHADOW SCHEMA" not in upper:
        raise ValueError("EuroLeague migration safety marker is missing")
    if re.search(r"\bBASKETBALL(?:_TEST)?\s*\.", upper):
        raise ValueError("migration references a forbidden Israeli schema")
    if re.search(r"\bDROP\s+(?:TABLE|SCHEMA|MATERIALIZED\s+VIEW)\b", upper):
        raise ValueError("migration contains a destructive relation DROP")
    return _split_sql_statements(ddl)


def relation_exists(connection: object) -> bool:
    cursor = connection.cursor()  # type: ignore[attr-defined]
    try:
        cursor.execute(
            "SELECT to_regclass('euroleague.default_clutch_lineup_totals_by_game') "
            "IS NOT NULL"
        )
        return bool(cursor.fetchone()[0])
    finally:
        cursor.close()


def verify_cache_parity(connection: object) -> tuple[int, int]:
    cursor = connection.cursor()  # type: ignore[attr-defined]
    try:
        cursor.execute(
            f"WITH expected AS ("
            f" SELECT {FACT_COLUMNS} FROM euroleague.clutch_team_game_facts("
            f"   ARRAY(SELECT game_id FROM euroleague.schedule ORDER BY game_id),"
            f"   5, 'all', 300, false"
            f" )), actual AS ("
            f" SELECT {FACT_COLUMNS} FROM euroleague.{TABLE_NAME}"
            f" ), missing AS (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual),"
            f" extra AS (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected)"
            f" SELECT (SELECT count(*) FROM missing), (SELECT count(*) FROM extra)"
        )
        missing, extra = cursor.fetchone()
        return int(missing), int(extra)
    finally:
        cursor.close()


def timed_count(connection: object, sql: str, repeats: int = 5) -> tuple[int, float]:
    cursor = connection.cursor()  # type: ignore[attr-defined]
    times: list[float] = []
    count = 0
    try:
        for _ in range(repeats):
            started = time.perf_counter()
            cursor.execute(sql)
            count = int(cursor.fetchone()[0])
            times.append(time.perf_counter() - started)
    finally:
        cursor.close()
    return count, statistics.median(times)


def main() -> int:
    ddl = DDL_PATH.read_text(encoding="utf-8")
    statements = validate_ddl(ddl)
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

        if relation_exists(connection):
            print(f"{TABLE_NAME} already exists; skipping apply")
        else:
            print(f"applying {DDL_PATH.name}: {len(statements)} reviewed statements")
            for index, statement in enumerate(statements, start=1):
                cursor.execute(statement)
                print(f"  [{index}/{len(statements)}] {statement.strip().splitlines()[-1][:72]}")

        missing, extra = verify_cache_parity(connection)
        print(f"cache parity: missing={missing}, extra={extra}")
        if missing or extra:
            raise RuntimeError("default-clutch cache differs from exact dynamic facts")

        game_ids = "ARRAY(SELECT game_id FROM euroleague.schedule ORDER BY game_id)"
        cached_sql = (
            "SELECT count(*) FROM euroleague.select_team_game_facts("
            f"{game_ids}, 5, 'all', 300, false)"
        )
        dynamic_sql = (
            "SELECT count(*) FROM euroleague.clutch_team_game_facts("
            f"{game_ids}, 5, 'all', 300, false)"
        )
        cached_count, cached_median = timed_count(connection, cached_sql)
        dynamic_count, dynamic_median = timed_count(connection, dynamic_sql)
        if cached_count != dynamic_count:
            raise RuntimeError(
                f"cached/dynamic row counts differ: {cached_count} != {dynamic_count}"
            )
        speedup = dynamic_median / cached_median if cached_median > 0 else float("inf")
        print(
            f"standard preset: rows={cached_count}, "
            f"cached_median={cached_median * 1000:.1f}ms, "
            f"dynamic_median={dynamic_median * 1000:.1f}ms, "
            f"speedup={speedup:.1f}x"
        )
        print("migration 020 verified")
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
