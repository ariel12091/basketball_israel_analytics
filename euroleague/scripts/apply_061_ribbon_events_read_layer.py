#!/usr/bin/env python
"""Apply and verify EuroLeague migration 061 (euroleague.ribbon_events_v).

Dry run by default: the view is created inside a transaction, every gate runs,
and the transaction rolls back. Set CONFIRM_EL_061_APPLY=1 to commit.

Additive only -- one new view and its grant -- so this refuses DROP, TRUNCATE,
CASCADE and any Israeli-schema reference, like the 053/054/059 applicators.

Gates, all before COMMIT:
1. Row floor, so nothing below passes on an empty view.
2. Zero rows with NULL player_ids: every credited lineup matched
   lineup_totals_by_game (Review Focus 3 of the Gameflow runs plan).
3. Each perspective of each event once: (game_id, team_id, source_event_order)
   is unique.
4. Timing: one game's read, a 2025 game and a 2026 game, under 1 s each with
   EXPLAIN ANALYZE (memory: time EuroLeague SQL on a full season).
5. app_readonly can SELECT the view and still cannot read
   action_team_context_actions.
"""

from __future__ import annotations

import os
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

DDL_PATH = REPO / "sql" / "061_ribbon_events_read_layer.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"
VIEW = "euroleague.ribbon_events_v"
DENIED = "euroleague.action_team_context_actions"
MIN_ROWS = 100000
MAX_SECONDS = 1.0


def executable(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines()
                     if not line.strip().startswith("--"))


def validate_ddl(ddl: str) -> list[str]:
    if "EUROLEAGUE SHADOW SCHEMA" not in ddl.upper():
        raise ValueError("missing EuroLeague safety marker")
    body = executable(ddl).upper()
    if re.search(r"\bBASKETBALL(?:_TEST)?\s*\.", body):
        raise ValueError("forbidden Israeli schema reference")
    if re.search(r"\b(?:DROP|TRUNCATE|CASCADE)\b", body):
        raise ValueError("migration must be additive")
    kept = []
    for statement in _split_sql_statements(ddl):
        normalized = " ".join(executable(statement).upper().split()).rstrip(";")
        if normalized not in {"BEGIN", "COMMIT"}:
            kept.append(statement)
    return kept


def one(cursor, sql: str, params: tuple = ()):
    cursor.execute(sql, params)
    return cursor.fetchone()


def main() -> int:
    apply = os.environ.get("CONFIRM_EL_061_APPLY") == "1"
    statements = validate_ddl(DDL_PATH.read_text(encoding="utf-8"))
    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError("EuroLeague schema unavailable: " + str(target))
        print("mode=" + ("EXECUTE" if apply else "DRY RUN (CONFIRM_EL_061_APPLY=1 to commit)"))

        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        cursor.execute("SET LOCAL statement_timeout='120s'")
        for statement in statements:
            cursor.execute(statement)

        total, null_players = one(cursor,
            f"SELECT count(*), count(*) FILTER (WHERE player_ids IS NULL) FROM {VIEW}")
        print(f"rows={total} null_player_ids={null_players}")
        if total < MIN_ROWS:
            raise RuntimeError(f"row floor not met: {total} < {MIN_ROWS}")
        if null_players:
            raise RuntimeError(f"{null_players} rows have no lineup_totals_by_game match")

        (dupes,) = one(cursor, f"""
            SELECT count(*) FROM (
              SELECT 1 FROM {VIEW}
              GROUP BY game_id, team_id, source_event_order HAVING count(*) > 1) d""")
        print(f"duplicate (game, team, event) rows={dupes}")
        if dupes:
            raise RuntimeError("an event perspective appears more than once")

        for season in (2025, 2026):
            row = one(cursor, """
                SELECT game_id FROM euroleague.final_schedule_mv
                WHERE game_year = %s ORDER BY game_date DESC, game_id DESC LIMIT 1""",
                (season,))
            if row is None:
                print(f"timing: no {season} game, skipped")
                continue
            gid = int(row[0])
            cursor.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) "
                           f"SELECT * FROM {VIEW} WHERE game_id = %s", (gid,))
            plan = "\n".join(r[0] for r in cursor.fetchall())
            ms = float(re.search(r"Execution Time: ([0-9.]+) ms", plan).group(1))
            nested_cte = bool(re.search(r"Nested Loop(.|\n)*CTE Scan", plan))
            print(f"timing season {season} game {gid}: {ms:.1f} ms "
                  f"nested-loop-over-CTE={nested_cte}")
            if ms > MAX_SECONDS * 1000 or nested_cte:
                print(plan)
                raise RuntimeError(f"timing gate failed for game {gid}")

        (granted,) = one(cursor, "SELECT has_table_privilege('app_readonly', %s, 'SELECT')", (VIEW,))
        (leaked,) = one(cursor, "SELECT has_table_privilege('app_readonly', %s, 'SELECT')", (DENIED,))
        print(f"app_readonly SELECT view={granted} raw={leaked}")
        if not granted or leaked:
            raise RuntimeError("grant gate failed")

        if apply:
            cursor.execute("COMMIT")
            print("migration 061 applied")
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
