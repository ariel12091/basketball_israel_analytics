#!/usr/bin/env python
"""Apply migration 043 behind exact legacy parity and a 500 ms warm gate."""

from __future__ import annotations

import argparse
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

DDL_PATH = REPO / "sql" / "043_team_minutes_pergame.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"
OLD = "euroleague.get_team_minutes_direct"
NEW = "euroleague.get_team_minutes_pergame"
PRESETS = (
    ("full season", ""),
    ("last 10", "p_last_n_games => 10"),
    ("home", "p_home_away => 'home'"),
    ("wins", "p_outcome => 'win'"),
    ("rounds 5-15", "p_min_gn => 5, p_max_gn => 15"),
    ("own starters >=4", "p_num_starters_off_min => 4"),
    ("opponent starters >=4", "p_num_starters_def_min => 4"),
    ("both starter bounds", "p_num_starters_off_min => 3, p_num_starters_def_max => 4"),
)


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


def run(cursor, function: str, extra: str) -> tuple[list[tuple], float]:
    args = "p_competition => 'E', p_game_year => 2025"
    if extra:
        args += ", " + extra
    started = time.perf_counter()
    cursor.execute(f"SELECT * FROM {function}({args}) ORDER BY team_id")
    return [tuple(row) for row in cursor.fetchall()], time.perf_counter() - started


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    statements = validate_ddl(DDL_PATH.read_text(encoding="utf-8"))

    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError("EuroLeague schema unavailable: " + str(target))
        print("mode=" + ("DRY RUN" if args.dry_run else "EXECUTE"))
        cursor.execute(
            "SELECT to_regprocedure(%s)",
            ("euroleague.get_team_minutes_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer)",),
        )
        if cursor.fetchone()[0] is not None:
            raise RuntimeError("migration 043 function already exists")

        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        cursor.execute("SET LOCAL statement_timeout='30s'")
        for statement in statements:
            cursor.execute(statement)

        print("parity:")
        for label, preset in PRESETS:
            old_rows, old_s = run(cursor, OLD, preset)
            new_rows, new_s = run(cursor, NEW, preset)
            if old_rows != new_rows:
                print(f"FAIL {label}: old={old_rows[:2]} new={new_rows[:2]}")
                raise RuntimeError(label + " parity failed")
            print(f"  OK {label:<24} rows={len(new_rows):<3} {old_s:.3f}s -> {new_s:.3f}s")

        print("performance:")
        for label, preset in PRESETS[:2]:
            _, cold = run(cursor, NEW, preset)
            warm = [run(cursor, NEW, preset)[1] for _ in range(3)]
            median = statistics.median(warm)
            print(f"  {label:<12} cold={cold:.3f}s warm_median={median:.3f}s")
            if median > 0.500:
                raise RuntimeError(f"{label} exceeds 500 ms warm gate")

        cursor.execute(
            "SELECT has_function_privilege('app_readonly', %s, 'EXECUTE')",
            ("euroleague.get_team_minutes_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer)",),
        )
        if not cursor.fetchone()[0]:
            raise RuntimeError("app_readonly lacks EXECUTE")

        if args.dry_run:
            cursor.execute("ROLLBACK")
            print("DRY RUN passed and rolled back")
        else:
            cursor.execute("COMMIT")
            print("migration 043 applied")
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
