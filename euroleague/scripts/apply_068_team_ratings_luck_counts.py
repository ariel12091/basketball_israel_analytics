#!/usr/bin/env python
"""Apply and verify EuroLeague migration 068 (Team Ratings shooting-luck counts).

Dry run by default: the whole migration runs inside one transaction, every
gate runs, and the transaction rolls back. Set CONFIRM_EL_068_APPLY=1 to commit.

The migration adds four columns to team_four_factors_by_game (refilled by the
loader's own refresh), replaces its writer (same signature), replaces the four
Team Ratings Summary readers (return type grows, so DROP + CREATE + grants) and
rebuilds team_ppp_ratings_mv with eight appended counts. The only DROPs allowed
are of those five objects; no TRUNCATE, no CASCADE, no Israeli schema.

Gates, all before COMMIT:
1. team_four_factors_by_game: same rows, every pre-existing column unchanged.
2. Every reader, every competition-season, with and without the standard clutch
   window where it takes one: every pre-existing column unchanged.
3. team_ppp_ratings_mv: every pre-existing column unchanged.
4. Grants on the readers and the view unchanged.
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

DDL_PATH = REPO / "sql" / "068_team_ratings_luck_counts.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"
TABLE = "euroleague.team_four_factors_by_game"
VIEW = "euroleague.team_ppp_ratings_mv"
CLUTCH = "p_max_margin=>5,p_margin_status=>'all',p_max_time_remaining=>300"
# (snapshot name, reader, extra args)
READERS = (
    ("r_pergame", "get_team_ratings_pergame", ""),
    ("r_dynamic", "get_team_ratings_dynamic", "," + CLUTCH),
    ("r_direct", "get_team_ratings_direct", ""),
    ("r_direct_clutch", "get_team_ratings_direct", "," + CLUTCH),
    ("r_dashboard", "get_team_dashboard_dynamic", "," + CLUTCH),
)
ALLOWED_DROPS = {
    "DROP FUNCTION EUROLEAGUE.GET_TEAM_RATINGS_PERGAME",
    "DROP FUNCTION EUROLEAGUE.GET_TEAM_RATINGS_DYNAMIC",
    "DROP FUNCTION EUROLEAGUE.GET_TEAM_RATINGS_DIRECT",
    "DROP FUNCTION EUROLEAGUE.GET_TEAM_DASHBOARD_DYNAMIC",
    "DROP MATERIALIZED VIEW EUROLEAGUE.TEAM_PPP_RATINGS_MV",
}
MAX_REFRESH_SECONDS = 300.0


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
    for drop in re.findall(r"\bDROP\s+(?:MATERIALIZED\s+VIEW|FUNCTION|TABLE|INDEX|VIEW)\s+[A-Z0-9_.]+", body):
        if " ".join(drop.split()) not in ALLOWED_DROPS:
            raise ValueError("DROP not allowed: " + drop)
    return [s for s in _split_sql_statements(ddl)
            if " ".join(executable(s).split()).rstrip(";").upper() not in {"BEGIN", "COMMIT", ""}]


def one(cursor, sql: str, params: tuple = ()):
    cursor.execute(sql, params)
    return cursor.fetchone()


def grants(cursor) -> tuple:
    cursor.execute("SELECT proname, proacl::text FROM pg_proc WHERE pronamespace = 'euroleague'::regnamespace "
                   "AND proname IN ('get_team_ratings_pergame','get_team_ratings_dynamic',"
                   "'get_team_ratings_direct','get_team_dashboard_dynamic') ORDER BY 1")
    procs = tuple(cursor.fetchall())
    (view,) = one(cursor, f"SELECT relacl::text FROM pg_class WHERE oid = '{VIEW}'::regclass")
    return procs, view


def snapshot(cursor, name: str, reader: str, extra: str) -> None:
    cursor.execute(f"""
        CREATE TEMP TABLE {name} ON COMMIT DROP AS
        SELECT s.competition, s.season, r.*
        FROM (SELECT DISTINCT competition, season FROM euroleague.schedule) s
        CROSS JOIN LATERAL euroleague.{reader}(s.competition, s.season{extra}) r""")


def columns(cursor, table: str) -> list[str]:
    cursor.execute("SELECT attname FROM pg_attribute WHERE attrelid = %s::regclass "
                   "AND attnum > 0 AND NOT attisdropped ORDER BY attnum", (table,))
    return [c for (c,) in cursor.fetchall()]


def differing(cursor, before: str, after: str, cols: list[str]) -> tuple[int, int, int, int]:
    cl = ", ".join(f'"{c}"' for c in cols)
    return one(cursor, f"""
        SELECT (SELECT count(*) FROM {before}), (SELECT count(*) FROM {after}),
               (SELECT count(*) FROM (SELECT {cl} FROM {before} EXCEPT ALL SELECT {cl} FROM {after}) x),
               (SELECT count(*) FROM (SELECT {cl} FROM {after} EXCEPT ALL SELECT {cl} FROM {before}) x)""")


def gate(label: str, result: tuple[int, int, int, int]) -> bool:
    n_before, n_after, only_before, only_after = result
    ok = n_before == n_after and only_before == 0 and only_after == 0
    print(f"{label}: rows {n_before} -> {n_after}, only before {only_before}, only after {only_after}"
          f" {'ok' if ok else 'FAILED'}")
    return ok


def main() -> int:
    apply = os.environ.get("CONFIRM_EL_068_APPLY") == "1"
    statements = validate_ddl(DDL_PATH.read_text(encoding="utf-8"))
    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError("EuroLeague schema unavailable: " + str(target))
        print("mode=" + ("EXECUTE" if apply else "DRY RUN (CONFIRM_EL_068_APPLY=1 to commit)"))
        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        cursor.execute("SET LOCAL statement_timeout='600s'")
        cursor.execute("SET LOCAL work_mem='256MB'")
        grants_before = grants(cursor)
        table_cols = columns(cursor, TABLE)
        view_cols = columns(cursor, VIEW)
        cursor.execute(f"CREATE TEMP TABLE t_before ON COMMIT DROP AS SELECT * FROM {TABLE}")
        cursor.execute(f"CREATE TEMP TABLE v_before ON COMMIT DROP AS SELECT * FROM {VIEW}")
        reader_cols = {}
        for name, reader, extra in READERS:
            snapshot(cursor, name + "_before", reader, extra)
            reader_cols[name] = columns(cursor, name + "_before")
        t0 = time.perf_counter()
        for statement in statements:
            cursor.execute(statement)
        secs = time.perf_counter() - t0
        print(f"migration statements (incl. full refresh of {TABLE}): {secs:.1f}s")
        if secs > MAX_REFRESH_SECONDS:
            raise RuntimeError(f"migration took {secs:.1f}s")
        ok = gate("gate 1 " + TABLE, differing(cursor, "t_before", TABLE, table_cols))
        for name, reader, extra in READERS:
            snapshot(cursor, name + "_after", reader, extra)
            ok &= gate(f"gate 2 {reader}{' (clutch)' if extra else ''}",
                       differing(cursor, name + "_before", name + "_after", reader_cols[name]))
        ok &= gate("gate 3 " + VIEW, differing(cursor, "v_before", VIEW, view_cols))
        grants_after = grants(cursor)
        print(f"gate 4 grants unchanged={grants_after == grants_before}")
        if grants_after != grants_before:
            print(f"  before={grants_before}\n  after={grants_after}")
            ok = False
        (zero_rows,) = one(cursor, f"SELECT count(*) FROM {TABLE} WHERE off_fta > 0 AND off_ftm = 0 AND off_fg3a = 0")
        print(f"rows with FTA but no FTM and no 3PA (would mean an unfilled backfill): {zero_rows}")
        if not ok:
            raise RuntimeError("a gate failed")
        if apply:
            cursor.execute("COMMIT")
            print("migration 068 applied")
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
