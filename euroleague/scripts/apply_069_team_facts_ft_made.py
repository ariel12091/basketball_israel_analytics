#!/usr/bin/env python
"""Apply and verify EuroLeague migration 069 (made free throws counted in the lineup facts).

Dry run by default: the whole migration runs inside one transaction, every
gate runs, and the transaction rolls back. Set CONFIRM_EL_069_APPLY=1 to commit.
Run VACUUM (ANALYZE) on both tables afterwards (outside a transaction).

The only DROPs allowed are of the three fact functions whose output grows
(filtered_/select_/clutch_team_game_facts); no TRUNCATE, no CASCADE, no
Israeli schema.

Gates, all before COMMIT:
1. lineup_totals_by_game: same rows, a hash of every pre-existing column
   unchanged; per team-game and side, ft_made adds up to the play-by-play's
   FTM count over the rows the table holds; ft_made never exceeds ft_attempts.
2. default_clutch_lineup_totals_by_game: every pre-existing column unchanged;
   ft_made never exceeds ft_attempts.
3. Every reader on filtered_team_game_facts, every competition-season: every
   pre-existing column unchanged -- including 068's off_ftm / def_ftm, so the
   counted FTM equals the formula it replaces.
4. Grants on every function the migration touches unchanged.
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

DDL_PATH = REPO / "sql" / "069_team_facts_ft_made.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"
LINEUP = "euroleague.lineup_totals_by_game"
CLUTCH_TABLE = "euroleague.default_clutch_lineup_totals_by_game"
CLUTCH = "p_max_margin=>5,p_margin_status=>'all',p_max_time_remaining=>300"
CUSTOM = "p_max_margin=>3,p_margin_status=>'trailing',p_max_time_remaining=>300"
READERS = (
    ("r_ratings", "get_team_ratings_dynamic", ""),
    ("r_ratings_clutch", "get_team_ratings_dynamic", "," + CLUTCH),
    ("r_ratings_custom", "get_team_ratings_dynamic", "," + CUSTOM),
    ("r_dashboard", "get_team_dashboard_dynamic", "," + CLUTCH),
    ("r_ff", "get_team_four_factors_dynamic", "," + CLUTCH),
    ("r_minutes", "get_team_minutes_dynamic", "," + CLUTCH),
    ("r_lineups", "fetch_lineups_dynamic", "," + CLUTCH + ",p_unit_size=>2"),
    ("r_players", "get_player_traditional_standard_clutch", ""),
)
TOUCHED = ("refresh_lineup_totals_by_game", "clutch_team_game_facts", "select_team_game_facts",
           "filtered_team_game_facts", "refresh_default_clutch_for_games",
           "get_team_ratings_dynamic", "get_team_dashboard_dynamic")
ALLOWED_DROPS = {
    "DROP FUNCTION EUROLEAGUE.FILTERED_TEAM_GAME_FACTS",
    "DROP FUNCTION EUROLEAGUE.SELECT_TEAM_GAME_FACTS",
    "DROP FUNCTION EUROLEAGUE.CLUTCH_TEAM_GAME_FACTS",
}
MAX_SECONDS = 400.0


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
                   "AND proname = ANY(%s) ORDER BY 1", (list(TOUCHED),))
    return tuple(cursor.fetchall())


def columns(cursor, table: str) -> list[str]:
    cursor.execute("SELECT attname FROM pg_attribute WHERE attrelid = %s::regclass "
                   "AND attnum > 0 AND NOT attisdropped ORDER BY attnum", (table,))
    return [c for (c,) in cursor.fetchall()]


def row_hash(cursor, table: str, cols: list[str]) -> tuple:
    cl = ", ".join(f'"{c}"' for c in cols)
    return one(cursor, f"SELECT count(*), sum(hashtextextended(ROW({cl})::text, 0)::numeric) FROM {table}")


def snapshot(cursor, name: str, reader: str, extra: str) -> None:
    cursor.execute(f"""
        CREATE TEMP TABLE {name} ON COMMIT DROP AS
        SELECT s.competition, s.season, r.*
        FROM (SELECT DISTINCT competition, season FROM euroleague.schedule) s
        CROSS JOIN LATERAL euroleague.{reader}(s.competition, s.season{extra}) r""")


def differing(cursor, before: str, after: str, cols: list[str]) -> tuple:
    cl = ", ".join(f'"{c}"' for c in cols)
    return one(cursor, f"""
        SELECT (SELECT count(*) FROM {before}), (SELECT count(*) FROM {after}),
               (SELECT count(*) FROM (SELECT {cl} FROM {before} EXCEPT ALL SELECT {cl} FROM {after}) x),
               (SELECT count(*) FROM (SELECT {cl} FROM {after} EXCEPT ALL SELECT {cl} FROM {before}) x)""")


def gate(label: str, result: tuple) -> bool:
    n_before, n_after, only_before, only_after = result
    ok = n_before == n_after and only_before == 0 and only_after == 0
    print(f"{label}: rows {n_before} -> {n_after}, only before {only_before}, only after {only_after}"
          f" {'ok' if ok else 'FAILED'}")
    return ok


def main() -> int:
    apply = os.environ.get("CONFIRM_EL_069_APPLY") == "1"
    statements = validate_ddl(DDL_PATH.read_text(encoding="utf-8"))
    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError("EuroLeague schema unavailable: " + str(target))
        print("mode=" + ("EXECUTE" if apply else "DRY RUN (CONFIRM_EL_069_APPLY=1 to commit)"))
        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        cursor.execute("SET LOCAL statement_timeout='600s'")
        cursor.execute("SET LOCAL work_mem='256MB'")
        grants_before = grants(cursor)
        lineup_cols = columns(cursor, LINEUP)
        lineup_before = row_hash(cursor, LINEUP, lineup_cols)
        # derived_at is the refill's own timestamp; every other column must hold.
        clutch_cols = [c for c in columns(cursor, CLUTCH_TABLE) if c != "derived_at"]
        cursor.execute(f"CREATE TEMP TABLE c_before ON COMMIT DROP AS SELECT * FROM {CLUTCH_TABLE}")
        reader_cols = {}
        for name, reader, extra in READERS:
            t0 = time.perf_counter()
            snapshot(cursor, name + "_before", reader, extra)
            reader_cols[name] = columns(cursor, name + "_before")
            print(f"snapshot {name}: {time.perf_counter() - t0:.1f}s")
        t0 = time.perf_counter()
        for statement in statements:
            cursor.execute(statement)
        secs = time.perf_counter() - t0
        print(f"migration statements (incl. backfills): {secs:.1f}s")
        if secs > MAX_SECONDS:
            raise RuntimeError(f"migration took {secs:.1f}s")
        lineup_after = row_hash(cursor, LINEUP, lineup_cols)
        ok = lineup_after == lineup_before
        print(f"gate 1 {LINEUP} pre-existing columns: rows/hash {lineup_before} -> {lineup_after}"
              f" {'ok' if ok else 'FAILED'}")
        (bad_sum, over) = one(cursor, f"""
            WITH pbp AS (
              SELECT game_id, team_id, type_lineup, sum((play_type = 'FTM')::int) n
              FROM euroleague.action_team_context_actions
              WHERE type_lineup IS NOT NULL
                AND (game_id, team_id) IN (SELECT DISTINCT game_id, team_id FROM {LINEUP})
              GROUP BY 1, 2, 3),
            tab AS (SELECT game_id, team_id, type_lineup, sum(ft_made) n FROM {LINEUP} GROUP BY 1, 2, 3)
            SELECT (SELECT count(*) FROM pbp FULL JOIN tab USING (game_id, team_id, type_lineup)
                     WHERE coalesce(pbp.n, 0) <> coalesce(tab.n, 0)),
                   (SELECT count(*) FROM {LINEUP} WHERE ft_made > ft_attempts)""")
        print(f"gate 1 ft_made: team-game sides not equal to the play-by-play FTM count {bad_sum}; "
              f"rows with ft_made > ft_attempts {over}")
        ok &= over == 0
        if bad_sum:
            print("  (reported: a team-game side whose FTM falls on a lineup the table does not hold"
                  " also loses its points there; compare with points)")
        ok &= gate(f"gate 2 {CLUTCH_TABLE}", differing(cursor, "c_before", CLUTCH_TABLE, clutch_cols))
        (over_c,) = one(cursor, f"SELECT count(*) FROM {CLUTCH_TABLE} WHERE ft_made > ft_attempts")
        print(f"gate 2 rows with ft_made > ft_attempts {over_c}")
        ok &= over_c == 0
        for name, reader, extra in READERS:
            snapshot(cursor, name + "_after", reader, extra)
            ok &= gate(f"gate 3 {name}", differing(cursor, name + "_before", name + "_after", reader_cols[name]))
        grants_after = grants(cursor)
        print(f"gate 4 grants unchanged={grants_after == grants_before}")
        if grants_after != grants_before:
            print(f"  before={grants_before}\n  after={grants_after}")
            ok = False
        if not ok:
            raise RuntimeError("a gate failed")
        if apply:
            cursor.execute("COMMIT")
            print("migration 069 applied")
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
