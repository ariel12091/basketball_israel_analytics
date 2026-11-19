#!/usr/bin/env python
"""Apply EuroLeague migration 067 (player height, position, headshot per season).

Dry run by default: the migration runs, every loaded competition-season is
filled from the provider's roster (euroleague_possessions.roster_collector),
every gate runs, and the transaction rolls back. Set CONFIRM_EL_067_APPLY=1 to
commit. Safe to re-run: the table is CREATE IF NOT EXISTS (the policy is
skipped when the table already exists) and the fill is an upsert.

The migration only creates one table; it refuses any DROP, TRUNCATE, CASCADE
or Israeli-schema reference.

Gates, all before COMMIT:
1. The table exists, app_readonly can SELECT it, RLS is on.
2. Each loaded competition-season: at least 90% of the players who appear in
   its loaded games (full_rosters) have a height, a position and a headshot.
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
from euroleague_possessions import roster_collector  # noqa: E402

DDL_PATH = REPO / "sql" / "067_player_season_profiles.sql"
ENV_PATH = Path(os.environ.get("EL_ENV_PATH", REPO.parent / "etl" / ".Renviron"))
MIN_COVERAGE = 0.90

COVERAGE_SQL = """
WITH played AS (
  SELECT DISTINCT s.competition, s.season, fr.team_id, fr.player_id
  FROM euroleague.full_rosters fr
  JOIN euroleague.schedule s USING (game_id)
  WHERE s.competition = %s AND s.season = %s
)
SELECT count(*),
       count(pr.height_m), count(pr.position_en), count(pr.headshot_url)
FROM played pl
LEFT JOIN euroleague.player_season_profiles pr
  ON pr.season = pl.season AND pr.team_id = pl.team_id AND pr.player_id = pl.player_id
"""


def executable(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))


def validate_ddl(ddl: str) -> list[str]:
    if "EUROLEAGUE SHADOW SCHEMA" not in ddl.upper():
        raise ValueError("missing EuroLeague safety marker")
    body = executable(ddl).upper()
    if re.search(r"\bBASKETBALL(?:_TEST)?\s*\.", body):
        raise ValueError("forbidden Israeli schema reference")
    if re.search(r"\b(?:DROP|TRUNCATE|CASCADE)\b", body):
        raise ValueError("DROP / TRUNCATE / CASCADE not allowed")
    return [s for s in _split_sql_statements(ddl)
            if " ".join(executable(s).split()).rstrip(";").upper() not in {"BEGIN", "COMMIT", ""}]


def coverage(cursor, competition: str, season: int) -> tuple[int, int, int, int]:
    cursor.execute(COVERAGE_SQL, (competition, season))
    return tuple(cursor.fetchone())


def main() -> int:
    apply = os.environ.get("CONFIRM_EL_067_APPLY") == "1"
    statements = validate_ddl(DDL_PATH.read_text(encoding="utf-8"))
    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError("EuroLeague schema unavailable: " + str(target))
        print("mode=" + ("EXECUTE" if apply else "DRY RUN (CONFIRM_EL_067_APPLY=1 to commit)"))
        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        cursor.execute("SELECT to_regclass('euroleague.player_season_profiles') IS NOT NULL")
        existed = cursor.fetchone()[0]
        for statement in statements:
            if existed and "CREATE POLICY" in executable(statement).upper():
                continue
            cursor.execute(statement)

        # Gate 1: table, grant, RLS.
        cursor.execute("""
            SELECT c.relrowsecurity,
                   has_table_privilege('app_readonly', c.oid, 'SELECT')
            FROM pg_class c WHERE c.oid = 'euroleague.player_season_profiles'::regclass""")
        rls, readable = cursor.fetchone()
        if not (rls and readable):
            raise RuntimeError(f"access wrong: rls={rls} app_readonly_select={readable}")

        cursor.execute("SELECT DISTINCT competition, season FROM euroleague.schedule ORDER BY 1, 2")
        failures = []
        for competition, season in cursor.fetchall():
            stats = roster_collector.refresh(cursor, competition, int(season))
            n, h, p, img = coverage(cursor, competition, int(season))
            print(f"{competition}/{season}: roster={stats['roster']} written={stats['written']} "
                  f"not-yet-played={stats['skipped']} | played={n} height={h} position={p} headshot={img}")
            for label, k in (("height", h), ("position", p), ("headshot", img)):
                if n and k / n < MIN_COVERAGE:
                    failures.append(f"{competition}/{season} {label} {k}/{n}")
        if failures:
            raise RuntimeError("coverage under 90%: " + "; ".join(failures))

        if apply:
            cursor.execute("COMMIT")
            print("committed")
        else:
            cursor.execute("ROLLBACK")
            print("rolled back (dry run)")
        return 0
    except Exception:
        cursor.execute("ROLLBACK")
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
