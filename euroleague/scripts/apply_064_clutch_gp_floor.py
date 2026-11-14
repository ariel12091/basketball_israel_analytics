#!/usr/bin/env python
"""Apply and verify EuroLeague migration 064 (clutch games played follow the floor, D40).

Dry run by default: the migration and a full refresh of
default_clutch_player_totals_by_game run inside one transaction, every gate
runs, and the transaction rolls back. Set CONFIRM_EL_064_APPLY=1 to commit.

The migration adds a column and replaces two functions (same signatures, so
grants are kept); it refuses any DROP, TRUNCATE, CASCADE or Israeli-schema
reference.

Gates, all before COMMIT:
1. Box-score columns: every row that existed keeps its values; a new row
   (a player on the floor with no action of his own) has only zeros.
2. Floor conservation: per clutch team-game, the players' floor_possessions
   add up to five per offensive possession end (reported; a lineup name that
   is not on the game roster would break it).
3. The standard-clutch player reader, every competition-season: every column
   but gp is unchanged for every player in both outputs; gp changes are
   listed and must be few (D40 measured 6 players in E 2025-26) and by one.
4. Grants on both functions are unchanged.
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

DDL_PATH = REPO / "sql" / "064_clutch_gp_floor.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"
TABLE = "euroleague.default_clutch_player_totals_by_game"
READER = "euroleague.get_player_traditional_standard_clutch"
REFRESH = "euroleague.refresh_default_clutch_player_for_games"
BOX = ["pts", "reb", "oreb", "dreb", "ast", "stl", "blk", "tov", "fg2m", "fg2a",
       "fg3m", "fg3a", "ftm", "fta", "player_ts_poss", "player_tov"]
MAX_GP_CHANGES = 40
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
    if re.search(r"\b(?:DROP|TRUNCATE|CASCADE)\b", body):
        raise ValueError("DROP / TRUNCATE / CASCADE not allowed")
    return [s for s in _split_sql_statements(ddl)
            if " ".join(executable(s).split()).rstrip(";").upper() not in {"BEGIN", "COMMIT", ""}]


def one(cursor, sql: str, params: tuple = ()):
    cursor.execute(sql, params)
    return cursor.fetchone()


def acl(cursor, name: str):
    (a,) = one(cursor, "SELECT proacl::text FROM pg_proc WHERE proname = %s "
                       "AND pronamespace = 'euroleague'::regnamespace", (name,))
    return a


def snapshot_reader(cursor, table: str) -> None:
    cursor.execute(f"""
        CREATE TEMP TABLE {table} ON COMMIT DROP AS
        SELECT s.competition, s.season, r.*
        FROM (SELECT DISTINCT competition, season FROM euroleague.schedule) s
        CROSS JOIN LATERAL {READER}(s.competition, s.season) r""")


def main() -> int:
    apply = os.environ.get("CONFIRM_EL_064_APPLY") == "1"
    statements = validate_ddl(DDL_PATH.read_text(encoding="utf-8"))
    connection = connect_from_env_file(ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        target = inspect_target(connection)
        if target["euroleague_schema"] != "euroleague":
            raise RuntimeError("EuroLeague schema unavailable: " + str(target))
        print("mode=" + ("EXECUTE" if apply else "DRY RUN (CONFIRM_EL_064_APPLY=1 to commit)"))

        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        cursor.execute("SET LOCAL statement_timeout='600s'")
        cursor.execute("SET LOCAL work_mem='64MB'")
        acl_before = (acl(cursor, "get_player_traditional_standard_clutch"),
                      acl(cursor, "refresh_default_clutch_player_for_games"))
        cursor.execute(f"CREATE TEMP TABLE ptb_before ON COMMIT DROP AS SELECT * FROM {TABLE}")
        snapshot_reader(cursor, "rdr_before")
        (n_before,) = one(cursor, "SELECT count(*) FROM ptb_before")
        print(f"player table rows before={n_before}")

        for statement in statements:
            cursor.execute(statement)
        t0 = time.perf_counter()
        (n,) = one(cursor, f"SELECT {REFRESH}(NULL)")
        secs = time.perf_counter() - t0
        print(f"full refresh: {n} rows in {secs:.1f}s")
        if secs > MAX_REFRESH_SECONDS:
            raise RuntimeError(f"full refresh took {secs:.1f}s")

        # Gate 1: box-score columns.
        cols = ", ".join(BOX)
        keys = "game_id, team_id, player_id"
        (changed,) = one(cursor, f"""
            SELECT count(*) FROM (
              SELECT {keys}, {cols} FROM ptb_before
              EXCEPT ALL
              SELECT {keys}, {cols} FROM {TABLE} t
              WHERE EXISTS (SELECT 1 FROM ptb_before b
                            WHERE (b.game_id, b.team_id, b.player_id) = (t.game_id, t.team_id, t.player_id))) x""")
        (lost,) = one(cursor, f"""
            SELECT count(*) FROM ptb_before b
            WHERE NOT EXISTS (SELECT 1 FROM {TABLE} t
                              WHERE (t.game_id, t.team_id, t.player_id) = (b.game_id, b.team_id, b.player_id))""")
        (new_rows, new_nonzero) = one(cursor, f"""
            SELECT count(*), count(*) FILTER (WHERE {' + '.join('abs(' + c + ')' for c in BOX)} > 0)
            FROM {TABLE} t
            WHERE NOT EXISTS (SELECT 1 FROM ptb_before b
                              WHERE (b.game_id, b.team_id, b.player_id) = (t.game_id, t.team_id, t.player_id))""")
        print(f"box columns: changed={changed} lost rows={lost} new rows={new_rows} "
              f"(with a non-zero box stat: {new_nonzero})")
        if changed or lost or new_nonzero:
            raise RuntimeError("box-score gate failed")

        # Gate 2: five players on the floor per possession end (reported).
        (team_games, off) = one(cursor, f"""
            WITH ends AS (
              SELECT atc.game_id, atc.team_id, sum(atc.possession_flag) AS ends
              FROM euroleague.action_team_context_actions atc
              WHERE atc.type_lineup = 'offense' AND atc.possession_flag > 0
                AND euroleague.clutch_event_qualifies(
                  atc.period, atc.event_elapsed_seconds,
                  atc.own_team_score - CASE WHEN atc.event_team_id = atc.team_id THEN atc.points ELSE 0 END,
                  atc.opp_team_score - CASE WHEN atc.event_team_id = atc.opponent_team_id THEN atc.points ELSE 0 END,
                  5, 'all', 300, false)
              GROUP BY 1, 2),
            fl AS (SELECT game_id, team_id, sum(floor_possessions) fp FROM {TABLE} GROUP BY 1, 2)
            SELECT count(*), count(*) FILTER (WHERE coalesce(fl.fp, 0) <> 5 * e.ends)
            FROM ends e LEFT JOIN fl USING (game_id, team_id)""")
        print(f"floor conservation: {off} of {team_games} clutch team-games not 5 per possession end")

        # Gate 3: the reader.
        snapshot_reader(cursor, "rdr_after")
        cursor.execute("SELECT column_name FROM information_schema.columns "
                       "WHERE table_name = 'rdr_before' AND column_name <> 'gp' "
                       "AND table_schema LIKE 'pg_temp%%' ORDER BY ordinal_position")
        other = ", ".join(f'"{c}"' for (c,) in cursor.fetchall())
        rkey = "competition, season, team_id, player_id"
        same = " AND ".join(f"a.{k} = b.{k}" for k in ("competition", "season", "team_id", "player_id"))
        (other_diff,) = one(cursor, f"""
            SELECT count(*) FROM (
              SELECT {other} FROM rdr_before b WHERE EXISTS (SELECT 1 FROM rdr_after a WHERE {same})
              EXCEPT ALL
              SELECT {other} FROM rdr_after a WHERE EXISTS (SELECT 1 FROM rdr_before b WHERE {same})) x""")
        cursor.execute(f"""
            SELECT competition, season, team_id, player_id,
                   coalesce(b."Player", a."Player"), b.gp, a.gp
            FROM rdr_before b FULL JOIN rdr_after a USING ({rkey})
            WHERE b.gp IS DISTINCT FROM a.gp
            ORDER BY 1, 2, 5""")
        gp_changes = cursor.fetchall()
        print(f"reader: other columns differing={other_diff}; gp changes={len(gp_changes)}")
        for comp, season, _t, _p, name, gb, ga in gp_changes:
            print(f"  {comp} {season} {name}: {gb} -> {ga}")
        big = [r for r in gp_changes if abs((r[6] or 0) - (r[5] or 0)) > 1]
        if other_diff or len(gp_changes) > MAX_GP_CHANGES or big:
            raise RuntimeError("reader gate failed")

        # Gate 4: grants.
        acl_after = (acl(cursor, "get_player_traditional_standard_clutch"),
                     acl(cursor, "refresh_default_clutch_player_for_games"))
        print(f"grants unchanged={acl_after == acl_before}")
        if acl_after != acl_before:
            raise RuntimeError("grants changed")

        if apply:
            cursor.execute("COMMIT")
            print("migration 064 applied")
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
