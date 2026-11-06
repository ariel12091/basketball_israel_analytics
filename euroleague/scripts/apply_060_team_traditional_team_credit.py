#!/usr/bin/env python
"""Apply migration 060a (partial index, CONCURRENTLY) and 060 (Team Ratings
Traditional readers that count team rebounds and team turnovers).

Reuses the 059 gates. Before COMMIT:
  * every 059 parity gate for the counts 060 does not touch (points, shots,
    free throws, assists, steals, blocks) against Player Stats;
  * full season, every competition: OREB, DREB and TOV == the players' box
    plus the team-credited play-by-play rows of the same games, and
    REB == OREB + DREB;
  * defense of T == offense of every team that played T, for every count;
  * pergame == direct with no window, both sides, every count (the box score
    plus team credit against the play-by-play alone);
  * pergame full season warm median under 0.5 s.
--dry-run builds the index (it is additive and needed to time the reader)
and rolls the functions back.
"""

from __future__ import annotations

import argparse
import statistics
import sys
from decimal import Decimal
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import apply_059_team_traditional_readers as m059  # noqa: E402

REPO = HERE.parent
INDEX_PATH = REPO / "sql" / "060a_team_credit_action_index.sql"
DDL_PATH = REPO / "sql" / "060_team_traditional_team_credit.sql"
CREDITED = ("oreb", "dreb", "tov")
UNTOUCHED = tuple(k for k in m059.BOX if k not in CREDITED)


def build_index() -> None:
    sql = m059.executable(INDEX_PATH.read_text(encoding="utf-8")).strip()
    upper = " ".join(sql.upper().split())
    if (not upper.startswith("CREATE INDEX CONCURRENTLY IF NOT EXISTS ")
            or " ON EUROLEAGUE.PLAYER_STATS_ACTIONS_BY_GAME " not in upper
            or sql.rstrip().rstrip(";").count(";")):
        raise ValueError("060a must be one CREATE INDEX CONCURRENTLY statement")
    connection = m059.connect_from_env_file(m059.ENV_PATH, direct_port=5432)
    try:
        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute("SET statement_timeout='15min'")
        cursor.execute(sql)
        cursor.execute(
            """SELECT i.indisvalid FROM pg_index i
               WHERE i.indexrelid = to_regclass(
                 'euroleague.euroleague_player_stats_actions_team_credit_idx')""")
        row = cursor.fetchone()
        if not row or not row[0]:
            raise RuntimeError("partial index missing or invalid")
        print("060a index present and valid")
    finally:
        connection.close()


def gate_team_credit(reader, cursor, competition, season, tag, failures) -> None:
    rows = m059.one(reader.rows("get_team_traditional_pergame")[0])
    cursor.execute(
        """WITH g AS (
             SELECT s.game_id, s.team_id FROM euroleague.final_schedule_mv s
             WHERE s.competition=%s AND s.game_year=%s
               AND EXISTS (SELECT 1 FROM euroleague.team_four_factors_by_game t
                           WHERE t.game_id=s.game_id AND t.team_id=s.team_id)),
           box AS (
             SELECT f.team_id, sum(f.oreb) oreb, sum(f.dreb) dreb, sum(f.tov) tov
             FROM euroleague.player_traditional_by_game f JOIN g USING (game_id, team_id)
             GROUP BY f.team_id),
           credit AS (
             SELECT a.team_id,
               count(*) FILTER (WHERE a.play_type='O' AND a.type_lineup='offense') oreb,
               count(*) FILTER (WHERE a.play_type='D' AND a.type_lineup='defense') dreb,
               coalesce(sum(a.turnovers) FILTER (WHERE a.type_lineup='offense'),0) tov
             FROM euroleague.player_stats_actions_by_game a JOIN g USING (game_id, team_id)
             WHERE a.action_player_id IS NULL GROUP BY a.team_id)
           SELECT b.team_id, b.oreb+coalesce(c.oreb,0), b.dreb+coalesce(c.dreb,0),
             b.tov+coalesce(c.tov,0), coalesce(c.oreb,0), coalesce(c.dreb,0), coalesce(c.tov,0)
           FROM box b LEFT JOIN credit c USING (team_id)""",
        (competition, season))
    totals = [0, 0, 0]
    for team, oreb, dreb, tov, *credit in cursor.fetchall():
        row = rows.get(team)
        m059.check(f"{tag} team credit", failures, row is not None, f"team {team} missing")
        if row is None:
            continue
        for k, v in zip(CREDITED, (oreb, dreb, tov)):
            m059.check(f"{tag} team credit", failures, Decimal(row[k]) == Decimal(v),
                       f"team {team} {k} {row[k]} != box+team {v}")
        m059.check(f"{tag} team credit", failures, row["reb"] == row["oreb"] + row["dreb"],
                   f"team {team} reb {row['reb']} != oreb+dreb")
        totals = [a + b for a, b in zip(totals, credit)]
    print(f"  {tag} OREB/DREB/TOV == box + team credit "
          f"(team rows added: {totals[0]} OREB, {totals[1]} DREB, {totals[2]} TOV)")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    statements = m059.validate_ddl(DDL_PATH.read_text(encoding="utf-8"))
    build_index()

    connection = m059.connect_from_env_file(m059.ENV_PATH, direct_port=5432)
    cursor = connection.cursor()
    try:
        print("mode=" + ("DRY RUN" if args.dry_run else "EXECUTE"))
        for sig in (m059.PERGAME_SIG, m059.DIRECT_SIG):
            cursor.execute("SELECT to_regprocedure(%s)", (sig,))
            if cursor.fetchone()[0] is None:
                raise RuntimeError("059 must be applied first: " + sig)
        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        cursor.execute("SET LOCAL statement_timeout='60s'")
        for statement in statements:
            cursor.execute(statement)

        failures: list[str] = []
        timings: dict = {}
        print("parity:")
        for competition, season in m059.SEASONS:
            reader = m059.Reader(cursor, competition, season)
            tag = f"{competition}{season}"
            m059.BOX = UNTOUCHED  # counts 060 leaves alone: still == Player Stats
            m059.gate_pergame(reader, tag, failures, timings,
                              m059.pbp_less(reader, competition, season))
            m059.gate_direct(reader, tag, failures, timings)
            m059.BOX = UNTOUCHED + CREDITED
            gate_team_credit(reader, cursor, competition, season, tag, failures)
            if (competition, season) == ("E", 2025):
                m059.gate_pergame_defense(reader, tag, failures)
            m059.gate_sources(reader, tag, failures)
        if failures:
            for f in failures[:40]:
                print("FAIL " + f)
            raise RuntimeError(f"{len(failures)} parity failures")

        reader = m059.Reader(cursor, "E", 2025)
        pergame = m059.warm(reader, "get_team_traditional_pergame", "")
        defense = m059.warm(reader, "get_team_traditional_pergame", "p_side => 'defense'")
        direct = m059.warm(reader, "get_team_traditional_direct", m059.CLUTCH_PRESETS[0][1])
        print(f"performance (E 2025, warm median of 3): pergame {pergame:.3f}s, "
              f"defense {defense:.3f}s, direct 5/300 {direct:.3f}s")
        if max(pergame, defense) > 0.5:
            raise RuntimeError("pergame exceeds 0.5 s warm gate")
        if direct > 5.0:
            raise RuntimeError("direct exceeds 5 s warm gate")

        if args.dry_run:
            cursor.execute("ROLLBACK")
            print("DRY RUN passed and rolled back (060a index kept)")
        else:
            cursor.execute("COMMIT")
            print("migration 060 applied")
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
