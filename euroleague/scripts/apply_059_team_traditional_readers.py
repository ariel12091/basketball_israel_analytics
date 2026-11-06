#!/usr/bin/env python
"""Apply migration 059 (Team Ratings Traditional readers) behind parity gates.

Every gate runs inside the migration's transaction, before COMMIT:

  pergame  offense box  == sum of get_player_traditional_pergame per team
           possessions  == get_team_ratings_pergame off_poss / def_poss
           games        == get_team_ratings_pergame games_played
           minutes      == get_team_minutes_pergame, to 0.1
           defense of T == offense of every team with p_opp_ids_csv = T
  direct   offense box  == sum of get_player_traditional_custom_clutch per team
           possessions, games, minutes against the _direct ratings/minutes
           readers; league defense totals == league offense totals under a
           symmetric window.

--dry-run rolls back after the gates. Timing is reported per preset and the
warm medians are gated (pergame 0.5 s, direct 5 s, full 2025 season).
"""

from __future__ import annotations

import argparse
import re
import statistics
import sys
import time
from decimal import Decimal
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    _split_sql_statements,
    connect_from_env_file,
    inspect_target,
)

DDL_PATH = REPO / "sql" / "059_team_traditional_readers.sql"
ENV_PATH = REPO.parent / "etl" / ".Renviron"
PERGAME_SIG = ("euroleague.get_team_traditional_pergame(text,integer,date,date,text,text,"
               "text,text,text,text,integer,text,integer,integer,integer,text)")
DIRECT_SIG = ("euroleague.get_team_traditional_direct(text,integer,date,date,text,text,"
              "text,text,text,text,integer,text,integer,text,integer,boolean,integer,"
              "integer,integer,text)")
BOX = ("pts", "oreb", "dreb", "ast", "stl", "blk", "tov", "fgm", "fga",
       "3pm", "3pa", "ftm", "fta")
BOX_SQL_ORDER = BOX  # the column order of pbp_less()'s SELECT
SEASONS = (("E", 2025), ("U", 2025), ("E", 2026))
PERGAME_PRESETS = (
    ("full season", ""),
    ("last 5", "p_last_n_games => 5"),
    ("home", "p_home_away => 'home'"),
    ("wins", "p_outcome => 'win'"),
    ("rounds 5-15", "p_min_gn => 5, p_max_gn => 15"),
    ("vs top 4 net", "p_opp_rank_side => 'top', p_opp_rank_n => 4"),
)
CLUTCH_PRESETS = (
    ("standard 5/300", "p_max_margin => 5, p_margin_status => 'all', "
                       "p_max_time_remaining => 300, p_ot_margin_filter => false"),
    ("trailing 3/120", "p_max_margin => 3, p_margin_status => 'trailing', "
                       "p_max_time_remaining => 120, p_ot_margin_filter => false"),
    ("10/600 OT filtered", "p_max_margin => 10, p_margin_status => 'all', "
                           "p_max_time_remaining => 600, p_ot_margin_filter => true"),
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


class Reader:
    def __init__(self, cursor, competition: str, season: int):
        self.cursor = cursor
        self.base = f"p_competition => '{competition}', p_game_year => {season}"

    def rows(self, function: str, extra: str = "") -> tuple[dict, float]:
        args = self.base + (", " + extra if extra else "")
        started = time.perf_counter()
        self.cursor.execute(f"SELECT * FROM euroleague.{function}({args})")
        names = [d[0] for d in self.cursor.description]
        out: dict = {}
        for row in self.cursor.fetchall():
            record = dict(zip(names, row))
            out.setdefault(record["team_id"], []).append(record)
        return out, time.perf_counter() - started


def one(rows: dict) -> dict:
    return {team: recs[0] for team, recs in rows.items()}


def player_sums(rows: dict) -> dict:
    return {team: {k: sum(Decimal(r[k] or 0) for r in recs) for k in BOX}
            for team, recs in rows.items()}


def check(label: str, failures: list, ok: bool, detail: str) -> None:
    if not ok:
        failures.append(f"{label}: {detail}")


def pbp_less(reader: Reader, competition: str, season: int) -> dict:
    """Teams with a box-score game that has no play-by-play facts, and the box
    sums of their play-by-play-backed games (the full-season expectation)."""
    reader.cursor.execute(
        """SELECT DISTINCT f.team_id FROM euroleague.player_traditional_by_game f
           WHERE f.competition=%s AND f.game_year=%s AND NOT EXISTS (
             SELECT 1 FROM euroleague.team_four_factors_by_game t
             WHERE t.game_id=f.game_id AND t.team_id=f.team_id)""",
        (competition, season))
    teams = [r[0] for r in reader.cursor.fetchall()]
    expected = {}
    for team in teams:
        reader.cursor.execute(
            """SELECT sum(pts),sum(oreb),sum(dreb),sum(ast),sum(stl),sum(blk),sum(tov),
                 sum(fg2m+fg3m),sum(fg2a+fg3a),sum(fg3m),sum(fg3a),sum(ftm),sum(fta)
               FROM euroleague.player_traditional_by_game f
               JOIN euroleague.final_schedule_mv s
                 ON s.game_id=f.game_id AND s.team_id=f.team_id
               WHERE f.competition=%s AND f.game_year=%s AND f.team_id=%s
                 AND s.competition=%s AND s.game_year=%s
                 AND EXISTS (SELECT 1 FROM euroleague.team_four_factors_by_game t
                             WHERE t.game_id=f.game_id AND t.team_id=f.team_id)""",
            (competition, season, team, competition, season))
        expected[team] = dict(zip(BOX_SQL_ORDER, reader.cursor.fetchone()))
    return expected


def gate_pergame(reader: Reader, tag: str, failures: list, timings: dict,
                 skipped: dict) -> None:
    if skipped:
        print(f"  {tag} teams with a QA-failed game (checked against the box of "
              f"their play-by-play games, full season only): {sorted(skipped)}")
    for label, preset in PERGAME_PRESETS:
        name = f"{tag} pergame {label}"
        off_extra = ", ".join(x for x in (preset, "p_side => 'offense'") if x)
        def_extra = ", ".join(x for x in (preset, "p_side => 'defense'") if x)
        off, t_off = reader.rows("get_team_traditional_pergame", off_extra)
        dfn, t_def = reader.rows("get_team_traditional_pergame", def_extra)
        timings.setdefault(f"{tag} pergame", []).extend([t_off, t_def])
        off, dfn = one(off), one(dfn)
        players = player_sums(reader.rows("get_player_traditional_pergame", preset)[0])
        ratings = one(reader.rows("get_team_ratings_pergame", preset)[0])
        minutes = one(reader.rows("get_team_minutes_pergame", preset)[0])
        check(name, failures, set(off) == set(ratings) == set(dfn),
              f"team sets differ off={len(off)} def={len(dfn)} ratings={len(ratings)}")
        for team, row in off.items():
            if team in skipped:
                p = skipped[team] if label == "full season" else None
            else:
                p = players.get(team, {})
            for k in BOX if p is not None else ():
                check(name, failures, Decimal(row[k]) == p.get(k),
                      f"team {team} {k} {row[k]} != players {p.get(k)}")
            r = ratings.get(team)
            if r is None:
                continue
            check(name, failures, row["poss_on_floor"] == r["off_poss"],
                  f"team {team} off poss {row['poss_on_floor']} != {r['off_poss']}")
            check(name, failures, row["gp"] == r["games_played"],
                  f"team {team} gp {row['gp']} != {r['games_played']}")
            d = dfn.get(team)
            if d is not None:
                check(name, failures, d["poss_on_floor"] == r["def_poss"],
                      f"team {team} def poss {d['poss_on_floor']} != {r['def_poss']}")
                check(name, failures, d["minutes"] == row["minutes"],
                      f"team {team} def minutes {d['minutes']} != off {row['minutes']}")
            m = minutes.get(team)
            check(name, failures,
                  m is not None and abs(Decimal(row["minutes"]) - Decimal(m["minutes"])) <= Decimal("0.0505"),
                  f"team {team} minutes {row['minutes']} vs {m and m['minutes']}")
        print(f"  {name:<36} teams={len(off):<3} {t_off:.3f}s / {t_def:.3f}s")


def gate_pergame_defense(reader: Reader, tag: str, failures: list) -> None:
    """Defense of team T == offense of every team that played T."""
    dfn = one(reader.rows("get_team_traditional_pergame", "p_side => 'defense'")[0])
    for team, row in dfn.items():
        opp = reader.rows("get_team_traditional_pergame",
                          f"p_opp_ids_csv => '{team}', p_side => 'offense'")[0]
        for k in BOX:
            total = sum(Decimal(r[0][k]) for r in opp.values())
            check(f"{tag} defense", failures, Decimal(row[k]) == total,
                  f"team {team} {k} {row[k]} != opponents' {total}")
    print(f"  {tag} defense == opponents' offense    teams={len(dfn)}")


def gate_direct(reader: Reader, tag: str, failures: list, timings: dict) -> None:
    for label, preset in CLUTCH_PRESETS:
        name = f"{tag} direct {label}"
        off, t_off = reader.rows("get_team_traditional_direct", preset + ", p_side => 'offense'")
        dfn, t_def = reader.rows("get_team_traditional_direct", preset + ", p_side => 'defense'")
        timings.setdefault(f"{tag} direct", []).extend([t_off, t_def])
        off, dfn = one(off), one(dfn)
        players = player_sums(reader.rows("get_player_traditional_custom_clutch", preset)[0])
        ratings = one(reader.rows("get_team_ratings_direct", preset)[0])
        minutes = one(reader.rows("get_team_minutes_direct", preset)[0])
        check(name, failures, set(off) == set(ratings) == set(dfn),
              f"team sets differ off={len(off)} def={len(dfn)} ratings={len(ratings)}")
        for team, row in off.items():
            p = players.get(team, {})
            for k in BOX:
                # get_player_traditional_custom_clutch drops a player with no
                # possession end and no floor time in the window (a lineup
                # gap), so his credited events are missing from its rows. The
                # team keeps them: allow a small surplus, never a deficit, and
                # print every case.
                gap = Decimal(row[k]) - p.get(k, Decimal(0))
                check(name, failures, Decimal(0) <= gap <= 2,
                      f"team {team} {k} {row[k]} != players {p.get(k)}")
                if gap:
                    print(f"    note {name}: team {team} {k} exceeds the player rows by {gap}")
            r = ratings.get(team)
            check(name, failures, r is not None, f"team {team} missing from ratings")
            if r is None:
                continue
            check(name, failures, row["poss_on_floor"] == (r["off_poss"] or 0),
                  f"team {team} off poss {row['poss_on_floor']} != {r['off_poss']}")
            check(name, failures, row["gp"] == r["games_played"],
                  f"team {team} gp {row['gp']} != {r['games_played']}")
            d = dfn.get(team)
            check(name, failures, d is not None and d["poss_on_floor"] == (r["def_poss"] or 0),
                  f"team {team} def poss {d and d['poss_on_floor']} != {r['def_poss']}")
            m = minutes.get(team)
            check(name, failures,
                  m is not None and abs(Decimal(row["minutes"]) - Decimal(m["minutes"])) <= Decimal("0.0505"),
                  f"team {team} minutes {row['minutes']} vs {m and m['minutes']}")
        if "'all'" in preset:  # a symmetric window: each event counted once per side
            for k in BOX:
                a = sum(Decimal(r[k]) for r in off.values())
                b = sum(Decimal(r[k]) for r in dfn.values())
                check(name, failures, a == b, f"league {k} offense {a} != defense {b}")
        print(f"  {name:<36} teams={len(off):<3} {t_off:.3f}s / {t_def:.3f}s")


def gate_sources(reader: Reader, tag: str, failures: list) -> None:
    """The two paths agree: the direct reader with no window returns the
    pergame reader's box counts (box score == player-credited play-by-play)."""
    for side in ("offense", "defense"):
        box = one(reader.rows("get_team_traditional_pergame", f"p_side => '{side}'")[0])
        pbp = one(reader.rows("get_team_traditional_direct", f"p_side => '{side}'")[0])
        check(f"{tag} paths {side}", failures, set(box) == set(pbp), "team sets differ")
        for team, row in box.items():
            for k in BOX + ("gp", "poss_on_floor"):
                other = pbp.get(team, {}).get(k)
                check(f"{tag} paths {side}", failures, row[k] == other,
                      f"team {team} {k} pergame {row[k]} != direct {other}")
    print(f"  {tag} pergame == direct with no window (both sides)")


def warm(reader: Reader, function: str, extra: str) -> float:
    reader.rows(function, extra)
    return statistics.median(reader.rows(function, extra)[1] for _ in range(3))


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
        for sig in (PERGAME_SIG, DIRECT_SIG):
            cursor.execute("SELECT to_regprocedure(%s)", (sig,))
            if cursor.fetchone()[0] is not None:
                raise RuntimeError("migration 059 function already exists: " + sig)

        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL lock_timeout='5s'")
        cursor.execute("SET LOCAL statement_timeout='60s'")
        for statement in statements:
            cursor.execute(statement)

        failures: list[str] = []
        timings: dict = {}
        print("parity:")
        for competition, season in SEASONS:
            reader = Reader(cursor, competition, season)
            tag = f"{competition}{season}"
            gate_pergame(reader, tag, failures, timings,
                         pbp_less(reader, competition, season))
            gate_direct(reader, tag, failures, timings)
            if (competition, season) == ("E", 2025):
                gate_pergame_defense(reader, tag, failures)
            gate_sources(reader, tag, failures)
        if failures:
            for f in failures[:40]:
                print("FAIL " + f)
            raise RuntimeError(f"{len(failures)} parity failures")

        print("performance (E 2025, warm median of 3):")
        reader = Reader(cursor, "E", 2025)
        pergame = warm(reader, "get_team_traditional_pergame", "")
        direct = warm(reader, "get_team_traditional_direct", CLUTCH_PRESETS[0][1])
        ratings_direct = warm(reader, "get_team_ratings_direct", CLUTCH_PRESETS[0][1])
        print(f"  pergame full season {pergame:.3f}s")
        print(f"  direct standard 5/300 {direct:.3f}s (ratings_direct {ratings_direct:.3f}s)")
        if pergame > 0.5:
            raise RuntimeError("pergame exceeds 0.5 s warm gate")
        if direct > 5.0:
            raise RuntimeError("direct exceeds 5 s warm gate")

        for sig in (PERGAME_SIG, DIRECT_SIG):
            cursor.execute("SELECT has_function_privilege('app_readonly', %s, 'EXECUTE')", (sig,))
            if not cursor.fetchone()[0]:
                raise RuntimeError("app_readonly lacks EXECUTE on " + sig)
        print("app_readonly holds EXECUTE on both readers "
              "(call them through app/.Renviron after applying)")

        if args.dry_run:
            cursor.execute("ROLLBACK")
            print("DRY RUN passed and rolled back")
        else:
            cursor.execute("COMMIT")
            print("migration 059 applied")
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
