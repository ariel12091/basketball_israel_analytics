#!/usr/bin/env python
"""Capture and diff the two four-factor grains around migration 009.

009 is an output-identical refactor, so the evidence is a comparison against
rows stored BEFORE the change. Two fresh runs inside one transaction cannot
show a derived_at difference, because now() is the transaction timestamp.

    .venv/Scripts/python.exe scripts/snapshot_four_factor_grains.py --save
    .venv/Scripts/python.exe scripts/snapshot_four_factor_grains.py --diff
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402

# Lineage columns are expected to move on a re-derivation and are excluded.
PLAYER_GRAIN = """
SELECT player_id, team_id, game_id, game_year, is_on_key, type_lineup,
       num_starters, own_starters, opp_starters,
       total_points, total_poss, ts_poss_count, oreb_count,
       oreb_opportunities, tov_count, steal_count, total_ft_attempts,
       total_fga, total_fgm, total_fg3_made,
       player_ts_poss_count, player_tov_count, minutes,
       fg2_made, fg2_att, fg3_made, fg3_att,
       layup_made, layup_att, dunk_made, dunk_att,
       deflection_count, c3_made, c3_att, c3_known_att, onoff_minutes
  FROM euroleague.player_four_factors_by_game
 ORDER BY game_id, team_id, player_id, is_on_key, type_lineup,
          own_starters, opp_starters
"""

TEAM_GRAIN = """
SELECT game_id, team_id, game_year, own_starters, opp_starters,
       off_pts, off_poss, off_ts_poss, off_oreb, off_oreb_opp, off_tov,
       off_fta, off_fga, off_fgm, off_fg3m,
       def_pts, def_poss, def_ts_poss, def_oreb, def_oreb_opp, def_tov,
       def_fta, def_fga, def_fgm, def_fg3m, def_steals
  FROM euroleague.team_four_factors_by_game
 ORDER BY game_id, team_id, own_starters, opp_starters
"""

# Season-level rates: what anything the app displays actually reads. A grain
# change that leaves every rate untouched is a different thing from one that
# moves them, and this is the check that told those apart during 008.
# Column names verified against the live catalog on 2026-08-08 -- note it is
# net_on_off, NOT net_rating_on/net_rating_off.
SEASON_ONOFF = """
SELECT game_year, team_id, player_id,
       off_on_points, off_on_poss, off_off_points, off_off_poss,
       def_on_points, def_on_poss, def_off_points, def_off_poss,
       minutes_on, off_rating_on, off_rating_off,
       def_rating_on, def_rating_off, net_on_off
  FROM euroleague.player_onoff_by_season
 ORDER BY game_year, team_id, player_id
"""

SEASON_FOUR_FACTORS = """
SELECT game_year, team_id, player_id,
       off_on_ts, off_off_ts, def_on_ts, def_off_ts,
       off_on_efg, off_off_efg, def_on_efg, def_off_efg,
       off_on_oreb, off_off_oreb, def_on_oreb, def_off_oreb,
       off_on_tov, off_off_tov, def_on_tov, def_off_tov,
       off_on_ftr, off_off_ftr, def_on_ftr, def_off_ftr,
       off_on_poss, off_off_poss, def_on_poss, def_off_poss
  FROM euroleague.player_four_factors_by_season
 ORDER BY game_year, team_id, player_id
"""

GRAINS = {
    "player_grain": PLAYER_GRAIN,
    "team_grain": TEAM_GRAIN,
    "season_onoff": SEASON_ONOFF,
    "season_four_factors": SEASON_FOUR_FACTORS,
}


def _write(conn: Any, sql: str, path: Path) -> int:
    cur = conn.cursor()
    cur.execute("SET LOCAL statement_timeout = '15min'")
    cur.execute(sql)
    rows = cur.fetchall()
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow([d.name for d in cur.description])
        for row in rows:
            writer.writerow(["" if v is None else str(v) for v in row])
    cur.close()
    return len(rows)


def snapshot(conn: Any, out_dir: Path) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    for name, sql in GRAINS.items():
        counts[name] = _write(conn, sql, out_dir / f"{name}.tsv")
    return counts


def diff(conn: Any, out_dir: Path) -> list[str]:
    problems: list[str] = []
    for name, sql in GRAINS.items():
        before = out_dir / f"{name}.tsv"
        if not before.exists():
            problems.append(f"{name}: no snapshot at {before}")
            continue
        after = out_dir / f"{name}.after.tsv"
        _write(conn, sql, after)
        old = before.read_text(encoding="utf-8").splitlines()
        new = after.read_text(encoding="utf-8").splitlines()
        if old == new:
            continue
        if len(old) != len(new):
            problems.append(f"{name}: {len(old)} rows before, {len(new)} after")
        shown = 0
        for i, (a, b) in enumerate(zip(old, new)):
            if a != b and shown < 3:
                problems.append(f"{name}: line {i}\n  before: {a}\n  after:  {b}")
                shown += 1
    return problems


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--save", action="store_true")
    parser.add_argument("--diff", action="store_true")
    parser.add_argument("--dir", default=str(REPO / "exports" / "009_snapshot"))
    args = parser.parse_args()
    if args.save == args.diff:
        parser.error("pass exactly one of --save or --diff")

    conn = connect_from_env_file(Path("../etl/.Renviron"), direct_port=5432)
    out_dir = Path(args.dir)
    try:
        if args.save:
            for name, n in snapshot(conn, out_dir).items():
                print(f"  saved {name}: {n:,} rows")
            print(f"\nSnapshot written to {out_dir}")
        else:
            problems = diff(conn, out_dir)
            for line in problems:
                print(f"  {line}")
            if problems:
                print(f"\n{len(problems)} DIFFERENCE(S)")
                sys.exit(1)
            print("IDENTICAL — every comparison byte-for-byte")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
