#!/usr/bin/env python
"""Would dropping overlapping player_stats_actions_by_game indexes slow the app?

Four indexes share one 8-column key. euroleague_player_stats_actions_lineups_idx
(the largest) INCLUDEs every column of _team_idx and extends _filter_idx's key,
so those two -- and possibly _minutes_idx, whose extra key columns are INCLUDEd
in _lineups_idx -- may be redundant.

Every reader that touches the table is timed with the index set intact and with
each candidate set dropped. Drops happen inside a transaction that is ALWAYS
rolled back, so nothing is changed. Variants are interleaved per repetition and
results are compared by hash, so a drop that changes an answer fails loudly.

    .venv/Scripts/python.exe scripts/benchmark_psa_index_drop.py --reps 5
"""
from __future__ import annotations

import argparse
import hashlib
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from euroleague_possessions.postgres_backend import connect_from_env_file  # noqa: E402

ENV = ROOT.parent / "etl" / ".Renviron"
# Each variant is the SQL run inside its always-rolled-back transaction before
# the calls are timed; the first variant is the reference for result hashes.
#
# History (2026-09-23): run against the pre-056 index set, dropping _team_idx
# alone, both _team_idx and _filter_idx (adopted as migration 056), and those
# two plus _minutes_idx -- rejected, it slowed broad custom Team Ratings
# 1.2s -> 2.9s and Lineups 1.4s -> 2.0s.
#
# Now that 056 is applied, "before" rebuilds the two dropped indexes with
# their original definitions (migrations 027 and 032) so the pre-056 state can
# still be compared against the live one. Build time is not timed.
VARIANTS = {
    "after (live)": [],
    "before (pre-056)": [
        "CREATE INDEX euroleague_player_stats_actions_team_idx "
        "ON euroleague.player_stats_actions_by_game (game_id, team_id, "
        "regulation_seconds_remaining, pre_abs_margin, pre_status, is_overtime, "
        "own_starters, opp_starters) INCLUDE (type_lineup, possession_flag, "
        "points, ts_possessions, orebounds, oreb_opportunities, turnovers, "
        "ft_attempts, fga, fgm, fg3_made)",
        "CREATE INDEX euroleague_player_stats_actions_filter_idx "
        "ON euroleague.player_stats_actions_by_game (game_id, team_id, "
        "regulation_seconds_remaining, pre_abs_margin, pre_status)",
    ],
}
CLUTCH = "p_max_margin=>3, p_margin_status=>'all', p_max_time_remaining=>120"
READERS = ["get_team_ratings_direct", "get_team_four_factors_direct",
           "get_team_minutes_direct", "fetch_lineups_direct",
           "get_player_traditional_custom_clutch"]


def calls(team_id: int, game_ids: list[int]) -> list[tuple[str, str, bool]]:
    out = []
    for scope, extra in (("all", ""), ("team", f", p_team_ids_csv=>'{team_id}'")):
        for fn in READERS:
            out.append((f"{fn}/{scope}",
                        f"SELECT * FROM euroleague.{fn}(p_competition=>'E', "
                        f"p_game_year=>2025, {CLUTCH}{extra})", True))
    ids = "ARRAY[" + ",".join(map(str, game_ids)) + "]::bigint[]"
    out.append(("refresh_lineup_totals_by_game/10",
                f"SELECT euroleague.refresh_lineup_totals_by_game({ids})", False))
    out.append(("refresh_default_clutch_for_games/10",
                f"SELECT euroleague.refresh_default_clutch_for_games({ids})", False))
    return out


def digest(rows) -> str:
    return hashlib.md5(repr(sorted(map(repr, rows))).encode()).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--variants", nargs="+", choices=list(VARIANTS),
                    help="run only these (default: all). Note that 'before' "
                         "rebuilds ~100 MB of index per rep, which evicts "
                         "cache and slows whatever runs next.")
    args = ap.parse_args()
    if args.variants:
        for name in [v for v in VARIANTS if v not in args.variants]:
            del VARIANTS[name]

    conn = connect_from_env_file(ENV)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("SELECT team_id FROM euroleague.teams WHERE competition='E' ORDER BY team_id LIMIT 1")
    team_id = cur.fetchone()[0]
    cur.execute("SELECT game_id FROM euroleague.schedule WHERE competition='E' AND season=2025 "
                "ORDER BY gamecode DESC LIMIT 10")
    game_ids = [r[0] for r in cur.fetchall()]
    conn.rollback()
    plan = calls(team_id, game_ids)

    times: dict[tuple[str, str], list[float]] = {}
    hashes: dict[tuple[str, str], set[str]] = {}
    for rep in range(args.reps + 1):  # rep 0 warms caches and is discarded
        names = list(VARIANTS)
        order = names[rep % len(names):] + names[:rep % len(names)]  # rotate: no order bias
        for variant in order:
            try:
                cur.execute("SET LOCAL lock_timeout = '3s'")
                cur.execute("SET LOCAL statement_timeout = '300s'")  # index builds
                for statement in VARIANTS[variant]:
                    cur.execute(statement)
                for label, sql, compare in plan:
                    t0 = time.perf_counter()
                    cur.execute(sql)
                    rows = cur.fetchall()
                    dt = time.perf_counter() - t0
                    if rep:
                        times.setdefault((label, variant), []).append(dt)
                    if compare:
                        hashes.setdefault((label, variant), set()).add(digest(rows))
            finally:
                conn.rollback()
        print(f"rep {rep} done", flush=True)

    mismatches = 0
    ref = next(iter(VARIANTS))
    print(f"\n{'call':45s} " + " ".join(f"{v:>18s}" for v in VARIANTS) + "   change")
    for label, _, compare in plan:
        cells, medians = [], {}
        for v in VARIANTS:
            ts = times[(label, v)]
            medians[v] = statistics.median(ts)
            cells.append(f"{medians[v]:6.2f}s±{statistics.pstdev(ts):4.2f}")
        change = " ".join(f"{(medians[ref] - medians[v]) * 1000:+6.0f}ms"
                          for v in VARIANTS if v != ref)
        same = ""
        if compare:
            base = hashes[(label, ref)]
            if len(base) != 1 or any(hashes[(label, v)] != base for v in VARIANTS):
                same = "  RESULT DIFFERS"
                mismatches += 1
        print(f"{label:45s} " + " ".join(f"{c:>18s}" for c in cells) + f"  {change}" + same)
    print(f"\nchange = {ref} minus each other variant (positive = {ref} slower)")
    print(f"median of {args.reps} reps, variant order rotated per rep; "
          f"result mismatches: {mismatches}")
    conn.close()
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
