"""Offline entitlement feasibility experiment, NOT a replacement parser.

Run: euroleague/.venv/Scripts/python.exe docs/handoff_scripts_2026-11-17/euroleague_ft_measure.py
Outputs detailed evidence under ignored euroleague/data/diagnostics/ft_stoppage_measure.
Clock buckets are deliberately an optimistic scope; they are not incident IDs.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "euroleague/src"))
import pandas as pd
from euroleague_possessions.parser import (
    FT_TYPES, MADE_SHOTS, SHOTS, group_events, is_committed_foul,
    is_one_shot_special, is_special_foul,
)
from euroleague_possessions.pbp_collector import read_cached_pbp


def solve(rows, allow_cancel=False):
    """Count up to two assignments of shooter-counts to individual foul rows.

    Personal: 0/2/3, or 1 with a same-shooter made basket. Special: 1 or 2.
    A unique opposing RV constrains the personal shooter; multiple RVs leave
    all drawn players eligible. No order/distance scores or parser parents used.
    Same-player FT rows are exchangeable here: attempt identity is NOT solved.
    """
    fts = [r for r in rows if r.play_type in FT_TYPES]
    fouls = [r for r in rows if is_committed_foul(r.play_type) and r.play_type != "OF"]
    if any(not r.team_code or not r.player_id for r in fts):
        return "missing_ft_actor", []
    remaining = Counter((r.team_code, r.player_id) for r in fts)
    keys = sorted(remaining)
    options = []
    for foul in fouls:
        if not foul.team_code:
            return "missing_foul_team", []
        drawn = {r.player_id for r in rows if r.play_type == "RV"
                 and r.team_code and r.team_code != foul.team_code and r.player_id}
        choices = [(None, 0)] if not is_special_foul(foul.play_type) or allow_cancel else []
        for key in keys:
            team, shooter = key
            if team == foul.team_code:
                continue
            if is_special_foul(foul.play_type):
                sizes = [1 if is_one_shot_special(foul.play_type) else 2]
            else:
                if drawn and shooter not in drawn:
                    continue
                sizes = [2, 3]
                if any(r.play_type in MADE_SHOTS and r.team_code == team
                       and r.player_id == shooter for r in rows):
                    sizes = [1] + sizes
            choices.extend((key, size) for size in sizes if size <= remaining[key])
        options.append((foul.source_event_order, choices))
    solutions = []
    budget = [0]

    def visit(i, assignment):
        budget[0] += 1
        if budget[0] > 20000 or len(solutions) >= 2:
            return
        if i == len(options):
            if not any(remaining.values()):
                solutions.append(assignment.copy())
            return
        order, choices = options[i]
        for key, size in choices:
            if size and remaining[key] < size:
                continue
            if size:
                remaining[key] -= size
                assignment.append((int(order), key, size))
            visit(i + 1, assignment)
            if size:
                assignment.pop()
                remaining[key] += size
    visit(0, [])
    status = "ambiguous" if len(solutions) > 1 else "unique" if solutions else "no_fit"
    if budget[0] > 20000 and len(solutions) < 2:
        status = "search_limit"
    return status, solutions[0] if solutions else []


def measure_game(frame, competition, game, buckets, counters, anomalies, contexts):
    grouped = group_events(frame).sort_values("source_event_order")
    ft = grouped[grouped.play_type.isin(FT_TYPES)]
    # PLAYINFO is a game-cumulative player counter, checked independently of trips.
    previous = {}
    for r in ft.itertuples(index=False):
        match = re.search(r"\((\d+)\s*/\s*(\d+)\s*-", r.play_info or "")
        reason = None
        if not match:
            reason = "missing_counter"
        else:
            actual = tuple(map(int, match.groups()))
            key = (r.team_code, r.player_id)
            prior = previous.get(key, (0, 0))
            expected = (prior[0] + int(r.play_type == "FTM"), prior[1] + 1)
            if actual != expected:
                reason = "counter_mismatch"
            previous[key] = actual
        counters["counter_rows"] += 1
        if reason:
            counters[reason] += 1
            anomalies.append(dict(game=game, order=int(r.source_event_order),
                                  reason=reason, player=r.player_id, info=r.play_info))
    # Record clock revisits: one bucket may combine separate live-ball incidents.
    grouped["clock_run"] = (grouped.period.ne(grouped.period.shift()) |
                             grouped.clock.ne(grouped.clock.shift())).cumsum()
    for (period, clock), block in grouped.groupby(["period", "clock"], dropna=False):
        free = block[block.play_type.isin(FT_TYPES)]
        if free.empty:
            continue
        rows = list(block.itertuples(index=False))
        status, assignment = solve(rows)
        relaxed, _ = solve(rows, allow_cancel=True)
        foul_codes = [r.play_type for r in rows if is_committed_foul(r.play_type)]
        shooting_fouls = [c for c in foul_codes if c != "OF"]
        live = [r.play_type for r in rows if r.play_type in SHOTS | {"D", "O", "TO", "OF", "JB"}]
        dirty = any(c in {"D", "O", "TO", "OF", "JB"} for c in live) or sum(c in SHOTS for c in live) > 1
        # Distinct entitlement allocations can still have ambiguous attempt rows
        # when the same shooter has multiple trips.
        repeated_shooter = len({a[1] for a in assignment}) < len(assignment)
        baseline_partition = sorted(sorted(int(x) for x in part.source_event_order)
                                    for _, part in free.groupby("synthetic_ft_trip_id", dropna=False))
        candidate_partition = []
        if status == "unique" and not repeated_shooter:
            for _, (team, shooter), _ in assignment:
                candidate_partition.append(sorted(int(r.source_event_order) for r in free.itertuples()
                                                  if r.team_code == team and r.player_id == shooter))
        row = dict(game=game, competition=competition, season=int(block.season.iloc[0]),
                   period=int(period), clock=clock, ft_rows=len(free),
                   fouls="+".join(sorted(shooting_fouls)), foul_count=len(shooting_fouls),
                   shooters=free.player_id.nunique(), clock_runs=block.clock_run.nunique(),
                   live_events="+".join(live), boundary_risk=dirty,
                   strict_fit=status, cancellation_fit=relaxed,
                   assignment=json.dumps(assignment), repeated_shooter_trips=repeated_shooter,
                   partition_matches=(sorted(candidate_partition) == baseline_partition)
                       if candidate_partition else None)
        for value in ("confirmed", "provisional", "unresolved"):
            row[value] = int(free.grouping_status.eq(value).sum())
        buckets.append(row)
        if row["unresolved"] or status != "unique":
            contexts.append({"game": game, "period": int(period), "clock": clock,
                             "fit": status, "events": block[["source_event_order", "play_type",
                             "team_code", "player_id", "player_name", "play_info",
                             "grouping_status", "decision_trace"]].to_dict("records")})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=ROOT / "euroleague/data/raw/pbp")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "euroleague/data/diagnostics/ft_stoppage_measure")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    paths = sorted(args.raw_dir.glob("*.pbp.json"))
    if args.limit:
        paths = paths[:args.limit]
    buckets, anomalies, contexts, errors, manifest = [], [], [], [], []
    counters = Counter()
    for n, path in enumerate(paths, 1):
        game = path.name.removesuffix(".pbp.json")
        try:
            frame = read_cached_pbp(path)
            measure_game(frame, game[0], game, buckets, counters, anomalies, contexts)
            manifest.append(dict(game=game, rows=len(frame), sha256=hashlib.sha256(path.read_bytes()).hexdigest()))
        except Exception as exc:
            errors.append(dict(game=game, error=f"{type(exc).__name__}: {exc}"))
        if n % 50 == 0 or n == len(paths):
            print(f"{n}/{len(paths)} games; errors={len(errors)}", flush=True)
    df = pd.DataFrame(buckets)
    df.to_csv(args.output_dir / "buckets.csv", index=False)
    pd.DataFrame(anomalies).to_csv(args.output_dir / "counter_anomalies.csv", index=False)
    (args.output_dir / "contexts.json").write_text(json.dumps(contexts, indent=2, default=str), encoding="utf-8")
    summary = {"checkout": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
               "parser_sha256": hashlib.sha256((ROOT / "euroleague/src/euroleague_possessions/parser.py").read_bytes()).hexdigest(),
               "games": len(manifest), "errors": errors, "counters": dict(counters),
               "populations": []}
    for keys, sub in df.groupby(["competition", "season"]):
        summary["populations"].append(dict(competition=keys[0], season=int(keys[1]),
            games=sub.game.nunique(), buckets=len(sub), ft_rows=int(sub.ft_rows.sum()),
            strict_fit=sub.strict_fit.value_counts().to_dict(),
            cancellation_fit=sub.cancellation_fit.value_counts().to_dict(),
            baseline_by_fit=sub.groupby("strict_fit")[["confirmed", "provisional", "unresolved"]].sum().to_dict("index"),
            boundary_risk=int(sub.boundary_risk.sum()), clock_revisits=int(sub.clock_runs.gt(1).sum())))
    (args.output_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (args.output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
