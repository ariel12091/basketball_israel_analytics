"""Reproduce the blocked-FT feed review and hand-labelled fixture experiment."""
import json
from pathlib import Path

import pandas as pd
from euroleague_ft_measure import ROOT, solve
from euroleague_possessions.parser import FT_TYPES, group_events, normalize_events
from euroleague_possessions.pbp_collector import read_cached_pbp

OUT = ROOT / "euroleague/data/diagnostics/ft_stoppage_measure"
CASES = {"E2025": [91, 98, 136, 302, 303, 383], "E2026": [31, 73],
         "U2025": [2, 79, 86], "U2026": [5, 6, 56, 109]}


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    lines = ["# Cached unresolved FT evidence", "", "Source order is the parser identity; clock is provider text.", ""]
    for prefix, codes in CASES.items():
        for code in codes:
            game = f"{prefix}_{code}"
            frame = group_events(read_cached_pbp(ROOT / f"euroleague/data/raw/pbp/{game}.pbp.json"))
            bad = frame[frame.play_type.isin(FT_TYPES) & frame.grouping_status.eq("unresolved")]
            lines += [f"## {game}: {len(bad)} unresolved FT rows", ""]
            for index, r in bad.iterrows():
                block = frame[frame.period.eq(r.period) & frame.clock.eq(r.clock)]
                status, assignment = solve(list(block.itertuples(index=False)))
                lines += [f"Row {r.source_event_order}, Q{r.period} {r.clock}: {r.player_name}. "
                          f"Clock-bucket fit: {status}. Assignment: {json.dumps(assignment)}.", "", "```text"]
                for e in frame.iloc[max(0, index - 8):index + 9].itertuples(index=False):
                    lines.append(f"{e.source_event_order:4} Q{e.period} {str(e.clock):5} {e.play_type:6} "
                                 f"{str(e.team_code):4} {str(e.player_name):24} {e.play_info}")
                lines += ["```", ""]
    (OUT / "blocked_cases.md").write_text("\n".join(lines), encoding="utf-8")
    fixtures = normalize_events(pd.read_csv(ROOT / "etl/euroleague/fixtures/event_grouping_edge_cases.csv"))
    results = []
    for keys, block in fixtures.groupby(["fixture_id", "season", "gamecode", "period", "clock"]):
        ft = block[block.play_type.isin(FT_TYPES)]
        if ft.empty:
            continue
        status, assignment = solve(list(block.itertuples(index=False)))
        relaxed, _ = solve(list(block.itertuples(index=False)), allow_cancel=True)
        repeated = len({a[1] for a in assignment}) < len(assignment)
        equal = None
        if status == "unique" and not repeated:
            candidate = sorted(sorted(int(r.source_event_order) for r in ft.itertuples()
                                      if r.team_code == team and r.player_id == shooter)
                               for _, (team, shooter), _ in assignment)
            expected = sorted(sorted(part.source_event_order.astype(int).tolist())
                              for _, part in ft.groupby("expected_ft_trip_id"))
            equal = candidate == expected
        results.append(dict(fixture=keys[0], clock=keys[-1], fit=status, cancellation_fit=relaxed,
                            repeated_shooter=repeated, partition_matches=equal))
    pd.DataFrame(results).to_csv(OUT / "fixture_comparison.csv", index=False)
    print(pd.DataFrame(results).to_string(index=False))


if __name__ == "__main__":
    main()
