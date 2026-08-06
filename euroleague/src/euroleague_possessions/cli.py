"""Command-line diagnostic for the deterministic possession parser."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from .counter import count_possessions


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Group EuroLeague events and count deterministic possessions."
    )
    parser.add_argument("input_csv", type=Path)
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="Optional directory for events, possessions, totals, and QA CSVs.",
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    raw = pd.read_csv(args.input_csv)
    result = count_possessions(raw)

    ft = result.events["play_type"].isin({"FTA", "FTM"})
    print(f"events={len(result.events)}")
    print(f"games={result.events['gamecode'].nunique()}")
    print(f"possessions={len(result.possessions)}")
    print(f"ft_rows={int(ft.sum())}")
    print(
        "unresolved_ft_rows="
        f"{int(result.events.loc[ft, 'grouping_status'].eq('unresolved').sum())}"
    )
    print(
        "structural_pass_games="
        f"{int(result.game_qa['structural_status'].eq('pass').sum())}"
    )

    if args.output_dir is not None:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        result.events.to_csv(args.output_dir / "events.csv", index=False)
        result.possessions.to_csv(
            args.output_dir / "possessions.csv", index=False
        )
        result.team_totals.to_csv(
            args.output_dir / "team_totals.csv", index=False
        )
        result.reason_totals.to_csv(
            args.output_dir / "reason_totals.csv", index=False
        )
        result.game_qa.to_csv(args.output_dir / "game_qa.csv", index=False)


if __name__ == "__main__":
    main()

