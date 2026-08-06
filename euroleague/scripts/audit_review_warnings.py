"""Export deterministic event context for EuroLeague publication warnings."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "euroleague" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions.counter import count_possessions  # noqa: E402
from euroleague_possessions.package_lineups import (  # noqa: E402
    audit_package_lineups,
)


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pbp_csv", type=Path)
    parser.add_argument("boxscore_dir", type=Path)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--gamecodes", default="1,2")
    parser.add_argument("--radius", type=int, default=6)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def _json_list(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False) if isinstance(value, list) else ""


def main() -> None:
    args = _arguments()
    gamecodes = sorted(
        {int(value.strip()) for value in args.gamecodes.split(",") if value.strip()}
    )
    pbp = pd.read_csv(args.pbp_csv)
    selected = pbp.loc[
        pbp["Season"].eq(args.season) & pbp["Gamecode"].isin(gamecodes)
    ].copy()
    possession_result = count_possessions(selected)
    events = possession_result.events
    provisional_ft = events.loc[
        events["play_type"].isin({"FTA", "FTM"})
        & events["grouping_status"].eq("provisional")
    ].copy()

    ft_context_parts: list[pd.DataFrame] = []
    for warning in provisional_ft.itertuples(index=False):
        context = events.loc[
            events["gamecode"].eq(int(warning.gamecode))
            & events["period"].eq(int(warning.period))
            & events["source_event_order"].between(
                int(warning.source_event_order) - args.radius,
                int(warning.source_event_order) + args.radius,
            )
        ].copy()
        context.insert(0, "warning_event_order", int(warning.source_event_order))
        context.insert(1, "is_warning", context["source_event_order"].eq(
            int(warning.source_event_order)
        ))
        ft_context_parts.append(context)
    ft_context = (
        pd.concat(ft_context_parts, ignore_index=True)
        if ft_context_parts
        else pd.DataFrame()
    )

    lineup_result = audit_package_lineups(
        selected,
        boxscore_dir=args.boxscore_dir,
    )
    invalid = lineup_result.invalid_actor_events.copy()
    invalid_context_parts: list[pd.DataFrame] = []
    for warning in invalid.itertuples(index=False):
        warning_order = int(warning.TRUE_NUMBEROFPLAY)
        context = lineup_result.events.loc[
            lineup_result.events["Gamecode"].eq(int(warning.Gamecode))
            & lineup_result.events["PERIOD"].eq(int(warning.PERIOD))
            & lineup_result.events["TRUE_NUMBEROFPLAY"].between(
                warning_order - args.radius,
                warning_order + args.radius,
            )
        ].copy()
        context.insert(0, "warning_event_order", warning_order)
        context.insert(
            1,
            "is_warning",
            context["TRUE_NUMBEROFPLAY"].eq(warning_order),
        )
        invalid_context_parts.append(context)
    invalid_context = (
        pd.concat(invalid_context_parts, ignore_index=True)
        if invalid_context_parts
        else pd.DataFrame()
    )

    if not invalid.empty:
        invalid["own_lineup"] = invalid.apply(
            lambda row: row["Lineup_A"] if bool(row["IsHomeTeam"]) else row["Lineup_B"],
            axis=1,
        )
        invalid["actor_in_own_lineup"] = invalid.apply(
            lambda row: str(row["PLAYER"]) in row["own_lineup"],
            axis=1,
        )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    ft_columns = [
        "warning_event_order",
        "is_warning",
        "season",
        "gamecode",
        "period",
        "source_event_order",
        "provider_number_of_play",
        "clock",
        "team_code",
        "play_type",
        "player_id",
        "player_name",
        "play_info",
        "score_a",
        "score_b",
        "synthetic_parent_order",
        "synthetic_ft_trip_id",
        "final_end_poss",
        "end_reason",
        "grouping_status",
        "grouping_confidence_pct",
        "decision_trace",
    ]
    ft_context.loc[:, [column for column in ft_columns if column in ft_context]].to_csv(
        args.output_dir / "provisional_ft_context.csv",
        index=False,
    )

    invalid_summary = invalid.copy()
    for column in ("Lineup_A", "Lineup_B", "own_lineup"):
        if column in invalid_summary:
            invalid_summary[column] = invalid_summary[column].map(_json_list)
    invalid_summary.to_csv(
        args.output_dir / "invalid_actor_warnings.csv",
        index=False,
    )
    for column in ("Lineup_A", "Lineup_B"):
        if column in invalid_context:
            invalid_context[column] = invalid_context[column].map(_json_list)
    invalid_context.to_csv(
        args.output_dir / "invalid_actor_context.csv",
        index=False,
    )

    summary = {
        "season": args.season,
        "gamecodes": gamecodes,
        "provisional_ft_rows": int(len(provisional_ft)),
        "provisional_ft_event_orders": provisional_ft[
            ["gamecode", "source_event_order"]
        ].astype(int).to_dict("records"),
        "invalid_actor_rows": int(len(invalid)),
        "invalid_actor_event_orders": invalid[
            ["Gamecode", "TRUE_NUMBEROFPLAY"]
        ].astype(int).to_dict("records"),
        "all_invalid_actors_absent_from_own_lineup": bool(
            not invalid.empty and (~invalid["actor_in_own_lineup"]).all()
        ),
    }
    (args.output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
