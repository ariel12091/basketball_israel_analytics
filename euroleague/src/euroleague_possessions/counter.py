"""Possession rows, totals, and QA derived from grouped EuroLeague events."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from .parser import FT_TYPES, group_events


@dataclass(frozen=True)
class PossessionResult:
    events: pd.DataFrame
    possessions: pd.DataFrame
    team_totals: pd.DataFrame
    reason_totals: pd.DataFrame
    game_qa: pd.DataFrame


def _same_team_transitions(possessions: pd.DataFrame) -> int:
    total = 0
    for _, period in possessions.groupby("period", sort=True):
        teams = period["offense_team"].tolist()
        total += sum(current == previous for previous, current in zip(teams, teams[1:]))
    return int(total)


def count_possessions(events: pd.DataFrame) -> PossessionResult:
    """Recompute grouping and return deterministic possession products."""

    grouped = group_events(events).sort_values(
        ["season", "gamecode", "period", "source_event_order"], kind="stable"
    )
    grouped = grouped.reset_index(drop=True)

    possession_columns = [
        "season",
        "gamecode",
        "period",
        "clock",
        "source_event_order",
        "team_code",
        "play_type",
        "player_id",
        "synthetic_parent_order",
        "synthetic_ft_trip_id",
        "end_reason",
        "grouping_status",
        "grouping_confidence_pct",
        "decision_trace",
    ]
    possessions = grouped.loc[grouped["final_end_poss"], possession_columns].copy()
    possessions = possessions.rename(columns={"team_code": "offense_team"})
    possessions["game_possession_number"] = (
        possessions.groupby(["season", "gamecode"], sort=False).cumcount() + 1
    )
    possessions["team_possession_number"] = (
        possessions.groupby(
            ["season", "gamecode", "offense_team"], sort=False, dropna=False
        ).cumcount()
        + 1
    )
    possessions = possessions.reset_index(drop=True)

    grouped["game_possession_number"] = pd.array(
        [pd.NA] * len(grouped), dtype="Int64"
    )
    grouped["team_possession_number"] = pd.array(
        [pd.NA] * len(grouped), dtype="Int64"
    )
    endpoint_indices = grouped.index[grouped["final_end_poss"]]
    if len(endpoint_indices):
        grouped.loc[endpoint_indices, "game_possession_number"] = possessions[
            "game_possession_number"
        ].to_numpy()
        grouped.loc[endpoint_indices, "team_possession_number"] = possessions[
            "team_possession_number"
        ].to_numpy()

    teams = (
        grouped.loc[grouped["team_code"].notna(), ["season", "gamecode", "team_code"]]
        .drop_duplicates()
        .rename(columns={"team_code": "offense_team"})
    )
    if possessions.empty:
        team_totals = teams.copy()
        team_totals["possessions"] = 0
        team_totals["provisional_possessions"] = 0
        reason_totals = pd.DataFrame(
            columns=[
                "season",
                "gamecode",
                "offense_team",
                "end_reason",
                "possessions",
            ]
        )
    else:
        totals = (
            possessions.groupby(
                ["season", "gamecode", "offense_team"], sort=False, dropna=False
            )
            .size()
            .rename("possessions")
            .reset_index()
        )
        provisional = (
            possessions.assign(
                _provisional=possessions["grouping_status"].ne("confirmed").astype(int)
            )
            .groupby(
                ["season", "gamecode", "offense_team"], sort=False, dropna=False
            )["_provisional"]
            .sum()
            .rename("provisional_possessions")
            .reset_index()
        )
        team_totals = teams.merge(
            totals,
            on=["season", "gamecode", "offense_team"],
            how="left",
            sort=False,
        ).merge(
            provisional,
            on=["season", "gamecode", "offense_team"],
            how="left",
            sort=False,
        )
        team_totals[["possessions", "provisional_possessions"]] = team_totals[
            ["possessions", "provisional_possessions"]
        ].fillna(0).astype(int)
        reason_totals = (
            possessions.groupby(
                ["season", "gamecode", "offense_team", "end_reason"],
                sort=False,
                dropna=False,
            )
            .size()
            .rename("possessions")
            .reset_index()
        )

    team_totals = team_totals.sort_values(
        ["season", "gamecode", "offense_team"], kind="stable"
    ).reset_index(drop=True)

    qa_rows: list[dict[str, object]] = []
    for (season, gamecode), game_events in grouped.groupby(
        ["season", "gamecode"], sort=True
    ):
        game_possessions = possessions.loc[
            (possessions["season"] == season)
            & (possessions["gamecode"] == gamecode)
        ]
        game_teams = team_totals.loc[
            (team_totals["season"] == season)
            & (team_totals["gamecode"] == gamecode)
        ]
        incident_endpoints = game_events.groupby(
            ["period", "synthetic_parent_order"], sort=False, dropna=False
        )["final_end_poss"].sum()
        duplicate_endpoints = int((incident_endpoints > 1).sum())

        source_keys = set(
            zip(
                game_events["period"].astype(int),
                game_events["source_event_order"].astype(int),
            )
        )
        parent_keys = list(
            zip(
                game_events["period"].astype(int),
                game_events["synthetic_parent_order"].astype(int),
            )
        )
        missing_parents = sum(key not in source_keys for key in parent_keys)
        ft_mask = game_events["play_type"].isin(FT_TYPES)
        unresolved_ft = int(
            game_events.loc[ft_mask, "grouping_status"].eq("unresolved").sum()
        )
        provisional_ft = int(
            game_events.loc[ft_mask, "grouping_status"].eq("provisional").sum()
        )
        same_team = _same_team_transitions(game_possessions)
        difference = (
            None
            if len(game_teams) < 2
            else int(
                game_teams["possessions"].max()
                - game_teams["possessions"].min()
            )
        )
        hard_failure = bool(
            unresolved_ft or duplicate_endpoints or missing_parents
        )
        needs_review = bool(
            hard_failure
            or provisional_ft
            or same_team
            or (difference is not None and difference > 1)
        )
        qa_rows.append(
            {
                "season": int(season),
                "gamecode": int(gamecode),
                "total_possessions": len(game_possessions),
                "possession_difference": difference,
                "same_team_transitions": same_team,
                "provisional_ft_rows": provisional_ft,
                "unresolved_ft_rows": unresolved_ft,
                "duplicate_endpoint_incidents": duplicate_endpoints,
                "missing_parent_targets": missing_parents,
                "structural_status": "fail" if hard_failure else "pass",
                "review_status": "review" if needs_review else "clear",
            }
        )

    game_qa = pd.DataFrame(qa_rows).sort_values(
        ["season", "gamecode"], kind="stable"
    ).reset_index(drop=True)
    return PossessionResult(
        events=grouped,
        possessions=possessions,
        team_totals=team_totals,
        reason_totals=reason_totals,
        game_qa=game_qa,
    )

