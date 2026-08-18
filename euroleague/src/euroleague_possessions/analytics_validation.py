"""Offline validation of the SQL analytics contract over cached games."""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from .counter import count_possessions
from .package_lineups import audit_package_lineups, cached_boxscore_frame
from .reconciliation import reconcile_boxscores
from .schema_coverage import _roster_players, _team_sides


METRICS = (
    "points",
    "possessions",
    "ts_possessions",
    "orebounds",
    "orebound_opportunities",
    "turnovers",
    "ft_attempts",
    "fga",
    "fgm",
    "fg3_made",
)


@dataclass(frozen=True)
class AnalyticsValidationResult:
    games: pd.DataFrame
    totals: Mapping[str, int]


def _clock_seconds(value: Any) -> int | None:
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if ":" not in text:
        return None
    minutes, seconds = text.split(":", 1)
    try:
        return int(minutes) * 60 + int(seconds)
    except ValueError:
        return None


def effective_period(
    provider_period: int,
    provider_minute: int | None,
    play_type: str | None,
) -> int:
    """Expand cumulative-minute overtime rows without changing raw PERIOD.

    EuroLeague's feed can keep PERIOD=5 in later overtimes while MINUTE
    continues through 46-50, 51-55, and so on.  End-period rows use the first
    minute of the following interval (46, 51, ...) and therefore belong to the
    overtime that just ended.
    """

    if provider_period <= 4 or provider_minute is None:
        return provider_period
    boundary_adjustment = 1 if str(play_type or "").strip().upper() in {"EP", "EG"} else 0
    overtime_index = max(provider_minute - 41 - boundary_adjustment, 0) // 5
    return 5 + overtime_index


def canonical_elapsed_seconds(
    game: pd.DataFrame,
) -> tuple[dict[int, int], int]:
    """Mirror the SQL cumulative canonical clock without changing raw fields."""

    ordered = game.sort_values("TRUE_NUMBEROFPLAY", kind="stable")
    effective_periods: list[int] = []
    for row in ordered.itertuples(index=False):
        raw_minute = getattr(row, "MINUTE", None)
        minute = None if raw_minute is None or pd.isna(raw_minute) else int(raw_minute)
        effective_periods.append(
            effective_period(
                int(row.PERIOD), minute, getattr(row, "PLAYTYPE", None)
            )
        )
    maximum_period = max(effective_periods)
    game_end = 2400 + max(maximum_period - 4, 0) * 300
    elapsed: dict[int, int] = {}
    previous = 0
    for row, period in zip(ordered.itertuples(index=False), effective_periods):
        period_start = (
            (period - 1) * 600
            if period <= 4
            else 2400 + (period - 5) * 300
        )
        period_length = 600 if period <= 4 else 300
        remaining = _clock_seconds(getattr(row, "MARKERTIME", None))
        raw = (
            period_start
            + period_length
            - max(0, min(remaining, period_length))
            if remaining is not None
            else None
        )
        canonical = max(previous, raw if raw is not None else period_start)
        elapsed[int(row.TRUE_NUMBEROFPLAY)] = canonical
        previous = canonical
    return elapsed, game_end


def _lineup_ids(
    names: object,
    team: str,
    name_to_id: Mapping[tuple[str, str], str],
) -> tuple[str, ...]:
    if not isinstance(names, list):
        raise ValueError(f"{team} lineup is not a list")
    return tuple(name_to_id[(team, str(name).strip())] for name in names)


def _metric_contributions(
    grouped: pd.DataFrame,
    possessions: pd.DataFrame,
    valid_teams: set[str],
) -> dict[int, dict[str, dict[str, int]]]:
    result: dict[int, dict[str, dict[str, int]]] = {
        int(order): {
            team: {metric: 0 for metric in METRICS} for team in valid_teams
        }
        for order in grouped["source_event_order"]
    }

    def add(order: int, team: Any, metric: str, value: int = 1) -> None:
        if team is None or pd.isna(team) or str(team) not in valid_teams:
            raise ValueError(f"invalid team for {metric}: {team!r}")
        result[int(order)][str(team)][metric] += int(value)

    by_order = {
        int(row.source_event_order): row
        for row in grouped.itertuples(index=False)
    }
    free_throws = grouped.loc[
        grouped["play_type"].isin({"FTA", "FTM"})
        & grouped["synthetic_ft_trip_id"].notna()
    ]
    for _, trip in free_throws.groupby("synthetic_ft_trip_id", sort=False):
        final = max(
            trip.itertuples(index=False),
            key=lambda row: int(row.source_event_order),
        )
        parent = by_order[int(final.synthetic_parent_order)]
        if str(parent.play_type) == "CM":
            add(
                int(final.source_event_order),
                final.team_code,
                "ts_possessions",
            )
            if str(final.play_type) == "FTA":
                add(
                    int(final.source_event_order),
                    final.team_code,
                    "orebound_opportunities",
                )

    for row in grouped.itertuples(index=False):
        order = int(row.source_event_order)
        play_type = str(row.play_type)
        team = row.team_code
        if play_type == "2FGM":
            add(order, team, "points", 2)
            add(order, team, "ts_possessions")
            add(order, team, "fga")
            add(order, team, "fgm")
        elif play_type == "2FGA":
            add(order, team, "ts_possessions")
            add(order, team, "orebound_opportunities")
            add(order, team, "fga")
        elif play_type == "3FGM":
            add(order, team, "points", 3)
            add(order, team, "ts_possessions")
            add(order, team, "fga")
            add(order, team, "fgm")
            add(order, team, "fg3_made")
        elif play_type == "3FGA":
            add(order, team, "ts_possessions")
            add(order, team, "orebound_opportunities")
            add(order, team, "fga")
        elif play_type == "FTM":
            add(order, team, "points")
            add(order, team, "ft_attempts")
        elif play_type == "FTA":
            add(order, team, "ft_attempts")
        elif play_type == "O":
            add(order, team, "orebounds")
        elif play_type == "TO":
            add(order, team, "turnovers")

    for possession in possessions.itertuples(index=False):
        add(
            int(possession.source_event_order),
            possession.offense_team,
            "possessions",
        )
    return result


def _joint_segments(
    orders: Sequence[int],
    own_lineups: Mapping[int, tuple[str, ...]],
    opponent_lineups: Mapping[int, tuple[str, ...]],
    elapsed: Mapping[int, int],
    game_end: int,
) -> list[dict[str, Any]]:
    starts: list[tuple[int, tuple[str, ...], tuple[str, ...]]] = []
    previous: tuple[tuple[str, ...], tuple[str, ...]] | None = None
    for order in orders:
        current = (own_lineups[order], opponent_lineups[order])
        if current != previous:
            starts.append((order, current[0], current[1]))
            previous = current
    result: list[dict[str, Any]] = []
    for index, (order, own, opponent) in enumerate(starts):
        end = elapsed[starts[index + 1][0]] if index + 1 < len(starts) else game_end
        result.append(
            {
                "own": own,
                "opponent": opponent,
                "duration": max(end - elapsed[order], 0),
            }
        )
    return result


def validate_analytics_sample(
    pbp: pd.DataFrame,
    boxscore_dir: Path,
    competition: str = "E",
) -> AnalyticsValidationResult:
    """Validate the SQL contract's inputs and conservation rules offline."""

    lineup_result = audit_package_lineups(
        pbp, boxscore_dir=boxscore_dir, competition=competition
    )
    possession_result = count_possessions(pbp)
    reconciliation = reconcile_boxscores(
        pbp, boxscore_dir=boxscore_dir, competition=competition
    )
    rows: list[dict[str, Any]] = []

    for (season_value, gamecode_value), game in pbp.groupby(
        ["Season", "Gamecode"], sort=True
    ):
        season = int(season_value)
        gamecode = int(gamecode_value)
        boxscore = cached_boxscore_frame(
            boxscore_dir / f"{competition}{season}_{gamecode}.json",
            season,
            gamecode,
            competition=competition,
        )
        home, away = _team_sides(boxscore)
        teams = (home, away)
        roster = _roster_players(boxscore)
        roster["provider_player_id"] = (
            roster["provider_player_id"].astype("string").str.strip()
        )
        roster["player_name"] = roster["player_name"].astype("string").str.strip()
        roster["team_code"] = roster["team_code"].astype("string").str.strip()
        name_to_id = {
            (str(row.team_code), str(row.player_name)): str(row.provider_player_id)
            for row in roster.itertuples(index=False)
        }
        starters = {
            team: set(
                roster.loc[
                    roster["team_code"].eq(team) & roster["IsStarter"].eq(1),
                    "provider_player_id",
                ].astype(str)
            )
            for team in teams
        }

        lineups = lineup_result.events.loc[
            lineup_result.events["Season"].eq(season)
            & lineup_result.events["Gamecode"].eq(gamecode)
        ].sort_values("TRUE_NUMBEROFPLAY", kind="stable")
        grouped = possession_result.events.loc[
            possession_result.events["season"].eq(season)
            & possession_result.events["gamecode"].eq(gamecode)
        ].sort_values("source_event_order", kind="stable")
        possessions = possession_result.possessions.loc[
            possession_result.possessions["season"].eq(season)
            & possession_result.possessions["gamecode"].eq(gamecode)
        ]
        elapsed, game_end = canonical_elapsed_seconds(game)
        orders = lineups["TRUE_NUMBEROFPLAY"].astype(int).tolist()
        lineup_by_team: dict[str, dict[int, tuple[str, ...]]] = {
            home: {},
            away: {},
        }
        for event in lineups.itertuples(index=False):
            order = int(event.TRUE_NUMBEROFPLAY)
            lineup_by_team[home][order] = _lineup_ids(
                event.Lineup_A, home, name_to_id
            )
            lineup_by_team[away][order] = _lineup_ids(
                event.Lineup_B, away, name_to_id
            )

        metrics = _metric_contributions(grouped, possessions, set(teams))
        time_budget_exact = True
        player_time_exact = True
        partitions_exact = True
        expected_fact_rows = 0
        dnp_players = 0
        team_seconds: dict[str, int] = {}

        for team, opponent in ((home, away), (away, home)):
            segments = _joint_segments(
                orders,
                lineup_by_team[team],
                lineup_by_team[opponent],
                elapsed,
                game_end,
            )
            team_seconds[team] = sum(int(segment["duration"]) for segment in segments)
            time_budget_exact &= team_seconds[team] == game_end
            starter_contexts = {
                (
                    sum(player in starters[team] for player in own),
                    sum(player in starters[opponent] for player in opp),
                )
                for own, opp in (
                    (lineup_by_team[team][order], lineup_by_team[opponent][order])
                    for order in orders
                )
            }
            team_roster = roster.loc[roster["team_code"].eq(team)]
            expected_fact_rows += len(team_roster) * len(starter_contexts) * 4

            team_totals = {
                metric: sum(
                    metrics[order][team][metric] for order in orders
                )
                for metric in METRICS
            }
            opponent_totals = {
                metric: sum(
                    metrics[order][opponent][metric] for order in orders
                )
                for metric in METRICS
            }
            for player_id in team_roster["provider_player_id"].astype(str):
                on_seconds = sum(
                    int(segment["duration"])
                    for segment in segments
                    if player_id in segment["own"]
                )
                off_seconds = team_seconds[team] - on_seconds
                player_time_exact &= on_seconds + off_seconds == game_end
                dnp_players += int(on_seconds == 0)

                for totals, source_team in (
                    (team_totals, team),
                    (opponent_totals, opponent),
                ):
                    on = defaultdict(int)
                    off = defaultdict(int)
                    for order in orders:
                        target = (
                            on
                            if player_id in lineup_by_team[team][order]
                            else off
                        )
                        for metric in METRICS:
                            target[metric] += metrics[order][source_team][metric]
                    partitions_exact &= all(
                        on[metric] + off[metric] == totals[metric]
                        for metric in METRICS
                    )

        recon_game = reconciliation.game_summary.loc[
            reconciliation.game_summary["season"].eq(season)
            & reconciliation.game_summary["gamecode"].eq(gamecode)
        ].iloc[0]
        rows.append(
            {
                "season": season,
                "gamecode": gamecode,
                "roster_players": int(len(roster)),
                "dnp_players": dnp_players,
                "expected_fact_rows": expected_fact_rows,
                "game_seconds": game_end,
                "home_team_seconds": team_seconds[home],
                "away_team_seconds": team_seconds[away],
                "time_budget_exact": bool(time_budget_exact),
                "player_time_exact": bool(player_time_exact),
                "onoff_partitions_exact": bool(partitions_exact),
                "official_additive_totals_exact": bool(recon_game["all_exact"]),
            }
        )

    games = pd.DataFrame(rows).sort_values(
        ["season", "gamecode"], kind="stable"
    ).reset_index(drop=True)
    totals = {
        "games": len(games),
        "roster_players": int(games["roster_players"].sum()),
        "dnp_players": int(games["dnp_players"].sum()),
        "expected_fact_rows": int(games["expected_fact_rows"].sum()),
        "time_budget_failures": int((~games["time_budget_exact"]).sum()),
        "player_time_failures": int((~games["player_time_exact"]).sum()),
        "partition_failures": int((~games["onoff_partitions_exact"]).sum()),
        "official_total_failures": int(
            (~games["official_additive_totals_exact"]).sum()
        ),
    }
    return AnalyticsValidationResult(games=games, totals=totals)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Validate EuroLeague ON/OFF analytics inputs offline."
    )
    parser.add_argument("pbp_csv", type=Path)
    parser.add_argument("boxscore_dir", type=Path)
    parser.add_argument("--competition", default="E")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    result = validate_analytics_sample(
        pd.read_csv(args.pbp_csv),
        args.boxscore_dir,
        competition=args.competition,
    )
    for key, value in result.totals.items():
        print(f"{key}={value}")
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        result.games.to_csv(args.output, index=False)


if __name__ == "__main__":
    main()
