"""Validate that package outputs resolve to the EuroLeague schema contract."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .counter import count_possessions
from .package_lineups import audit_package_lineups, cached_boxscore_frame


@dataclass(frozen=True)
class SchemaCoverageResult:
    game_summary: pd.DataFrame
    issues: pd.DataFrame


def _text(series: pd.Series) -> pd.Series:
    return series.astype("string").fillna("").str.strip()


def _roster_players(boxscore: pd.DataFrame) -> pd.DataFrame:
    roster = boxscore.copy()
    roster["provider_player_id"] = _text(roster["Player_ID"])
    roster["player_name"] = _text(roster["Player"])
    roster["team_code"] = _text(roster["Team"])
    aggregate_ids = roster["provider_player_id"].str.casefold().isin(
        {"team", "total"}
    )
    aggregate_names = roster["player_name"].str.casefold().isin(
        {"team", "total"}
    )
    return roster.loc[
        roster["provider_player_id"].ne("")
        & roster["player_name"].ne("")
        & roster["team_code"].ne("")
        & ~aggregate_ids
        & ~aggregate_names
    ].copy()


def _team_sides(boxscore: pd.DataFrame) -> tuple[str, str]:
    sides = boxscore[["Home", "Team"]].copy()
    sides["Team"] = _text(sides["Team"])
    sides = sides.loc[sides["Team"].ne("")].drop_duplicates()
    home = sides.loc[sides["Home"].eq(1), "Team"].unique()
    away = sides.loc[sides["Home"].eq(0), "Team"].unique()
    if len(home) != 1 or len(away) != 1 or home[0] == away[0]:
        raise ValueError("box score does not resolve one home and one away team")
    return str(home[0]), str(away[0])


def _lineup_names(
    lineup_events: pd.DataFrame,
    home_team: str,
    away_team: str,
) -> set[tuple[str, str]]:
    result: set[tuple[str, str]] = set()
    for column, team in (("Lineup_A", home_team), ("Lineup_B", away_team)):
        for players in lineup_events[column]:
            if isinstance(players, list):
                result.update((team, str(player).strip()) for player in players)
    return result


def _issue(
    issues: list[dict[str, Any]],
    season: int,
    gamecode: int,
    category: str,
    count: int,
    details: str,
) -> None:
    if count:
        issues.append(
            {
                "season": season,
                "gamecode": gamecode,
                "category": category,
                "count": int(count),
                "details": details,
            }
        )


def audit_schema_coverage(
    pbp: pd.DataFrame,
    boxscore_dir: Path,
    competition: str = "E",
) -> SchemaCoverageResult:
    """Audit natural-key and package-output coverage without database I/O."""

    lineup_result = audit_package_lineups(
        pbp, boxscore_dir=boxscore_dir, competition=competition
    )
    possession_result = count_possessions(pbp)
    summaries: list[dict[str, Any]] = []
    issues: list[dict[str, Any]] = []

    for (season_value, gamecode_value), game in pbp.groupby(
        ["Season", "Gamecode"], sort=True
    ):
        season = int(season_value)
        gamecode = int(gamecode_value)
        boxscore_path = boxscore_dir / f"{competition}{season}_{gamecode}.json"
        try:
            boxscore = cached_boxscore_frame(
                boxscore_path,
                season,
                gamecode,
                competition=competition,
            )
            home_team, away_team = _team_sides(boxscore)
            roster = _roster_players(boxscore)
        except (OSError, KeyError, TypeError, ValueError) as exc:
            details = f"{type(exc).__name__}: {exc}"
            _issue(issues, season, gamecode, "boxscore_unusable", 1, details)
            summaries.append(
                {
                    "season": season,
                    "gamecode": gamecode,
                    "schema_ready": False,
                    "blocking_issues": 1,
                    "informational_non_player_actor_rows": 0,
                    "package_invalid_actor_rows": 0,
                    "details": details,
                }
            )
            continue

        game_lineups = lineup_result.events.loc[
            lineup_result.events["Season"].eq(season)
            & lineup_result.events["Gamecode"].eq(gamecode)
        ].copy()
        game_possessions = possession_result.possessions.loc[
            possession_result.possessions["season"].eq(season)
            & possession_result.possessions["gamecode"].eq(gamecode)
        ].copy()

        roster_pairs = set(
            zip(roster["team_code"], roster["provider_player_id"])
        )
        roster_names = set(zip(roster["team_code"], roster["player_name"]))
        name_counts = roster.groupby(
            ["team_code", "player_name"], sort=False
        )["provider_player_id"].nunique()
        ambiguous_roster_names = int(name_counts.gt(1).sum())

        action_players = game.copy()
        action_players["team_code"] = _text(action_players["CODETEAM"])
        action_players["provider_player_id"] = _text(
            action_players["PLAYER_ID"]
        )
        action_players["player_name"] = _text(action_players["PLAYER"])
        action_players = action_players.loc[
            action_players["provider_player_id"].ne("")
        ]
        action_pairs = list(
            zip(
                action_players["team_code"],
                action_players["provider_player_id"],
            )
        )
        absent_from_roster = pd.Series(
            [pair not in roster_pairs for pair in action_pairs],
            index=action_players.index,
            dtype=bool,
        )
        missing_named_players = int(
            (absent_from_roster & action_players["player_name"].ne("")).sum()
        )
        non_player_actors = int(
            (absent_from_roster & action_players["player_name"].eq("")).sum()
        )

        package_summary = lineup_result.game_summary.loc[
            lineup_result.game_summary["season"].eq(season)
            & lineup_result.game_summary["gamecode"].eq(gamecode)
        ]
        package_ok = bool(
            len(package_summary) == 1
            and package_summary.iloc[0]["status"] == "ok"
        )
        package_structure_valid = bool(
            package_ok
            and package_summary.iloc[0]["lineup_structure_valid"]
            and package_summary.iloc[0]["starters_valid"]
        )
        package_invalid_actor_rows = (
            int(package_summary.iloc[0]["invalid_actor_rows"])
            if package_ok
            else 0
        )

        lineup_names = _lineup_names(game_lineups, home_team, away_team)
        missing_lineup_names = lineup_names.difference(roster_names)

        source_orders = pd.to_numeric(
            game["TRUE_NUMBEROFPLAY"], errors="coerce"
        )
        action_order_valid = bool(
            source_orders.notna().all()
            and source_orders.ge(0).all()
            and not source_orders.duplicated().any()
        )

        valid_teams = {home_team, away_team}
        invalid_offense_teams = int(
            (~game_possessions["offense_team"].isin(valid_teams)).sum()
        )
        endpoint_orders = set(
            game_possessions["source_event_order"].astype(int).tolist()
        )
        lineup_orders = set(
            pd.to_numeric(
                game_lineups["TRUE_NUMBEROFPLAY"], errors="coerce"
            )
            .dropna()
            .astype(int)
            .tolist()
        )
        missing_endpoint_lineups = len(endpoint_orders.difference(lineup_orders))

        starter_counts = roster.loc[roster["IsStarter"].eq(1)].groupby(
            "team_code"
        ).size()
        starters_valid = bool(
            set(starter_counts.index) == valid_teams
            and starter_counts.eq(5).all()
        )

        blocking_counts = {
            "missing_named_action_players": missing_named_players,
            "ambiguous_roster_names": ambiguous_roster_names,
            "missing_lineup_names": len(missing_lineup_names),
            "invalid_offense_teams": invalid_offense_teams,
            "missing_endpoint_lineups": missing_endpoint_lineups,
            "invalid_action_order": int(not action_order_valid),
            "invalid_starters": int(not starters_valid),
            "invalid_package_lineup_structure": int(
                not package_structure_valid
            ),
        }
        for category, count in blocking_counts.items():
            detail = ""
            if category == "missing_lineup_names" and missing_lineup_names:
                detail = repr(sorted(missing_lineup_names))
            _issue(issues, season, gamecode, category, count, detail)

        blocking_issues = int(sum(blocking_counts.values()))
        summaries.append(
            {
                "season": season,
                "gamecode": gamecode,
                "home_team": home_team,
                "away_team": away_team,
                "events": int(len(game)),
                "roster_players": int(len(roster)),
                "possessions": int(len(game_possessions)),
                "package_invalid_actor_rows": package_invalid_actor_rows,
                "informational_non_player_actor_rows": non_player_actors,
                "blocking_issues": blocking_issues,
                "schema_ready": blocking_issues == 0,
                "details": "",
            }
        )

    issue_columns = ["season", "gamecode", "category", "count", "details"]
    return SchemaCoverageResult(
        game_summary=pd.DataFrame(summaries).sort_values(
            ["season", "gamecode"], kind="stable"
        ).reset_index(drop=True),
        issues=pd.DataFrame(issues, columns=issue_columns),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit whether package outputs satisfy the schema contract."
    )
    parser.add_argument("pbp_csv", type=Path)
    parser.add_argument("boxscore_dir", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--competition", default="E")
    args = parser.parse_args()

    pbp = pd.read_csv(args.pbp_csv)
    result = audit_schema_coverage(
        pbp, args.boxscore_dir, competition=args.competition
    )
    ready = result.game_summary["schema_ready"].astype(bool)
    print(f"games={len(result.game_summary)}")
    print(f"schema_ready_games={int(ready.sum())}")
    print(f"blocking_issues={int(result.game_summary['blocking_issues'].sum())}")
    print(
        "informational_non_player_actor_rows="
        f"{int(result.game_summary['informational_non_player_actor_rows'].sum())}"
    )
    print(
        "package_invalid_actor_rows="
        f"{int(result.game_summary['package_invalid_actor_rows'].sum())}"
    )

    if args.output_dir is not None:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        result.game_summary.to_csv(
            args.output_dir / "schema_coverage_games.csv", index=False
        )
        result.issues.to_csv(
            args.output_dir / "schema_coverage_issues.csv", index=False
        )


if __name__ == "__main__":
    main()
