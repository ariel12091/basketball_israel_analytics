"""Build deterministic EuroLeague table load plans without database writes."""

from __future__ import annotations

import argparse
import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .counter import count_possessions
from .package_lineups import audit_package_lineups, cached_boxscore_frame
from .reconciliation import METRIC_TO_BOXSCORE
from .schema_coverage import _roster_players, _team_sides


@dataclass(frozen=True)
class LoadPlanResult:
    games: pd.DataFrame
    table_totals: pd.DataFrame
    issues: pd.DataFrame


def canonical_lineup_hash(provider_player_ids: Iterable[str]) -> str:
    """Return a composition hash independent of package list position."""

    canonical = "|".join(sorted(str(value).strip() for value in provider_player_ids))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _run_count(values: Iterable[str]) -> int:
    count = 0
    previous: str | None = None
    for value in values:
        if count == 0 or value != previous:
            count += 1
        previous = value
    return count


def _lineup_identity(
    players: object,
    team_code: str,
    name_to_id: dict[tuple[str, str], str],
) -> tuple[str, tuple[str, ...]]:
    if not isinstance(players, list) or not players:
        raise ValueError(f"{team_code} lineup is not a populated list")
    ids: list[str] = []
    for player in players:
        key = (team_code, str(player).strip())
        if key not in name_to_id:
            raise ValueError(f"lineup player does not resolve: {key!r}")
        ids.append(name_to_id[key])
    return canonical_lineup_hash(ids), tuple(ids)


def build_load_plan(
    pbp: pd.DataFrame,
    boxscore_dir: Path,
    competition: str = "E",
) -> LoadPlanResult:
    """Return deterministic per-game and total row plans with no database I/O."""

    lineup_result = audit_package_lineups(
        pbp, boxscore_dir=boxscore_dir, competition=competition
    )
    possession_result = count_possessions(pbp)
    game_rows: list[dict[str, Any]] = []
    issue_rows: list[dict[str, Any]] = []
    all_team_keys: set[tuple[str, str]] = set()
    all_player_keys: set[tuple[str, str]] = set()

    for (season_value, gamecode_value), game in pbp.groupby(
        ["Season", "Gamecode"], sort=True
    ):
        season = int(season_value)
        gamecode = int(gamecode_value)
        issue = ""
        try:
            boxscore = cached_boxscore_frame(
                boxscore_dir / f"{competition}{season}_{gamecode}.json",
                season,
                gamecode,
                competition=competition,
            )
            home_team, away_team = _team_sides(boxscore)
            roster = _roster_players(boxscore)
            name_groups = roster.groupby(
                ["team_code", "player_name"], sort=False
            )["provider_player_id"].agg(lambda values: sorted(set(values)))
            ambiguous = name_groups.loc[name_groups.map(len).gt(1)]
            if len(ambiguous):
                raise ValueError(
                    f"ambiguous roster names: {list(ambiguous.index)!r}"
                )
            name_to_id = {
                key: values[0] for key, values in name_groups.items()
            }
            starter_ids = {
                team: set(
                    roster.loc[
                        roster["team_code"].eq(team)
                        & roster["IsStarter"].eq(1),
                        "provider_player_id",
                    ].astype(str)
                )
                for team in (home_team, away_team)
            }

            game_lineups = lineup_result.events.loc[
                lineup_result.events["Season"].eq(season)
                & lineup_result.events["Gamecode"].eq(gamecode)
            ].sort_values("TRUE_NUMBEROFPLAY", kind="stable")
            game_possessions = possession_result.possessions.loc[
                possession_result.possessions["season"].eq(season)
                & possession_result.possessions["gamecode"].eq(gamecode)
            ]

            home_sequence: list[str] = []
            away_sequence: list[str] = []
            lineup_members: dict[tuple[str, str], tuple[str, ...]] = {}
            lineup_by_order: dict[int, tuple[str, str]] = {}
            starter_contexts: dict[str, set[tuple[int, int]]] = {
                home_team: set(),
                away_team: set(),
            }
            for event in game_lineups.itertuples(index=False):
                home_hash, home_ids = _lineup_identity(
                    event.Lineup_A, home_team, name_to_id
                )
                away_hash, away_ids = _lineup_identity(
                    event.Lineup_B, away_team, name_to_id
                )
                home_sequence.append(home_hash)
                away_sequence.append(away_hash)
                lineup_members[(home_team, home_hash)] = home_ids
                lineup_members[(away_team, away_hash)] = away_ids
                lineup_by_order[int(event.TRUE_NUMBEROFPLAY)] = (
                    home_hash,
                    away_hash,
                )
                home_starters = sum(
                    player_id in starter_ids[home_team]
                    for player_id in home_ids
                )
                away_starters = sum(
                    player_id in starter_ids[away_team]
                    for player_id in away_ids
                )
                starter_contexts[home_team].add(
                    (home_starters, away_starters)
                )
                starter_contexts[away_team].add(
                    (away_starters, home_starters)
                )

            possession_orders = set(
                game_possessions["source_event_order"].astype(int)
            )
            missing_pws = possession_orders.difference(lineup_by_order)
            invalid_offense = set(
                game_possessions["offense_team"].dropna().astype(str)
            ).difference({home_team, away_team})
            if missing_pws:
                raise ValueError(
                    f"possession endpoints missing lineups: {sorted(missing_pws)!r}"
                )
            if invalid_offense:
                raise ValueError(
                    f"invalid possession offense teams: {sorted(invalid_offense)!r}"
                )

            team_keys = {(competition, home_team), (competition, away_team)}
            player_keys = {
                (competition, provider_id)
                for provider_id in roster["provider_player_id"]
            }
            all_team_keys.update(team_keys)
            all_player_keys.update(player_keys)

            game_rows.append(
                {
                    "season": season,
                    "gamecode": gamecode,
                    "loadable": True,
                    "schedule": 1,
                    "source_artifacts": 3,
                    "team_upsert_candidates": 2,
                    "player_upsert_candidates": len(player_keys),
                    "full_rosters": len(roster),
                    "team_boxscores": 2,
                    "actions_raw": len(game),
                    "actions_clean": len(game),
                    "possessions": len(game_possessions),
                    "lineups": len(lineup_members),
                    "lineup_players": sum(
                        len(members) for members in lineup_members.values()
                    ),
                    "action_lineups": len(game_lineups),
                    "stints": _run_count(home_sequence)
                    + _run_count(away_sequence),
                    "pws": len(game_possessions),
                    "player_four_factors_by_game": sum(
                        int(roster["team_code"].eq(team).sum())
                        * len(starter_contexts[team])
                        * 4
                        for team in (home_team, away_team)
                    ),
                    "reconciliation_metrics": 2 * len(METRIC_TO_BOXSCORE),
                    "game_qa": 1,
                    "issues": "",
                }
            )
        except (OSError, KeyError, TypeError, ValueError) as exc:
            issue = f"{type(exc).__name__}: {exc}"
            issue_rows.append(
                {
                    "season": season,
                    "gamecode": gamecode,
                    "issue": issue,
                }
            )
            game_rows.append(
                {
                    "season": season,
                    "gamecode": gamecode,
                    "loadable": False,
                    "schedule": 0,
                    "source_artifacts": 0,
                    "team_upsert_candidates": 0,
                    "player_upsert_candidates": 0,
                    "full_rosters": 0,
                    "team_boxscores": 0,
                    "actions_raw": 0,
                    "actions_clean": 0,
                    "possessions": 0,
                    "lineups": 0,
                    "lineup_players": 0,
                    "action_lineups": 0,
                    "stints": 0,
                    "pws": 0,
                    "player_four_factors_by_game": 0,
                    "reconciliation_metrics": 0,
                    "game_qa": 0,
                    "issues": issue,
                }
            )

    games = pd.DataFrame(game_rows).sort_values(
        ["season", "gamecode"], kind="stable"
    ).reset_index(drop=True)
    fact_tables = [
        "schedule",
        "full_rosters",
        "team_boxscores",
        "actions_raw",
        "actions_clean",
        "possessions",
        "lineups",
        "lineup_players",
        "action_lineups",
        "stints",
        "pws",
        "player_four_factors_by_game",
        "reconciliation_metrics",
        "game_qa",
    ]
    totals = [
        {"table": "load_runs", "planned_rows": 1},
        {"table": "teams", "planned_rows": len(all_team_keys)},
        {"table": "players", "planned_rows": len(all_player_keys)},
        # Each restartable checkpoint retains schedule, PBP, and box score.
        {
            "table": "source_artifacts",
            "planned_rows": int(games["source_artifacts"].sum()),
        },
    ]
    totals.extend(
        {
            "table": table,
            "planned_rows": int(games[table].sum()),
        }
        for table in fact_tables
    )
    totals.append({"table": "qa_incidents", "planned_rows": 0})
    return LoadPlanResult(
        games=games,
        table_totals=pd.DataFrame(totals),
        issues=pd.DataFrame(
            issue_rows, columns=["season", "gamecode", "issue"]
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plan EuroLeague table loads without connecting to PostgreSQL."
    )
    parser.add_argument("pbp_csv", type=Path)
    parser.add_argument("boxscore_dir", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--competition", default="E")
    args = parser.parse_args()

    result = build_load_plan(
        pd.read_csv(args.pbp_csv),
        args.boxscore_dir,
        competition=args.competition,
    )
    print(f"games={len(result.games)}")
    print(f"loadable_games={int(result.games['loadable'].sum())}")
    print(f"issues={len(result.issues)}")
    print(result.table_totals.to_string(index=False))

    if args.output_dir is not None:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        result.games.to_csv(args.output_dir / "load_plan_games.csv", index=False)
        result.table_totals.to_csv(
            args.output_dir / "load_plan_table_totals.csv", index=False
        )
        result.issues.to_csv(args.output_dir / "load_plan_issues.csv", index=False)


if __name__ == "__main__":
    main()
