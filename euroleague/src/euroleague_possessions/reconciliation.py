"""Reconcile EuroLeague play-by-play totals with official box-score totals."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from .parser import normalize_events


METRIC_TO_BOXSCORE = {
    "points": "Points",
    "fg2_made": "FieldGoalsMade2",
    "fg2_attempted": "FieldGoalsAttempted2",
    "fg3_made": "FieldGoalsMade3",
    "fg3_attempted": "FieldGoalsAttempted3",
    "ft_made": "FreeThrowsMade",
    "ft_attempted": "FreeThrowsAttempted",
    "offensive_rebounds": "OffensiveRebounds",
    "defensive_rebounds": "DefensiveRebounds",
    "assists": "Assistances",
    "steals": "Steals",
    "turnovers": "Turnovers",
    "blocks_favour": "BlocksFavour",
    "blocks_against": "BlocksAgainst",
    "fouls_committed": "FoulsCommited",
    "fouls_received": "FoulsReceived",
}

CORE_METRICS = tuple(
    metric
    for metric in METRIC_TO_BOXSCORE
    if metric not in {"fouls_committed", "fouls_received"}
)


@dataclass(frozen=True)
class ReconciliationResult:
    team_metrics: pd.DataFrame
    metric_summary: pd.DataFrame
    game_summary: pd.DataFrame
    missing_boxscores: pd.DataFrame
    score_progression: pd.DataFrame
    score_anomalies: pd.DataFrame


def _event_counts(events: pd.DataFrame) -> pd.DataFrame:
    normalized = normalize_events(events)
    normalized = normalized.loc[normalized["team_code"].notna()].copy()
    grouped_rows: list[dict[str, Any]] = []
    for (season, gamecode, team), team_events in normalized.groupby(
        ["season", "gamecode", "team_code"], sort=True
    ):
        counts = team_events["play_type"].value_counts()

        def count(*play_types: str) -> int:
            return int(sum(counts.get(play_type, 0) for play_type in play_types))

        fg2_made = count("2FGM")
        fg3_made = count("3FGM")
        ft_made = count("FTM")
        grouped_rows.append(
            {
                "season": int(season),
                "gamecode": int(gamecode),
                "team_code": team,
                "points": 2 * fg2_made + 3 * fg3_made + ft_made,
                "fg2_made": fg2_made,
                "fg2_attempted": fg2_made + count("2FGA"),
                "fg3_made": fg3_made,
                "fg3_attempted": fg3_made + count("3FGA"),
                "ft_made": ft_made,
                "ft_attempted": ft_made + count("FTA"),
                "offensive_rebounds": count("O"),
                "defensive_rebounds": count("D"),
                "assists": count("AS"),
                "steals": count("ST"),
                "turnovers": count("TO"),
                "blocks_favour": count("FV"),
                "blocks_against": count("AG"),
                # Official FoulsCommited includes player technicals (CMT/CMTI)
                # but excludes bench/coach technical rows (B/C).
                "fouls_committed": count("CM", "CMU", "OF", "CMT", "CMTI"),
                "fouls_received": count("RV"),
            }
        )
    return pd.DataFrame(grouped_rows)


def _team_code(stats_side: dict[str, Any]) -> str:
    team_row = stats_side.get("tmr") or {}
    code = str(team_row.get("Team") or "").strip()
    if code:
        return code
    for player in stats_side.get("PlayersStats") or []:
        code = str(player.get("Team") or "").strip()
        if code:
            return code
    raise ValueError("box-score Stats side has no team code")


def _official_totals(
    boxscore_dir: Path, expected_games: pd.DataFrame, competition: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []
    for season, gamecode in expected_games[["season", "gamecode"]].itertuples(
        index=False, name=None
    ):
        path = boxscore_dir / f"{competition}{int(season)}_{int(gamecode)}.json"
        if not path.exists():
            missing.append(
                {
                    "season": int(season),
                    "gamecode": int(gamecode),
                    "reason": "file_missing",
                }
            )
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            stats = payload["Stats"]
            if not isinstance(stats, list) or len(stats) != 2:
                raise ValueError("Stats does not contain exactly two teams")
            for side_index, side in enumerate(stats):
                total = side["totr"]
                row = {
                    "season": int(season),
                    "gamecode": int(gamecode),
                    "team_code": _team_code(side),
                    "home": side_index == 0,
                }
                for metric, source_column in METRIC_TO_BOXSCORE.items():
                    row[metric] = int(total[source_column])
                rows.append(row)
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            missing.append(
                {
                    "season": int(season),
                    "gamecode": int(gamecode),
                    "reason": f"{type(exc).__name__}: {exc}",
                }
            )
    columns = ["season", "gamecode", "reason"]
    return pd.DataFrame(rows), pd.DataFrame(missing, columns=columns)


def _score_progression(
    events: pd.DataFrame, official: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    normalized = normalize_events(events).sort_values(
        ["season", "gamecode", "period", "source_event_order"], kind="stable"
    )
    game_rows: list[dict[str, Any]] = []
    anomalies: list[dict[str, Any]] = []
    point_value = {"2FGM": 2, "3FGM": 3, "FTM": 1}

    for (season, gamecode), game in normalized.groupby(
        ["season", "gamecode"], sort=True
    ):
        official_game = official.loc[
            (official["season"] == season)
            & (official["gamecode"] == gamecode)
        ]
        home_rows = official_game.loc[official_game["home"]]
        away_rows = official_game.loc[~official_game["home"]]
        if len(home_rows) != 1 or len(away_rows) != 1:
            game_rows.append(
                {
                    "season": int(season),
                    "gamecode": int(gamecode),
                    "home_team": None,
                    "away_team": None,
                    "scoring_events": 0,
                    "missing_score_rows": 0,
                    "nonmonotonic_rows": 0,
                    "unexpected_delta_rows": 0,
                    "wrong_team_delta_rows": 0,
                    "final_score_matches": False,
                    "progression_exact": False,
                }
            )
            continue

        home = home_rows.iloc[0]
        away = away_rows.iloc[0]
        scoring = game.loc[game["play_type"].isin(point_value)].copy()
        previous_a = 0
        previous_b = 0
        missing_score_rows = 0
        nonmonotonic_rows = 0
        unexpected_delta_rows = 0
        wrong_team_delta_rows = 0
        scoring_observations: list[dict[str, Any]] = []
        game_anomaly_start = len(anomalies)

        for row in scoring.itertuples(index=False):
            score_a = row.score_a
            score_b = row.score_b
            reasons: list[str] = []
            expected_home = row.team_code == home["team_code"]
            expected_away = row.team_code == away["team_code"]
            scoring_side_missing = (
                (expected_home and pd.isna(score_a))
                or (expected_away and pd.isna(score_b))
                or (not expected_home and not expected_away)
            )
            if scoring_side_missing:
                missing_score_rows += 1
                reasons.append("missing_scoring_side_score")

            # POINTS_A/POINTS_B are sparse: the provider commonly supplies only
            # the side that changed. Carry the other side forward.
            current_a = previous_a if pd.isna(score_a) else int(score_a)
            current_b = previous_b if pd.isna(score_b) else int(score_b)
            delta_a = current_a - previous_a
            delta_b = current_b - previous_b
            expected = point_value[row.play_type]
            if delta_a < 0 or delta_b < 0:
                nonmonotonic_rows += 1
                reasons.append("nonmonotonic")
            if delta_a + delta_b != expected or min(delta_a, delta_b) != 0:
                unexpected_delta_rows += 1
                reasons.append("unexpected_delta")
            if not expected_home and not expected_away:
                wrong_team_delta_rows += 1
                reasons.append("unknown_scoring_team")
            elif expected_home and (delta_a != expected or delta_b != 0):
                wrong_team_delta_rows += 1
                reasons.append("wrong_home_delta")
            elif expected_away and (delta_b != expected or delta_a != 0):
                wrong_team_delta_rows += 1
                reasons.append("wrong_away_delta")

            if reasons:
                anomalies.append(
                    {
                        "season": int(season),
                        "gamecode": int(gamecode),
                        "period": int(row.period),
                        "source_event_order": int(row.source_event_order),
                        "team_code": row.team_code,
                        "play_type": row.play_type,
                        "score_a": current_a,
                        "score_b": current_b,
                        "delta_a": delta_a,
                        "delta_b": delta_b,
                        "expected_points": expected,
                        "reason": ",".join(reasons),
                        "tolerated_snapshot_lead": False,
                    }
                )
            expected_delta_a = expected if expected_home else 0
            expected_delta_b = expected if expected_away else 0
            scoring_observations.append(
                {
                    "source_event_order": int(row.source_event_order),
                    "delta_a": delta_a,
                    "delta_b": delta_b,
                    "expected_delta_a": expected_delta_a,
                    "expected_delta_b": expected_delta_b,
                }
            )
            previous_a = current_a
            previous_b = current_b

        # In a few provider rows, the score snapshot pre-applies the immediately
        # following made basket. Accept only an exact adjacent pair: event N has
        # both events' score delta and event N+1 has zero delta.
        tolerated_orders: set[int] = set()
        snapshot_lead_pairs = 0
        for first, second in zip(scoring_observations, scoring_observations[1:]):
            combined_a = first["expected_delta_a"] + second["expected_delta_a"]
            combined_b = first["expected_delta_b"] + second["expected_delta_b"]
            if (
                first["delta_a"] == combined_a
                and first["delta_b"] == combined_b
                and second["delta_a"] == 0
                and second["delta_b"] == 0
            ):
                tolerated_orders.update(
                    {first["source_event_order"], second["source_event_order"]}
                )
                snapshot_lead_pairs += 1

        game_anomaly_orders = {
            anomaly["source_event_order"]
            for anomaly in anomalies[game_anomaly_start:]
        }
        for anomaly in anomalies[game_anomaly_start:]:
            anomaly["tolerated_snapshot_lead"] = (
                anomaly["source_event_order"] in tolerated_orders
            )
        unreconciled_anomaly_orders = game_anomaly_orders - tolerated_orders

        final_score_matches = bool(
            previous_a == int(home["points"])
            and previous_b == int(away["points"])
        )
        progression_exact = bool(
            final_score_matches
            and missing_score_rows == 0
            and nonmonotonic_rows == 0
            and unexpected_delta_rows == 0
            and wrong_team_delta_rows == 0
        )
        progression_reconciled = bool(
            final_score_matches
            and missing_score_rows == 0
            and nonmonotonic_rows == 0
            and not unreconciled_anomaly_orders
        )
        game_rows.append(
            {
                "season": int(season),
                "gamecode": int(gamecode),
                "home_team": home["team_code"],
                "away_team": away["team_code"],
                "scoring_events": len(scoring),
                "missing_score_rows": missing_score_rows,
                "nonmonotonic_rows": nonmonotonic_rows,
                "unexpected_delta_rows": unexpected_delta_rows,
                "wrong_team_delta_rows": wrong_team_delta_rows,
                "snapshot_lead_pairs": snapshot_lead_pairs,
                "final_score_a": previous_a,
                "final_score_b": previous_b,
                "official_home_points": int(home["points"]),
                "official_away_points": int(away["points"]),
                "final_score_matches": final_score_matches,
                "progression_exact": progression_exact,
                "progression_reconciled": progression_reconciled,
            }
        )

    anomaly_columns = [
        "season",
        "gamecode",
        "period",
        "source_event_order",
        "team_code",
        "play_type",
        "score_a",
        "score_b",
        "delta_a",
        "delta_b",
        "expected_points",
        "reason",
        "tolerated_snapshot_lead",
    ]
    return pd.DataFrame(game_rows), pd.DataFrame(anomalies, columns=anomaly_columns)


def reconcile_boxscores(
    events: pd.DataFrame,
    boxscore_dir: Path,
    *,
    competition: str = "E",
) -> ReconciliationResult:
    pbp = _event_counts(events)
    expected_games = pbp[["season", "gamecode"]].drop_duplicates()
    official, missing = _official_totals(
        boxscore_dir, expected_games, competition
    )
    score_progression, score_anomalies = _score_progression(events, official)
    merged = pbp.merge(
        official,
        on=["season", "gamecode", "team_code"],
        how="outer",
        suffixes=("_pbp", "_boxscore"),
        indicator=True,
    )

    metric_rows: list[pd.DataFrame] = []
    for metric in METRIC_TO_BOXSCORE:
        comparison = merged[
            ["season", "gamecode", "team_code", "_merge"]
        ].copy()
        comparison["metric"] = metric
        comparison["pbp_value"] = merged[f"{metric}_pbp"]
        comparison["boxscore_value"] = merged[f"{metric}_boxscore"]
        comparison["difference"] = (
            comparison["pbp_value"] - comparison["boxscore_value"]
        )
        comparison["matches"] = (
            comparison["_merge"].eq("both")
            & comparison["difference"].eq(0)
        )
        metric_rows.append(comparison)
    team_metrics = pd.concat(metric_rows, ignore_index=True).sort_values(
        ["season", "gamecode", "team_code", "metric"], kind="stable"
    )
    team_metrics = team_metrics.reset_index(drop=True)

    metric_summary = (
        team_metrics.assign(_available=team_metrics["_merge"].eq("both"))
        .groupby("metric", sort=False)
        .agg(
            comparisons=("matches", "size"),
            available_comparisons=("_available", "sum"),
            matches=("matches", "sum"),
            total_absolute_difference=("difference", lambda values: values.abs().sum()),
            maximum_absolute_difference=("difference", lambda values: values.abs().max()),
        )
        .reset_index()
    )
    metric_summary["match_rate_pct"] = (
        100
        * metric_summary["matches"]
        / metric_summary["available_comparisons"].replace(0, pd.NA)
    )

    game_rows: list[dict[str, Any]] = []
    for (season, gamecode), game in team_metrics.groupby(
        ["season", "gamecode"], sort=True
    ):
        core = game.loc[game["metric"].isin(CORE_METRICS)]
        mismatches = game.loc[~game["matches"], "metric"].drop_duplicates().tolist()
        game_rows.append(
            {
                "season": int(season),
                "gamecode": int(gamecode),
                "team_rows": int(game["team_code"].nunique()),
                "core_comparisons": len(core),
                "core_matches": int(core["matches"].sum()),
                "core_exact": bool(core["matches"].all()),
                "all_exact": bool(game["matches"].all()),
                "mismatched_metrics": ",".join(mismatches),
            }
        )
    game_summary = pd.DataFrame(game_rows)
    game_summary = game_summary.merge(
        score_progression[
            [
                "season",
                "gamecode",
                "final_score_matches",
                "progression_exact",
                "progression_reconciled",
            ]
        ],
        on=["season", "gamecode"],
        how="left",
    )
    return ReconciliationResult(
        team_metrics=team_metrics,
        metric_summary=metric_summary,
        game_summary=game_summary,
        missing_boxscores=missing,
        score_progression=score_progression,
        score_anomalies=score_anomalies,
    )


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("pbp_csv", type=Path)
    parser.add_argument("boxscore_dir", type=Path)
    parser.add_argument("--competition", default="E")
    parser.add_argument("--output-dir", type=Path)
    return parser.parse_args()


def main() -> None:
    args = _arguments()
    events = pd.read_csv(args.pbp_csv)
    result = reconcile_boxscores(
        events, args.boxscore_dir, competition=args.competition
    )
    print(f"games={result.game_summary.shape[0]}")
    print(f"missing_boxscores={result.missing_boxscores.shape[0]}")
    print(
        "core_exact_games="
        f"{int(result.game_summary['core_exact'].sum())}"
    )
    print(
        "score_progression_exact_games="
        f"{int(result.game_summary['progression_exact'].sum())}"
    )
    print(
        "score_progression_reconciled_games="
        f"{int(result.game_summary['progression_reconciled'].sum())}"
    )
    print(f"score_anomaly_rows={len(result.score_anomalies)}")
    print(result.metric_summary.to_string(index=False))

    if args.output_dir is not None:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        result.team_metrics.to_csv(
            args.output_dir / "team_metrics.csv", index=False
        )
        result.metric_summary.to_csv(
            args.output_dir / "metric_summary.csv", index=False
        )
        result.game_summary.to_csv(
            args.output_dir / "game_summary.csv", index=False
        )
        result.missing_boxscores.to_csv(
            args.output_dir / "missing_boxscores.csv", index=False
        )
        result.score_progression.to_csv(
            args.output_dir / "score_progression.csv", index=False
        )
        result.score_anomalies.to_csv(
            args.output_dir / "score_anomalies.csv", index=False
        )


if __name__ == "__main__":
    main()
