"""Apply and audit lineups produced by ``euroleague-api``.

This module intentionally delegates lineup construction to the installed
package.  It only adapts cached box-score payloads and CSV null values so the
package can be used without making another network request.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pandas as pd
from euroleague_api.boxscore_data import BoxScoreData
from euroleague_api.utils import get_pbp_lineups


@dataclass(frozen=True)
class PackageLineupResult:
    events: pd.DataFrame
    game_summary: pd.DataFrame
    invalid_actor_events: pd.DataFrame


class _CachedBoxScoreData(BoxScoreData):
    """Reuse the package's box-score normalization with a cached response."""

    def __init__(self, payload: dict[str, Any], competition: str) -> None:
        super().__init__(competition=competition)
        self._payload = payload

    def get_boxscore_data(
        self,
        season: int,
        gamecode: int,
        boxscore_type: str = "ByQuarter",
    ) -> list[dict[str, Any]]:
        del season, gamecode
        data = self._payload.get(boxscore_type)
        if not isinstance(data, list):
            raise ValueError(f"cached payload has no {boxscore_type} list")
        return data


def cached_boxscore_frame(
    path: Path,
    season: int,
    gamecode: int,
    competition: str = "E",
) -> pd.DataFrame:
    """Normalize a cached response through ``euroleague-api`` itself."""

    payload = json.loads(path.read_text(encoding="utf-8"))
    adapter = _CachedBoxScoreData(payload, competition=competition)
    return adapter.get_players_boxscore_stats(season, gamecode)


def apply_package_lineups(
    game_events: pd.DataFrame,
    boxscore: pd.DataFrame,
) -> pd.DataFrame:
    """Return one game's events enriched by the package lineup function."""

    required = {
        "Season",
        "Gamecode",
        "PLAYTYPE",
        "PLAYER",
        "CODETEAM",
        "MARKERTIME",
    }
    missing = sorted(required.difference(game_events.columns))
    if missing:
        raise ValueError(f"play-by-play is missing columns: {', '.join(missing)}")

    game = game_events.copy()
    if "TRUE_NUMBEROFPLAY" in game.columns:
        game = game.sort_values("TRUE_NUMBEROFPLAY", kind="stable")
    game = game.reset_index(drop=True)

    # A CSV round trip changes provider nulls to NaN.  The package expects
    # player nulls as None and blank team codes as empty strings.
    game["PLAYER"] = game["PLAYER"].where(game["PLAYER"].notna(), None)
    game["CODETEAM"] = game["CODETEAM"].where(
        game["CODETEAM"].notna(), ""
    )
    return get_pbp_lineups(game, boxscore, validate=True)


def _game_audit(
    events: pd.DataFrame,
    boxscore: pd.DataFrame,
) -> dict[str, Any]:
    starter_counts = boxscore.loc[boxscore["IsStarter"] == 1].groupby("Team").size()
    bad_size = (events["Lineup_A"].map(len) != 5) | (
        events["Lineup_B"].map(len) != 5
    )
    duplicate_players = (
        events["Lineup_A"].map(lambda players: len(set(players))) != 5
    ) | (events["Lineup_B"].map(lambda players: len(set(players))) != 5)
    invalid_actor = ~events["validate_on_court_player"].astype(bool)

    return {
        "events": int(len(events)),
        "starter_teams": int(len(starter_counts)),
        "starters_valid": bool(
            len(starter_counts) == 2 and (starter_counts == 5).all()
        ),
        "bad_lineup_size_rows": int(bad_size.sum()),
        "duplicate_player_rows": int(duplicate_players.sum()),
        "invalid_actor_rows": int(invalid_actor.sum()),
        "lineup_structure_valid": bool(not bad_size.any() and not duplicate_players.any()),
    }


def audit_package_lineups(
    pbp: pd.DataFrame,
    boxscore_dir: Path,
    competition: str = "E",
) -> PackageLineupResult:
    """Run package lineups and lightweight QA across a PBP dataframe."""

    all_events: list[pd.DataFrame] = []
    summaries: list[dict[str, Any]] = []
    invalid_events: list[pd.DataFrame] = []
    package_version = version("euroleague-api")

    for (season, gamecode), game in pbp.groupby(
        ["Season", "Gamecode"], sort=True
    ):
        season = int(season)
        gamecode = int(gamecode)
        path = boxscore_dir / f"{competition}{season}_{gamecode}.json"
        base = {
            "season": season,
            "gamecode": gamecode,
            "package_version": package_version,
        }
        try:
            boxscore = cached_boxscore_frame(
                path, season, gamecode, competition=competition
            )
            enriched = apply_package_lineups(game, boxscore)
            audit = _game_audit(enriched, boxscore)
            summaries.append({**base, "status": "ok", "error": "", **audit})
            all_events.append(enriched)

            invalid = enriched.loc[
                ~enriched["validate_on_court_player"].astype(bool)
            ].copy()
            if not invalid.empty:
                invalid_events.append(invalid)
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            summaries.append(
                {
                    **base,
                    "status": "error",
                    "error": f"{type(exc).__name__}: {exc}",
                    "events": int(len(game)),
                    "starter_teams": 0,
                    "starters_valid": False,
                    "bad_lineup_size_rows": 0,
                    "duplicate_player_rows": 0,
                    "invalid_actor_rows": 0,
                    "lineup_structure_valid": False,
                }
            )

    event_columns = list(pbp.columns) + [
        column
        for column in ["IsHomeTeam", "Lineup_A", "Lineup_B", "validate_on_court_player"]
        if column not in pbp.columns
    ]
    events_frame = (
        pd.concat(all_events, ignore_index=True)
        if all_events
        else pd.DataFrame(columns=event_columns)
    )
    invalid_frame = (
        pd.concat(invalid_events, ignore_index=True)
        if invalid_events
        else pd.DataFrame(columns=events_frame.columns)
    )
    return PackageLineupResult(
        events=events_frame,
        game_summary=pd.DataFrame(summaries),
        invalid_actor_events=invalid_frame,
    )


def _csv_safe(events: pd.DataFrame) -> pd.DataFrame:
    result = events.copy()
    for column in ("Lineup_A", "Lineup_B"):
        if column in result.columns:
            result[column] = result[column].map(
                lambda players: json.dumps(players, ensure_ascii=False)
                if isinstance(players, list)
                else players
            )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apply euroleague-api lineups to cached play-by-play data."
    )
    parser.add_argument("pbp_csv", type=Path)
    parser.add_argument("boxscore_dir", type=Path)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--competition", default="E")
    args = parser.parse_args()

    pbp = pd.read_csv(args.pbp_csv)
    result = audit_package_lineups(
        pbp, args.boxscore_dir, competition=args.competition
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    _csv_safe(result.events).to_csv(
        args.output_dir / "package_lineup_events.csv", index=False
    )
    result.game_summary.to_csv(
        args.output_dir / "package_lineup_game_audit.csv", index=False
    )
    _csv_safe(result.invalid_actor_events).to_csv(
        args.output_dir / "package_lineup_invalid_actor_events.csv", index=False
    )

    summary = result.game_summary
    ok = summary["status"].eq("ok")
    print(f"games={len(summary)}")
    print(f"successful_games={int(ok.sum())}")
    print(f"starter_valid_games={int((ok & summary['starters_valid']).sum())}")
    print(
        "lineup_structure_valid_games="
        f"{int((ok & summary['lineup_structure_valid']).sum())}"
    )
    print(f"invalid_actor_rows={int(summary['invalid_actor_rows'].sum())}")


if __name__ == "__main__":
    main()
