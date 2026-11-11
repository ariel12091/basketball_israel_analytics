"""Build and audit play-by-play lineups.

Box scores are normalized through ``euroleague-api`` from cached payloads. The
lineups were built by the package's ``get_pbp_lineups()`` until 2026-11-10;
``build_pbp_lineups()`` keeps its contract and fixes its cross-team pairing of
substitutions (D37).
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
from euroleague_api.boxscore_data import BoxScoreData

logger = logging.getLogger(__name__)


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
    competition: str = "E",
) -> pd.DataFrame:
    """Return one game's events enriched with lineups (``build_pbp_lineups``)."""

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
    return build_pbp_lineups(game, boxscore, competition=competition)


# Provider substitution rows that are wrong at source, keyed by
# (competition, season, gamecode, TRUE_NUMBEROFPLAY). Each entry names the row
# it expects (play type and player), so a correction whose row no longer looks
# like that is skipped with a warning instead of editing a different row.
# "player" replaces the substitution's player for lineup purposes only; None
# ignores the row. The stored event is never edited. Evidence for every entry
# is the box-score minutes and the players acting on the floor; see
# docs/shared_definitions.md D37.
KNOWN_SUBSTITUTION_CORRECTIONS: dict[
    tuple[str, int, int, int], tuple[str, str, Optional[str]]
] = {
    # Empty on purpose. EuroCup 2026-27 game 18 (Trento's Q1 5:06 block of
    # eight substitutions, rows 63-70) was investigated and held back by the
    # user on 2026-11-10: ignoring it removes every duplicate, but about five
    # minutes still disagree with the box score. See D37. Since 0.2.4 the
    # phantom_pair rule repairs that block by itself (D38); the game is still
    # not reloaded.
}


def _substitution_corrections(
    game: pd.DataFrame, competition: str
) -> dict[int, Optional[str]]:
    """Row index -> corrected substitution player (None = ignore the row)."""

    season = int(game["Season"].iloc[0])
    gamecode = int(game["Gamecode"].iloc[0])
    corrections: dict[int, Optional[str]] = {}
    if "TRUE_NUMBEROFPLAY" not in game.columns:
        return corrections
    for (comp, corr_season, corr_game, order), (
        play_type,
        player,
        replacement,
    ) in KNOWN_SUBSTITUTION_CORRECTIONS.items():
        if (comp, corr_season, corr_game) != (competition, season, gamecode):
            continue
        rows = game.index[game["TRUE_NUMBEROFPLAY"].eq(order)]
        row = game.loc[rows[0]] if len(rows) == 1 else None
        if row is None or row["PLAYTYPE"] != play_type or row["PLAYER"] != player:
            logger.warning(
                "substitution correction %s/%s/%s #%s no longer matches the feed; skipped",
                comp, corr_season, corr_game, order,
            )
            continue
        corrections[int(rows[0])] = replacement
    return corrections


# Two feed errors are repaired by rule, from the rest of the game, not by a
# per-game entry (D37). Both rest on one invariant: a player's substitution
# rows alternate IN, OUT, IN, ... and he acts only while on the floor.
#
# wrong_outgoing: the OUT row names a player who is not on the floor (so the
#   pair would change nothing). If exactly one player on the floor is next seen
#   coming IN, with no action of his before that, he is the one who left.
#   (EuroCup 2025-26 game 32: OUT Olinde at Q2 6:45 was Golden.)
# phantom_pair: the outgoing player is OUT again later without coming IN, and
#   acts in between; the incoming player is IN again later without going OUT.
#   The first pair did not happen. (EuroCup 2026-27 game 71, Q2 9:20.)
#
# Anything else -- no candidate, or more than one -- is left as the feed says.
INFER_SUBSTITUTION_REPAIRS = True


class _PlayerTimeline:
    """Each player's substitution rows and action rows, by row position."""

    def __init__(self, play_types, team_codes, players, ignored) -> None:
        self.subs: dict[tuple[str, str], list[tuple[int, str]]] = {}
        self.actions: dict[str, list[int]] = {}
        for position, (play_type, team, player) in enumerate(
            zip(play_types, team_codes, players)
        ):
            if player is None or position in ignored:
                continue
            if play_type in ("IN", "OUT"):
                self.subs.setdefault((team, player), []).append((position, play_type))
            else:
                self.actions.setdefault(player, []).append(position)

    def next_sub(self, team: str, player: str, after: int) -> Optional[tuple[int, str]]:
        return next(
            (event for event in self.subs.get((team, player), []) if event[0] > after),
            None,
        )

    def acts_between(self, player: str, start: int, end: int) -> bool:
        return any(start < position < end for position in self.actions.get(player, []))


def _infer_repair(
    timeline: _PlayerTimeline,
    team: str,
    current: list[str],
    in_pos: int,
    out_pos: int,
    player_in: str,
    player_out: str,
) -> Optional[tuple[str, Optional[str]]]:
    if player_in == player_out or player_in in current:
        return None
    after = max(in_pos, out_pos)
    if player_out not in current:
        candidates = []
        for player in current:
            event = timeline.next_sub(team, player, after)
            if (
                event is not None
                and event[1] == "IN"
                and not timeline.acts_between(player, out_pos, event[0])
            ):
                candidates.append(player)
        if len(candidates) == 1:
            return "wrong_outgoing", candidates[0]
        return None
    out_next = timeline.next_sub(team, player_out, after)
    in_next = timeline.next_sub(team, player_in, after)
    if (
        out_next is not None
        and out_next[1] == "OUT"
        and timeline.acts_between(player_out, out_pos, out_next[0])
        and in_next is not None
        and in_next[1] == "IN"
    ):
        return "phantom_pair", None
    return None


def build_pbp_lineups(
    game: pd.DataFrame,
    boxscore: pd.DataFrame,
    *,
    competition: str = "E",
) -> pd.DataFrame:
    """Lineups on every row of one game, from the box-score starters and the subs.

    Same contract as ``euroleague_api.utils.get_pbp_lineups(validate=True)``:
    each IN or OUT row pairs with the next unprocessed row of the opposite type,
    and the pair swaps the outgoing player for the incoming one. The one change
    is that the pair must be the SAME TEAM's row. The package pairs across teams,
    so when both teams substitute at one stoppage and the feed interleaves their
    rows, it swaps one team's player into the other's lineup or lists a player
    twice (D37). Known feed errors are corrected first
    (``KNOWN_SUBSTITUTION_CORRECTIONS``), and two kinds are repaired by rule
    while pairing (``INFER_SUBSTITUTION_REPAIRS``, D38).
    """

    if game.empty:
        return game
    result = game.copy()
    if result["Gamecode"].nunique() > 1 or result["Season"].nunique() > 1:
        raise ValueError("lineups are built for one game at a time")

    teams = boxscore[["Home", "Team"]].drop_duplicates()
    home_team = teams.loc[teams["Home"] == 1, "Team"].values[0]
    away_team = teams.loc[teams["Home"] == 0, "Team"].values[0]

    def clean(name: str) -> str:
        return name.replace("  ", " ").replace(" , ", ", ").strip()

    starters = boxscore.loc[boxscore["IsStarter"] == 1, ["Team", "Player"]]
    five = {
        team: [clean(player) for player in starters.loc[starters["Team"] == team, "Player"]]
        for team in (home_team, away_team)
    }
    corrections = _substitution_corrections(result, competition)
    play_types = result["PLAYTYPE"].tolist()
    team_codes = result["CODETEAM"].tolist()
    players = [
        corrections.get(index, player) if index in corrections else player
        for index, player in zip(result.index, result["PLAYER"].tolist())
    ]
    ignored = {position for position, index in enumerate(result.index)
               if index in corrections and corrections[index] is None}

    timeline = _PlayerTimeline(play_types, team_codes, players, ignored)
    processed: set[int] = set()
    lineup_home: list[list[str]] = []
    lineup_away: list[list[str]] = []
    for position, (play_type, team) in enumerate(zip(play_types, team_codes)):
        if (
            play_type in ("IN", "OUT")
            and team in five
            and position not in processed
            and position not in ignored
        ):
            opposite = "OUT" if play_type == "IN" else "IN"
            match = next(
                (
                    later
                    for later in range(position + 1, len(play_types))
                    if later not in processed
                    and later not in ignored
                    and play_types[later] == opposite
                    and team_codes[later] == team
                ),
                None,
            )
            if match is None:
                logger.warning(
                    "no matching %s for %s row %s", opposite, team, position
                )
            else:
                processed.update((position, match))
                in_pos, out_pos = (position, match) if play_type == "IN" else (match, position)
                player_in = players[in_pos]
                player_out = players[out_pos]
                current = five[team]
                if INFER_SUBSTITUTION_REPAIRS:
                    repair = _infer_repair(
                        timeline, team, current, in_pos, out_pos, player_in, player_out
                    )
                    if repair is not None:
                        rule, value = repair
                        logger.info(
                            "inferred substitution repair (%s) %s rows %s/%s: %s",
                            rule, team, in_pos, out_pos, value,
                        )
                        if rule == "phantom_pair":
                            player_out = player_in  # the pair did not happen
                        else:
                            player_out = value
                if player_in != player_out and player_out in current:
                    slot = current.index(player_out)
                    five[team] = current[:slot] + [player_in] + current[slot + 1:]
        lineup_home.append(list(five[home_team]))
        lineup_away.append(list(five[away_team]))

    result["Lineup_A"] = lineup_home
    result["Lineup_B"] = lineup_away
    if "IsHomeTeam" not in result.columns:
        result["IsHomeTeam"] = [
            True if team == home_team else False if team == away_team else None
            for team in team_codes
        ]
    result["validate_on_court_player"] = [
        player is None or play_type == "OUT" or player in (home + away)
        for player, play_type, home, away in zip(
            result["PLAYER"].tolist(), play_types, lineup_home, lineup_away
        )
    ]
    if "TRUE_NUMBEROFPLAY" in result.columns:
        # The package renumbers the sorted rows; kept for an identical contract.
        result["TRUE_NUMBEROFPLAY"] = np.arange(len(result))
    return result


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
            enriched = apply_package_lineups(game, boxscore, competition=competition)
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
