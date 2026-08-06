"""Build a complete, database-independent staged snapshot for one game."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from .counter import count_possessions
from .load_plan import canonical_lineup_hash
from .package_lineups import audit_package_lineups, cached_boxscore_frame
from .reconciliation import reconcile_boxscores
from .schema_coverage import _roster_players, _team_sides
from .transaction_writer import GameSnapshot, NaturalGameKey


COLLECTOR_VERSION = "0.2.0"
PARSER_VERSION = "0.2.0"


@dataclass(frozen=True)
class GameBootstrap:
    """Rows written before the replaceable per-game snapshot."""

    key: NaturalGameKey
    package_version: str
    collector_version: str
    teams: Sequence[Mapping[str, Any]]
    players: Sequence[Mapping[str, Any]]
    schedule: Mapping[str, Any]
    source_artifacts: Sequence[Mapping[str, Any]]


@dataclass(frozen=True)
class StagedGame:
    bootstrap: GameBootstrap
    snapshot: GameSnapshot


def _value(value: Any) -> Any:
    """Convert pandas/numpy values to database- and JSON-safe Python values."""

    if isinstance(value, Mapping):
        return {str(key): _value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_value(item) for item in value]
    if value is None:
        return None
    try:
        if bool(pd.isna(value)):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except (TypeError, ValueError):
            pass
    return value


def _text(value: Any) -> str | None:
    value = _value(value)
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _integer(value: Any) -> int | None:
    value = _value(value)
    if value is None:
        return None
    return int(value)


def _jersey(value: Any) -> str | None:
    value = _value(value)
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value).strip() or None


def _minutes_seconds(value: Any) -> int | None:
    text = _text(value)
    if text is None or text.upper() == "DNP" or ":" not in text:
        return None
    minutes, seconds = text.split(":", 1)
    return int(minutes) * 60 + int(seconds)


def _json_sha256(payload: Any) -> str:
    canonical = json.dumps(
        _value(payload),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _lineup_key(team_code: str, lineup_hash: str) -> str:
    return f"{team_code}:{lineup_hash}"


def _stint_key(team_code: str, stint_number: int) -> str:
    return f"{team_code}:{stint_number}"


def _team_display_names(
    game: pd.DataFrame,
    team_codes: Sequence[str],
) -> dict[str, str]:
    names: dict[str, str] = {}
    for team_code in team_codes:
        values = game.loc[
            game["CODETEAM"].astype("string").str.strip().eq(team_code),
            "TEAM",
        ].dropna()
        names[team_code] = (
            str(values.iloc[0]).strip() if len(values) else team_code
        )
    return names


def _official_team_rows(
    reconciliation: pd.DataFrame,
    home_team: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for team_code, team in reconciliation.groupby("team_code", sort=True):
        metrics = {
            str(row.metric): int(row.boxscore_value)
            for row in team.itertuples(index=False)
        }
        rows.append(
            {
                "_team_code": str(team_code),
                "is_home": str(team_code) == home_team,
                "points": metrics["points"],
                "fg2_made": metrics["fg2_made"],
                "fg2_attempted": metrics["fg2_attempted"],
                "fg3_made": metrics["fg3_made"],
                "fg3_attempted": metrics["fg3_attempted"],
                "ft_made": metrics["ft_made"],
                "ft_attempted": metrics["ft_attempted"],
                "offensive_rebounds": metrics["offensive_rebounds"],
                "defensive_rebounds": metrics["defensive_rebounds"],
                "assists": metrics["assists"],
                "steals": metrics["steals"],
                "turnovers": metrics["turnovers"],
                "blocks_favour": metrics["blocks_favour"],
                "blocks_against": metrics["blocks_against"],
                "fouls_committed": metrics["fouls_committed"],
                "fouls_received": metrics["fouls_received"],
                "raw_totals": metrics,
            }
        )
    return rows


def _stints_for_team(
    lineup_events: pd.DataFrame,
    team_code: str,
    lineup_key_by_order: Mapping[int, Mapping[str, str]],
) -> tuple[list[dict[str, Any]], dict[int, str]]:
    orders = lineup_events["TRUE_NUMBEROFPLAY"].astype(int).tolist()
    if not orders:
        return [], {}

    rows: list[dict[str, Any]] = []
    event_stints: dict[int, str] = {}
    start_index = 0
    stint_number = 1
    maximum_order = max(orders)

    def emit(end_index: int) -> None:
        nonlocal start_index, stint_number
        start_order = orders[start_index]
        end_order = (
            orders[end_index] if end_index < len(orders) else maximum_order + 1
        )
        lineup_key = lineup_key_by_order[start_order][team_code]
        window = lineup_events.iloc[start_index:end_index]
        invalid_actor_rows = int(
            (
                ~window["validate_on_court_player"].astype(bool)
                & window["CODETEAM"].astype("string").str.strip().eq(team_code)
            ).sum()
        )
        key = _stint_key(team_code, stint_number)
        for order in orders[start_index:end_index]:
            event_stints[order] = key
        rows.append(
            {
                "_stint_key": key,
                "_team_code": team_code,
                "_lineup_key": lineup_key,
                "stint_number": stint_number,
                "start_event_order": start_order,
                "end_event_order_exclusive": end_order,
                "start_elapsed_seconds": None,
                "end_elapsed_seconds": None,
                "duration_seconds": None,
                "invalid_actor_rows": invalid_actor_rows,
                "lineup_structure_valid": True,
                "qa_status": "review" if invalid_actor_rows else "clear",
                "publishable": invalid_actor_rows == 0,
            }
        )
        stint_number += 1
        start_index = end_index

    previous_key = lineup_key_by_order[orders[0]][team_code]
    for index, order in enumerate(orders[1:], start=1):
        current_key = lineup_key_by_order[order][team_code]
        if current_key != previous_key:
            emit(index)
            previous_key = current_key
    emit(len(orders))
    return rows, event_stints


def build_staged_game(
    pbp: pd.DataFrame,
    boxscore_dir: Path,
    season: int,
    gamecode: int,
    competition: str = "E",
    retrieved_at: datetime | None = None,
    schedule_meta: dict | None = None,
) -> StagedGame:
    """Build all rows needed for one atomic shadow-schema game replacement.

    ``schedule_meta`` carries the round/phase/tip-off fields for this gamecode,
    which the cached box score and play-by-play do not record. It is passed in
    rather than fetched here so staging stays offline and reproducible; see
    ``schedule_collector.fetch_season_schedule_meta``. When it is absent the
    fields stay NULL, which is what left the first three loaded games without a
    date and disabled every date/round/phase filter downstream.
    """

    game = pbp.loc[
        pbp["Season"].eq(season) & pbp["Gamecode"].eq(gamecode)
    ].copy()
    if game.empty:
        raise ValueError(f"game not found: season={season}, gamecode={gamecode}")
    game = game.sort_values("TRUE_NUMBEROFPLAY", kind="stable").reset_index(
        drop=True
    )
    if game["TRUE_NUMBEROFPLAY"].duplicated().any():
        raise ValueError("source event order is not unique")

    boxscore_path = boxscore_dir / f"{competition}{season}_{gamecode}.json"
    boxscore_payload = json.loads(boxscore_path.read_text(encoding="utf-8"))
    boxscore = cached_boxscore_frame(
        boxscore_path,
        season,
        gamecode,
        competition=competition,
    )
    home_team, away_team = _team_sides(boxscore)
    team_codes = (home_team, away_team)
    roster = _roster_players(boxscore)
    roster["provider_player_id"] = (
        roster["provider_player_id"].astype("string").str.strip()
    )
    roster["team_code"] = roster["team_code"].astype("string").str.strip()
    roster["player_name"] = roster["player_name"].astype("string").str.strip()
    roster_pairs = set(zip(roster["team_code"], roster["provider_player_id"]))
    starter_ids = set(
        roster.loc[roster["IsStarter"].eq(1), "provider_player_id"]
    )
    name_to_id = {
        (str(row.team_code), str(row.player_name)): str(row.provider_player_id)
        for row in roster.itertuples(index=False)
    }

    lineup_result = audit_package_lineups(
        game,
        boxscore_dir=boxscore_dir,
        competition=competition,
    )
    lineup_summary = lineup_result.game_summary.iloc[0]
    if lineup_summary["status"] != "ok":
        raise ValueError(f"package lineup failure: {lineup_summary['error']}")
    lineup_events = lineup_result.events.sort_values(
        "TRUE_NUMBEROFPLAY", kind="stable"
    ).reset_index(drop=True)
    package_version = str(lineup_summary["package_version"])

    possession_result = count_possessions(game)
    reconciliation = reconcile_boxscores(
        game,
        boxscore_dir=boxscore_dir,
        competition=competition,
    )
    reconciliation_game = reconciliation.game_summary.iloc[0]
    score_game = reconciliation.score_progression.iloc[0]

    team_names = _team_display_names(game, team_codes)
    team_rows = [
        {
            "provider_team_code": team_code,
            "display_name": team_names[team_code],
            "first_seen_season": season,
            "last_seen_season": season,
            "source_metadata": {"source": "play_by_play"},
        }
        for team_code in team_codes
    ]
    player_rows = [
        {
            "provider_player_id": str(row.provider_player_id),
            "display_name": str(row.player_name),
            "source_metadata": {"source": "boxscore"},
        }
        for row in roster.itertuples(index=False)
    ]

    official_team_rows = _official_team_rows(
        reconciliation.team_metrics,
        home_team,
    )
    official_by_team = {
        str(row["_team_code"]): row for row in official_team_rows
    }
    meta = schedule_meta or {}
    schedule_row = {
        "round_number": meta.get("round_number"),
        "phase": meta.get("phase"),
        "scheduled_at": meta.get("scheduled_at"),
        "status": "played",
        "home_team_code": home_team,
        "away_team_code": away_team,
        "home_points": int(official_by_team[home_team]["points"]),
        "away_points": int(official_by_team[away_team]["points"]),
        "source_metadata": {"source": "cached_boxscore_and_pbp"},
    }

    timestamp = retrieved_at or datetime.now(timezone.utc)
    raw_game_payload = [_value(row) for row in game.to_dict("records")]
    schedule_payload = {
        "competition": competition,
        "season": season,
        "gamecode": gamecode,
        **schedule_row,
    }
    source_artifacts = [
        {
            "source_key": f"schedule:{competition}:{season}:{gamecode}",
            "artifact_type": "schedule",
            "package_method": "exploratory_game_context",
            "source_endpoint": None,
            "retrieved_at": timestamp,
            "http_status": None,
            "row_count": 1,
            "content_sha256": _json_sha256(schedule_payload),
            "storage_uri": None,
            "payload": schedule_payload,
            "metadata": {"scope": "per_game_snapshot"},
        },
        {
            "source_key": f"pbp:{competition}:{season}:{gamecode}",
            "artifact_type": "play_by_play",
            "package_method": "PlayByPlay.get_game_play_by_play_data",
            "source_endpoint": None,
            "retrieved_at": timestamp,
            "http_status": None,
            "row_count": len(raw_game_payload),
            "content_sha256": _json_sha256(raw_game_payload),
            "storage_uri": None,
            "payload": raw_game_payload,
            "metadata": {"package_version": package_version},
        },
        {
            "source_key": f"boxscore:{competition}:{season}:{gamecode}",
            "artifact_type": "boxscore",
            "package_method": None,
            "source_endpoint": "https://live.euroleague.net/api/Boxscore",
            "retrieved_at": timestamp,
            "http_status": None,
            "row_count": len(boxscore),
            "content_sha256": _json_sha256(boxscore_payload),
            "storage_uri": None,
            "payload": _value(boxscore_payload),
            "metadata": {
                "package_version": package_version,
                "extraction_adapter": "direct_reliability_wrapper",
                "reason": (
                    "euroleague-api 0.1.1 does not expose the complete raw "
                    "box-score response needed for restartable persistence"
                ),
            },
        },
    ]

    roster_rows: list[dict[str, Any]] = []
    explicit_roster_columns = {
        "Season",
        "Gamecode",
        "Home",
        "Player_ID",
        "Team",
        "Player",
        "Dorsal",
        "IsStarter",
        "IsPlaying",
        "Minutes",
        "provider_player_id",
        "player_name",
        "team_code",
    }
    for _, row in roster.iterrows():
        roster_rows.append(
            {
                "_team_code": str(row["team_code"]),
                "_player_provider_id": str(row["provider_player_id"]),
                "source_player_name": str(row["player_name"]),
                "jersey_number": _jersey(row["Dorsal"]),
                "is_starter": bool(row["IsStarter"] == 1),
                "is_playing": (
                    None
                    if _value(row["IsPlaying"]) is None
                    else bool(row["IsPlaying"] == 1)
                ),
                "raw_minutes": _text(row["Minutes"]),
                "minutes_seconds": _minutes_seconds(row["Minutes"]),
                "roster_source": "boxscore",
                "boxscore_stats": {
                    column: _value(row[column])
                    for column in roster.columns
                    if column not in explicit_roster_columns
                },
            }
        )

    raw_rows: list[dict[str, Any]] = []
    pbp_source_key = f"pbp:{competition}:{season}:{gamecode}"
    for raw in game.to_dict("records"):
        team_code = _text(raw.get("CODETEAM"))
        provider_player_id = _text(raw.get("PLAYER_ID"))
        normalized_player_id = (
            provider_player_id
            if (team_code, provider_player_id) in roster_pairs
            else None
        )
        raw_rows.append(
            {
                "_source_key": pbp_source_key,
                "_team_code": team_code if team_code in team_codes else None,
                "_player_provider_id": normalized_player_id,
                "source_event_order": int(raw["TRUE_NUMBEROFPLAY"]),
                "period": int(raw["PERIOD"]),
                "provider_event_type": _text(raw.get("TYPE")),
                "provider_play_number": _text(raw.get("NUMBEROFPLAY")),
                "provider_team_code": team_code,
                "provider_player_id": provider_player_id,
                "play_type": _text(raw.get("PLAYTYPE")),
                "player_name": _text(raw.get("PLAYER")),
                "team_name": _text(raw.get("TEAM")),
                "jersey_number": _jersey(raw.get("DORSAL")),
                "minute": _integer(raw.get("MINUTE")),
                "marker_time": _text(raw.get("MARKERTIME")),
                "points_home": _integer(raw.get("POINTS_A")),
                "points_away": _integer(raw.get("POINTS_B")),
                "comment": _text(raw.get("COMMENT")),
                "play_info": _text(raw.get("PLAYINFO")),
                "raw_event": _value(raw),
            }
        )

    clean_rows = [
        {
            "source_event_order": int(row.source_event_order),
            "synthetic_parent_order": int(row.synthetic_parent_order),
            "synthetic_ft_trip_id": _text(row.synthetic_ft_trip_id),
            "final_end_possession": bool(row.final_end_poss),
            "endpoint_reason": _text(row.end_reason),
            "grouping_status": str(row.grouping_status),
            "grouping_confidence_pct": int(row.grouping_confidence_pct),
            "decision_trace": (
                str(row.decision_trace).split(" | ")
                if str(row.decision_trace)
                else []
            ),
            "parser_version": PARSER_VERSION,
        }
        for row in possession_result.events.itertuples(index=False)
    ]

    possession_rows = [
        {
            "_team_code": str(row.offense_team),
            "game_possession_number": int(row.game_possession_number),
            "team_possession_number": int(row.team_possession_number),
            "endpoint_source_event_order": int(row.source_event_order),
            "period": int(row.period),
            "endpoint_reason": str(row.end_reason),
            "grouping_status": str(row.grouping_status),
            "grouping_confidence_pct": int(row.grouping_confidence_pct),
            "parser_version": PARSER_VERSION,
        }
        for row in possession_result.possessions.itertuples(index=False)
    ]

    lineup_rows_by_key: dict[str, dict[str, Any]] = {}
    lineup_player_rows_by_key: dict[str, list[dict[str, Any]]] = {}
    lineup_key_by_order: dict[int, dict[str, str]] = {}
    lineup_ids_by_key: dict[str, tuple[str, ...]] = {}
    action_lineup_rows: list[dict[str, Any]] = []

    for event in lineup_events.itertuples(index=False):
        order = int(event.TRUE_NUMBEROFPLAY)
        keys: dict[str, str] = {}
        for team_code, members in (
            (home_team, event.Lineup_A),
            (away_team, event.Lineup_B),
        ):
            if not isinstance(members, list):
                raise ValueError(f"lineup is not a list at event {order}")
            provider_ids = tuple(
                name_to_id[(team_code, str(player).strip())]
                for player in members
            )
            lineup_hash = canonical_lineup_hash(provider_ids)
            key = _lineup_key(team_code, lineup_hash)
            keys[team_code] = key
            lineup_ids_by_key[key] = provider_ids
            if key not in lineup_rows_by_key:
                unique_count = len(set(provider_ids))
                structure_valid = len(provider_ids) == 5 and unique_count == 5
                lineup_rows_by_key[key] = {
                    "_lineup_key": key,
                    "_team_code": team_code,
                    "lineup_hash": lineup_hash,
                    "player_count": len(provider_ids),
                    "starter_count": sum(
                        player_id in starter_ids for player_id in provider_ids
                    ),
                    "structure_valid": structure_valid,
                    "source_package_version": package_version,
                }
                lineup_player_rows_by_key[key] = [
                    {
                        "_lineup_key": key,
                        "_player_provider_id": player_id,
                        "package_slot": slot,
                        "is_starter": player_id in starter_ids,
                    }
                    for slot, player_id in enumerate(provider_ids, start=1)
                ]
        lineup_key_by_order[order] = keys
        action_lineup_rows.append(
            {
                "source_event_order": order,
                "_home_lineup_key": keys[home_team],
                "_away_lineup_key": keys[away_team],
                "validate_on_court_player": bool(
                    event.validate_on_court_player
                ),
                "lineup_structure_valid": bool(
                    lineup_rows_by_key[keys[home_team]]["structure_valid"]
                    and lineup_rows_by_key[keys[away_team]]["structure_valid"]
                ),
                "source_package_version": package_version,
            }
        )

    home_stints, home_stint_by_order = _stints_for_team(
        lineup_events,
        home_team,
        lineup_key_by_order,
    )
    away_stints, away_stint_by_order = _stints_for_team(
        lineup_events,
        away_team,
        lineup_key_by_order,
    )
    stint_rows = home_stints + away_stints
    stint_by_order = {
        home_team: home_stint_by_order,
        away_team: away_stint_by_order,
    }

    action_validation = {
        int(row.TRUE_NUMBEROFPLAY): bool(row.validate_on_court_player)
        for row in lineup_events.itertuples(index=False)
    }
    pws_rows: list[dict[str, Any]] = []
    for possession in possession_result.possessions.itertuples(index=False):
        order = int(possession.source_event_order)
        offense_team = str(possession.offense_team)
        defense_team = away_team if offense_team == home_team else home_team
        offense_lineup_key = lineup_key_by_order[order][offense_team]
        defense_lineup_key = lineup_key_by_order[order][defense_team]
        pws_rows.append(
            {
                "game_possession_number": int(
                    possession.game_possession_number
                ),
                "_offense_lineup_key": offense_lineup_key,
                "_defense_lineup_key": defense_lineup_key,
                "_offense_stint_key": stint_by_order[offense_team][order],
                "_defense_stint_key": stint_by_order[defense_team][order],
                "num_starters_offense": sum(
                    player_id in starter_ids
                    for player_id in lineup_ids_by_key[offense_lineup_key]
                ),
                "num_starters_defense": sum(
                    player_id in starter_ids
                    for player_id in lineup_ids_by_key[defense_lineup_key]
                ),
                "lineup_validation_clear": bool(
                    action_validation[order]
                    and lineup_rows_by_key[offense_lineup_key][
                        "structure_valid"
                    ]
                    and lineup_rows_by_key[defense_lineup_key][
                        "structure_valid"
                    ]
                ),
            }
        )

    reconciliation_rows = [
        {
            "_team_code": str(row.team_code),
            "metric": str(row.metric),
            "pbp_value": int(row.pbp_value),
            "official_value": int(row.boxscore_value),
        }
        for row in reconciliation.team_metrics.itertuples(index=False)
    ]

    possession_qa = possession_result.game_qa.iloc[0]
    lineup_structure_valid = bool(lineup_summary["lineup_structure_valid"])
    lineup_invalid_actor_rows = int(lineup_summary["invalid_actor_rows"])
    release_checks_clear = bool(
        possession_qa["structural_status"] == "pass"
        and reconciliation_game["all_exact"]
        and reconciliation_game["progression_reconciled"]
        and lineup_structure_valid
    )
    review_needed = bool(
        possession_qa["review_status"] == "review"
        or lineup_invalid_actor_rows
    )
    publication_status = (
        "clear"
        if release_checks_clear and not review_needed
        else "review" if release_checks_clear else "blocked"
    )
    game_qa_rows = [
        {
            "total_possessions": int(possession_qa["total_possessions"]),
            "possession_difference": int(
                possession_qa["possession_difference"]
            ),
            "same_team_transitions": int(
                possession_qa["same_team_transitions"]
            ),
            "provisional_ft_rows": int(
                possession_qa["provisional_ft_rows"]
            ),
            "unresolved_ft_rows": int(possession_qa["unresolved_ft_rows"]),
            "duplicate_endpoint_incidents": int(
                possession_qa["duplicate_endpoint_incidents"]
            ),
            "missing_parent_targets": int(
                possession_qa["missing_parent_targets"]
            ),
            "possession_structural_status": str(
                possession_qa["structural_status"]
            ),
            "possession_review_status": str(possession_qa["review_status"]),
            "boxscore_metrics_exact": bool(reconciliation_game["all_exact"]),
            "score_progression_exact": bool(score_game["progression_exact"]),
            "score_progression_reconciled": bool(
                score_game["progression_reconciled"]
            ),
            "lineup_structure_valid": lineup_structure_valid,
            "lineup_invalid_actor_rows": lineup_invalid_actor_rows,
            "publication_status": publication_status,
        }
    ]

    key = NaturalGameKey(
        competition=competition,
        season=season,
        gamecode=gamecode,
    )
    snapshot = GameSnapshot(
        key=key,
        rows={
            "full_rosters": roster_rows,
            "team_boxscores": official_team_rows,
            "actions_raw": raw_rows,
            "actions_clean": clean_rows,
            "possessions": possession_rows,
            "lineups": list(lineup_rows_by_key.values()),
            "lineup_players": [
                row
                for key_rows in lineup_player_rows_by_key.values()
                for row in key_rows
            ],
            "action_lineups": action_lineup_rows,
            "stints": stint_rows,
            "pws": pws_rows,
            "reconciliation_metrics": reconciliation_rows,
            "game_qa": game_qa_rows,
            "qa_incidents": (),
        },
    )
    snapshot.validate()
    bootstrap = GameBootstrap(
        key=key,
        package_version=package_version,
        collector_version=COLLECTOR_VERSION,
        teams=team_rows,
        players=player_rows,
        schedule=schedule_row,
        source_artifacts=source_artifacts,
    )
    return StagedGame(bootstrap=bootstrap, snapshot=snapshot)


def staged_counts(staged: StagedGame) -> dict[str, int]:
    """Return deterministic bootstrap and snapshot row counts."""

    counts = {
        "teams": len(staged.bootstrap.teams),
        "players": len(staged.bootstrap.players),
        "schedule": 1,
        "source_artifacts": len(staged.bootstrap.source_artifacts),
    }
    counts.update(
        {table: len(rows) for table, rows in staged.snapshot.rows.items()}
    )
    return counts
