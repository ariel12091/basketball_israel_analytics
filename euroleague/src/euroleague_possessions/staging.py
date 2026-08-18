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
from .package_lineups import audit_package_lineups, cached_boxscore_frame
from .reconciliation import reconcile_boxscores
from .schema_coverage import _roster_players, _team_sides
from .transaction_writer import GameSnapshot, NaturalGameKey


COLLECTOR_VERSION = "0.2.0"
PARSER_VERSION = "0.2.1"


# Every field emitted by euroleague-api 0.1.1 after lineup enrichment must be
# represented in the canonical actions table. A package upgrade that changes
# this contract is intentionally blocking until its new fields are mapped.
PACKAGE_EVENT_TO_ACTION_COLUMN = {
    "Season": "season",
    "Gamecode": "gamecode",
    "TYPE": "provider_event_type",
    "NUMBEROFPLAY": "provider_play_number",
    "CODETEAM": "provider_team_code",
    "PLAYER_ID": "provider_player_id",
    "PLAYTYPE": "play_type",
    "PLAYER": "player_name",
    "TEAM": "team_name",
    "DORSAL": "jersey_number",
    "MINUTE": "minute",
    "MARKERTIME": "marker_time",
    "POINTS_A": "points_a",
    "POINTS_B": "points_b",
    "COMMENT": "comment",
    "PLAYINFO": "play_info",
    "PERIOD": "period",
    "TRUE_NUMBEROFPLAY": "source_event_order",
    "Lineup_A": "lineup_a",
    "Lineup_B": "lineup_b",
    "IsHomeTeam": "is_home_team",
    "validate_on_court_player": "validate_on_court_player",
}


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


def _package_text(value: Any) -> str | None:
    """Preserve a package string exactly while normalizing only null values."""

    value = _value(value)
    return None if value is None else str(value)


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


def _package_events_by_order(events: pd.DataFrame) -> dict[int, dict[str, Any]]:
    """Return complete package PBP rows keyed by deterministic event order."""

    required = {
        "TRUE_NUMBEROFPLAY",
        "Lineup_A",
        "Lineup_B",
        "validate_on_court_player",
    }
    missing = sorted(required.difference(events.columns))
    if missing:
        raise ValueError(
            "package play-by-play is missing columns: " + ", ".join(missing)
        )

    rows: dict[int, dict[str, Any]] = {}
    for event in events.to_dict("records"):
        order = int(event["TRUE_NUMBEROFPLAY"])
        if order in rows:
            raise ValueError(f"package event order is not unique: {order}")
        rows[order] = _value(event)
    return rows


def _restore_package_home_team_marker(
    events: pd.DataFrame,
    home_team: str,
    away_team: str,
) -> pd.DataFrame:
    """Restore the package-derived home marker lost by combined CSV caches."""

    restored = events.copy()
    values: list[bool | None] = []
    for event in restored.to_dict("records"):
        team_code = _text(event.get("CODETEAM"))
        expected = (
            True
            if team_code == home_team
            else False
            if team_code == away_team
            else None
        )
        existing = _value(event.get("IsHomeTeam"))
        if existing is not None and bool(existing) is not expected:
            raise ValueError(
                "package IsHomeTeam contradicts box-score sides: "
                f"team={team_code!r}, value={existing!r}"
            )
        values.append(expected)
    restored["IsHomeTeam"] = values
    return restored


def _columnar_package_event(event: Mapping[str, Any]) -> dict[str, Any]:
    """Convert one complete package event to the canonical typed columns."""

    expected = set(PACKAGE_EVENT_TO_ACTION_COLUMN)
    actual = set(event)
    if actual != expected:
        raise ValueError(
            "package event columns differ from actions contract; "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )

    lineup_a = _value(event["Lineup_A"])
    lineup_b = _value(event["Lineup_B"])
    if not isinstance(lineup_a, list) or not isinstance(lineup_b, list):
        raise ValueError("package lineups must be lists")

    provider_team_code = _value(event["CODETEAM"])
    play_type = _value(event["PLAYTYPE"])
    if provider_team_code is None or play_type is None:
        raise ValueError("package CODETEAM and PLAYTYPE must be present")

    return {
        "season": int(event["Season"]),
        "gamecode": int(event["Gamecode"]),
        "provider_event_type": int(event["TYPE"]),
        "provider_play_number": int(event["NUMBEROFPLAY"]),
        # Preserve the package's empty string for teamless events. The
        # nullable resolved team_id is handled separately.
        "provider_team_code": str(provider_team_code),
        "provider_player_id": _package_text(event["PLAYER_ID"]),
        "play_type": str(play_type),
        "player_name": _package_text(event["PLAYER"]),
        "team_name": _package_text(event["TEAM"]),
        "jersey_number": _integer(event["DORSAL"]),
        "minute": int(event["MINUTE"]),
        "marker_time": _package_text(event["MARKERTIME"]),
        "points_a": _integer(event["POINTS_A"]),
        "points_b": _integer(event["POINTS_B"]),
        "comment": _package_text(event["COMMENT"]),
        "play_info": _package_text(event["PLAYINFO"]),
        "period": int(event["PERIOD"]),
        "source_event_order": int(event["TRUE_NUMBEROFPLAY"]),
        "lineup_a": [str(player) for player in lineup_a],
        "lineup_b": [str(player) for player in lineup_b],
        "is_home_team": (
            None
            if _value(event["IsHomeTeam"]) is None
            else bool(event["IsHomeTeam"])
        ),
        "validate_on_court_player": bool(event["validate_on_court_player"]),
    }


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
    lineup_events = _restore_package_home_team_marker(
        lineup_events, home_team, away_team
    )
    package_events_by_order = _package_events_by_order(lineup_events)
    source_orders = set(game["TRUE_NUMBEROFPLAY"].astype(int))
    if set(package_events_by_order) != source_orders:
        raise ValueError("package lineup output does not cover every source event")
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
        source_event_order = int(raw["TRUE_NUMBEROFPLAY"])
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
                "source_event_order": source_event_order,
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
                "raw_event": package_events_by_order[source_event_order],
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

    clean_by_order = {
        int(row["source_event_order"]): row for row in clean_rows
    }
    possession_by_endpoint = {
        int(row["endpoint_source_event_order"]): row for row in possession_rows
    }
    action_rows: list[dict[str, Any]] = []
    for source_event_order in sorted(package_events_by_order):
        package_event = package_events_by_order[source_event_order]
        package_columns = _columnar_package_event(package_event)
        decision = clean_by_order[source_event_order]
        possession = possession_by_endpoint.get(source_event_order)
        team_code = _text(package_event["CODETEAM"])
        provider_player_id = _text(package_event["PLAYER_ID"])
        normalized_player_id = (
            provider_player_id
            if (team_code, provider_player_id) in roster_pairs
            else None
        )
        action_rows.append(
            {
                "_source_key": pbp_source_key,
                "_team_code": team_code if team_code in team_codes else None,
                "_player_provider_id": normalized_player_id,
                "_possession_offense_team_code": (
                    None if possession is None else possession["_team_code"]
                ),
                **package_columns,
                "source_package_version": package_version,
                "synthetic_parent_order": decision["synthetic_parent_order"],
                "synthetic_ft_trip_id": decision["synthetic_ft_trip_id"],
                "end_possession": decision["final_end_possession"],
                "endpoint_reason": decision["endpoint_reason"],
                "grouping_status": decision["grouping_status"],
                "grouping_confidence_pct": decision[
                    "grouping_confidence_pct"
                ],
                "decision_trace": decision["decision_trace"],
                "parser_version": decision["parser_version"],
                "game_possession_number": (
                    None
                    if possession is None
                    else possession["game_possession_number"]
                ),
                "team_possession_number": (
                    None
                    if possession is None
                    else possession["team_possession_number"]
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
            "actions": action_rows,
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
