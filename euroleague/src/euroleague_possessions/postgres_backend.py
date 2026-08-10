"""PostgreSQL adapter for atomic EuroLeague per-game snapshot replacement."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from .staging import GameBootstrap
from .transaction_writer import DELETE_ORDER, INSERT_ORDER, NaturalGameKey


SCHEMA = "euroleague"

TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "full_rosters": (
        "game_id",
        "team_id",
        "player_id",
        "load_run_id",
        "source_player_name",
        "jersey_number",
        "is_starter",
        "is_playing",
        "raw_minutes",
        "minutes_seconds",
        "roster_source",
        "boxscore_stats",
    ),
    "team_boxscores": (
        "game_id",
        "team_id",
        "load_run_id",
        "is_home",
        "points",
        "fg2_made",
        "fg2_attempted",
        "fg3_made",
        "fg3_attempted",
        "ft_made",
        "ft_attempted",
        "offensive_rebounds",
        "defensive_rebounds",
        "assists",
        "steals",
        "turnovers",
        "blocks_favour",
        "blocks_against",
        "fouls_committed",
        "fouls_received",
        "raw_totals",
    ),
    "actions_raw": (
        "game_id",
        "source_event_order",
        "load_run_id",
        "source_artifact_id",
        "period",
        "provider_event_type",
        "provider_play_number",
        "team_id",
        "player_id",
        "provider_team_code",
        "provider_player_id",
        "play_type",
        "player_name",
        "team_name",
        "jersey_number",
        "minute",
        "marker_time",
        "points_home",
        "points_away",
        "comment",
        "play_info",
        "raw_event",
    ),
    "actions": (
        "game_id",
        "source_event_order",
        "load_run_id",
        "source_artifact_id",
        "season",
        "gamecode",
        "provider_event_type",
        "provider_play_number",
        "provider_team_code",
        "provider_player_id",
        "play_type",
        "player_name",
        "team_name",
        "jersey_number",
        "minute",
        "marker_time",
        "points_a",
        "points_b",
        "comment",
        "play_info",
        "period",
        "is_home_team",
        "lineup_a",
        "lineup_b",
        "validate_on_court_player",
        "team_id",
        "player_id",
        "source_package_version",
        "synthetic_parent_order",
        "synthetic_ft_trip_id",
        "end_possession",
        "endpoint_reason",
        "grouping_status",
        "grouping_confidence_pct",
        "decision_trace",
        "parser_version",
        "game_possession_number",
        "possession_offense_team_id",
        "team_possession_number",
    ),
    "reconciliation_metrics": (
        "load_run_id",
        "game_id",
        "team_id",
        "metric",
        "pbp_value",
        "official_value",
    ),
    "game_qa": (
        "load_run_id",
        "game_id",
        "total_possessions",
        "possession_difference",
        "same_team_transitions",
        "provisional_ft_rows",
        "unresolved_ft_rows",
        "duplicate_endpoint_incidents",
        "missing_parent_targets",
        "possession_structural_status",
        "possession_review_status",
        "boxscore_metrics_exact",
        "score_progression_exact",
        "score_progression_reconciled",
        "lineup_structure_valid",
        "lineup_invalid_actor_rows",
        "publication_status",
    ),
    "qa_incidents": (
        "load_run_id",
        "game_id",
        "source_event_order",
        "category",
        "severity",
        "status",
        "rule_code",
        "summary",
        "details",
        "resolved_at",
    ),
}

JSON_COLUMNS = {
    ("full_rosters", "boxscore_stats"),
    ("team_boxscores", "raw_totals"),
    ("actions_raw", "raw_event"),
    ("qa_incidents", "details"),
}

GAME_ID_TABLES = frozenset(TABLE_COLUMNS)
LOAD_RUN_TABLES = frozenset(
    {
        "full_rosters",
        "team_boxscores",
        "actions_raw",
        "actions",
        "reconciliation_metrics",
        "game_qa",
        "qa_incidents",
    }
)
AUDIT_TABLES = frozenset(
    {"reconciliation_metrics", "game_qa", "qa_incidents"}
)


@dataclass(frozen=True)
class BootstrapResult:
    load_run_id: int
    game_id: int


def _json_parameter(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _split_sql_statements(sql: str) -> list[str]:
    """Split DDL while preserving strings and dollar-quoted function bodies."""

    statements: list[str] = []
    current: list[str] = []
    in_single = False
    in_double = False
    in_line_comment = False
    dollar_tag: str | None = None
    index = 0
    while index < len(sql):
        char = sql[index]
        following = sql[index + 1] if index + 1 < len(sql) else ""
        if dollar_tag is not None:
            if sql.startswith(dollar_tag, index):
                current.extend(dollar_tag)
                index += len(dollar_tag)
                dollar_tag = None
            else:
                current.append(char)
                index += 1
            continue
        if in_line_comment:
            current.append(char)
            if char == "\n":
                in_line_comment = False
            index += 1
            continue
        if not in_single and not in_double and char == "$":
            end = sql.find("$", index + 1)
            if end != -1:
                candidate = sql[index : end + 1]
                tag_body = candidate[1:-1]
                if not tag_body or (
                    (tag_body[0].isalpha() or tag_body[0] == "_")
                    and all(
                        character.isalnum() or character == "_"
                        for character in tag_body
                    )
                ):
                    dollar_tag = candidate
                    current.extend(candidate)
                    index = end + 1
                    continue
        if not in_single and not in_double and char == "-" and following == "-":
            current.extend((char, following))
            in_line_comment = True
            index += 2
            continue
        if char == "'" and not in_double:
            current.append(char)
            if in_single and following == "'":
                current.append(following)
                index += 2
                continue
            in_single = not in_single
            index += 1
            continue
        if char == '"' and not in_single:
            current.append(char)
            if in_double and following == '"':
                current.append(following)
                index += 2
                continue
            in_double = not in_double
            index += 1
            continue
        if char == ";" and not in_single and not in_double:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            index += 1
            continue
        current.append(char)
        index += 1
    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    if in_single or in_double or dollar_tag is not None:
        raise ValueError("unclosed SQL string, identifier, or dollar quote")
    return statements


def apply_shadow_schema(connection: Any, ddl_path: Path) -> None:
    """Execute the reviewed non-destructive shadow DDL statement by statement."""

    ddl = ddl_path.read_text(encoding="utf-8")
    upper = ddl.upper()
    if "EUROLEAGUE SHADOW SCHEMA" not in upper:
        raise ValueError("shadow DDL safety marker is missing")
    if "DROP " in upper:
        raise ValueError("shadow DDL contains a destructive DROP statement")
    if "BASKETBALL." in upper or "BASKETBALL_TEST." in upper:
        raise ValueError("shadow DDL references an Israeli schema")
    cursor = connection.cursor()
    try:
        for statement in _split_sql_statements(ddl):
            cursor.execute(statement)
    except Exception:
        try:
            cursor.execute("ROLLBACK")
        finally:
            cursor.close()
        raise
    cursor.close()


def read_env_file(path: Path) -> dict[str, str]:
    """Read a simple KEY=VALUE environment file without mutating process env."""

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        values[key.strip()] = value
    return values


def connect_from_env_file(path: Path, direct_port: int = 5432) -> Any:
    """Create a psycopg connection using existing repository credentials."""

    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "psycopg is required for live PostgreSQL loading"
        ) from exc
    values = read_env_file(path)
    missing = [
        key
        for key in ("PG_HOST", "PG_DB", "PG_USER", "PG_PASS")
        if not values.get(key)
    ]
    if missing:
        raise ValueError(f"database environment file missing keys: {missing}")
    return psycopg.connect(
        host=values["PG_HOST"],
        port=direct_port,
        dbname=values["PG_DB"],
        user=values["PG_USER"],
        password=values["PG_PASS"],
        sslmode=values.get("PG_SSLMODE", "require"),
        autocommit=True,
    )


def inspect_target(connection: Any) -> dict[str, Any]:
    cursor = connection.cursor()
    cursor.execute(
        "SELECT current_database(), current_user, "
        "inet_server_addr()::text, inet_server_port(), "
        "to_regnamespace('euroleague')::text"
    )
    row = cursor.fetchone()
    cursor.close()
    return {
        "database": row[0],
        "user": row[1],
        "server_address": row[2],
        "server_port": row[3],
        "euroleague_schema": row[4],
    }


def assert_shadow_schema_compatible(connection: Any) -> None:
    """Reject an existing EuroLeague schema containing unknown base tables."""

    cursor = connection.cursor()
    cursor.execute(
        "SELECT tablename FROM pg_catalog.pg_tables "
        "WHERE schemaname = 'euroleague' ORDER BY tablename"
    )
    existing = {str(row[0]) for row in cursor.fetchall()}
    cursor.close()
    expected = {
        "load_runs",
        "teams",
        "players",
        "schedule",
        "source_artifacts",
        # Derived analytics facts. Not written by the loader -- each is
        # rebuilt by its own refresh_*_for_games() function -- but they live
        # in the schema, so the guard has to know about them or it refuses to
        # publish. Add any new derived table here in the same change that
        # creates it.
        "player_four_factors_by_game",
        "team_four_factors_by_game",
        "matchup_segments_actions",
        "action_team_context_actions",
        *TABLE_COLUMNS.keys(),
    }
    unknown = existing.difference(expected)
    if unknown:
        raise RuntimeError(
            f"existing euroleague schema has unknown tables: {sorted(unknown)}"
        )


def _insert_load_run(
    cursor: Any,
    bootstrap: GameBootstrap,
    *,
    requested_games: int,
    request_parameters: Mapping[str, Any],
) -> int:
    cursor.execute(
        "INSERT INTO euroleague.load_runs ("
        "competition, season, package_version, collector_version, "
        "requested_games, request_parameters"
        ") VALUES (%s, %s, %s, %s, %s, %s::jsonb) "
        "RETURNING load_run_id",
        (
            bootstrap.key.competition,
            bootstrap.key.season,
            bootstrap.package_version,
            bootstrap.collector_version,
            requested_games,
            _json_parameter(request_parameters),
        ),
    )
    return int(cursor.fetchone()[0])


def start_load_run(
    connection: Any,
    bootstrap: GameBootstrap,
    *,
    requested_games: int,
    request_parameters: Mapping[str, Any],
) -> int:
    """Create one run shared by a deterministic competition/season batch."""

    if requested_games < 1:
        raise ValueError("requested_games must be at least 1")
    cursor = connection.cursor()
    try:
        cursor.execute("BEGIN")
        load_run_id = _insert_load_run(
            cursor,
            bootstrap,
            requested_games=requested_games,
            request_parameters=request_parameters,
        )
        cursor.execute("COMMIT")
    except Exception:
        try:
            cursor.execute("ROLLBACK")
        finally:
            cursor.close()
        raise
    cursor.close()
    return load_run_id


def bootstrap_game(
    connection: Any,
    bootstrap: GameBootstrap,
    *,
    load_run_id: int | None = None,
) -> BootstrapResult:
    """Upsert one game's dimensions/artifacts under a new or shared run."""

    cursor = connection.cursor()
    try:
        cursor.execute("BEGIN")
        cursor.execute("SET LOCAL search_path TO euroleague, public")
        if load_run_id is None:
            load_run_id = _insert_load_run(
                cursor,
                bootstrap,
                requested_games=1,
                request_parameters={
                    "season": bootstrap.key.season,
                    "gamecode": bootstrap.key.gamecode,
                    "scope": "one_game_snapshot",
                },
            )
        else:
            load_run_id = int(load_run_id)
            cursor.execute(
                "SELECT 1 FROM euroleague.load_runs "
                "WHERE load_run_id = %s AND competition = %s AND season = %s "
                "AND package_version = %s AND collector_version = %s "
                "AND status = 'running'",
                (
                    load_run_id,
                    bootstrap.key.competition,
                    bootstrap.key.season,
                    bootstrap.package_version,
                    bootstrap.collector_version,
                ),
            )
            if cursor.fetchone() is None:
                raise RuntimeError(
                    "shared load run does not match the game bootstrap"
                )

        team_ids: dict[str, int] = {}
        for team in bootstrap.teams:
            cursor.execute(
                "INSERT INTO euroleague.teams ("
                "competition, provider_team_code, display_name, "
                "first_seen_season, last_seen_season, source_metadata"
                ") VALUES (%s, %s, %s, %s, %s, %s::jsonb) "
                "ON CONFLICT (competition, provider_team_code) DO UPDATE SET "
                "display_name = EXCLUDED.display_name, "
                "first_seen_season = LEAST("
                "COALESCE(euroleague.teams.first_seen_season, EXCLUDED.first_seen_season), "
                "EXCLUDED.first_seen_season), "
                "last_seen_season = GREATEST("
                "COALESCE(euroleague.teams.last_seen_season, EXCLUDED.last_seen_season), "
                "EXCLUDED.last_seen_season), "
                "source_metadata = EXCLUDED.source_metadata "
                "RETURNING team_id",
                (
                    bootstrap.key.competition,
                    team["provider_team_code"],
                    team["display_name"],
                    team["first_seen_season"],
                    team["last_seen_season"],
                    _json_parameter(team["source_metadata"]),
                ),
            )
            team_ids[str(team["provider_team_code"])] = int(
                cursor.fetchone()[0]
            )

        for player in bootstrap.players:
            cursor.execute(
                "INSERT INTO euroleague.players ("
                "competition, provider_player_id, display_name, source_metadata"
                ") VALUES (%s, %s, %s, %s::jsonb) "
                "ON CONFLICT (competition, provider_player_id) DO UPDATE SET "
                "display_name = EXCLUDED.display_name, "
                "source_metadata = EXCLUDED.source_metadata "
                "RETURNING player_id",
                (
                    bootstrap.key.competition,
                    player["provider_player_id"],
                    player["display_name"],
                    _json_parameter(player["source_metadata"]),
                ),
            )
            cursor.fetchone()

        schedule = bootstrap.schedule
        cursor.execute(
            "INSERT INTO euroleague.schedule ("
            "competition, season, gamecode, round_number, phase, scheduled_at, "
            "status, home_team_id, away_team_id, home_points, away_points, "
            "first_seen_load_run_id, last_seen_load_run_id, source_metadata"
            ") VALUES ("
            "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb"
            ") ON CONFLICT (competition, season, gamecode) DO UPDATE SET "
            "round_number = EXCLUDED.round_number, phase = EXCLUDED.phase, "
            "scheduled_at = EXCLUDED.scheduled_at, status = EXCLUDED.status, "
            "home_team_id = EXCLUDED.home_team_id, "
            "away_team_id = EXCLUDED.away_team_id, "
            "home_points = EXCLUDED.home_points, away_points = EXCLUDED.away_points, "
            "last_seen_load_run_id = EXCLUDED.last_seen_load_run_id, "
            "source_metadata = EXCLUDED.source_metadata "
            "RETURNING game_id",
            (
                bootstrap.key.competition,
                bootstrap.key.season,
                bootstrap.key.gamecode,
                schedule["round_number"],
                schedule["phase"],
                schedule["scheduled_at"],
                schedule["status"],
                team_ids[str(schedule["home_team_code"])],
                team_ids[str(schedule["away_team_code"])],
                schedule["home_points"],
                schedule["away_points"],
                load_run_id,
                load_run_id,
                _json_parameter(schedule["source_metadata"]),
            ),
        )
        game_id = int(cursor.fetchone()[0])

        for artifact in bootstrap.source_artifacts:
            cursor.execute(
                "INSERT INTO euroleague.source_artifacts ("
                "load_run_id, game_id, source_key, artifact_type, package_method, "
                "source_endpoint, retrieved_at, http_status, row_count, "
                "content_sha256, storage_uri, payload, metadata"
                ") VALUES ("
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb"
                ")",
                (
                    load_run_id,
                    game_id,
                    artifact["source_key"],
                    artifact["artifact_type"],
                    artifact["package_method"],
                    artifact["source_endpoint"],
                    artifact["retrieved_at"],
                    artifact["http_status"],
                    artifact["row_count"],
                    artifact["content_sha256"],
                    artifact["storage_uri"],
                    _json_parameter(artifact["payload"]),
                    _json_parameter(artifact["metadata"]),
                ),
            )
        cursor.execute("COMMIT")
    except Exception:
        try:
            cursor.execute("ROLLBACK")
        finally:
            cursor.close()
        raise
    cursor.close()
    return BootstrapResult(load_run_id=load_run_id, game_id=game_id)


def finish_load_run(
    connection: Any,
    load_run_id: int,
    success: bool,
    error: str | None = None,
    *,
    successful_games: int | None = None,
    failed_games: int | None = None,
    errors: Sequence[Mapping[str, Any]] | None = None,
) -> None:
    successful = (
        int(successful_games) if successful_games is not None else (1 if success else 0)
    )
    failed = int(failed_games) if failed_games is not None else (0 if success else 1)
    if successful < 0 or failed < 0:
        raise ValueError("load-run game counts cannot be negative")
    status = "completed" if failed == 0 else "failed" if successful == 0 else "partial"
    error_rows: Sequence[Mapping[str, Any]] = (
        errors
        if errors is not None
        else (() if failed == 0 else ({"error": error or "unknown"},))
    )
    cursor = connection.cursor()
    try:
        cursor.execute("BEGIN")
        cursor.execute("SELECT euroleague.refresh_app_materialized_views()")
        cursor.execute(
            "UPDATE euroleague.load_runs SET status = %s, completed_at = now(), "
            "successful_games = %s, failed_games = %s, error_summary = %s::jsonb "
            "WHERE load_run_id = %s",
            (
                status,
                successful,
                failed,
                _json_parameter(error_rows),
                load_run_id,
            ),
        )
        cursor.execute("COMMIT")
    except Exception:
        try:
            cursor.execute("ROLLBACK")
        finally:
            cursor.close()
        raise
    cursor.close()


class PostgresTransactionBackend:
    """Resolve staged natural keys and replace one game's facts atomically."""

    def __init__(self, connection: Any, load_run_id: int) -> None:
        self.connection = connection
        self.load_run_id = int(load_run_id)
        self.game_id: int | None = None
        self.team_ids: dict[str, int] = {}
        self.player_ids: dict[str, int] = {}
        self.artifact_ids: dict[str, int] = {}
        self.expected_counts: dict[str, int] = {
            table: 0 for table in INSERT_ORDER
        }

    def begin(self, key: NaturalGameKey) -> int:
        cursor = self.connection.cursor()
        try:
            cursor.execute("BEGIN")
            cursor.execute("SET LOCAL search_path TO euroleague, public")
            cursor.execute(
                "SELECT game_id FROM euroleague.schedule "
                "WHERE competition = %s AND season = %s AND gamecode = %s",
                (key.competition, key.season, key.gamecode),
            )
            row = cursor.fetchone()
            if row is None:
                raise ValueError(f"schedule key does not exist: {key!r}")
            self.game_id = int(row[0])

            cursor.execute(
                "SELECT provider_team_code, team_id FROM euroleague.teams "
                "WHERE competition = %s",
                (key.competition,),
            )
            self.team_ids = {str(code): int(identifier) for code, identifier in cursor.fetchall()}
            cursor.execute(
                "SELECT provider_player_id, player_id FROM euroleague.players "
                "WHERE competition = %s",
                (key.competition,),
            )
            self.player_ids = {str(code): int(identifier) for code, identifier in cursor.fetchall()}
            cursor.execute(
                "SELECT source_key, source_artifact_id "
                "FROM euroleague.source_artifacts "
                "WHERE load_run_id = %s AND game_id = %s",
                (self.load_run_id, self.game_id),
            )
            self.artifact_ids = {
                str(code): int(identifier) for code, identifier in cursor.fetchall()
            }
            self.expected_counts = {table: 0 for table in INSERT_ORDER}
        except Exception:
            try:
                cursor.execute("ROLLBACK")
            finally:
                cursor.close()
            raise
        cursor.close()
        return self.game_id

    def delete_game_rows(self, table: str, game_id: int) -> None:
        if table not in DELETE_ORDER:
            raise ValueError(f"table is not replaceable: {table}")
        cursor = self.connection.cursor()
        if table == "actions":
            # These two facts are rebuilt from canonical actions during
            # validate_game(), so clear them child-first before replacing the
            # action rows they reference.
            cursor.execute(
                "DELETE FROM euroleague.action_team_context_actions "
                "WHERE game_id = %s",
                (game_id,),
            )
            cursor.execute(
                "DELETE FROM euroleague.matchup_segments_actions "
                "WHERE game_id = %s",
                (game_id,),
            )
            cursor.execute(
                "DELETE FROM euroleague.actions WHERE game_id = %s",
                (game_id,),
            )
        elif table in AUDIT_TABLES:
            cursor.execute(
                f"DELETE FROM euroleague.{table} "
                "WHERE game_id = %s AND load_run_id = %s",
                (game_id, self.load_run_id),
            )
        else:
            cursor.execute(
                f"DELETE FROM euroleague.{table} WHERE game_id = %s",
                (game_id,),
            )
        cursor.close()

    def _lookup(
        self,
        mapping: Mapping[str, int],
        value: Any,
        label: str,
        nullable: bool = False,
    ) -> int | None:
        if value is None and nullable:
            return None
        key = str(value)
        if key not in mapping:
            raise ValueError(f"unresolved {label}: {key!r}")
        return mapping[key]

    def _resolve_row(
        self,
        table: str,
        game_id: int,
        staged: Mapping[str, Any],
    ) -> tuple[dict[str, Any], str | None, str | None]:
        unknown_metadata = {
            key
            for key in staged
            if key.startswith("_")
            and key
            not in {
                "_team_code",
                "_player_provider_id",
                "_source_key",
                "_possession_offense_team_code",
            }
        }
        if unknown_metadata:
            raise ValueError(
                f"unknown staged metadata for {table}: {sorted(unknown_metadata)}"
            )
        row = {key: value for key, value in staged.items() if not key.startswith("_")}
        if table in GAME_ID_TABLES:
            row["game_id"] = game_id
        if table in LOAD_RUN_TABLES:
            row["load_run_id"] = self.load_run_id
        if "_team_code" in staged:
            row["team_id"] = self._lookup(
                self.team_ids,
                staged["_team_code"],
                "team code",
                nullable=table in {"actions_raw", "actions"},
            )
        if "_player_provider_id" in staged:
            row["player_id"] = self._lookup(
                self.player_ids,
                staged["_player_provider_id"],
                "player ID",
                nullable=table in {"actions_raw", "actions"},
            )
        if "_possession_offense_team_code" in staged:
            row["possession_offense_team_id"] = self._lookup(
                self.team_ids,
                staged["_possession_offense_team_code"],
                "possession offense team code",
                nullable=True,
            )
        if "_source_key" in staged:
            row["source_artifact_id"] = self._lookup(
                self.artifact_ids,
                staged["_source_key"],
                "source artifact",
            )
        return row, None, None

    def _insert_sql(self, table: str, returning: str | None = None) -> str:
        columns = TABLE_COLUMNS[table]
        placeholders = [
            "%s::jsonb" if (table, column) in JSON_COLUMNS else "%s"
            for column in columns
        ]
        sql = (
            f"INSERT INTO euroleague.{table} ({', '.join(columns)}) "
            f"VALUES ({', '.join(placeholders)})"
        )
        if returning:
            sql += f" RETURNING {returning}"
        return sql

    def _parameters(self, table: str, row: Mapping[str, Any]) -> tuple[Any, ...]:
        expected = set(TABLE_COLUMNS[table])
        actual = set(row)
        if actual != expected:
            raise ValueError(
                f"{table} columns differ; missing={sorted(expected - actual)}, "
                f"extra={sorted(actual - expected)}"
            )
        return tuple(
            _json_parameter(row[column])
            if (table, column) in JSON_COLUMNS
            else row[column]
            for column in TABLE_COLUMNS[table]
        )

    def insert_rows(
        self,
        table: str,
        game_id: int,
        rows: Sequence[Mapping[str, Any]],
    ) -> None:
        if table not in INSERT_ORDER:
            raise ValueError(f"table is not insertable: {table}")
        self.expected_counts[table] = len(rows)
        resolved = [self._resolve_row(table, game_id, row) for row in rows]
        cursor = self.connection.cursor()
        try:
            cursor.executemany(
                self._insert_sql(table),
                [self._parameters(table, row) for row, _, _ in resolved],
            )
        finally:
            cursor.close()

    def _count_all_rows(self, cursor: Any, game_id: int) -> dict[str, int]:
        """Count every insertable table for a game in ONE round trip.

        Same per-table predicates as _count_rows, gathered into one statement
        to avoid a remote round trip per relation.
        """
        selects = []
        params: list[Any] = []
        for table in INSERT_ORDER:
            if table in AUDIT_TABLES:
                selects.append(
                    f"(SELECT count(*) FROM euroleague.{table} "
                    "WHERE game_id = %s AND load_run_id = %s)"
                )
                params.extend((game_id, self.load_run_id))
            else:
                selects.append(
                    f"(SELECT count(*) FROM euroleague.{table} WHERE game_id = %s)"
                )
                params.append(game_id)
        cursor.execute("SELECT " + ", ".join(selects), params)
        record = cursor.fetchone()
        return {table: int(record[i]) for i, table in enumerate(INSERT_ORDER)}

    def _count_rows(self, cursor: Any, table: str, game_id: int) -> int:
        if table in AUDIT_TABLES:
            cursor.execute(
                f"SELECT count(*) FROM euroleague.{table} "
                "WHERE game_id = %s AND load_run_id = %s",
                (game_id, self.load_run_id),
            )
        else:
            cursor.execute(
                f"SELECT count(*) FROM euroleague.{table} WHERE game_id = %s",
                (game_id,),
            )
        return int(cursor.fetchone()[0])

    def validate_game(self, game_id: int) -> None:
        cursor = self.connection.cursor()
        try:
            mismatches: list[str] = []
            # Rebuild the analytical facts directly from canonical actions
            # before refreshing their player/team consumers.
            cursor.execute(
                "SELECT euroleague.refresh_actions_consumer_candidates("
                "ARRAY[%s]::bigint[])",
                (game_id,),
            )
            cursor.fetchone()
            cursor.execute(
                "SELECT euroleague.refresh_player_four_factors_by_game_for_games("
                "ARRAY[%s]::bigint[])",
                (game_id,),
            )
            analytics_rows = int(cursor.fetchone()[0])
            # Team-grain four factors (migration 006) are maintained the same
            # per-game way as the player-grain fact above. Without this a newly
            # published game has player analytics but no team analytics, and the
            # team ratings surfaces silently omit it.
            cursor.execute(
                "SELECT euroleague.refresh_team_four_factors_by_game_for_games("
                "ARRAY[%s]::bigint[])",
                (game_id,),
            )
            cursor.fetchone()
            actual_counts = self._count_all_rows(cursor, game_id)
            for table in INSERT_ORDER:
                actual = actual_counts[table]
                expected = self.expected_counts[table]
                if actual != expected:
                    mismatches.append(f"{table}: expected {expected}, got {actual}")
            if actual_counts["actions_raw"] != actual_counts["actions"]:
                mismatches.append("raw/canonical action counts differ")
            if actual_counts["game_qa"] != 1:
                mismatches.append("current load run must have exactly one game_qa row")
            # Coverage, not a predicted row count. Predicting the count meant
            # deriving the expectation from matchup_segments, which was a real
            # check only while this refresh derived its own segments. Migration
            # 009 pointed it at the same table, so the comparison would be a
            # value against itself. The bidirectional grain diff now runs in
            # load_games.py --verify-only, where it can still fail.
            if analytics_rows <= 0:
                mismatches.append("player four-factor refresh produced no rows")
            cursor.execute(
                "WITH game_duration AS ("
                "  SELECT game_id, "
                "    (2400 + greatest(max(period) - 4, 0) * 300)::numeric AS seconds "
                "  FROM euroleague.actions WHERE game_id = %s GROUP BY game_id"
                "), team_time AS ("
                "  SELECT game_id, team_id, sum(segment_seconds)::numeric AS seconds "
                "  FROM euroleague.matchup_segments_actions "
                "  WHERE game_id = %s GROUP BY game_id, team_id"
                ") SELECT count(*) FROM team_time tt JOIN game_duration gd USING (game_id) "
                "WHERE tt.seconds IS DISTINCT FROM gd.seconds",
                (game_id, game_id),
            )
            invalid_team_time = int(cursor.fetchone()[0])
            if invalid_team_time:
                mismatches.append(
                    f"{invalid_team_time} team stint budgets differ from game duration"
                )
            cursor.execute(
                "WITH player_totals AS ("
                "  SELECT game_id, team_id, player_id, type_lineup, "
                "    own_starters, opp_starters, "
                "    sum(total_points) AS points, sum(total_poss) AS possessions, "
                "    sum(ts_poss_count) AS ts_possessions, "
                "    sum(oreb_count) AS orebounds, "
                "    sum(oreb_opportunities) AS oreb_opportunities, "
                "    sum(tov_count) AS turnovers, sum(total_fga) AS fga, "
                "    sum(total_fgm) AS fgm, sum(total_ft_attempts) AS fta "
                "  FROM euroleague.player_four_factors_by_game "
                "  WHERE game_id = %s "
                "  GROUP BY game_id, team_id, player_id, type_lineup, "
                "    own_starters, opp_starters"
                "), context_ranges AS ("
                "  SELECT game_id, team_id, type_lineup, own_starters, opp_starters, "
                "    min(points) = max(points) "
                "    AND min(possessions) = max(possessions) "
                "    AND min(ts_possessions) = max(ts_possessions) "
                "    AND min(orebounds) = max(orebounds) "
                "    AND min(oreb_opportunities) = max(oreb_opportunities) "
                "    AND min(turnovers) = max(turnovers) "
                "    AND min(fga) = max(fga) "
                "    AND min(fgm) = max(fgm) "
                "    AND min(fta) = max(fta) AS partition_exact "
                "  FROM player_totals "
                "  GROUP BY game_id, team_id, type_lineup, "
                "    own_starters, opp_starters"
                ") SELECT count(*) FROM context_ranges WHERE NOT partition_exact",
                (game_id,),
            )
            invalid_partitions = int(cursor.fetchone()[0])
            if invalid_partitions:
                mismatches.append(
                    f"{invalid_partitions} player ON/OFF partitions differ"
                )
            if mismatches:
                raise ValueError("database validation failed: " + "; ".join(mismatches))
        finally:
            cursor.close()

    def commit(self) -> None:
        cursor = self.connection.cursor()
        cursor.execute("COMMIT")
        cursor.close()
        self.game_id = None

    def rollback(self) -> None:
        cursor = self.connection.cursor()
        cursor.execute("ROLLBACK")
        cursor.close()
        self.game_id = None
