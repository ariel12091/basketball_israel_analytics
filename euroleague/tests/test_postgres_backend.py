from __future__ import annotations

import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "euroleague" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    INSERT_ORDER,
    TABLE_COLUMNS,
    PostgresTransactionBackend,
    _split_sql_statements,
    assert_shadow_schema_compatible,
    finish_load_run,
    start_load_run,
)
from euroleague_possessions.staging import GameBootstrap  # noqa: E402
from euroleague_possessions.transaction_writer import NaturalGameKey  # noqa: E402


DDL_PATH = REPO_ROOT / "euroleague" / "sql" / "001_core_shadow_schema.sql"


class RecordingCursor:
    def __init__(self, statements: list[tuple[str, object]]) -> None:
        self.statements = statements

    def execute(self, sql: str, parameters: object = None) -> None:
        self.statements.append((sql, parameters))

    def close(self) -> None:
        pass


class RecordingConnection:
    def __init__(self) -> None:
        self.statements: list[tuple[str, object]] = []

    def cursor(self) -> RecordingCursor:
        return RecordingCursor(self.statements)


class LoadRunCursor(RecordingCursor):
    def fetchone(self) -> tuple[int]:
        return (73,)


class LoadRunConnection(RecordingConnection):
    def cursor(self) -> LoadRunCursor:
        return LoadRunCursor(self.statements)


class CanonicalActionTest(unittest.TestCase):
    """Canonical actions resolve dimensions while preserving package arrays."""

    def _backend(self, connection: object) -> PostgresTransactionBackend:
        backend = PostgresTransactionBackend(connection, load_run_id=17)
        backend.team_ids = {"AAA": 5, "BBB": 6}
        return backend

    def test_canonical_action_resolves_ids_and_preserves_arrays(self) -> None:
        backend = self._backend(RecordingConnection())
        backend.team_ids = {"AAA": 10, "BBB": 11}
        backend.player_ids = {"P1": 20}
        backend.artifact_ids = {"pbp:E:2025:7": 30}
        lineup_a = ["A1", "A2", "A3", "A4", "A5"]
        lineup_b = ["B1", "B2", "B3", "B4", "B5"]

        row, lineup_key, stint_key = backend._resolve_row(
            "actions",
            game_id=23,
            staged={
                "_source_key": "pbp:E:2025:7",
                "_team_code": "AAA",
                "_player_provider_id": "P1",
                "_possession_offense_team_code": "BBB",
                "source_event_order": 7,
                "season": 2025,
                "gamecode": 7,
                "provider_event_type": 0,
                "provider_play_number": 9,
                "provider_team_code": "AAA",
                "provider_player_id": "P1",
                "play_type": "2FGM",
                "player_name": "Player One",
                "team_name": "Team A",
                "jersey_number": 4,
                "minute": 1,
                "marker_time": "09:10",
                "points_a": 2,
                "points_b": None,
                "comment": None,
                "play_info": "Two pointer made",
                "period": 1,
                "is_home_team": True,
                "lineup_a": lineup_a,
                "lineup_b": lineup_b,
                "validate_on_court_player": True,
                "source_package_version": "0.1.1",
                "synthetic_parent_order": 7,
                "synthetic_ft_trip_id": None,
                "end_possession": True,
                "endpoint_reason": "made_field_goal",
                "grouping_status": "confirmed",
                "grouping_confidence_pct": 100,
                "decision_trace": ["made_fg_endpoint"],
                "parser_version": "0.2.0",
                "game_possession_number": 1,
                "team_possession_number": 1,
            },
        )

        self.assertIsNone(lineup_key)
        self.assertIsNone(stint_key)
        self.assertEqual(row["team_id"], 10)
        self.assertEqual(row["player_id"], 20)
        self.assertEqual(row["possession_offense_team_id"], 11)
        self.assertEqual(row["lineup_a"], lineup_a)
        self.assertEqual(row["lineup_b"], lineup_b)
        self.assertEqual(row["decision_trace"], ["made_fg_endpoint"])
        backend._parameters("actions", row)

class CountAllRowsTest(unittest.TestCase):
    """The batched validation counts must mirror the per-table definition."""

    class _CountCursor(RecordingCursor):
        def __init__(self, statements: list[tuple[str, object]]) -> None:
            super().__init__(statements)
            self.result = tuple(range(len(INSERT_ORDER)))

        def fetchone(self) -> tuple[int, ...]:
            return self.result

    def test_one_statement_carries_every_table_in_insert_order(self) -> None:
        connection = RecordingConnection()
        cursor = self._CountCursor(connection.statements)
        backend = PostgresTransactionBackend(connection, load_run_id=17)

        counts = backend._count_all_rows(cursor, game_id=23)

        self.assertEqual(len(connection.statements), 1, "counts must be one round trip")
        sql, parameters = connection.statements[0]
        self.assertEqual(
            counts, {table: index for index, table in enumerate(INSERT_ORDER)}
        )
        # Audit tables are scoped to the current run; all persisted game facts
        # are directly keyed by game_id.
        self.assertNotIn("lineups", sql)
        self.assertEqual(sql.count("load_run_id = %s"), 3)
        self.assertEqual(len(parameters), len(INSERT_ORDER) + 3)
        self.assertEqual(set(parameters), {23, 17})

    def test_batched_counts_agree_with_the_per_table_counts(self) -> None:
        """_count_rows is the definition; the batched form must mirror it."""
        connection = RecordingConnection()
        backend = PostgresTransactionBackend(connection, load_run_id=17)
        batched = self._CountCursor(connection.statements)
        backend._count_all_rows(batched, game_id=23)
        batched_sql = connection.statements[0][0]

        for table in INSERT_ORDER:
            single = RecordingConnection()
            cursor = self._CountCursor(single.statements)
            backend._count_rows(cursor, table, game_id=23)
            predicate = single.statements[0][0].split("FROM ", 1)[1]
            self.assertIn(
                " ".join(predicate.split()),
                " ".join(batched_sql.split()),
                f"{table} predicate differs between batched and single counts",
            )


class PostgresBackendTest(unittest.TestCase):
    def test_batch_load_run_records_requested_game_scope(self) -> None:
        connection = LoadRunConnection()
        bootstrap = GameBootstrap(
            key=NaturalGameKey("E", 2025, 1),
            package_version="0.1.1",
            collector_version="0.2.0",
            teams=(),
            players=(),
            schedule={},
            source_artifacts=(),
        )

        load_run_id = start_load_run(
            connection,
            bootstrap,
            requested_games=3,
            request_parameters={"scope": "batch", "gamecodes": [1, 2, 3]},
        )

        self.assertEqual(load_run_id, 73)
        insert_sql, parameters = connection.statements[1]
        self.assertIn("INSERT INTO euroleague.load_runs", insert_sql)
        self.assertEqual(parameters[4], 3)
        self.assertIn('"scope":"batch"', str(parameters[5]))
        self.assertEqual(connection.statements[0][0], "BEGIN")
        self.assertEqual(connection.statements[-1][0], "COMMIT")

    def test_partial_batch_run_records_success_failure_and_errors(self) -> None:
        connection = RecordingConnection()

        finish_load_run(
            connection,
            load_run_id=17,
            success=False,
            successful_games=2,
            failed_games=1,
            errors=({"gamecode": 3, "error": "bad game"},),
        )

        self.assertEqual(connection.statements[0], ("BEGIN", None))
        self.assertEqual(
            connection.statements[1],
            ("SELECT euroleague.refresh_app_materialized_views()", None),
        )
        sql, parameters = connection.statements[2]
        self.assertIn("UPDATE euroleague.load_runs", sql)
        self.assertEqual(parameters[0:3], ("partial", 2, 1))
        self.assertIn('"gamecode":3', str(parameters[3]))
        self.assertEqual(parameters[4], 17)
        self.assertEqual(connection.statements[-1], ("COMMIT", None))

    def test_schema_splitter_preserves_semicolons_inside_strings(self) -> None:
        statements = _split_sql_statements(DDL_PATH.read_text(encoding="utf-8"))

        self.assertEqual(len(statements), 45)
        self.assertTrue(statements[0].rstrip().endswith("BEGIN"))
        self.assertEqual(statements[-1], "COMMIT")
        internal_comment = next(
            statement
            for statement in statements
            if "Internal possession-counting" in statement
        )
        self.assertIn("audit grouping; not a separately", internal_comment)

    def test_schema_splitter_preserves_dollar_quoted_function_bodies(self) -> None:
        sql = (
            "CREATE FUNCTION f() RETURNS void LANGUAGE plpgsql AS $body$ "
            "BEGIN PERFORM 'a;b'; END; $body$; SELECT 1;"
        )

        statements = _split_sql_statements(sql)

        self.assertEqual(len(statements), 2)
        self.assertIn("PERFORM 'a;b'; END;", statements[0])
        self.assertEqual(statements[1], "SELECT 1")

    def test_action_delete_clears_actions_based_facts_first(self) -> None:
        connection = RecordingConnection()
        backend = PostgresTransactionBackend(connection, load_run_id=17)

        backend.delete_game_rows("actions", game_id=23)

        executed = [sql for sql, _ in connection.statements]
        self.assertEqual(len(executed), 3, executed)
        self.assertIn("DELETE FROM euroleague.action_team_context_actions", executed[0])
        self.assertIn("DELETE FROM euroleague.matchup_segments_actions", executed[1])
        self.assertIn("DELETE FROM euroleague.actions", executed[2])
        self.assertTrue(all(parameters == (23,) for _, parameters in connection.statements))

    def test_audit_retry_deletes_only_the_current_load_run(self) -> None:
        connection = RecordingConnection()
        backend = PostgresTransactionBackend(connection, load_run_id=17)

        backend.delete_game_rows("game_qa", game_id=23)

        sql, parameters = connection.statements[0]
        self.assertIn("load_run_id = %s", sql)
        self.assertEqual(parameters, (23, 17))

class _SchemaCursor:
    """Returns a fixed pg_tables result, so the guard can be driven directly."""

    def __init__(self, tables: set[str]) -> None:
        self._tables = tables

    def execute(self, sql: str, parameters: object = None) -> None:
        pass

    def fetchall(self) -> list[tuple[str]]:
        return [(name,) for name in sorted(self._tables)]

    def close(self) -> None:
        pass


class _SchemaConnection:
    def __init__(self, tables: set[str]) -> None:
        self._tables = tables

    def cursor(self) -> _SchemaCursor:
        return _SchemaCursor(self._tables)


class ActionTeamContextWiringTest(unittest.TestCase):
    """The derived fact must be known to the guard and refreshed on publish."""

    def test_schema_allowlist_accepts_the_derived_fact(self) -> None:
        """The guard must accept a schema containing the two derived tables.

        Asserted through behaviour, not through inspect.getsource: a text match
        passes when the names appear only in a comment, and fails on a harmless
        rename. Both tables are absent from INSERT_ORDER by design, so the
        allowlist is the only thing that lets publication start.
        """
        existing = set(TABLE_COLUMNS) | {
            "load_runs", "teams", "players", "schedule", "source_artifacts",
            "player_four_factors_by_game", "team_four_factors_by_game",
            "matchup_segments_actions", "action_team_context_actions",
        }
        assert_shadow_schema_compatible(_SchemaConnection(existing))

    def test_schema_allowlist_still_rejects_an_unknown_table(self) -> None:
        """The guard is only worth having if it refuses what it does not know."""
        existing = set(TABLE_COLUMNS) | {
            "load_runs", "teams", "players", "schedule", "source_artifacts",
            "player_four_factors_by_game", "team_four_factors_by_game",
            "matchup_segments_actions", "action_team_context_actions",
            "something_unexpected",
        }
        with self.assertRaises(RuntimeError) as caught:
            assert_shadow_schema_compatible(_SchemaConnection(existing))
        self.assertIn("something_unexpected", str(caught.exception))

    def test_validate_game_refreshes_the_fact_before_four_factors(self) -> None:
        # LoadRunConnection, not RecordingConnection: RecordingCursor has no
        # fetchone(), so the first refresh that reads its result aborts the
        # mock with AttributeError and every later statement goes unrecorded.
        # That would make this test pass or fail on which refresh happens to
        # come first, rather than on their order.
        connection = LoadRunConnection()
        backend = PostgresTransactionBackend(connection, load_run_id=17)
        try:
            backend.validate_game(game_id=23)
        except Exception:
            pass  # LoadRunCursor's 1-tuple cannot satisfy the later count checks

        executed = [sql for sql, _ in connection.statements]
        fact = next(
            i
            for i, s in enumerate(executed)
            if "refresh_actions_consumer_candidates" in s
        )
        player = next(
            i
            for i, s in enumerate(executed)
            if "refresh_player_four_factors_by_game_for_games" in s
        )
        self.assertLess(fact, player, "the fact must be refreshed first")

    def test_validate_game_uses_only_actions_based_segments(self) -> None:
        import inspect

        from euroleague_possessions import postgres_backend

        source = inspect.getsource(postgres_backend.PostgresTransactionBackend.validate_game)
        self.assertIn("matchup_segments_actions", source)
        self.assertNotIn("euroleague.stints", source)


if __name__ == "__main__":
    unittest.main()
