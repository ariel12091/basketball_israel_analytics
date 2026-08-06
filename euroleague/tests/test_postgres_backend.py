from __future__ import annotations

import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "euroleague" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions.postgres_backend import (  # noqa: E402
    PostgresTransactionBackend,
    _split_sql_statements,
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

    def test_lineup_player_delete_is_scoped_through_game_lineups(self) -> None:
        connection = RecordingConnection()
        backend = PostgresTransactionBackend(connection, load_run_id=17)

        backend.delete_game_rows("lineup_players", game_id=23)

        sql, parameters = connection.statements[0]
        self.assertIn("USING euroleague.lineups", sql)
        self.assertIn("l.game_id = %s", sql)
        self.assertEqual(parameters, (23,))

    def test_audit_retry_deletes_only_the_current_load_run(self) -> None:
        connection = RecordingConnection()
        backend = PostgresTransactionBackend(connection, load_run_id=17)

        backend.delete_game_rows("game_qa", game_id=23)

        sql, parameters = connection.statements[0]
        self.assertIn("load_run_id = %s", sql)
        self.assertEqual(parameters, (23, 17))

    def test_staged_lineup_resolves_team_and_run_ids(self) -> None:
        backend = PostgresTransactionBackend(RecordingConnection(), load_run_id=17)
        backend.team_ids = {"AAA": 5}
        row, lineup_key, stint_key = backend._resolve_row(
            "lineups",
            game_id=23,
            staged={
                "_lineup_key": "AAA:hash",
                "_team_code": "AAA",
                "lineup_hash": "a" * 64,
                "player_count": 5,
                "starter_count": 3,
                "structure_valid": True,
                "source_package_version": "0.1.1",
            },
        )

        self.assertEqual(lineup_key, "AAA:hash")
        self.assertIsNone(stint_key)
        self.assertEqual(row["game_id"], 23)
        self.assertEqual(row["team_id"], 5)
        self.assertEqual(row["load_run_id"], 17)
        backend._parameters("lineups", row)


if __name__ == "__main__":
    unittest.main()
