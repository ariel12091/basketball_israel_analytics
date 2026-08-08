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


class GeneratedIdCursor(RecordingCursor):
    """Stand in for psycopg's ``executemany(..., returning=True)``.

    It yields one result set per input row, in input order, read with
    ``fetchone()`` and advanced with ``nextset()``. ``result_sets`` overrides
    how many come back, to simulate a stream that does not line up with the
    rows inserted.
    """

    def __init__(
        self,
        statements: list[tuple[str, object]],
        first_id: int,
        *,
        result_sets: int | None = None,
    ) -> None:
        super().__init__(statements)
        self.first_id = first_id
        self.result_sets = result_sets
        self.executemany_calls: list[tuple[str, int, bool]] = []
        self._sets: list[tuple[object, ...] | None] = []
        self._index = 0

    def executemany(
        self,
        sql: str,
        parameters_seq: object,
        *,
        returning: bool = False,
    ) -> None:
        rows = list(parameters_seq)  # type: ignore[arg-type]
        self.statements.append((sql, rows))
        self.executemany_calls.append((sql, len(rows), returning))
        if not returning:
            return
        count = len(rows) if self.result_sets is None else self.result_sets
        self._sets = [(self.first_id + index,) for index in range(count)]
        self._index = 0

    def fetchone(self) -> tuple[object, ...] | None:
        if self._index >= len(self._sets):
            return None
        return self._sets[self._index]

    def nextset(self) -> bool | None:
        self._index += 1
        return True if self._index < len(self._sets) else None


class GeneratedIdConnection(RecordingConnection):
    def __init__(self, **cursor_kwargs: object) -> None:
        super().__init__()
        self.cursor_kwargs = cursor_kwargs
        self.cursors: list[GeneratedIdCursor] = []

    def cursor(self) -> GeneratedIdCursor:
        cursor = GeneratedIdCursor(self.statements, **self.cursor_kwargs)  # type: ignore[arg-type]
        self.cursors.append(cursor)
        return cursor


def _staged_lineup(team_code: str, suffix: str) -> dict[str, object]:
    return {
        "_lineup_key": f"{team_code}:{suffix}",
        "_team_code": team_code,
        "lineup_hash": suffix * 64,
        "player_count": 5,
        "starter_count": 5,
        "structure_valid": True,
        "source_package_version": "0.1.1",
    }


def _staged_stint(team_code: str, number: int) -> dict[str, object]:
    return {
        "_stint_key": f"{team_code}:{number}",
        "_lineup_key": f"{team_code}:a",
        "_team_code": team_code,
        "stint_number": number,
        "start_event_order": number * 10,
        "end_event_order_exclusive": number * 10 + 10,
        "start_elapsed_seconds": 0,
        "end_elapsed_seconds": 60,
        "duration_seconds": 60,
        "invalid_actor_rows": 0,
        "lineup_structure_valid": True,
        "qa_status": "clear",
        "publishable": True,
    }


class BatchedGeneratedIdTest(unittest.TestCase):
    """The lineups/stints pipelined insert and its generated-id mapping."""

    def _backend(self, connection: object) -> PostgresTransactionBackend:
        backend = PostgresTransactionBackend(connection, load_run_id=17)
        backend.team_ids = {"AAA": 5, "BBB": 6}
        return backend

    def test_lineup_ids_pair_with_their_own_row_in_one_round_trip(self) -> None:
        connection = GeneratedIdConnection(first_id=100)
        backend = self._backend(connection)

        backend.insert_rows(
            "lineups",
            game_id=23,
            rows=[
                _staged_lineup("AAA", "a"),
                _staged_lineup("AAA", "b"),
                _staged_lineup("BBB", "c"),
            ],
        )

        self.assertEqual(
            backend.lineup_ids, {"AAA:a": 100, "AAA:b": 101, "BBB:c": 102}
        )
        calls = connection.cursors[0].executemany_calls
        self.assertEqual(len(calls), 1, "lineups must cost one round trip")
        sql, row_count, returning = calls[0]
        self.assertEqual((row_count, returning), (3, True))
        self.assertTrue(sql.endswith(" RETURNING lineup_id"))
        self.assertEqual(sql.count("VALUES ("), 1, "one row per parameter set")

    def test_stint_ids_pair_with_their_own_row_in_one_round_trip(self) -> None:
        connection = GeneratedIdConnection(first_id=500)
        backend = self._backend(connection)
        backend.lineup_ids = {"AAA:a": 100, "BBB:a": 101}

        backend.insert_rows(
            "stints",
            game_id=23,
            rows=[
                _staged_stint("AAA", 1),
                _staged_stint("BBB", 1),
                _staged_stint("AAA", 2),
            ],
        )

        self.assertEqual(
            backend.stint_ids, {"AAA:1": 500, "BBB:1": 501, "AAA:2": 502}
        )
        calls = connection.cursors[0].executemany_calls
        self.assertEqual(len(calls), 1, "stints must cost one round trip")
        self.assertEqual(calls[0][1:], (3, True))
        self.assertTrue(calls[0][0].endswith(" RETURNING stint_id"))

    def test_missing_staged_key_is_refused_before_insert(self) -> None:
        connection = GeneratedIdConnection(first_id=100)
        backend = self._backend(connection)
        keyless = dict(_staged_lineup("AAA", "a"), _lineup_key=None)

        with self.assertRaisesRegex(ValueError, "missing _lineup_key"):
            backend.insert_rows("lineups", game_id=23, rows=[keyless])

        self.assertFalse(
            connection.cursors[0].executemany_calls,
            "the bad row must be caught before anything is written",
        )

    def test_too_few_result_sets_is_an_error_not_a_silent_gap(self) -> None:
        connection = GeneratedIdConnection(first_id=100, result_sets=2)
        backend = self._backend(connection)

        with self.assertRaisesRegex(ValueError, "result sets for 3 inserted rows"):
            backend.insert_rows(
                "lineups",
                game_id=23,
                rows=[
                    _staged_lineup("AAA", "a"),
                    _staged_lineup("AAA", "b"),
                    _staged_lineup("BBB", "c"),
                ],
            )

    def test_too_many_result_sets_is_an_error(self) -> None:
        connection = GeneratedIdConnection(first_id=100, result_sets=3)
        backend = self._backend(connection)

        with self.assertRaisesRegex(ValueError, "more result sets"):
            backend.insert_rows(
                "lineups",
                game_id=23,
                rows=[_staged_lineup("AAA", "a"), _staged_lineup("AAA", "b")],
            )

    def test_empty_batch_writes_nothing(self) -> None:
        connection = GeneratedIdConnection(first_id=100)
        backend = self._backend(connection)

        backend.insert_rows("lineups", game_id=23, rows=[])

        self.assertEqual(backend.lineup_ids, {})
        self.assertFalse(connection.cursors[0].executemany_calls)


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
        # lineup_players is counted through its lineups, audit tables are
        # scoped to the current run, everything else is plain game_id.
        self.assertIn("JOIN euroleague.lineups AS l ON l.lineup_id = lp.lineup_id", sql)
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
            "matchup_segments", "action_team_context",
        }
        assert_shadow_schema_compatible(_SchemaConnection(existing))

    def test_schema_allowlist_still_rejects_an_unknown_table(self) -> None:
        """The guard is only worth having if it refuses what it does not know."""
        existing = set(TABLE_COLUMNS) | {
            "load_runs", "teams", "players", "schedule", "source_artifacts",
            "player_four_factors_by_game", "team_four_factors_by_game",
            "matchup_segments", "action_team_context", "something_unexpected",
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
            if "refresh_action_team_context_for_games" in s
        )
        player = next(
            i
            for i, s in enumerate(executed)
            if "refresh_player_four_factors_by_game_for_games" in s
        )
        self.assertLess(fact, player, "the fact must be refreshed first")

    def test_deleting_lineups_clears_the_derived_fact_first(self) -> None:
        """A republish must not be blocked by the fact's FK onto lineups.

        The composite foreign keys are deliberate, so the derived rows have to
        go before the lineups they reference. Without this the probe fails with
        ForeignKeyViolation on matchup_segments_game_id_own_lineup_id_fkey.
        """
        connection = RecordingConnection()
        backend = PostgresTransactionBackend(connection, load_run_id=17)
        backend.delete_game_rows("lineups", game_id=23)

        executed = [sql for sql, _ in connection.statements]
        self.assertEqual(len(executed), 3, executed)
        self.assertIn("DELETE FROM euroleague.action_team_context", executed[0])
        self.assertIn("DELETE FROM euroleague.matchup_segments", executed[1])
        self.assertIn("DELETE FROM euroleague.lineups", executed[2])


if __name__ == "__main__":
    unittest.main()
