from __future__ import annotations

import ast
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "euroleague" / "src"))

from euroleague_possessions.publication import (  # noqa: E402
    Publication, fence_active, publishing, publishing_europe,
)
from euroleague_possessions.batch_pipeline import publish_staged_games  # noqa: E402


def connection():
    conn = MagicMock()
    conn.autocommit = True
    conn.cursor.return_value.fetchone.return_value = (5432,)
    return conn


class PublicationTests(unittest.TestCase):
    def test_lock_precedes_begin_and_failure_closes_session(self):
        conn = connection()
        with self.assertRaisesRegex(RuntimeError, "game failure"):
            with publishing(lambda: conn, "euroleague") as pub:
                pub.fence(MagicMock())
                raise RuntimeError("game failure after commit")
        calls = conn.cursor.return_value.execute.call_args_list
        self.assertIn("pg_advisory_lock", calls[1].args[0])
        self.assertIn("begin_publication", calls[2].args[0])
        self.assertIn("finish_publication", calls[-1].args[0])
        self.assertFalse(calls[-1].args[1][-1])
        conn.close.assert_called_once()

    def test_success_and_double_close(self):
        conn = connection()
        with publishing(lambda: conn, "eurocup") as pub:
            self.assertFalse(pub.closed)
        pub.close(False)
        self.assertTrue(conn.cursor.return_value.execute.call_args.args[1][-1])
        conn.close.assert_called_once()
        with self.assertRaisesRegex(RuntimeError, "closed"):
            pub.fence(MagicMock())

    def test_pooler_and_implicit_transaction_are_rejected(self):
        for port, autocommit in ((6543, True), (5432, False)):
            with self.subTest(port=port, autocommit=autocommit):
                conn = connection()
                conn.autocommit = autocommit
                conn.cursor.return_value.fetchone.return_value = (port,)
                with self.assertRaises(ValueError):
                    Publication.open(lambda: conn, "euroleague")
                conn.close.assert_called_once()

    def test_begin_failure_releases_lock_connection(self):
        conn = connection()
        conn.cursor.return_value.execute.side_effect = [None, None, RuntimeError("begin")]
        with self.assertRaisesRegex(RuntimeError, "begin"):
            Publication.open(lambda: conn, "eurocup")
        conn.close.assert_called_once()

    def test_finish_failure_still_releases_lock(self):
        conn = connection()
        pub = Publication.open(lambda: conn, "euroleague")
        conn.cursor.return_value.execute.side_effect = RuntimeError("finish")
        with self.assertRaisesRegex(RuntimeError, "finish"):
            pub.close(True)
        conn.close.assert_called_once()

    def test_global_refresh_scope_fences_both_competitions_and_restores_context(self):
        conns = [connection(), connection()]
        cursor = MagicMock()
        with publishing_europe(iter(conns).__next__):
            fence_active(cursor)
            self.assertEqual([c.args[1][0] for c in cursor.execute.call_args_list],
                             ["eurocup", "euroleague"])
        cursor.reset_mock()
        fence_active(cursor)
        cursor.execute.assert_not_called()

    def test_empty_run_does_not_open_publication(self):
        with patch("euroleague_possessions.publication.Publication.open") as opened:
            self.assertEqual(publish_staged_games([], env_file=Path("unused")), [])
        opened.assert_not_called()

    def test_verification_runs_before_finish_and_failure_marks_failed(self):
        conns = [connection(), connection()]
        record = MagicMock(status="staged")
        record.staged = object()
        result = MagicMock(status="published")
        def verify(*args):
            for conn in conns:
                self.assertNotIn("finish_publication", conn.cursor.return_value.execute.call_args.args[0])
            raise RuntimeError("verification failure")
        with (patch("euroleague_possessions.batch_pipeline.connect_from_env_file", side_effect=conns),
              patch("euroleague_possessions.batch_pipeline._publish_staged_games", return_value=[result]),
              patch("euroleague_possessions.batch_pipeline._verify_publication", side_effect=verify)):
            with self.assertRaisesRegex(RuntimeError, "verification failure"):
                publish_staged_games([record], env_file=Path("unused"))
        for conn in conns:
            self.assertFalse(conn.cursor.return_value.execute.call_args.args[1][-1])

    def test_every_backend_begin_fences_before_any_other_operation(self):
        path = ROOT / "euroleague/src/euroleague_possessions/postgres_backend.py"
        tree = ast.parse(path.read_text())
        found = 0
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Try, ast.FunctionDef)):
                continue
            for index, statement in enumerate(node.body):
                if (isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call)
                    and ast.unparse(statement.value).endswith("execute('BEGIN')")):
                    found += 1
                    self.assertEqual(ast.unparse(node.body[index + 1]), "fence_active(cursor)")
        self.assertEqual(found, 5)

    def test_real_refresh_wrapper_rolls_back_before_any_served_write_when_fenced_out(self):
        from euroleague_possessions.postgres_backend import refresh_derived_for_games
        from euroleague_possessions import publication

        conn = connection()
        owner = MagicMock()
        owner.fence.side_effect = RuntimeError("fenced_out")
        token = publication._active.set((owner,))
        try:
            with self.assertRaisesRegex(RuntimeError, "fenced_out"):
                refresh_derived_for_games(conn, [123])
        finally:
            publication._active.reset(token)
        self.assertEqual([c.args[0] for c in conn.cursor.return_value.execute.call_args_list],
                         ["BEGIN", "ROLLBACK"])


@unittest.skipUnless(os.environ.get("RUN_DB_TESTS") == "1", "RUN_DB_TESTS=1 required")
class PublicationDatabaseTests(unittest.TestCase):
    """Same SQL definitions in an isolated schema; never write production games."""
    def setUp(self):
        from euroleague_possessions.postgres_backend import connect_from_env_file
        self.factory = lambda: connect_from_env_file(ROOT / "etl/.Renviron", direct_port=5432)
        self.conn = self.factory()
        self.schema = "pubtest_" + uuid4().hex[:16]
        with self.conn.cursor() as cur:
            cur.execute("SELECT to_regclass('basketball_test.publication')")
            self.has_live_publication = cur.fetchone()[0] is not None
            self.before = None
            if self.has_live_publication:
                cur.execute("SELECT league, state, version, owner_run FROM basketball_test.publication ORDER BY league")
                self.before = cur.fetchall()
            cur.execute(f"CREATE SCHEMA {self.schema}")
            sql = (ROOT / "sql/publication/001_publication.sql").read_text().replace("__SCHEMA__", self.schema)
            cur.execute(sql)

    def tearDown(self):
        with self.conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA {self.schema} CASCADE")
            if self.has_live_publication:
                cur.execute("SELECT league, state, version, owner_run FROM basketball_test.publication ORDER BY league")
                self.assertEqual(cur.fetchall(), self.before)
        self.conn.close()

    def status(self):
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {self.schema}.publication_status('euroleague')")
            return cur.fetchone()

    def test_success_failure_stale_and_takeover(self):
        with publishing(self.factory, "euroleague", self.schema) as pub:
            with self.conn.cursor() as cur:
                cur.execute("BEGIN")
                pub.fence(cur)
                cur.execute(f"CREATE TABLE {self.schema}.fixture (id int)")
                cur.execute(f"INSERT INTO {self.schema}.fixture VALUES (1)")
                cur.execute("COMMIT")
        self.assertEqual(self.status(), ("published", 2))
        with self.assertRaisesRegex(RuntimeError, "after commit"):
            with publishing(self.factory, "euroleague", self.schema) as pub:
                with self.conn.cursor() as cur:
                    cur.execute("BEGIN")
                    pub.fence(cur)
                    cur.execute(f"INSERT INTO {self.schema}.fixture VALUES (2)")
                    cur.execute("COMMIT")
                raise RuntimeError("after commit")
        self.assertEqual(self.status(), ("failed", 3))
        stale = Publication.open(self.factory, "euroleague", self.schema)
        stale.connection.close()
        self.assertEqual(self.status()[0], "stale")
        with publishing(self.factory, "euroleague", self.schema):
            with self.conn.cursor() as cur:
                cur.execute("BEGIN")
                with self.assertRaises(Exception):
                    stale.fence(cur)
                cur.execute("ROLLBACK")
            # Exercise the actual loader wrapper: takeover must reject it
            # before any served euroleague data is touched.
            from euroleague_possessions import publication
            from euroleague_possessions.postgres_backend import refresh_derived_for_games
            token = publication._active.set((stale,))
            try:
                with self.assertRaises(Exception):
                    refresh_derived_for_games(self.conn, [0])
            finally:
                publication._active.reset(token)
        self.assertEqual(self.status(), ("published", 5))


if __name__ == "__main__":
    unittest.main()
