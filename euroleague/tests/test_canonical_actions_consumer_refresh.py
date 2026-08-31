from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "sql" / "048_canonical_actions_consumer_refresh.sql").read_text(encoding="utf-8")
SCRIPT = (ROOT / "scripts" / "apply_048_canonical_actions_consumer_refresh.py").read_text(encoding="utf-8")


class CanonicalActionsConsumerRefreshTest(unittest.TestCase):
    def test_migration_is_one_literal_function_not_a_catalog_patch(self):
        self.assertEqual(
            1,
            SQL.count("CREATE OR REPLACE FUNCTION euroleague.refresh_actions_consumer_candidates"),
        )
        for forbidden in ("pg_get_functiondef", "p.prosrc", "EXECUTE definition"):
            self.assertNotIn(forbidden, SQL)

    def test_canonical_body_contains_both_historical_changes(self):
        self.assertNotIn("actions-v1", SQL)
        self.assertEqual(2, SQL.count("'actions-v2'"))
        self.assertIn("euroleague.effective_period(a.period, a.minute, a.play_type)", SQL)
        self.assertIn("euroleague.effective_period(sd.period, sd.minute, sd.play_type)", SQL)
        self.assertIn("event_sides AS MATERIALIZED", SQL)
        self.assertIn("lineup_sided AS MATERIALIZED", SQL)

    def test_migration_preserves_private_function_boundary(self):
        self.assertIn("REVOKE ALL ON FUNCTION", SQL)
        self.assertIn("FROM PUBLIC, anon, authenticated, app_readonly, service_role", SQL)
        self.assertNotIn("GRANT EXECUTE", SQL)

    def test_applicator_is_fail_closed_and_defaults_to_rollback(self):
        self.assertIn('parser.add_argument("--apply", action="store_true")', SCRIPT)
        self.assertIn("EXPECTED_BODY_MD5", SCRIPT)
        self.assertIn("if after != before", SCRIPT)
        self.assertIn("connection.rollback()", SCRIPT)
        self.assertIn("live body drifted", SCRIPT)


if __name__ == "__main__":
    unittest.main()
