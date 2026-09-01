from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "benchmark_050_combined_team_dashboard.py"
SOURCE = SCRIPT.read_text(encoding="utf-8")


class CombinedTeamDashboardBenchmarkTest(unittest.TestCase):
    def test_is_rollback_only_and_bounded(self):
        self.assertIn("connection.rollback()", SOURCE)
        self.assertNotIn("connection.commit", SOURCE)
        self.assertIn("lock_timeout='5s'", SOURCE)
        self.assertIn("statement_timeout='90s'", SOURCE)

    def test_warm_samples_alternate_call_order(self):
        self.assertIn("if index % 2 == 0", SOURCE)
        self.assertIn("default=15", SOURCE)
        self.assertIn("legacy_buffers", SOURCE)
        self.assertIn("candidate_buffers", SOURCE)

    def test_first_use_samples_run_both_orders_and_capture_backend(self):
        self.assertIn("for _ in range(runs) for order in (True, False)", SOURCE)
        self.assertIn("pg_backend_pid()", SOURCE)
        self.assertIn('"connection_seconds"', SOURCE)
        self.assertIn('"ddl_seconds"', SOURCE)
        self.assertIn("session-mode batch did not receive distinct backends", SOURCE)
        self.assertIn("sessions.append", SOURCE)

    def test_covers_each_route_kind(self):
        for kind in ("pergame", "dynamic", "direct"):
            self.assertIn(f'Route("{kind}"', SOURCE)


if __name__ == "__main__":
    unittest.main()
