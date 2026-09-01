from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "scripts" / "benchmark_050_actual_ui_paths.py").read_text(encoding="utf-8")


class ActualUiPathBenchmarkTest(unittest.TestCase):
    def test_measures_mutually_exclusive_views_separately(self):
        self.assertIn('for view in ("summary", "four_factors")', SOURCE)
        self.assertIn("get_team_minutes_dynamic", SOURCE)
        self.assertIn("get_team_dashboard_dynamic", SOURCE)
        self.assertNotIn("get_team_ratings_dynamic\", \"get_team_four_factors_dynamic", SOURCE)

    def test_uses_alternating_warm_and_distinct_cold_sessions(self):
        self.assertIn("if index % 2", SOURCE)
        self.assertIn("sessions = [open_session() for _ in range(4)]", SOURCE)
        self.assertIn("len(set(pids)) != 4", SOURCE)
        self.assertIn("candidate-first", SOURCE)
        self.assertIn("legacy-first", SOURCE)

    def test_is_read_only(self):
        self.assertNotIn("CREATE ", SOURCE)
        self.assertNotIn("DROP ", SOURCE)
        self.assertNotIn("commit()", SOURCE)
        self.assertIn("connection.rollback()", SOURCE)


if __name__ == "__main__":
    unittest.main()
