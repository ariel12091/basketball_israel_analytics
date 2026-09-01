from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = (ROOT / "scripts" / "benchmark_051_israeli_standard_clutch_dashboard.py").read_text(encoding="utf-8")
COLD_SCRIPT = (ROOT / "scripts" / "benchmark_051_israeli_session_cold.py").read_text(encoding="utf-8")
SQL = (ROOT / "sql" / "candidates" / "051_israeli_standard_clutch_dashboard_probe.sql").read_text(encoding="utf-8")


class IsraeliStandardClutchDashboardBenchmarkTest(unittest.TestCase):
    def test_probe_is_rollback_only_and_disposable(self):
        self.assertNotIn("COMMIT", SQL.upper())
        self.assertNotIn("DROP ", SQL.upper())
        self.assertIn("probe_20260901", SQL)
        self.assertNotIn("--apply", SCRIPT)
        self.assertIn("con.rollback()", SCRIPT)

    def test_candidate_materializes_one_filtered_action_set(self):
        self.assertEqual(1, SQL.count("facts AS MATERIALIZED"))
        self.assertEqual(2, SQL.count("df_pts_poss_lineups_longer_mv"))
        self.assertIn("LEFT JOIN basketball_test.shot_zones", SQL)
        self.assertIn("parent.parameters_type parent_param", SQL)
        self.assertIn("durations AS", SQL)

    def test_gate_covers_all_three_current_contracts(self):
        self.assertIn("RATING_COLUMNS", SCRIPT)
        self.assertIn("FF_COLUMNS", SCRIPT)
        self.assertIn("minute_query", SCRIPT)
        self.assertIn("assert_parity", SCRIPT)
        self.assertIn('for view in ("summary", "four_factors")', SCRIPT)
        self.assertIn("alternating {view}+minutes calls", SCRIPT)

    def test_cold_probe_is_session_local_and_holds_distinct_backends(self):
        self.assertIn('"pg_temp.get_team_dashboard_standard_clutch_probe_20260901"', COLD_SCRIPT)
        self.assertIn("len(set(pids)) != len(pids)", COLD_SCRIPT)
        self.assertIn("probes = [open_probe_connection() for _ in range(8)]", COLD_SCRIPT)
        self.assertIn("connection.rollback()", COLD_SCRIPT)
        self.assertNotIn("commit()", COLD_SCRIPT)


if __name__ == "__main__":
    unittest.main()
