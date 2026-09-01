from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "sql" / "candidates" / "051_israeli_four_factors_minutes.sql").read_text(encoding="utf-8")
SCRIPT = (ROOT / "scripts" / "benchmark_051_israeli_four_factors_minutes.py").read_text(encoding="utf-8")


class IsraeliFourFactorsMinutesBenchmarkTest(unittest.TestCase):
    def test_candidate_is_narrow_and_shares_one_fact(self):
        self.assertEqual(1, SQL.count("facts AS MATERIALIZED"))
        self.assertNotIn("shot_zones", SQL)
        self.assertNotIn("layup", SQL.lower())
        self.assertNotIn("wins", SQL.lower())
        self.assertNotIn("dense_rank()", SQL.lower())
        self.assertIn("minutes NUMERIC", SQL)

    def test_gate_is_nonpersistent_and_covers_actual_view(self):
        self.assertNotIn("COMMIT", SQL.upper())
        self.assertNotIn("DROP ", SQL.upper())
        self.assertIn("get_team_four_factors_dynamic", SCRIPT)
        self.assertIn("minute_query", SCRIPT)
        self.assertIn("pg_temp", SCRIPT)
        self.assertIn("sessions = [open_temp_session() for _ in range(8)]", SCRIPT)
        self.assertNotIn("commit()", SCRIPT)
        self.assertIn("connection.rollback()", SCRIPT)


if __name__ == "__main__":
    unittest.main()
