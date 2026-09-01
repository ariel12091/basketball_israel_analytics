from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "sql" / "050_standard_clutch_team_dashboard.sql").read_text(
    encoding="utf-8"
)
CANDIDATE = (
    ROOT / "sql" / "candidates" / "050_two_call_team_dashboard_readers.sql"
).read_text(encoding="utf-8")


class StandardClutchTeamDashboardReaderTest(unittest.TestCase):
    def test_migration_contains_only_the_accepted_reader(self):
        self.assertEqual(1, SQL.count("CREATE OR REPLACE FUNCTION"))
        self.assertIn(
            "CREATE OR REPLACE FUNCTION euroleague.get_team_dashboard_dynamic", SQL
        )
        self.assertNotIn("get_team_metrics_pergame", SQL)
        self.assertNotIn("get_team_metrics_direct", SQL)

    def test_body_is_the_exact_measured_candidate_slice(self):
        start = "CREATE OR REPLACE FUNCTION euroleague.get_team_dashboard_dynamic"
        accepted_body = SQL.split(start, 1)[1].split("$function$;", 1)[0]
        candidate_body = CANDIDATE.split(start, 1)[1].split("$function$;", 1)[0]
        self.assertEqual(candidate_body, accepted_body)

    def test_materializes_the_shared_fact_once(self):
        self.assertEqual(1, SQL.count("facts AS MATERIALIZED"))
        self.assertEqual(1, SQL.count("filtered_team_game_facts("))
        self.assertIn("sum(f.seconds) FILTER(WHERE f.type_lineup='offense')", SQL)

    def test_is_additive_private_and_explicitly_granted(self):
        self.assertNotIn("DROP FUNCTION", SQL.upper())
        self.assertNotIn("CASCADE", SQL.upper())
        self.assertIn("REVOKE ALL ON FUNCTION euroleague.get_team_dashboard_dynamic", SQL)
        self.assertIn("GRANT EXECUTE ON FUNCTION euroleague.get_team_dashboard_dynamic", SQL)


if __name__ == "__main__":
    unittest.main()
