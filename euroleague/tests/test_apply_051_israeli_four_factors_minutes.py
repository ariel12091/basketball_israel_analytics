from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "scripts" / "apply_051_israeli_four_factors_minutes.py").read_text(encoding="utf-8")
DDL = (ROOT.parent / "sql" / "functions" / "get_team_four_factors_dashboard_dynamic.sql").read_text(encoding="utf-8")
CANDIDATE = (ROOT / "sql" / "candidates" / "051_israeli_four_factors_minutes.sql").read_text(encoding="utf-8")


def function_body(source):
    return source.split("AS $function$", 1)[1].split("$function$;", 1)[0].strip()


class ApplyIsraeliFourFactorsMinutesTest(unittest.TestCase):
    def test_ddl_is_additive_private_and_narrow(self):
        self.assertEqual(1, DDL.count("CREATE OR REPLACE FUNCTION"))
        self.assertNotIn("DROP ", DDL.upper())
        self.assertNotIn("CASCADE", DDL.upper())
        self.assertNotIn("shot_zones", DDL)
        self.assertIn("facts AS MATERIALIZED", DDL)
        self.assertIn("REVOKE ALL ON FUNCTION", DDL)
        self.assertIn("GRANT EXECUTE ON FUNCTION", DDL)

    def test_production_function_preserves_the_measured_candidate_body(self):
        self.assertEqual(function_body(CANDIDATE), function_body(DDL))

    def test_applicator_defaults_to_rollback_and_gates_parity(self):
        self.assertIn('parser.add_argument("--apply", action="store_true")', SOURCE)
        self.assertIn("assert_parity", SOURCE)
        self.assertIn("for preset in PRESETS", SOURCE)
        self.assertIn("connection.rollback()", SOURCE)
        self.assertIn("connection.commit()", SOURCE)
        self.assertIn("has_function_privilege('public'", SOURCE)


if __name__ == "__main__":
    unittest.main()
