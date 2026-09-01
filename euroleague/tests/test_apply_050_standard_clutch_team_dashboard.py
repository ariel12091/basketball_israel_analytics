from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = (ROOT / "scripts" / "apply_050_standard_clutch_team_dashboard.py").read_text(
    encoding="utf-8"
)


class ApplyStandardClutchTeamDashboardTest(unittest.TestCase):
    def test_applicator_is_guarded_and_defaults_to_rollback(self):
        self.assertIn('parser.add_argument("--apply", action="store_true")', SCRIPT)
        self.assertIn("connection.rollback()", SCRIPT)
        self.assertIn("unexpected target", SCRIPT)
        self.assertIn("migration 050 must remain additive", SCRIPT)
        self.assertIn("must define exactly one function", SCRIPT)

    def test_apply_requires_exact_parity_and_privileges(self):
        self.assertIn("expected != actual", SCRIPT)
        self.assertIn("DYNAMIC_ROUTES", SCRIPT)
        self.assertIn("has_function_privilege('public'", SCRIPT)
        self.assertIn("has_function_privilege('app_readonly'", SCRIPT)
        self.assertIn("connection.commit()", SCRIPT)


if __name__ == "__main__":
    unittest.main()
