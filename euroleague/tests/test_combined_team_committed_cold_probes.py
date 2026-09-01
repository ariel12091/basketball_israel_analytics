from pathlib import Path
import importlib.util
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "benchmark_050_committed_cold_probes.py"
SOURCE = SCRIPT.read_text(encoding="utf-8")


def load_module():
    spec = importlib.util.spec_from_file_location("committed_cold_probes", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CombinedTeamCommittedColdProbesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_module()

    def test_uses_distinct_disposable_names(self):
        self.assertIn('"pergame": "get_team_metrics_pergame"', SOURCE)
        self.assertIn('"dynamic": "get_team_dashboard_dynamic"', SOURCE)
        self.assertIn('"direct": "get_team_metrics_direct"', SOURCE)
        self.assertIn('f"{name}_cold_probe"', SOURCE)
        self.assertIn("refusing to replace existing probe functions", SOURCE)
        self.assertIn('source.replace("CREATE OR REPLACE FUNCTION", "CREATE FUNCTION")', SOURCE)

    def test_commits_only_probe_lifecycle(self):
        self.assertIn("deploy_probes", SOURCE)
        self.assertIn("drop_probes", SOURCE)
        self.assertIn("connection.commit()", SOURCE)
        self.assertIn("probe cleanup verification failed after commit", SOURCE)
        self.assertNotIn("CASCADE", SOURCE.upper())

    def test_holds_deploy_backend_and_requires_two_other_pids(self):
        self.assertIn("deploy_backend_pid", SOURCE)
        self.assertIn("measurement did not receive untouched distinct backends", SOURCE)
        self.assertIn("open_measurement_session()", SOURCE)

    def test_always_attempts_cleanup_after_commit(self):
        self.assertIn("if probes_committed:", SOURCE)
        self.assertIn("recovery connection", SOURCE)
        self.assertIn("DROP FUNCTION IF EXISTS euroleague.", SOURCE)

    def test_atomic_variant_converts_exactly_three_complete_bodies(self):
        statements = self.module.probe_statements(atomic=True)
        atomic = [statement for statement in statements if "BEGIN ATOMIC" in statement]
        self.assertEqual(3, len(atomic))
        for statement in atomic:
            self.assertNotIn("$function$", statement)
            self.assertTrue(statement.rstrip().endswith("END"))


if __name__ == "__main__":
    unittest.main()
