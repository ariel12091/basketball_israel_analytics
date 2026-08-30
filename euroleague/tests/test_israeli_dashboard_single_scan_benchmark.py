from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "benchmark_israeli_dashboard_single_scan.py"


def load_module():
    spec = importlib.util.spec_from_file_location("israeli_single_scan_benchmark", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class IsraeliSingleScanBenchmarkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_module()
        cls.ddl = cls.module.candidate_ddl()

    def test_candidate_has_distinct_disposable_name(self):
        self.assertIn(
            "CREATE OR REPLACE FUNCTION basketball_test.four_factors_dashboard_single_scan_candidate(",
            self.ddl,
        )
        self.assertNotIn("DROP FUNCTION", self.ddl)

    def test_candidate_is_the_exact_prepared_production_body(self):
        source = self.module.SOURCE.read_text(encoding="utf-8")
        source = source[source.index("CREATE OR REPLACE FUNCTION"):]
        source = source[:source.index("\nREVOKE ALL ON FUNCTION")]
        expected = source.replace(
            "basketball_test.four_factors_dashboard_compute(",
            f"basketball_test.{self.module.CANDIDATE}(",
            1,
        )
        self.assertEqual(expected, self.ddl)

    def test_candidate_is_one_fact_aggregation(self):
        self.assertEqual(1, self.ddl.count("FROM basketball_test.player_four_factors_by_game pf"))
        self.assertNotIn("four_factors_compute(", self.ddl)
        self.assertNotIn("onoff_compute(", self.ddl)

    def test_candidate_returns_dashboard_contract(self):
        for column in ('"Net RTG Diff"', '"Off ON Diff"', '"Def ON Diff"', "minutes"):
            self.assertIn(column, self.ddl)

    def test_script_is_rollback_only(self):
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertNotIn("--apply", source)
        self.assertNotIn("connection.commit", source)
        self.assertIn("connection.rollback()", source)
        self.assertIn("has_function_privilege('app_readonly'", source)
        self.assertIn("configured credentials cannot SET ROLE", source)


if __name__ == "__main__":
    unittest.main()
