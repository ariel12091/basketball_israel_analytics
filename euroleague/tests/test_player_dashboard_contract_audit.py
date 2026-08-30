from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "audit_player_dashboard_contracts.py"


def load_module():
    spec = importlib.util.spec_from_file_location("player_dashboard_contract_audit", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys_modules = __import__("sys").modules
    sys_modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class PlayerDashboardContractAuditTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_module()
        cls.source = SCRIPT.read_text(encoding="utf-8")

    def test_audit_is_database_read_only(self):
        self.assertIn("SET TRANSACTION READ ONLY", self.source)
        self.assertIn("connection.rollback()", self.source)
        for mutation in ("CREATE FUNCTION", "DROP FUNCTION", "connection.commit"):
            self.assertNotIn(mutation, self.source)

    def test_both_leagues_compare_dashboard_to_legacy_contract(self):
        self.assertEqual("euroleague", self.module.EUROLEAGUE.schema)
        self.assertEqual("basketball_test", self.module.ISRAELI.schema)
        for token in ("four_factors_compute", "onoff_compute", "four_factors_dashboard_compute"):
            self.assertIn(token, self.source)
        self.assertEqual(4, len(self.module.RATING_COLUMNS))

    def test_mismatch_report_identifies_the_first_column(self):
        columns = ("player_id", "team_id", "metric")
        detail = self.module.mismatch_detail(
            (columns, [(1, 2, 3)]),
            (columns, [(1, 2, 4)]),
        )
        self.assertIn("key=(1, 2)", detail)
        self.assertIn("column='metric'", detail)

    def test_numeric_scale_is_not_behavioral_drift(self):
        from decimal import Decimal

        columns = ("player_id", "team_id", "metric")
        left = self.module.canonical(columns, [(1, 2, Decimal("0"))])
        right = self.module.canonical(columns, [(1, 2, Decimal("0.0"))])
        self.assertEqual(left, right)

    def test_both_matrices_cover_shared_filter_classes_and_reject_vacuous_cases(self):
        for contract in (self.module.EUROLEAGUE, self.module.ISRAELI):
            matrix = " ".join(extra for _, extra in contract.presets)
            for token in (
                "p_last_n_games", "p_opp_ids_csv", "p_home_away", "p_outcome",
                "p_opp_rank_side", "p_num_starters_off_min", "p_num_starters_def_max",
                "p_min_gn",
            ):
                self.assertIn(token, matrix)
            self.assertEqual("empty", contract.presets[-1][0])
        self.assertIn("vacuous zero-row preset", self.source)


if __name__ == "__main__":
    unittest.main()
