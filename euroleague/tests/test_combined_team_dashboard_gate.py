from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "gate_050_combined_team_dashboard_readers.py"


def load_module():
    spec = importlib.util.spec_from_file_location("combined_team_gate", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class CombinedTeamDashboardGateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_module()
        cls.source = SCRIPT.read_text(encoding="utf-8")

    def test_gate_covers_every_route_and_metric_companion(self):
        self.assertEqual({"pergame", "dynamic", "direct"}, {r.kind for r in self.module.ROUTES})
        self.assertEqual({"pergame", "dynamic", "direct"}, {r.kind for r in self.module.SMOKE_ROUTES})
        for token in (
            'f"get_team_ratings_{route.kind}"',
            'f"get_team_four_factors_{route.kind}"',
            'f"get_team_minutes_{route.kind}"',
            'f"get_team_metrics_{route.kind}"',
            '"get_team_dashboard_dynamic"',
        ):
            self.assertIn(token, self.source)

    def test_gate_is_rollback_only_and_time_bounded(self):
        self.assertIn("connection.rollback()", self.source)
        self.assertNotIn("connection.commit", self.source)
        self.assertNotIn("--apply", self.source)
        self.assertIn("lock_timeout='5s'", self.source)
        self.assertIn("statement_timeout='90s'", self.source)

    def test_gate_compares_the_complete_combined_contract(self):
        self.assertEqual(25, len(self.module.COMBINED_COLUMNS))
        for name in ("off_ppp", "net_rtg", "rank_net_rtg", "off_efg", "def_ftr", "minutes"):
            self.assertIn(name, self.module.COMBINED_COLUMNS)


if __name__ == "__main__":
    unittest.main()
