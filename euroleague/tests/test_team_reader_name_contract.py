from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
APP = (ROOT.parent / "app" / "R" / "server_tab9_euro_team.R").read_text(encoding="utf-8")
CONTRACT_PATH = ROOT / "scripts" / "euroleague_function_contract.py"


def load_contract():
    spec = importlib.util.spec_from_file_location("euroleague_function_contract", CONTRACT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class TeamReaderNameContractTest(unittest.TestCase):
    def test_all_team_readers_are_explicit_and_declared(self):
        contract = load_contract()
        expected = {
            f"{base}_{kind}"
            for base in ("get_team_ratings", "get_team_four_factors", "get_team_minutes")
            for kind in ("pergame", "dynamic", "direct")
        }
        self.assertTrue(expected <= contract.DIRECT_APP_READERS)
        for reader in expected:
            self.assertEqual(1, APP.count(f'"euroleague.{reader}"'))
        self.assertIn("get_team_dashboard_dynamic", contract.DIRECT_APP_READERS)
        self.assertEqual(1, APP.count('"euroleague.get_team_dashboard_dynamic"'))

    def test_standard_clutch_dashboard_is_shared_by_all_three_consumers(self):
        self.assertIn("et_dynamic_dashboard <- reactive({", APP)
        self.assertIn("return(et_dynamic_dashboard())", APP)
        self.assertIn("et_dynamic_dashboard()\n      } else {\n        run_team_ff(p)", APP)
        self.assertIn("df <- et_dynamic_dashboard()", APP)
        self.assertIn("et_prev_dynamic_dashboard <- reactive({", APP)
        self.assertEqual(2, APP.count("return(et_prev_dynamic_dashboard())"))

    def test_function_names_are_not_assembled_from_fragments(self):
        self.assertNotIn('base, "_", kind', APP)
        self.assertNotIn('"FROM euroleague.", reader', APP)
        self.assertIn('stop("Unsupported EuroLeague team reader route:', APP)


if __name__ == "__main__":
    unittest.main()
