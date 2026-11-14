import importlib.util
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "apply_063_sub_lineups_stats_per_season.py"
DDL = (ROOT / "sql" / "063_sub_lineups_stats_per_season.sql").read_text(encoding="utf-8")


def load_script():
    spec = importlib.util.spec_from_file_location("apply_063", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ApplySubLineupsStatsPerSeasonTest(unittest.TestCase):
    def setUp(self):
        self.apply = load_script()

    def test_the_migration_passes_its_own_validation(self):
        statements = self.apply.validate_ddl(DDL)
        self.assertTrue(any("refresh_sub_lineups_stats" in s for s in statements))

    def test_only_the_replaced_view_may_be_dropped(self):
        with self.assertRaisesRegex(ValueError, "unexpected DROP"):
            self.apply.validate_ddl(DDL + "\nDROP TABLE euroleague.sub_lineups;\n")
        with self.assertRaisesRegex(ValueError, "TRUNCATE"):
            self.apply.validate_ddl(DDL.replace("COMMIT;", "TRUNCATE euroleague.sub_lineups;\nCOMMIT;"))

    def test_the_full_season_gate_runs_the_functions_own_select(self):
        query = self.apply.function_select(DDL)
        self.assertTrue(query.lstrip().startswith("WITH game_starters"))
        self.assertNotIn("p_game_ids", query)
        self.assertIn("NULL::bigint[]", query)
        # Every output column of the view, in the view's order.
        self.assertTrue(query.rstrip().endswith("names"))

    def test_the_whole_schema_refresh_no_longer_rebuilds_it(self):
        body = DDL.split("FUNCTION euroleague.refresh_app_materialized_views()", 1)[1]
        self.assertNotIn("sub_lineups_stats_mv", body)
        self.assertEqual(7, len(re.findall(r"REFRESH MATERIALIZED VIEW", body)))


if __name__ == "__main__":
    unittest.main()
