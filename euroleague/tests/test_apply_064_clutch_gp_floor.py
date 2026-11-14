import importlib.util
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "apply_064_clutch_gp_floor.py"
DDL = (ROOT / "sql" / "064_clutch_gp_floor.sql").read_text(encoding="utf-8")


def load_script():
    spec = importlib.util.spec_from_file_location("apply_064", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ApplyClutchGpFloorTest(unittest.TestCase):
    def setUp(self):
        self.apply = load_script()

    def test_the_migration_passes_its_own_validation(self):
        statements = self.apply.validate_ddl(DDL)
        self.assertEqual(3, len(statements))

    def test_it_never_drops_or_truncates(self):
        for extra in ("DROP TABLE euroleague.sub_lineups;", "TRUNCATE euroleague.sub_lineups;",
                      "SELECT 1 FROM basketball_test.schedule;"):
            with self.assertRaises(ValueError):
                self.apply.validate_ddl(DDL + "\n" + extra + "\n")

    def test_floor_presence_is_the_physical_lineup(self):
        refresh = DDL.split("FUNCTION euroleague.refresh_default_clutch_player_for_games", 1)[1]
        refresh = self.apply.executable(refresh.split("$function$;", 1)[0])
        self.assertIn("unnest(atc.own_lineup)", refresh)
        self.assertNotIn("credit_own_lineup", refresh)

    def test_only_gp_follows_the_floor(self):
        reader = DDL.split("FUNCTION euroleague.get_player_traditional_standard_clutch", 1)[1]
        self.assertIn("count(DISTINCT game_id) FILTER (WHERE plays)::integer AS gp", reader)
        # The sums keep their old rows: team usage terms only from in_sums rows.
        self.assertIn("CASE WHEN in_sums THEN coalesce(tu.team_poss, 0) ELSE 0 END", reader)
        self.assertIn("HAVING count(DISTINCT game_id) FILTER (WHERE plays) > 0", reader)


if __name__ == "__main__":
    unittest.main()
