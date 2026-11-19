import importlib.util
from decimal import Decimal
from pathlib import Path
import unittest

from euroleague_possessions.roster_collector import parse_roster

ROOT = Path(__file__).resolve().parents[1]
DDL = (ROOT / "sql" / "067_player_season_profiles.sql").read_text(encoding="utf-8")


def row(code="009661", club="BAR", height=196, position="Guard",
        images=None, person_images=None):
    return {
        "person": {"code": code, "height": height, "images": person_images or {}},
        "club": {"code": club},
        "positionName": position,
        "images": images or {},
    }


class ParseRosterTest(unittest.TestCase):
    def test_height_in_metres_and_position_code(self):
        (p,) = parse_roster([row()])
        self.assertEqual("P009661", p.provider_player_id)
        self.assertEqual("BAR", p.provider_team_code)
        self.assertEqual(Decimal("1.96"), p.height_m)
        self.assertEqual(("G", "Guard"), (p.position_en, p.position_name))

    def test_each_provider_position_maps_to_a_basket_code(self):
        codes = [parse_roster([row(position=n)])[0].position_en
                 for n in ("Guard", "Forward", "Center")]
        self.assertEqual(["G", "F", "C"], codes)

    def test_the_club_season_headshot_wins_over_the_person_one(self):
        (p,) = parse_roster([row(images={"headshot": "https://a/club.png"},
                                 person_images={"headshot": "https://a/person.png"})])
        self.assertEqual("https://a/club.png", p.headshot_url)
        (p,) = parse_roster([row(person_images={"headshot": "https://a/person.png",
                                                "action": "https://a/action.png"})])
        self.assertEqual("https://a/person.png", p.headshot_url)

    def test_an_action_shot_is_not_a_headshot(self):
        (p,) = parse_roster([row(images={"action": "https://a/action.png"})])
        self.assertIsNone(p.headshot_url)

    def test_missing_or_implausible_values_are_null_not_guessed(self):
        (p,) = parse_roster([row(height=None, position=None)])
        self.assertEqual((None, None, None), (p.height_m, p.position_en, p.headshot_url))
        self.assertIsNone(parse_roster([row(height=0)])[0].height_m)
        self.assertIsNone(parse_roster([row(images={"headshot": "http://a/x.png"})])[0].headshot_url)

    def test_a_transfer_keeps_one_row_per_club(self):
        profiles = parse_roster([row(club="PAR"), row(club="MUN"), row(club="PAR")])
        self.assertEqual({"PAR", "MUN"}, {p.provider_team_code for p in profiles})


class Migration067Test(unittest.TestCase):
    def setUp(self):
        spec = importlib.util.spec_from_file_location(
            "apply_067", ROOT / "scripts" / "apply_067_player_profiles.py")
        self.apply = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.apply)

    def test_the_migration_passes_its_own_validation(self):
        self.assertEqual(6, len(self.apply.validate_ddl(DDL)))

    def test_it_never_drops_or_touches_the_israeli_schema(self):
        for extra in ("DROP TABLE euroleague.players;", "TRUNCATE euroleague.players;",
                      "SELECT 1 FROM basketball_test.schedule;"):
            with self.assertRaises(ValueError):
                self.apply.validate_ddl(DDL + "\n" + extra + "\n")

    def test_the_positions_the_table_accepts_are_the_parsers(self):
        from euroleague_possessions.roster_collector import POSITION_CODES
        self.assertIn("position_en IN ('G', 'F', 'C')", DDL)
        self.assertEqual({"G", "F", "C"}, set(POSITION_CODES.values()))


if __name__ == "__main__":
    unittest.main()
