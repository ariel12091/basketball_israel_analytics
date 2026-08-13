from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "sql" / "031_direct_team_custom_readers.sql").read_text(encoding="utf-8")
INDEX_SQL = (ROOT / "sql" / "032_team_action_covering_index.sql").read_text(encoding="utf-8")
APP = (ROOT.parent / "app" / "R" / "server_tab9_euro_team.R").read_text(encoding="utf-8")


class DirectTeamReadersMigrationTest(unittest.TestCase):
    def test_readers_follow_single_action_scan_shape(self):
        self.assertIn("CREATE OR REPLACE FUNCTION euroleague.get_team_ratings_direct", SQL)
        self.assertIn("CREATE OR REPLACE FUNCTION euroleague.get_team_four_factors_direct", SQL)
        self.assertIn("acts AS (", SQL)
        self.assertIn("FROM euroleague.player_stats_actions_by_game a", SQL)
        self.assertNotIn("filtered_team_game_facts(", SQL)
        self.assertNotIn("clutch_team_game_facts(", SQL)
        self.assertNotIn("clutch_segment_durations(", SQL)

    def test_regulation_and_overtime_share_the_israeli_single_scan(self):
        self.assertEqual(SQL.count("FROM euroleague.player_stats_actions_by_game a"), 2)
        self.assertNotIn("WHERE NOT a.is_overtime", SQL)
        self.assertNotIn("WHERE a.is_overtime", SQL)
        self.assertGreaterEqual(
            SQL.count("OR (a.is_overtime AND NOT coalesce(p_ot_margin_filter,false))"),
            4,
        )

    def test_app_keeps_standard_cache_and_routes_other_requests_direct(self):
        self.assertIn("use_direct_team_reader <- function(p)", APP)
        self.assertIn('"get_team_ratings_direct"', APP)
        self.assertIn('"get_team_four_factors_direct"', APP)
        self.assertIn('"get_team_ratings_dynamic"', APP)
        self.assertIn('"get_team_four_factors_dynamic"', APP)

    def test_covering_index_contains_only_direct_team_inputs(self):
        self.assertIn("CREATE INDEX CONCURRENTLY", INDEX_SQL)
        self.assertIn("player_stats_actions_by_game", INDEX_SQL)
        self.assertIn("INCLUDE (", INDEX_SQL)
        self.assertNotIn("own_lineup", INDEX_SQL)
        self.assertNotIn("action_player_id", INDEX_SQL)


if __name__ == "__main__":
    unittest.main()
