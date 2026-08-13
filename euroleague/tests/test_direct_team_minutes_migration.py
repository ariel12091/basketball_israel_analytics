from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "sql" / "033_direct_team_minutes.sql").read_text(encoding="utf-8")
INDEX_SQL = (ROOT / "sql" / "034_team_minutes_covering_index.sql").read_text(encoding="utf-8")
APP = (ROOT.parent / "app" / "R" / "server_tab9_euro_team.R").read_text(encoding="utf-8")

class DirectTeamMinutesMigrationTest(unittest.TestCase):
    def test_matches_israeli_filtered_segment_shape(self):
        self.assertIn("FROM euroleague.player_stats_actions_by_game a", SQL)
        self.assertIn("max(event_elapsed_seconds)-min(event_elapsed_seconds)", SQL)
        self.assertIn("GROUP BY game_id,team_id,segment_id", SQL)
        self.assertNotIn("filtered_team_game_facts(", SQL)
        self.assertNotIn("clutch_segment_durations(", SQL)

    def test_app_routes_custom_minutes_direct(self):
        self.assertIn('"get_team_minutes_direct"', APP)
        self.assertIn('"get_team_minutes_dynamic"', APP)

    def test_minutes_index_covers_segment_duration(self):
        self.assertIn("CREATE INDEX CONCURRENTLY", INDEX_SQL)
        self.assertIn("segment_id,event_elapsed_seconds", INDEX_SQL)
        self.assertNotIn("own_lineup", INDEX_SQL)

if __name__ == "__main__":
    unittest.main()
