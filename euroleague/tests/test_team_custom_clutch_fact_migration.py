from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "sql" / "030_team_custom_clutch_action_fact.sql").read_text(encoding="utf-8")


class TeamCustomClutchFactMigrationTest(unittest.TestCase):
    def test_reuses_existing_fact_and_refreshes_canonical_source(self):
        self.assertIn("ALTER TABLE euroleague.player_stats_actions_by_game", SQL)
        self.assertIn("FROM euroleague.action_team_context_actions atc", SQL)
        self.assertIn("SELECT euroleague.refresh_player_stats_actions_for_games(NULL::bigint[])", SQL)

    def test_custom_fact_uses_action_fact_and_shared_duration_helper(self):
        self.assertIn("FROM euroleague.player_stats_actions_by_game a", SQL)
        self.assertIn("euroleague.clutch_segment_durations(", SQL)
        self.assertNotIn("FROM euroleague.clutch_team_game_facts(", SQL)


if __name__ == "__main__":
    unittest.main()
