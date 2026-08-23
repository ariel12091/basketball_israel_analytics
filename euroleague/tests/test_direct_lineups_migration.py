from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[1]
SQL=(ROOT/'sql'/'035_direct_lineups_reader.sql').read_text(encoding='utf-8')
INDEX_SQL=(ROOT/'sql'/'036_lineups_covering_index.sql').read_text(encoding='utf-8')
# Scope: this file tests the SQL migration only. The app-side routing
# contract (which reader each tab picks) lives in clutch_reader_kind() and is
# tested in app/tests/testthat/test-euro-clutch.R, behaviourally. Do not grep
# app/R from here -- a cross-boundary source assertion went stale unnoticed
# when that routing was refactored into the shared helper.

class DirectLineupsMigrationTest(unittest.TestCase):
    def test_one_action_set_feeds_events_and_duration(self):
        self.assertEqual(SQL.count('FROM euroleague.player_stats_actions_by_game a'),1)
        self.assertIn('acts AS MATERIALIZED',SQL)
        self.assertIn('max(event_elapsed_seconds)-min(event_elapsed_seconds)',SQL)
        self.assertNotIn('filtered_team_game_facts(',SQL)
        self.assertNotIn('clutch_segment_durations(',SQL)

    def test_five_players_bypass_sub_lineups(self):
        self.assertIn('WHERE p_unit_size=5',SQL)
        self.assertIn('WHERE p_unit_size BETWEEN 2 AND 4',SQL)

    def test_covering_index_has_identity_metrics_and_duration(self):
        self.assertIn('CREATE INDEX IF NOT EXISTS',INDEX_SQL)
        self.assertIn('own_lineup,segment_id,event_elapsed_seconds',INDEX_SQL)
        self.assertIn('possession_flag',INDEX_SQL)
        self.assertNotIn('action_player_id',INDEX_SQL)

if __name__=='__main__':unittest.main()
