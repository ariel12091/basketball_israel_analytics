from __future__ import annotations

import unittest
from pathlib import Path


MIGRATION = Path(__file__).resolve().parents[1] / "sql" / "021_player_traditional_stats.sql"
CLUTCH_MIGRATION = Path(__file__).resolve().parents[1] / "sql" / "022_default_clutch_player_stats.sql"
FAST_PATH_MIGRATION = Path(__file__).resolve().parents[1] / "sql" / "023_player_stats_standard_clutch_fast_path.sql"
SELECTOR_MIGRATION = Path(__file__).resolve().parents[1] / "sql" / "024_player_stats_clutch_source_selector.sql"
CUSTOM_MINUTES_MIGRATION = Path(__file__).resolve().parents[1] / "sql" / "025_custom_clutch_action_segment_minutes.sql"
SINGLE_SCAN_MIGRATION = Path(__file__).resolve().parents[1] / "sql" / "026_player_stats_single_action_scan.sql"
ACTION_FACT_MIGRATION = Path(__file__).resolve().parents[1] / "sql" / "027_player_stats_action_fact.sql"


class PlayerTraditionalMigrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sql = MIGRATION.read_text(encoding="utf-8")
        cls.upper = cls.sql.upper()
        cls.clutch_sql = CLUTCH_MIGRATION.read_text(encoding="utf-8")
        cls.fast_path_sql = FAST_PATH_MIGRATION.read_text(encoding="utf-8")
        cls.selector_sql = SELECTOR_MIGRATION.read_text(encoding="utf-8")
        cls.custom_minutes_sql = CUSTOM_MINUTES_MIGRATION.read_text(encoding="utf-8")
        cls.single_scan_sql = SINGLE_SCAN_MIGRATION.read_text(encoding="utf-8")
        cls.action_fact_sql = ACTION_FACT_MIGRATION.read_text(encoding="utf-8")

    def test_reuses_existing_player_game_evidence(self) -> None:
        self.assertIn("EUROLEAGUE.FULL_ROSTERS", self.upper)
        self.assertIn("BOXScore_STATS".upper(), self.upper)
        self.assertIn("EUROLEAGUE.PLAYER_FOUR_FACTORS_BY_GAME", self.upper)
        self.assertNotIn("CREATE TABLE EUROLEAGUE.PLAYER_TRADITIONAL", self.upper)

    def test_adds_indexed_default_fast_path(self) -> None:
        self.assertIn(
            "CREATE MATERIALIZED VIEW EUROLEAGUE.PLAYER_TRADITIONAL_STATS_MV",
            self.upper,
        )
        self.assertIn("CREATE UNIQUE INDEX", self.upper)
        self.assertIn(
            "REFRESH MATERIALIZED VIEW EUROLEAGUE.PLAYER_TRADITIONAL_STATS_MV",
            self.upper,
        )

    def test_adds_player_clutch_fast_path(self) -> None:
        self.assertIn("PLAYER_TS_POSS", self.upper)
        self.assertIn("PLAYER_TOV", self.upper)
        self.assertIn("REFRESH_DEFAULT_CLUTCH_PLAYER_FOR_GAMES", self.clutch_sql.upper())
        self.assertIn("CLUTCH_EVENT_QUALIFIES", self.clutch_sql.upper())
        fast = self.fast_path_sql.upper()
        self.assertIn("GET_PLAYER_TRADITIONAL_STANDARD_CLUTCH", fast)
        self.assertIn("DEFAULT_CLUTCH_PLAYER_TOTALS_BY_GAME", fast)
        self.assertIn("FILTERED_TEAM_GAME_FACTS", fast)
        self.assertNotIn("ACTION_TEAM_CONTEXT_ACTIONS", fast)

    def test_custom_clutch_matches_team_source_selector_design(self) -> None:
        sql = self.selector_sql.upper()
        self.assertIn("SELECT_PLAYER_CLUTCH_COUNTS", sql)
        self.assertIn("DEFAULT_CLUTCH_PLAYER_TOTALS_BY_GAME", sql)
        self.assertIn("ACTION_TEAM_CONTEXT_ACTIONS", sql)
        self.assertIn("CLUTCH_EVENT_QUALIFIES", sql)
        self.assertIn("GET_PLAYER_TRADITIONAL_CLUTCH", sql)
        self.assertIn("FILTERED_TEAM_GAME_FACTS", sql)

    def test_custom_minutes_share_israeli_action_segment_convention(self) -> None:
        sql = self.custom_minutes_sql.upper()
        self.assertIn("CREATE OR REPLACE FUNCTION EUROLEAGUE.CLUTCH_SEGMENT_DURATIONS", sql)
        self.assertIn("MAX(ATC.EVENT_ELAPSED_SECONDS) - MIN(ATC.EVENT_ELAPSED_SECONDS)", sql)
        self.assertIn("EUROLEAGUE.CLUTCH_EVENT_QUALIFIES", sql)
        self.assertNotIn("MATCHUP_SEGMENTS_ACTIONS", sql)

    def test_custom_player_stats_follow_israeli_single_acts_shape(self) -> None:
        sql = self.single_scan_sql.upper()
        self.assertEqual(sql.count("FROM EUROLEAGUE.ACTION_TEAM_CONTEXT_ACTIONS"), 1)
        self.assertIn("FROM EUROLEAGUE.PLAYER_STATS_ACTION_CONTEXT", sql)
        self.assertIn("ACTS AS MATERIALIZED", sql)
        for cte in ("LINEUP_MAP AS", "POSS_END AS", "PLAYER_USAGE AS",
                    "TEAM_POSSESSION_TOTALS AS", "SEG_TIMES AS",
                    "PLAYER_MINUTES AS", "STATS AS", "TEAM_USAGE_TOTALS AS"):
            self.assertIn(cte, sql)
            self.assertIn(cte + " MATERIALIZED", sql)
        stats_sql = sql.split("STATS AS MATERIALIZED (", 1)[1].split(
            "TEAM_USAGE_TOTALS AS MATERIALIZED", 1
        )[0]
        self.assertIn("FROM ACTS A", stats_sql)
        self.assertNotIn("JOIN REAL_ROSTER", stats_sql)
        self.assertIn("WHERE NOT A.IS_OVERTIME", sql)
        self.assertIn("WHERE A.IS_OVERTIME", sql)
        self.assertIn("UNION ALL", sql)

    def test_narrow_player_action_fact_is_incremental_and_private(self) -> None:
        sql = self.action_fact_sql.upper()
        self.assertIn("CREATE TABLE EUROLEAGUE.PLAYER_STATS_ACTIONS_BY_GAME", sql)
        self.assertIn("REFRESH_PLAYER_STATS_ACTIONS_FOR_GAMES", sql)
        self.assertIn("DELETE FROM EUROLEAGUE.PLAYER_STATS_ACTIONS_BY_GAME", sql)
        self.assertIn("CREATE OR REPLACE VIEW EUROLEAGUE.PLAYER_STATS_ACTION_CONTEXT", sql)
        self.assertIn("REVOKE ALL ON TABLE EUROLEAGUE.PLAYER_STATS_ACTIONS_BY_GAME FROM APP_READONLY", sql)

    def test_dynamic_path_reuses_shared_filters_and_clutch_semantics(self) -> None:
        self.assertIn("EUROLEAGUE.FILTERED_TEAM_GAME_FACTS", self.upper)
        self.assertIn("EUROLEAGUE.CLUTCH_EVENT_QUALIFIES", self.upper)
        self.assertIn("ATC.OWN_TEAM_SCORE", self.upper)
        self.assertIn("ATC.EVENT_TEAM_ID = ATC.TEAM_ID", self.upper)
        self.assertIn("SECURITY DEFINER", self.upper)
        self.assertIn("GRANT EXECUTE ON FUNCTION", self.upper)

    def test_ts_and_usage_use_canonical_pbp_possession_terms(self) -> None:
        self.assertIn("EUROLEAGUE.ACTION_TEAM_CONTEXT_ACTIONS", self.upper)
        self.assertIn("PF.PLAYER_TS_POSS_COUNT", self.upper)
        self.assertIn("EUROLEAGUE.TEAM_FOUR_FACTORS_BY_GAME", self.upper)
        self.assertIn("SUM(ATC.TS_POSSESSIONS)", self.upper)
        self.assertIn("SUM(ATC.TURNOVERS)", self.upper)
        self.assertIn("2 * PLAYER_TS_POSS", self.upper)
        self.assertIn("PLAYER_TS_POSS + PLAYER_TOV", self.upper)
        self.assertIn("TEAM_TS_POSS + TEAM_TOV", self.upper)
        self.assertIn("* POSS_ON_FLOOR", self.upper)
        self.assertNotIn("0.44", self.sql)
        self.assertNotIn("NULL::NUMERIC AS USG_PCT", self.upper)

    def test_provider_missing_deflections_remain_null(self) -> None:
        self.assertGreaterEqual(self.upper.count("NULL::NUMERIC AS DFL"), 2)
        self.assertNotIn("AS DFL,", self.upper.replace("NULL::NUMERIC AS DFL,", ""))


if __name__ == "__main__":
    unittest.main()
