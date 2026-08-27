from __future__ import annotations

import unittest
from pathlib import Path


MIGRATION = Path(__file__).resolve().parents[1] / "sql" / "042_player_traditional_pergame.sql"


class PlayerTraditionalPergameMigrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.sql = MIGRATION.read_text(encoding="utf-8")
        cls.upper = cls.sql.upper()

    def test_fact_is_typed_incremental_and_private(self) -> None:
        self.assertIn("CREATE TABLE EUROLEAGUE.PLAYER_TRADITIONAL_BY_GAME", self.upper)
        self.assertIn("REFRESH_PLAYER_TRADITIONAL_BY_GAME_FOR_GAMES", self.upper)
        self.assertIn("DELETE FROM EUROLEAGUE.PLAYER_TRADITIONAL_BY_GAME", self.upper)
        self.assertIn("REVOKE ALL ON TABLE EUROLEAGUE.PLAYER_TRADITIONAL_BY_GAME FROM APP_READONLY", self.upper)
        self.assertIn("ENABLE ROW LEVEL SECURITY", self.upper)

    def test_preserves_euroleague_sources_and_additive_grain(self) -> None:
        self.assertIn("EUROLEAGUE.FULL_ROSTERS", self.upper)
        self.assertIn("FR.BOXSCORE_STATS", self.upper)
        self.assertIn("EUROLEAGUE.LINEUP_TOTALS_BY_GAME", self.upper)
        self.assertIn("UNNEST(L.PLAYER_IDS)", self.upper)
        self.assertIn("EUROLEAGUE.PLAYER_FOUR_FACTORS_BY_GAME", self.upper)
        self.assertIn("EUROLEAGUE.TEAM_FOUR_FACTORS_BY_GAME", self.upper)
        self.assertNotIn("BASKETBALL.", self.upper)
        self.assertNotIn("BASKETBALL_TEST.", self.upper)

    def test_reader_filters_games_then_aggregates_the_fact(self) -> None:
        self.assertIn("GET_PLAYER_TRADITIONAL_PERGAME", self.upper)
        self.assertIn("GAMES_FILTERED AS MATERIALIZED", self.upper)
        self.assertIn("FROM EUROLEAGUE.PLAYER_TRADITIONAL_BY_GAME F", self.upper)
        self.assertNotIn("ANY(OWN_LINEUP)", self.upper)
        self.assertIn("SECURITY DEFINER", self.upper)
        self.assertIn("GRANT EXECUTE ON FUNCTION", self.upper)

    def test_refresh_precedes_reader_and_backfills(self) -> None:
        refresh = self.upper.index("SELECT EUROLEAGUE.REFRESH_PLAYER_TRADITIONAL_BY_GAME_FOR_GAMES")
        reader = self.upper.index("CREATE OR REPLACE FUNCTION EUROLEAGUE.GET_PLAYER_TRADITIONAL_PERGAME")
        self.assertLess(refresh, reader)
        self.assertIn("ANALYZE EUROLEAGUE.PLAYER_TRADITIONAL_BY_GAME", self.upper)


if __name__ == "__main__":
    unittest.main()
