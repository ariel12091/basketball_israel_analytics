from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "sql" / "043_team_minutes_pergame.sql").read_text(encoding="utf-8")
APP = (ROOT.parent / "app" / "R" / "server_tab9_euro_team.R").read_text(encoding="utf-8")


class TeamMinutesPergameMigrationTest(unittest.TestCase):
    def test_reader_uses_existing_mapped_pergame_fact(self) -> None:
        upper = SQL.upper()
        self.assertIn("GET_TEAM_MINUTES_PERGAME", upper)
        self.assertIn("EUROLEAGUE.LINEUP_TOTALS_BY_GAME", upper)
        self.assertIn("L.TYPE_LINEUP = 'OFFENSE'", upper)
        self.assertIn("SUM(L.ACTION_SPAN_SECONDS)", upper)

    def test_refresh_materializes_israeli_action_span_without_new_fact(self) -> None:
        upper = SQL.upper()
        self.assertIn("ADD COLUMN ACTION_SPAN_SECONDS", upper)
        self.assertIn("MAX(A.EVENT_ELAPSED_SECONDS) - MIN(A.EVENT_ELAPSED_SECONDS)", upper)
        self.assertIn("CREATE OR REPLACE FUNCTION EUROLEAGUE.REFRESH_LINEUP_TOTALS_BY_GAME", upper)
        self.assertIn("UPDATE EUROLEAGUE.LINEUP_TOTALS_BY_GAME", upper)
        self.assertNotIn("REFRESH_LINEUP_TOTALS_BY_GAME(NULL::BIGINT[])", upper)
        self.assertNotIn("CREATE TABLE", upper)

    def test_reader_has_no_clutch_parameters(self) -> None:
        signature = SQL.split("RETURNS TABLE", 1)[0].upper()
        self.assertNotIn("P_MAX_MARGIN", signature)
        self.assertNotIn("P_MAX_TIME_REMAINING", signature)

    def test_app_has_three_way_minutes_routing(self) -> None:
        self.assertIn('pergame = "get_team_minutes_pergame"', APP)
        self.assertIn('dynamic = "get_team_minutes_dynamic"', APP)
        self.assertIn('"get_team_minutes_direct"', APP)


if __name__ == "__main__":
    unittest.main()
