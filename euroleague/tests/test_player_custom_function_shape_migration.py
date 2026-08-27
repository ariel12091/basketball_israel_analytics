from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "sql" / "044_player_custom_function_shape.sql").read_text(encoding="utf-8")


class PlayerCustomFunctionShapeMigrationTest(unittest.TestCase):
    def test_reuses_resolved_ids_without_new_storage(self) -> None:
        self.assertIn("euroleague.lineup_totals_by_game", SQL)
        self.assertIn("lineup_identities AS (", SQL)
        self.assertIn("unnest(li.player_ids)", SQL)
        self.assertNotIn("source_player_name=ANY", SQL)
        self.assertNotIn("CREATE TABLE", SQL)
        self.assertNotIn("ALTER TABLE", SQL)
        self.assertNotIn("CREATE INDEX", SQL)

    def test_uses_best_measured_execution_shape(self) -> None:
        self.assertIn("LANGUAGE plpgsql STABLE SECURITY DEFINER", SQL)
        self.assertIn("SET plan_cache_mode = force_custom_plan", SQL)
        self.assertIn("SET jit = off", SQL)
        self.assertIn("RETURN QUERY\nWITH normalized AS", SQL)
        acts = SQL.split("acts AS MATERIALIZED", 1)[1].split("observed_lineups AS", 1)[0]
        self.assertEqual(acts.count("FROM euroleague.player_stats_action_context a"), 2)
        self.assertIn("UNION ALL", acts)

    def test_deduplicates_before_player_expansion(self) -> None:
        identities = SQL.split("lineup_identities AS (", 1)[1].split("lineup_map AS", 1)[0]
        mapping = SQL.split("lineup_map AS MATERIALIZED", 1)[1].split("poss_end AS", 1)[0]
        self.assertIn("SELECT DISTINCT", identities)
        self.assertIn("l.player_ids", identities)
        self.assertNotIn("DISTINCT", mapping)

    def test_is_additive_and_euroleague_scoped(self) -> None:
        upper = SQL.upper()
        self.assertNotIn("DROP ", upper)
        self.assertNotIn("TRUNCATE ", upper)
        self.assertNotIn("BASKETBALL.", upper)
        self.assertNotIn("BASKETBALL_TEST.", upper)


if __name__ == "__main__":
    unittest.main()
