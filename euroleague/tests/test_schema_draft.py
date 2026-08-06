from __future__ import annotations

import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
DDL_PATH = REPO_ROOT / "euroleague" / "sql" / "001_core_shadow_schema.sql"
ONOFF_SQL_PATH = (
    REPO_ROOT
    / "euroleague"
    / "sql"
    / "analytics"
    / "player_onoff_ppp_readonly.sql"
)
ANALYTICS_DDL_PATH = (
    REPO_ROOT
    / "euroleague"
    / "sql"
    / "002_existing_analytics_compatibility.sql"
)
APP_MV_DDL_PATH = (
    REPO_ROOT
    / "euroleague"
    / "sql"
    / "003_app_materialized_views.sql"
)


class ShadowSchemaDraftTest(unittest.TestCase):
    def setUp(self) -> None:
        self.ddl = DDL_PATH.read_text(encoding="utf-8")

    def test_is_isolated_and_non_destructive(self) -> None:
        upper = self.ddl.upper()
        self.assertIn("EUROLEAGUE SHADOW SCHEMA", upper)
        self.assertNotRegex(upper, r"\bDROP\s+")
        self.assertNotIn("BASKETBALL_TEST.", upper)
        self.assertNotIn("BASKETBALL.", upper)
        self.assertIn("CREATE SCHEMA IF NOT EXISTS EUROLEAGUE", upper)

    def test_contains_required_shadow_grains(self) -> None:
        tables = set(
            re.findall(
                r"CREATE TABLE IF NOT EXISTS euroleague\.([a-z_]+)",
                self.ddl,
            )
        )
        self.assertEqual(
            tables,
            {
                "load_runs",
                "teams",
                "players",
                "schedule",
                "source_artifacts",
                "full_rosters",
                "team_boxscores",
                "actions_raw",
                "actions_clean",
                "possessions",
                "lineups",
                "lineup_players",
                "action_lineups",
                "stints",
                "pws",
                "reconciliation_metrics",
                "game_qa",
                "qa_incidents",
            },
        )

    def test_records_package_lineage_and_publication_gate(self) -> None:
        self.assertIn("package_version text NOT NULL", self.ddl)
        self.assertIn("source_package_version text NOT NULL", self.ddl)
        self.assertIn("validate_on_court_player boolean NOT NULL", self.ddl)
        self.assertIn("publication_status <> 'clear'", self.ddl)
        self.assertIn("NOT publishable OR", self.ddl)

    def test_player_onoff_walkthrough_is_read_only_and_action_grained(self) -> None:
        sql = ONOFF_SQL_PATH.read_text(encoding="utf-8")
        executable = "\n".join(
            line for line in sql.splitlines() if not line.lstrip().startswith("--")
        ).upper()
        self.assertNotRegex(
            executable,
            r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE)\b",
        )
        self.assertIn("EUROLEAGUE.ACTION_LINEUPS", executable)
        self.assertIn("EUROLEAGUE.POSSESSIONS", executable)
        self.assertIn("EUROLEAGUE.FULL_ROSTERS", executable)
        self.assertIn("100.0 * PP.OFF_ON_POINTS", executable)
        self.assertIn("NOT IN ('TEAM', 'TOTAL')", executable)

    def test_analytics_compatibility_layer_reuses_additive_contract(self) -> None:
        ddl = ANALYTICS_DDL_PATH.read_text(encoding="utf-8")
        upper = ddl.upper()

        self.assertIn("EUROLEAGUE SHADOW SCHEMA", upper)
        self.assertNotRegex(upper, r"\bDROP\s+")
        self.assertNotIn("BASKETBALL_TEST.", upper)
        self.assertNotIn("BASKETBALL.", upper)
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS EUROLEAGUE.PLAYER_FOUR_FACTORS_BY_GAME",
            upper,
        )
        self.assertIn(
            "REFRESH_PLAYER_FOUR_FACTORS_BY_GAME_FOR_GAMES", upper
        )
        self.assertIn("PLAYER_ONOFF_BY_SEASON", upper)
        self.assertIn("PLAYER_FOUR_FACTORS_BY_SEASON", upper)
        self.assertIn("A.TOTAL_POINTS / (2 * NULLIF(A.TS_POSS_COUNT, 0))", upper)
        self.assertIn("A.TOV_COUNT / NULLIF(A.TOTAL_POSS, 0)", upper)
        self.assertIn(
            "A.TOTAL_FT_ATTEMPTS / NULLIF(A.TOTAL_FGA, 0)", upper
        )

    def test_analytics_contract_uses_package_lineups_and_complete_roster_grid(self) -> None:
        upper = ANALYTICS_DDL_PATH.read_text(encoding="utf-8").upper()

        self.assertIn("EUROLEAGUE.ACTION_LINEUPS", upper)
        self.assertIn("EUROLEAGUE.LINEUP_PLAYERS", upper)
        self.assertIn("EUROLEAGUE.POSSESSIONS", upper)
        self.assertIn("COMPLETE_GRID", upper)
        self.assertIn("VALUES (0::SMALLINT), (1::SMALLINT)", upper)
        self.assertIn("LOWER(P.PROVIDER_PLAYER_ID) NOT IN ('TEAM', 'TOTAL')", upper)
        self.assertIn("MAX(RE.RAW_EVENT_ELAPSED_SECONDS) OVER", upper)
        self.assertIn("GREATEST(MAX(AR.PERIOD) - 4, 0) * 300", upper)

    def test_app_aggregates_use_indexed_materialized_views(self) -> None:
        upper = APP_MV_DDL_PATH.read_text(encoding="utf-8").upper()

        self.assertNotIn("BASKETBALL_TEST.", upper)
        self.assertNotIn("BASKETBALL.", upper)
        for relation in (
            "FINAL_SCHEDULE_MV",
            "PLAYER_ONOFF_BY_SEASON_MV",
            "PLAYER_FOUR_FACTORS_BY_SEASON_MV",
        ):
            self.assertIn(
                f"CREATE MATERIALIZED VIEW IF NOT EXISTS EUROLEAGUE.{relation}",
                upper,
            )
        self.assertEqual(upper.count("CREATE UNIQUE INDEX IF NOT EXISTS"), 3)
        self.assertIn("REFRESH_APP_MATERIALIZED_VIEWS", upper)
        self.assertLess(
            upper.index("REFRESH MATERIALIZED VIEW EUROLEAGUE.FINAL_SCHEDULE_MV"),
            upper.index(
                "REFRESH MATERIALIZED VIEW EUROLEAGUE.PLAYER_ONOFF_BY_SEASON_MV"
            ),
        )


if __name__ == "__main__":
    unittest.main()
