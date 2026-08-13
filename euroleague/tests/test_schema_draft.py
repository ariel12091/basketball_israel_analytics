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
CANONICAL_ACTIONS_DDL_PATH = (
    REPO_ROOT
    / "euroleague"
    / "sql"
    / "010_canonical_actions.sql"
)
ACTIONS_CONSUMER_DDL_PATH = (
    REPO_ROOT
    / "euroleague"
    / "sql"
    / "011_actions_consumer_candidates.sql"
)
ACTIONS_CUTOVER_DDL_PATH = (
    REPO_ROOT
    / "euroleague"
    / "sql"
    / "012_actions_consumer_cutover.sql"
)
TEAM_MINUTES_DDL_PATH = (
    REPO_ROOT / "euroleague" / "sql" / "018_team_minutes_read_layer.sql"
)
CLUTCH_DDL_PATH = (
    REPO_ROOT / "euroleague" / "sql" / "019_clutch_read_layer.sql"
)
DEFAULT_CLUTCH_DDL_PATH = (
    REPO_ROOT / "euroleague" / "sql" / "020_default_clutch_fast_path.sql"
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

    def test_canonical_actions_migration_is_additive_and_validated(self) -> None:
        ddl = CANONICAL_ACTIONS_DDL_PATH.read_text(encoding="utf-8")
        upper = ddl.upper()

        self.assertIn("EUROLEAGUE SHADOW SCHEMA", upper)
        self.assertNotRegex(upper, r"\bDROP\s+")
        self.assertNotIn("BASKETBALL_TEST.", upper)
        self.assertNotIn("BASKETBALL.", upper)
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS EUROLEAGUE.ACTIONS", upper
        )
        self.assertIn("LINEUP_A TEXT[] NOT NULL", upper)
        self.assertIn("LINEUP_B TEXT[] NOT NULL", upper)
        self.assertIn("END_POSSESSION BOOLEAN NOT NULL", upper)
        self.assertIn("POSSESSION_OFFENSE_TEAM_ID BIGINT", upper)
        self.assertIn("FROM EUROLEAGUE.ACTIONS_RAW AR", upper)
        self.assertIn("JOIN EUROLEAGUE.ACTIONS_CLEAN AC", upper)
        actions_definition = upper.split(
            "CREATE TABLE IF NOT EXISTS EUROLEAGUE.ACTIONS", 1
        )[1].split(");", 1)[0]
        self.assertNotIn("RAW_EVENT", actions_definition)

    def test_actions_consumer_candidate_is_isolated_and_additive(self) -> None:
        ddl = ACTIONS_CONSUMER_DDL_PATH.read_text(encoding="utf-8")
        upper = ddl.upper()

        self.assertIn("EUROLEAGUE SHADOW SCHEMA", upper)
        self.assertNotRegex(upper, r"\bDROP\s+")
        self.assertNotIn("BASKETBALL_TEST.", upper)
        self.assertNotIn("BASKETBALL.", upper)
        self.assertIn("FROM EUROLEAGUE.ACTIONS A", upper)
        self.assertIn("OWN_LINEUP TEXT[] NOT NULL", upper)
        self.assertIn("OPP_LINEUP TEXT[] NOT NULL", upper)
        self.assertIn("ACTIONS-BASED EVENT FACT DIFFERS", upper)
        self.assertIn("ACTIONS-BASED MATCHUP SEGMENTS DIFFER", upper)

    def test_actions_cutover_drops_only_named_euroleague_relations(self) -> None:
        ddl = ACTIONS_CUTOVER_DDL_PATH.read_text(encoding="utf-8")
        upper = ddl.upper()

        self.assertIn("EUROLEAGUE SHADOW SCHEMA", upper)
        self.assertNotIn("BASKETBALL_TEST.", upper)
        self.assertNotIn("BASKETBALL.", upper)
        self.assertNotRegex(upper, r"\bCASCADE\b")
        dropped_tables = set(
            re.findall(r"DROP TABLE EUROLeague\.([a-z_]+)", ddl, re.IGNORECASE)
        )
        self.assertEqual(
            dropped_tables,
            {
                "action_team_context",
                "matchup_segments",
                "pws",
                "stints",
                "action_lineups",
                "lineup_players",
                "lineups",
                "possessions",
                "actions_clean",
            },
        )
        self.assertIn("FROM EUROLEAGUE.ACTION_TEAM_CONTEXT_ACTIONS", upper)
        self.assertIn("FROM EUROLEAGUE.MATCHUP_SEGMENTS_ACTIONS", upper)
        self.assertIn("LEFT JOIN EUROLEAGUE.ACTIONS A", upper)
        self.assertIn("OUTPUT DIFFERS FROM BASELINE", upper)
        self.assertIn("AS OFF_PPP", upper)
        self.assertIn("AS DEF_PPP", upper)
        self.assertIn("AS GAMES_PLAYED", upper)
        self.assertIn("AS RANK_NET_RTG", upper)
        self.assertNotIn("AS OFF_RTG", upper)
        self.assertNotIn("AS DEF_RTG", upper)
        self.assertIn("- 'DERIVED_AT'", upper)

    def test_team_minutes_read_layer_uses_canonical_game_segments(self) -> None:
        ddl = TEAM_MINUTES_DDL_PATH.read_text(encoding="utf-8")
        upper = ddl.upper()

        self.assertNotIn("BASKETBALL_TEST.", upper)
        self.assertNotIn("BASKETBALL.", upper)
        self.assertIn("GET_TEAM_MINUTES_DYNAMIC", upper)
        self.assertIn("EUROLEAGUE.MATCHUP_SEGMENTS_ACTIONS", upper)
        self.assertIn("SUM(MS.SEGMENT_SECONDS) / 60.0", upper)
        self.assertIn("GROUP BY MS.GAME_ID, MS.TEAM_ID", upper)
        self.assertIn("RETURNS TABLE (TEAM_ID BIGINT, MINUTES NUMERIC)", upper)
        self.assertIn("SECURITY DEFINER", upper)
        self.assertIn("SET SEARCH_PATH = PG_CATALOG, EUROLEAGUE, PUBLIC", upper)

    def test_clutch_read_layer_uses_pre_event_scores_and_exact_intervals(self) -> None:
        ddl = CLUTCH_DDL_PATH.read_text(encoding="utf-8")
        upper = ddl.upper()

        self.assertNotIn("BASKETBALL_TEST.", upper)
        self.assertNotIn("BASKETBALL.", upper)
        self.assertIn("CLUTCH_EVENT_QUALIFIES", upper)
        self.assertIn("CLUTCH_SEGMENT_DURATIONS", upper)
        self.assertIn("FILTERED_TEAM_GAME_FACTS", upper)
        self.assertIn("ATC.OWN_TEAM_SCORE", upper)
        self.assertIn("ATC.EVENT_TEAM_ID = ATC.TEAM_ID", upper)
        self.assertIn("ATC.EVENT_TEAM_ID = ATC.OPPONENT_TEAM_ID", upper)
        self.assertIn("LEAD(SS.STATE_START", upper)
        self.assertIn("MS.START_ELAPSED_SECONDS", upper)
        self.assertIn("MS.END_ELAPSED_SECONDS", upper)
        self.assertIn("2400::NUMERIC", upper)
        self.assertIn("P_PERIOD > 4", upper)
        self.assertIn("NOT COALESCE(P_OT_MARGIN_FILTER, FALSE)", upper)
        self.assertIn("EUROLEAGUE.ACTION_TEAM_CONTEXT_ACTIONS", upper)
        self.assertIn("EUROLEAGUE.MATCHUP_SEGMENTS_ACTIONS", upper)
        self.assertIn("SECURITY DEFINER", upper)
        self.assertIn("REVOKE ALL ON FUNCTION", upper)

    def test_default_clutch_cache_is_incremental_and_keeps_custom_dynamic(self) -> None:
        ddl = DEFAULT_CLUTCH_DDL_PATH.read_text(encoding="utf-8")
        upper = ddl.upper()

        self.assertNotIn("BASKETBALL_TEST.", upper)
        self.assertNotIn("BASKETBALL.", upper)
        self.assertIn("CREATE TABLE EUROLEAGUE.DEFAULT_CLUTCH_LINEUP_TOTALS_BY_GAME", upper)
        self.assertNotIn("CREATE MATERIALIZED VIEW EUROLEAGUE.DEFAULT_CLUTCH", upper)
        self.assertIn("REFRESH_DEFAULT_CLUTCH_FOR_GAMES", upper)
        self.assertIn("DELETE FROM EUROLEAGUE.DEFAULT_CLUTCH_LINEUP_TOTALS_BY_GAME", upper)
        self.assertIn("TARGET_GAME_IDS, 5, 'ALL', 300, FALSE", upper)
        self.assertIn("ELSIF P_MAX_MARGIN = 5", upper)
        self.assertIn("P_MAX_TIME_REMAINING = 300", upper)
        self.assertIn("FROM EUROLEAGUE.LINEUP_TOTALS_BY_GAME", upper)
        self.assertIn("FROM EUROLEAGUE.DEFAULT_CLUTCH_LINEUP_TOTALS_BY_GAME", upper)
        self.assertIn("FROM EUROLEAGUE.CLUTCH_TEAM_GAME_FACTS", upper)
        self.assertIn("SECURITY DEFINER", upper)
        self.assertIn("ENABLE ROW LEVEL SECURITY", upper)
        self.assertIn("CREATE POLICY APP_READONLY_SELECT_ALL", upper)
        self.assertIn("REVOKE ALL ON TABLE", upper)

    def test_player_onoff_walkthrough_is_read_only_and_action_grained(self) -> None:
        sql = ONOFF_SQL_PATH.read_text(encoding="utf-8")
        executable = "\n".join(
            line for line in sql.splitlines() if not line.lstrip().startswith("--")
        ).upper()
        self.assertNotRegex(
            executable,
            r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE|CREATE)\b",
        )
        self.assertIn("EUROLEAGUE.ACTION_TEAM_CONTEXT_ACTIONS", executable)
        self.assertIn("EUROLEAGUE.ACTIONS", executable)
        self.assertIn("EUROLEAGUE.FULL_ROSTERS", executable)
        self.assertIn("100.0 * PP.OFF_ON_POINTS", executable)
        self.assertIn("NOT IN ('TEAM', 'TOTAL')", executable)
        self.assertNotIn("EUROLEAGUE.ACTION_LINEUPS", executable)
        self.assertNotIn("EUROLEAGUE.POSSESSIONS", executable)

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
