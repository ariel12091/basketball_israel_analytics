from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
SQL = (ROOT / "sql" / "candidates" / "050_two_call_team_dashboard_readers.sql").read_text(
    encoding="utf-8"
)


class CombinedTeamDashboardReadersTest(unittest.TestCase):
    def test_adds_the_approved_one_call_and_two_call_readers(self):
        for reader in (
            "get_team_metrics_pergame",
            "get_team_dashboard_dynamic",
            "get_team_metrics_direct",
        ):
            self.assertEqual(1, SQL.count(f"CREATE OR REPLACE FUNCTION euroleague.{reader}"))

    def test_pergame_filters_schedule_once_and_keeps_duration_separate(self):
        body = SQL.split(
            "CREATE OR REPLACE FUNCTION euroleague.get_team_metrics_pergame", 1
        )[1].split(
            "CREATE OR REPLACE FUNCTION euroleague.get_team_dashboard_dynamic", 1
        )[0]
        self.assertEqual(1, body.count("games_filtered AS MATERIALIZED"))
        self.assertEqual(1, body.count("FROM euroleague.team_four_factors_by_game"))
        self.assertNotIn("lineup_totals_by_game", body)
        self.assertIn("metric_agg AS (", body)
        self.assertNotIn("minutes_agg AS (", body)

    def test_dynamic_materializes_shared_fact_once(self):
        body = SQL.split(
            "CREATE OR REPLACE FUNCTION euroleague.get_team_dashboard_dynamic", 1
        )[1].split(
            "CREATE OR REPLACE FUNCTION euroleague.get_team_metrics_direct", 1
        )[0]
        self.assertEqual(1, body.count("facts AS MATERIALIZED"))
        self.assertEqual(1, body.count("filtered_team_game_facts("))
        self.assertIn("sum(f.seconds) FILTER(WHERE f.type_lineup='offense')", body)

    def test_direct_streams_one_metric_scan_and_leaves_minutes_separate(self):
        body = SQL.split(
            "CREATE OR REPLACE FUNCTION euroleague.get_team_metrics_direct", 1
        )[1]
        self.assertEqual(1, body.count("acts AS ("))
        self.assertEqual(1, body.count("FROM euroleague.player_stats_actions_by_game"))
        self.assertIn("metric_agg AS (", body)
        self.assertNotIn("segment_minutes AS (", body)
        self.assertNotIn("event_elapsed_seconds", body)

    def test_common_metrics_use_single_round_net_rating(self):
        bodies = re.findall(
            r"CREATE OR REPLACE FUNCTION euroleague\.get_team_(?:dashboard|metrics)_\w+[\s\S]*?\$function\$;",
            SQL,
        )
        self.assertEqual(3, len(bodies))
        for body in bodies:
            self.assertIn(
                "round(100.0*r.off_pts/nullif(r.off_poss,0)-100.0*r.def_pts/nullif(r.def_poss,0),1)",
                body,
            )

    def test_security_contract_is_explicit(self):
        for reader in ("get_team_metrics_pergame", "get_team_dashboard_dynamic", "get_team_metrics_direct"):
            signature = f"euroleague.{reader}"
            self.assertEqual(1, SQL.count(f"REVOKE ALL ON FUNCTION {signature}"))
            self.assertEqual(1, SQL.count(f"GRANT EXECUTE ON FUNCTION {signature}"))
        self.assertNotIn("CASCADE", SQL.upper())
        self.assertNotRegex(SQL.upper(), r"\bBASKETBALL(?:_TEST)?\s*\.")


if __name__ == "__main__":
    unittest.main()
