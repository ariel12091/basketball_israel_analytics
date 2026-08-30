import re
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]
SQL = (ROOT / "sql" / "046_player_dashboard_reader.sql").read_text(encoding="utf-8")
SCRIPT = (ROOT / "scripts" / "apply_046_player_dashboard_reader.py").read_text(encoding="utf-8")
APP = (ROOT.parent / "app" / "R" / "server_tab8_euro.R").read_text(encoding="utf-8")


class PlayerDashboardReaderTests(unittest.TestCase):
    def test_is_additive_and_euroleague_scoped(self):
        body = "\n".join(line for line in SQL.splitlines() if not line.lstrip().startswith("--"))
        self.assertIn("CREATE OR REPLACE FUNCTION euroleague.four_factors_dashboard_compute", body)
        self.assertNotRegex(body.upper(), r"\b(DROP|ALTER|TRUNCATE|DELETE|UPDATE|INSERT)\b")
        self.assertNotRegex(body.upper(), r"\bBASKETBALL(?:_TEST)?\s*\.")

    def test_one_fact_aggregation_supplies_four_factors_and_ratings(self):
        self.assertEqual(SQL.count("FROM euroleague.player_four_factors_by_game"), 1)
        self.assertIn("sum(c.total_points)::numeric pts", SQL)
        self.assertIn("sum(c.onoff_minutes)::numeric mins", SQL)
        self.assertIn('"Net RTG Diff" numeric', SQL)
        self.assertIn('"Off ON Diff" numeric', SQL)
        self.assertIn('"Def ON Diff" numeric', SQL)

    def test_ratios_are_calculated_after_additive_sums(self):
        agg = SQL.split("), agg AS (", 1)[1].split("), rates AS (", 1)[0]
        self.assertNotIn("/", agg)
        self.assertIn("round(100.0*a.pts/nullif(a.poss,0),1) ppp", SQL)

    def test_execute_is_not_public(self):
        self.assertIn("REVOKE ALL ON FUNCTION", SQL)
        self.assertIn("TO app_readonly", SQL)

    def test_gate_defaults_to_rollback_and_compares_current_app_composition(self):
        self.assertIn('parser.add_argument("--apply", action="store_true")', SCRIPT)
        self.assertIn("con.rollback()", SCRIPT)
        self.assertIn('"four_factors_compute"', SCRIPT)
        self.assertIn('"onoff_compute"', SCRIPT)
        self.assertIn('"four_factors_dashboard_compute"', SCRIPT)

    def test_gate_covers_filter_classes_and_latency(self):
        for token in (
            "p_last_n_games", "p_team_ids_csv", "p_phase_csv", "p_opp_ids_csv",
            "p_home_away", "p_outcome", "p_opp_rank_side",
            "p_num_starters_off_min", "p_num_starters_def_max", "p_min_gn",
        ):
            self.assertIn(token, SCRIPT)
        self.assertIn("statistics.median", SCRIPT)

    def test_app_filtered_four_factors_uses_one_combined_call(self):
        self.assertIn("euroleague.four_factors_dashboard_compute", APP)
        block = APP.split("ff_ranked_df <- reactive({", 1)[1].split(
            "# --- Final Switcher ---", 1
        )[0]
        fallback = block.split("} else {", 1)[0]
        self.assertIn("df <- live_ff_result_df()", fallback)
        self.assertNotIn("run_onoff_compute_14", fallback)

        israeli = (ROOT.parent / "app" / "R" / "server_tab1.R").read_text(encoding="utf-8")
        self.assertIn("basketball_test.four_factors_dashboard_compute", israeli)
        block = israeli.split("ff_ranked_df <- reactive({", 1)[1].split(
            "# --- Full ranked Shot Profile data", 1
        )[0]
        self.assertNotIn("run_onoff_compute_14", block.split("} else {", 1)[0])

    def test_security_allowlists_include_combined_reader(self):
        for path in (
            ROOT.parent / "sql" / "security" / "enable_readonly_rls.sql",
            ROOT.parent / "sql" / "security" / "audit_app_access.sql",
        ):
            self.assertIn("four_factors_dashboard_compute", path.read_text(encoding="utf-8"))

    def test_israeli_candidate_is_additive_and_replaces_full_onoff_call(self):
        path = ROOT.parent / "sql" / "functions" / "four_factors_dashboard_compute.sql"
        sql = path.read_text(encoding="utf-8")
        self.assertIn("CREATE OR REPLACE FUNCTION basketball_test.four_factors_dashboard_compute", sql)
        self.assertIn("basketball_test.four_factors_compute", sql)
        self.assertNotIn("basketball_test.onoff_compute", sql)
        self.assertIn("basketball_test.player_four_factors_by_game", sql)


if __name__ == "__main__":
    unittest.main()
