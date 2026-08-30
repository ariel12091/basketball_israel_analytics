import re
import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]
SQL = (ROOT / "sql" / "046_player_dashboard_reader.sql").read_text(encoding="utf-8")
SCRIPT = (ROOT / "scripts" / "apply_046_player_dashboard_reader.py").read_text(encoding="utf-8")
APP = (ROOT.parent / "app" / "R" / "server_tab8_euro.R").read_text(encoding="utf-8")
ISRAELI_SQL = (ROOT.parent / "sql" / "functions" / "four_factors_dashboard_compute.sql").read_text(encoding="utf-8")


def return_columns(sql):
    block = re.search(r"RETURNS TABLE\s*\((.*?)\)\s*LANGUAGE", sql, re.S | re.I).group(1)
    columns = []
    for item in block.split(","):
        match = re.match(r'\s*(?:"([^"]+)"|([A-Za-z_][A-Za-z0-9_]*))\s+', item)
        columns.append(match.group(1) or match.group(2))
    return columns


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
        self.assertIn("CREATE OR REPLACE FUNCTION basketball_test.four_factors_dashboard_compute", ISRAELI_SQL)
        self.assertNotIn("basketball_test.four_factors_compute(", ISRAELI_SQL)
        self.assertNotIn("basketball_test.onoff_compute(", ISRAELI_SQL)
        self.assertEqual(1, ISRAELI_SQL.count("FROM basketball_test.player_four_factors_by_game"))

    def test_both_leagues_share_the_dashboard_result_contract(self):
        self.assertEqual(47, len(return_columns(SQL)))
        self.assertEqual(return_columns(SQL), return_columns(ISRAELI_SQL))

    def test_both_leagues_share_one_scan_aggregation_stages(self):
        for sql, fact in (
            (SQL, "euroleague.player_four_factors_by_game"),
            (ISRAELI_SQL, "basketball_test.player_four_factors_by_game"),
        ):
            self.assertEqual(1, sql.count("FROM " + fact))
            for stage in ("games AS MATERIALIZED", "agg AS", "rates AS", "p AS"):
                self.assertIn(stage, sql)
            for additive in (
                "total_points", "total_poss", "onoff_minutes", "ts_poss_count",
                "oreb_count", "oreb_opportunities", "tov_count", "steal_count",
                "deflection_count", "total_ft_attempts", "total_fga", "total_fgm",
                "total_fg3_made",
            ):
                self.assertIn(additive, sql)
            self.assertNotIn("four_factors_compute(", sql.split("AS $function$", 1)[1])
            self.assertNotIn("onoff_compute(", sql.split("AS $function$", 1)[1])

    def test_both_leagues_share_rate_formula_contract(self):
        formulas = (
            "a.pts/nullif(2*a.ts_poss,0)::numeric",
            "(a.fgm+0.5*a.fg3m)/nullif(a.fga,0)::numeric",
            "a.oreb/nullif(a.oreb_opp,0)::numeric",
            "a.tov/nullif(a.poss,0)::numeric",
            "(a.steals+a.deflections)/nullif(a.poss,0)::numeric",
            "a.fta/nullif(a.fga,0)::numeric",
            "round(100.0*a.pts/nullif(a.poss,0),1)",
            "(p.off_on_ppp-p.off_off_ppp)-(p.def_on_ppp-p.def_off_ppp)",
        )
        for sql in (SQL, ISRAELI_SQL):
            compact = re.sub(r"\s+", "", sql.lower())
            for formula in formulas:
                self.assertIn(formula, compact)

    def test_euro_gate_rejects_vacuous_presets(self):
        self.assertIn("MUST_RETURN_ROWS", SCRIPT)
        self.assertIn("parity is vacuous", SCRIPT)


if __name__ == "__main__":
    unittest.main()
