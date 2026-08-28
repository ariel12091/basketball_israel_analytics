"""Static contract and gate tests for the Tab 8 query remediation (045).

No database connection is used. The candidate SQL is checked against the
definitions it is derived from, and the apply script's gate functions are
exercised directly to prove each one rejects a bad candidate.
"""

from __future__ import annotations

import importlib.util
import re
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SOURCE = (ROOT / "sql" / "004_app_read_layer.sql").read_text(encoding="utf-8")
CANDIDATE_A = (ROOT / "sql" / "candidates" / "045a_direct_fact.sql").read_text(encoding="utf-8")
CANDIDATE_B = (ROOT / "sql" / "candidates" / "045b_single_aggregate.sql").read_text(encoding="utf-8")
MIGRATION_PATH = ROOT / "sql" / "045_tab8_query_shape.sql"
SCRIPT_PATH = ROOT / "scripts" / "apply_045_tab8_query_shape.py"
SCRIPT_TEXT = SCRIPT_PATH.read_text(encoding="utf-8")

FUNCTIONS = ("onoff_compute", "four_factors_compute")
VIEW = "euroleague.player_game_context"
FACT = "euroleague.player_four_factors_by_game"


def load_script():
    spec = importlib.util.spec_from_file_location("apply_045", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def strip_comments(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines()
                     if not line.strip().startswith("--"))


def definition(sql: str, name: str) -> str:
    start = sql.index("CREATE OR REPLACE FUNCTION euroleague." + name + "(")
    end = sql.index("\n$function$;", start) + len("\n$function$;")
    return sql[start:end]


def deployable() -> list[tuple[str, str]]:
    """Every DDL file that may end up defining the live functions."""
    files = [("045a", CANDIDATE_A), ("045b", CANDIDATE_B)]
    if MIGRATION_PATH.exists():
        files.append(("045", MIGRATION_PATH.read_text(encoding="utf-8")))
    return files


class CandidateSqlContractTest(unittest.TestCase):
    def test_is_additive_and_euroleague_scoped(self) -> None:
        for name, sql in deployable():
            with self.subTest(file=name):
                upper = strip_comments(sql).upper()
                self.assertIn("EUROLEAGUE SHADOW SCHEMA", sql.upper())
                for forbidden in ("DROP ", "TRUNCATE ", "CASCADE", "BASKETBALL.",
                                  "BASKETBALL_TEST.", "CREATE SCHEMA", "OWNER TO",
                                  "GRANT ", "REVOKE "):
                    self.assertNotIn(forbidden, upper, forbidden)

    def test_public_signatures_and_return_contracts_are_unchanged(self) -> None:
        for name, sql in deployable():
            for function in FUNCTIONS:
                with self.subTest(file=name, function=function):
                    original = definition(SOURCE, function)
                    candidate = definition(sql, function)
                    head = "\n)\nLANGUAGE plpgsql"
                    self.assertEqual(
                        original[:original.index(head)],
                        candidate[:candidate.index(head)],
                        "signature or RETURNS TABLE contract changed")
                    for clause in ("LANGUAGE plpgsql", "STABLE",
                                   "SET plan_cache_mode = force_custom_plan"):
                        self.assertIn(clause, candidate)
                    self.assertNotIn("SECURITY DEFINER", candidate)
                    self.assertNotIn("SET jit", candidate)
                    self.assertNotIn("enable_seqscan", candidate)

    def test_no_drop_function_so_execute_grants_survive(self) -> None:
        for name, sql in deployable():
            with self.subTest(file=name):
                self.assertNotIn("DROP FUNCTION", strip_comments(sql).upper())
                self.assertEqual(2, sql.count("CREATE OR REPLACE FUNCTION euroleague."))

    def test_reads_the_base_fact_and_not_the_context_view(self) -> None:
        for name, sql in deployable():
            for function in FUNCTIONS:
                with self.subTest(file=name, function=function):
                    body = definition(sql, function)
                    self.assertIn("FROM " + FACT + " c\n", body)
                    self.assertNotIn(VIEW, body)

    def test_fact_is_joined_on_both_game_id_and_team_id(self) -> None:
        for name, sql in deployable():
            for function in FUNCTIONS:
                with self.subTest(file=name, function=function):
                    body = definition(sql, function)
                    join = body.split("FROM " + FACT + " c\n", 1)[1].splitlines()[0]
                    self.assertEqual(
                        "    JOIN games g ON g.game_id = c.game_id "
                        "AND g.team_id = c.team_id", join)

    def test_schedule_context_is_resolved_only_in_the_games_cte(self) -> None:
        for name, sql in deployable():
            for function in FUNCTIONS:
                with self.subTest(file=name, function=function):
                    body = definition(sql, function)
                    aggregation = body.split("  games AS (", 1)[1].split(
                        "\n  ),\n", 1)[1]
                    self.assertNotIn("euroleague.schedule", aggregation)
                    self.assertNotIn("euroleague.final_schedule", aggregation)


class CandidateADerivationTest(unittest.TestCase):
    """Candidate A must be the live definitions with one line changed."""

    def test_only_the_aggregation_source_line_differs(self) -> None:
        for function in FUNCTIONS:
            with self.subTest(function=function):
                original = definition(SOURCE, function).splitlines()
                candidate = definition(CANDIDATE_A, function).splitlines()
                self.assertEqual(len(original), len(candidate))
                differences = [
                    (a, b) for a, b in zip(original, candidate) if a != b]
                self.assertEqual(
                    [("    FROM " + VIEW + " c",
                      "    FROM " + FACT + " c")],
                    differences)


class CandidateBDerivationTest(unittest.TestCase):
    """Candidate B keeps A's source swap and only collapses the aggregation."""

    def test_single_aggregation_at_the_output_key(self) -> None:
        onoff = definition(CANDIDATE_B, "onoff_compute")
        self.assertNotIn("  agg AS (", onoff)
        self.assertEqual(1, onoff.count("GROUP BY c.player_id, c.team_id\n"))
        self.assertNotIn("GROUP BY a.player_id", onoff)
        self.assertNotIn("max(", onoff)
        # 8 points/possession totals + ON minutes + 16 shooting splits.
        self.assertEqual(25, onoff.count("FILTER (WHERE c.type_lineup"))

    def test_four_factors_divides_only_after_summing(self) -> None:
        body = definition(CANDIDATE_B, "four_factors_compute")
        totals = body.split("  totals AS (", 1)[1].split("\n  ),", 1)[0]
        pivoted = body.split("  pivoted AS (", 1)[1].split("\n  )\n", 1)[0]
        self.assertNotIn("/", totals, "no ratio may be computed before the sum")
        self.assertNotIn("sum(", pivoted, "no aggregate may survive into the rates")
        self.assertEqual(1, body.count("GROUP BY c.player_id, c.team_id\n"))
        self.assertNotIn("  rates AS (", body)
        self.assertNotIn("max(", body)

    def test_denominator_guards_are_preserved_one_for_one(self) -> None:
        original = definition(SOURCE, "four_factors_compute")
        body = definition(CANDIDATE_B, "four_factors_compute")
        rates = original.split("  rates AS (", 1)[1].split("\n  ),", 1)[0]
        pivoted = body.split("  pivoted AS (", 1)[1].split("\n  )\n", 1)[0]
        # The original guarded six ratios per offense/defense x ON/OFF group.
        # The rewrite computes the same ratios once per combination: five that
        # exist for all four combinations plus defence-only disruption rate.
        self.assertEqual(6, rates.count("NULLIF"))
        self.assertEqual(4 * 5 + 2, pivoted.count("NULLIF"))
        self.assertIn("NULLIF(2 * t.off_on_ts_poss, 0)::numeric", pivoted)
        self.assertNotIn("0.44", pivoted)

    def test_totals_carry_the_original_numeric_types(self) -> None:
        onoff = definition(CANDIDATE_B, "onoff_compute")
        self.assertEqual(
            ["off_on_pts", "off_on_poss", "off_off_pts", "off_off_poss",
             "def_on_pts", "def_on_poss", "def_off_pts", "def_off_poss",
             "mins_on"],
            re.findall(r"\)::numeric AS (\w+)", onoff))
        self.assertEqual(
            16, len(re.findall(r"\)::bigint AS \w+_fg[23]_(?:made|att)", onoff)))
        ff = definition(CANDIDATE_B, "four_factors_compute")
        totals = ff.split("  totals AS (", 1)[1].split("\n  ),", 1)[0]
        # Points stay numeric; every other summed measure stays bigint.
        self.assertEqual(4, len(re.findall(r"\)::numeric AS \w+_pts,", totals)))
        self.assertEqual(44, totals.count("FILTER (WHERE c.type_lineup"))
        self.assertEqual(40, len(re.findall(r"\)::bigint AS ", totals)))


class MigrationMatchesAcceptedCandidateTest(unittest.TestCase):
    """Migration 045 must ship candidate A's bodies verbatim, plus work_mem."""

    def setUp(self) -> None:
        if not MIGRATION_PATH.exists():
            self.skipTest("sql/045_tab8_query_shape.sql not written yet")
        self.sql = MIGRATION_PATH.read_text(encoding="utf-8")

    def test_function_bodies_are_byte_identical_to_candidate_a(self) -> None:
        for function in FUNCTIONS:
            with self.subTest(function=function):
                self.assertEqual(definition(CANDIDATE_A, function),
                                 definition(self.sql, function))

    def test_carries_the_index_and_the_bounded_work_mem(self) -> None:
        body = strip_comments(self.sql)
        self.assertIn("CREATE INDEX IF NOT EXISTS euroleague_pff_game_team_idx", body)
        self.assertIn("ON euroleague.player_four_factors_by_game (game_id, team_id)", body)
        self.assertEqual(2, body.count("SET work_mem = '16MB'"))
        self.assertEqual(2, body.count("ALTER FUNCTION euroleague."))
        self.assertNotIn("SET work_mem", body.split("ALTER FUNCTION", 1)[0])

    def test_is_one_transaction(self) -> None:
        body = strip_comments(self.sql)
        self.assertEqual(1, body.count("BEGIN;"))
        self.assertEqual(1, body.count("COMMIT;"))
        self.assertIn("SET LOCAL search_path TO euroleague, public;", body)


class ApplyScriptSafetyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.module = load_script()

    def test_index_is_exactly_game_id_then_team_id(self) -> None:
        self.assertEqual(
            "CREATE INDEX IF NOT EXISTS euroleague_pff_game_team_idx "
            "ON euroleague.player_four_factors_by_game (game_id, team_id)",
            self.module.INDEX_DDL)
        for name, sql in deployable():
            if name == "045":
                with self.subTest(file=name):
                    indexes = re.findall(r"CREATE INDEX[^;]*", sql)
                    self.assertEqual(1, len(indexes))
                    self.assertIn("(game_id, team_id)", indexes[0])
                    self.assertNotIn("INCLUDE", indexes[0])

    def test_defaults_to_rollback(self) -> None:
        self.assertIn('cur.execute("COMMIT" if args.apply else "ROLLBACK")', SCRIPT_TEXT)
        self.assertIn('parser.add_argument("--apply", action="store_true"', SCRIPT_TEXT)
        self.assertIn("--apply requires --candidate", SCRIPT_TEXT)

    def test_companion_is_measured_in_the_same_session_before_any_ddl(self) -> None:
        # The gate must compare against a companion timed under the same
        # instance conditions, not a constant from another session.
        before_ddl = SCRIPT_TEXT.split('cur.execute("BEGIN")', 1)[0]
        self.assertIn("companion = measure_companion(cur)", before_ddl)
        self.assertIn("baseline = capture_baseline(cur, expect)", before_ddl)
        self.assertLess(before_ddl.index("baseline = capture_baseline(cur, expect)"),
                        before_ddl.index("companion = measure_companion(cur)"))

    def test_israeli_schema_is_read_only_by_the_benchmark_never_by_the_ddl(self) -> None:
        for name, sql in deployable():
            with self.subTest(file=name):
                self.assertNotIn("basketball", strip_comments(sql).lower())
        self.assertIn("basketball_test.onoff_compute", SCRIPT_TEXT)
        for statement in SCRIPT_TEXT.split("COMPANION_CALLS", 1)[1][:2000].splitlines():
            lowered = statement.lower()
            if "basketball_test" in lowered:
                self.assertIn("select * from", lowered)

    def test_gates_run_on_direct_port_only(self) -> None:
        self.assertIn("candidate gating requires direct port 5432", SCRIPT_TEXT)
        self.assertIn("direct_port=5432", SCRIPT_TEXT)

    def test_candidate_parser_rejects_unsafe_ddl(self) -> None:
        bad = {
            "no marker": "CREATE OR REPLACE FUNCTION euroleague.f() RETURNS int AS $$ SELECT 1 $$;",
            "israeli": "-- EUROLEAGUE SHADOW SCHEMA\nSELECT * FROM basketball_test.schedule;",
            "destructive": "-- EUROLEAGUE SHADOW SCHEMA\nDROP FUNCTION euroleague.f();",
            "grant": "-- EUROLEAGUE SHADOW SCHEMA\nGRANT SELECT ON euroleague.x TO app_readonly;",
        }
        for label, sql in bad.items():
            with self.subTest(case=label):
                path = Path(self.id() + ".sql")
                try:
                    path.write_text(sql, encoding="utf-8")
                    with self.assertRaises(ValueError):
                        self.module.candidate_statements(path)
                finally:
                    path.unlink(missing_ok=True)

    def test_candidate_a_and_b_pass_the_parser(self) -> None:
        for name in ("045a_direct_fact.sql", "045b_single_aggregate.sql"):
            with self.subTest(file=name):
                statements = self.module.candidate_statements(
                    ROOT / "sql" / "candidates" / name)
                self.assertEqual(
                    2, sum(1 for s in statements
                           if s.lstrip().upper().startswith("CREATE OR REPLACE FUNCTION")))
                self.assertFalse(any("BEGIN" == s.strip().upper() for s in statements))


class GateRejectionTest(unittest.TestCase):
    """Task 2's gate: each check must actually fail on a bad candidate."""

    def setUp(self) -> None:
        self.module = load_script()

    def test_parity_gate_fails_on_a_single_mutated_row(self) -> None:
        before = ['{"a":1}', '{"a":2}', '{"a":3}']
        self.module.compare_rows("x", True, before, list(before))
        mutated = ['{"a":1}', '{"a":99}', '{"a":3}']
        with self.assertRaises(RuntimeError) as caught:
            self.module.compare_rows("x", True, before, mutated)
        self.assertIn("row 1 differs", str(caught.exception))

    def test_parity_gate_fails_on_a_dropped_or_added_row(self) -> None:
        for after in (['{"a":1}'], ['{"a":1}', '{"a":2}', '{"a":3}', '{"a":4}']):
            with self.assertRaises(RuntimeError):
                self.module.compare_rows("x", False, ['{"a":1}', '{"a":2}', '{"a":3}'], after)

    def test_ordered_gate_catches_reordering_that_multiset_allows(self) -> None:
        before, after = ['{"a":1}', '{"a":2}'], ['{"a":2}', '{"a":1}']
        with self.assertRaises(RuntimeError):
            self.module.compare_rows("onoff", True, before, after)
        self.module.compare_rows("ff", False, before, after)

    def test_regression_gate_fails_on_a_slow_candidate(self) -> None:
        self.module.check_regression("x", 1.000, 1.100)
        with self.assertRaises(RuntimeError):
            self.module.check_regression("x", 1.000, 1.101)
        self.module.check_regression("x", 0.200, 0.300)
        with self.assertRaises(RuntimeError):
            self.module.check_regression("x", 0.200, 0.301)

    def test_absolute_gates_match_addendum_a(self) -> None:
        # Companion measurement plus the spec's own max(10%, 100 ms) tolerance.
        self.assertEqual(1.745, self.module.COMPANION["onoff"]["broad_median"])
        self.assertEqual(1.583, self.module.COMPANION["ff"]["broad_median"])
        self.assertEqual(0.549, self.module.COMPANION_LAST10_MEDIAN)
        self.assertAlmostEqual(1.920, self.module.GATE_BROAD_MEDIAN["onoff"], places=3)
        self.assertAlmostEqual(1.741, self.module.GATE_BROAD_MEDIAN["ff"], places=3)
        self.assertAlmostEqual(1.977, self.module.GATE_BROAD_P90["onoff"], places=3)
        self.assertAlmostEqual(1.758, self.module.GATE_BROAD_P90["ff"], places=3)
        self.assertAlmostEqual(0.649, self.module.GATE_LAST10_MEDIAN, places=3)

    def test_tolerance_rule_is_max_of_ten_percent_and_hundred_ms(self) -> None:
        self.assertAlmostEqual(1.100, self.module.with_tolerance(1.000), places=3)
        self.assertAlmostEqual(0.300, self.module.with_tolerance(0.200), places=3)

    def test_absolute_gate_rejects_a_slow_candidate(self) -> None:
        self.module.check_absolute("x", 0.500, 0.750, 0.500, 0.750)
        with self.assertRaises(RuntimeError):
            self.module.check_absolute("x", 0.501, 0.700, 0.500, 0.750)
        with self.assertRaises(RuntimeError):
            self.module.check_absolute("x", 0.400, 0.751, 0.500, 0.750)

    def test_plan_gate_fails_on_more_buffers_a_new_sort_or_a_full_scan(self) -> None:
        base = {"shared_hit": 1000, "shared_read": 0, "temp_written": 0}
        self.module.check_plan("x", base, dict(base), narrow=True)
        with self.assertRaises(RuntimeError):
            self.module.check_plan("x", base, dict(base, shared_hit=1001), narrow=False)
        with self.assertRaises(RuntimeError):
            self.module.check_plan("x", base, dict(base, temp_written=8), narrow=False)
        wide = {"shared_hit": 90000, "shared_read": 0, "temp_written": 0}
        with self.assertRaises(RuntimeError):
            self.module.check_plan("x", wide, dict(wide, shared_hit=25000), narrow=True)
        self.module.check_plan("x", wide, dict(wide, shared_hit=25000), narrow=False)

    def test_p90_index(self) -> None:
        self.assertEqual(14.0, self.module.p90_of([float(x) for x in range(1, 16)]))
        self.assertEqual(5.0, self.module.p90_of([1.0, 2.0, 3.0, 4.0, 5.0]))


class PresetMatrixTest(unittest.TestCase):
    def setUp(self) -> None:
        self.module = load_script()

    def test_covers_every_filter_class_the_spec_requires(self) -> None:
        labels = {label for label, _, _ in self.module.PRESETS}
        for required in ("broad season", "broad app dates", "last 10",
                         "one team", "one phase RS",
                         "bounded dates", "one opponent", "home", "away", "win",
                         "loss", "round range 5-15", "empty result",
                         "opp rank top5 off", "opp rank top5 def",
                         "opp rank bottom5 net", "starters off min 5",
                         "starters off max 3", "starters def min 5",
                         "starters def max 3", "team+lastN+starters",
                         "eurocup broad"):
            self.assertIn(required, labels)

    def test_every_preset_parameter_is_a_real_function_parameter(self) -> None:
        for label, preset, count in self.module.PRESETS:
            with self.subTest(preset=label):
                self.assertGreaterEqual(count, 5)
                for key in preset:
                    self.assertIn(key, self.module.PARAM_TYPES)

    def test_gated_presets_take_fifteen_warm_samples(self) -> None:
        counts = {label: count for label, _, count in self.module.PRESETS}
        for label in ("broad season", "broad app dates", "last 10", "one team"):
            self.assertEqual(15, counts[label])

    def test_four_factors_never_receives_onoff_only_parameters(self) -> None:
        for label, preset, _ in self.module.PRESETS:
            call, values = self.module.build_call(self.module.FF_SIG, preset)
            with self.subTest(preset=label):
                for key in self.module.ONOFF_ONLY:
                    self.assertNotIn(key, call)
                self.assertEqual(call.count("%s"), len(values))

    def test_calls_are_fully_parameterised(self) -> None:
        for label, preset, _ in self.module.PRESETS:
            for signature in (self.module.ONOFF_SIG, self.module.FF_SIG):
                call, values = self.module.build_call(signature, preset)
                with self.subTest(preset=label):
                    self.assertNotIn("'", call)
                    self.assertEqual(call.count("%s"), len(values))


if __name__ == "__main__":
    unittest.main()
