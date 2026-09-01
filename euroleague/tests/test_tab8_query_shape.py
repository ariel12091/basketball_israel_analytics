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
DIRECT_APPLY_PATH = ROOT / "scripts" / "apply_045_query_alignment.py"

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
    """Migration 045 ships only candidate A's function-body alignment."""

    def setUp(self) -> None:
        if not MIGRATION_PATH.exists():
            self.skipTest("sql/045_tab8_query_shape.sql not written yet")
        self.sql = MIGRATION_PATH.read_text(encoding="utf-8")

    def test_function_bodies_are_byte_identical_to_candidate_a(self) -> None:
        for function in FUNCTIONS:
            with self.subTest(function=function):
                self.assertEqual(definition(CANDIDATE_A, function),
                                 definition(self.sql, function))

    def test_does_not_bundle_physical_tuning(self) -> None:
        body = strip_comments(self.sql)
        self.assertNotIn("CREATE INDEX", body)
        self.assertNotIn("SET work_mem", body)
        self.assertNotIn("ALTER FUNCTION", body)

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
        # The composite index remains available to the historical A/B probes,
        # but the parity-first reviewed migration deliberately does not ship it.
        indexes = re.findall(
            r"CREATE INDEX[^;]*",
            strip_comments(MIGRATION_PATH.read_text(encoding="utf-8")))
        self.assertEqual([], indexes)

    def test_defaults_to_rollback(self) -> None:
        self.assertIn('cur.execute("COMMIT" if args.apply else "ROLLBACK")', SCRIPT_TEXT)
        self.assertIn('parser.add_argument("--apply", action="store_true"', SCRIPT_TEXT)
        self.assertIn("--apply requires --candidate", SCRIPT_TEXT)

    def test_apply_requires_both_expected_function_hashes(self) -> None:
        self.assertIn(
            "--apply requires both expected pre-change function hashes",
            SCRIPT_TEXT)
        self.assertIn("not args.expect_onoff_sha256", SCRIPT_TEXT)
        self.assertIn("not args.expect_ff_sha256", SCRIPT_TEXT)

    def test_direct_alignment_applicator_disables_inherited_timeouts(self) -> None:
        text = DIRECT_APPLY_PATH.read_text(encoding="utf-8")
        migration = MIGRATION_PATH.read_text(encoding="utf-8")
        self.assertIn("SET LOCAL lock_timeout = 0;", migration)
        self.assertIn("SET LOCAL statement_timeout = 0;", migration)
        self.assertIn('effective_lock_timeout != "0"', text)
        self.assertIn('effective_statement_timeout != "0"', text)
        self.assertIn("candidate_statements(MIGRATION)", text)
        self.assertIn("verify_aligned(cur)", text)

    def test_apply_cannot_commit_a_probe_or_skip_gates(self) -> None:
        self.assertIn(
            "--apply may commit only the reviewed MIGRATION artifact",
            SCRIPT_TEXT)
        self.assertIn("--apply requires the same-session companion gate",
                      SCRIPT_TEXT)
        self.assertIn("--apply requires the complete preset matrix",
                      SCRIPT_TEXT)

    def test_catalog_lock_retry_reuses_baseline_transaction_boundary(self) -> None:
        class LockTimeout(Exception):
            sqlstate = "55P03"

        class Cursor:
            def __init__(self):
                self.calls = []
                self.failures = 2

            def execute(self, statement):
                self.calls.append(statement)
                if statement == "DDL" and self.failures:
                    self.failures -= 1
                    raise LockTimeout("catalog busy")

        cursor = Cursor()
        original = self.module.report_ddl_retry_context
        self.module.report_ddl_retry_context = lambda cur: None
        try:
            self.module.begin_candidate_ddl_with_retry(
                cursor, ["DDL"], sleep=lambda seconds: None)
        finally:
            self.module.report_ddl_retry_context = original

        self.assertEqual(3, cursor.calls.count("BEGIN"))
        self.assertEqual(2, cursor.calls.count("ROLLBACK"))
        self.assertEqual(3, cursor.calls.count("DDL"))
        self.assertIn("captured baseline is retained", SCRIPT_TEXT)

    def test_non_lock_ddl_failure_is_not_retried(self) -> None:
        class Cursor:
            calls = []

            def execute(self, statement):
                self.calls.append(statement)
                if statement == "DDL":
                    raise RuntimeError("bad DDL")

        cursor = Cursor()
        with self.assertRaisesRegex(RuntimeError, "bad DDL"):
            self.module.begin_candidate_ddl_with_retry(
                cursor, ["DDL"], sleep=lambda seconds: None)
        self.assertEqual(1, cursor.calls.count("BEGIN"))
        self.assertEqual(1, cursor.calls.count("ROLLBACK"))

    def test_commit_is_followed_by_pooled_gate_and_compensating_recovery(self) -> None:
        commit = SCRIPT_TEXT.index('cur.execute("COMMIT" if args.apply else "ROLLBACK")')
        pooled = SCRIPT_TEXT.index("pooled_post_commit_gate(baseline)", commit)
        cleanup = SCRIPT_TEXT.index("RECOVERY_FILE.unlink", pooled)
        self.assertLess(commit, pooled)
        self.assertLess(pooled, cleanup)
        self.assertIn("restore_prechange(con, pre_definitions, pre_hashes)", SCRIPT_TEXT)
        recovery = SCRIPT_TEXT.split("def restore_prechange", 1)[1].split(
            "def restore_from_artifact", 1)[0]
        self.assertNotIn("RESET work_mem", recovery)
        self.assertNotIn("DROP INDEX", recovery)
        self.assertIn('direct_port=POOL_PORT', SCRIPT_TEXT)

    def test_recovery_artifact_has_an_explicit_resume_mode(self) -> None:
        self.assertIn('parser.add_argument("--restore-from"', SCRIPT_TEXT)
        self.assertIn("function_definitions", SCRIPT_TEXT)
        self.assertIn("function_hashes", SCRIPT_TEXT)
        self.assertIn("restore_from_artifact(con, args.restore_from)", SCRIPT_TEXT)

    def test_recovery_rejects_incomplete_or_tampered_definitions_before_db(self) -> None:
        class NoDatabase:
            def cursor(self):
                raise AssertionError("database must not be touched")

        with self.assertRaisesRegex(RuntimeError, "does not cover"):
            self.module.restore_prechange(
                NoDatabase(), {"onoff": "x"}, {"onoff": "bad"})
        definitions = {"onoff": "one", "ff": "two"}
        hashes = {key: "bad" for key in definitions}
        with self.assertRaisesRegex(RuntimeError, "definition hash mismatch"):
            self.module.restore_prechange(NoDatabase(), definitions, hashes)

    def test_companion_is_timed_adjacent_to_the_candidate(self) -> None:
        # Instance conditions drift over minutes, so the companion must be timed
        # inside the transaction next to the candidate, not before the DDL.
        head, tail = SCRIPT_TEXT.split('cur.execute("BEGIN")', 1)
        self.assertNotIn("companion = measure_companion(cur)", head)
        self.assertIn("companion = measure_companion(cur)", tail)
        self.assertLess(tail.index("parity / regression"),
                        tail.index("companion = measure_companion(cur)"))

    def test_candidate_and_companion_samples_are_interleaved(self) -> None:
        helper = SCRIPT_TEXT.split("def measure_companion(", 1)[1].split(
            "def with_tolerance(", 1)[0]
        self.assertIn("if index % 2", helper)
        self.assertIn("companion_once(); candidate_once()", helper)
        self.assertIn("candidate_once(); companion_once()", helper)

    def test_only_shape_matched_presets_carry_an_absolute_gate(self) -> None:
        module = self.module
        labels = {label for label, _, _ in module.PRESETS}
        # Every gated preset sends an explicit season window, as both apps do.
        for label in module.GATED_BROAD + (module.GATED_LAST10,):
            with self.subTest(preset=label):
                self.assertIn(label, labels)
                preset = next(p for lbl, p, _ in module.PRESETS if lbl == label)
                self.assertIn("p_start_date", preset)
                self.assertIn("p_end_date", preset)
        # The NULL-date presets have no companion counterpart and must not be
        # gated: basketball_test.onoff_compute cannot accept NULL dates at all.
        for label in module.REPORT_ONLY:
            with self.subTest(preset=label):
                preset = next(p for lbl, p, _ in module.PRESETS if lbl == label)
                self.assertNotIn("p_start_date", preset)
        self.assertFalse(set(module.REPORT_ONLY) & set(module.GATED_BROAD))
        self.assertNotIn(module.GATED_LAST10, module.REPORT_ONLY)

    def test_companion_calls_all_send_an_explicit_window(self) -> None:
        module = self.module
        for key, (sql, params) in module.COMPANION_CALLS.items():
            with self.subTest(companion=key):
                self.assertIn("p_start_date", sql)
                self.assertIn("p_end_date", sql)
        for key, (sql, params) in module.COMPANION_LAST10_CALLS.items():
            with self.subTest(companion=key):
                self.assertIn("p_start_date", sql)
                self.assertIn("p_last_n_games => 10", sql)
        # onoff_compute has no NULL guard on p_min_net; NULL silently yields
        # zero rows, so the companion call must pass a real floor.
        sql, params = module.COMPANION_LAST10_CALLS["onoff"]
        self.assertIn("p_min_net", sql)
        self.assertIn(-999, params)

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
        self.assertAlmostEqual(1.977, self.module.GATE_BROAD_UPPER["onoff"], places=3)
        self.assertAlmostEqual(1.758, self.module.GATE_BROAD_UPPER["ff"], places=3)
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

    def test_trim_drops_the_stalls_and_keeps_the_signal(self) -> None:
        # A real observed candidate sample: twelve values inside 0.07s, then
        # three instance stalls.
        observed = [1.34, 1.35, 1.37, 1.37, 1.37, 1.37, 1.38, 1.39, 1.39,
                    1.40, 1.41, 1.63, 3.05, 5.42, 7.16]
        median, p90 = self.module.summarize(observed)
        self.assertAlmostEqual(1.390, median, places=3)
        self.assertAlmostEqual(1.630, p90, places=3)
        self.assertEqual(9, len(self.module.trim(observed)))
        # The estimator must not flatter the candidate: the median is the same
        # trimmed or not; only the stall-dominated p90 changes.
        import statistics as s
        self.assertAlmostEqual(s.median(observed), median, places=3)

    def test_trim_is_symmetric_so_no_side_gains_an_advantage(self) -> None:
        fast_outlier = [0.01] + [1.0] * 13 + [9.0]
        kept = self.module.trim(fast_outlier)
        self.assertNotIn(0.01, kept)
        self.assertNotIn(9.0, kept)
        self.assertEqual(0.20, self.module.TRIM_FRACTION)

    def test_trim_is_skipped_when_there_are_too_few_samples(self) -> None:
        for series in ([1.0, 2.0, 9.0], [1.0, 9.0], [1.0]):
            with self.subTest(n=len(series)):
                self.assertEqual(sorted(series), self.module.trim(series))

    def test_blocking_latency_statistics_are_trimmed_and_raw_p90_is_reported(self) -> None:
        # Blocking medians/upper-central values go through summarize(); raw p90
        # is deliberately computed separately as a visible, non-blocking tail
        # observation.
        callers = SCRIPT_TEXT.split("def capture_baseline(", 1)[1]
        self.assertNotIn("statistics.median(timings)", callers)
        self.assertEqual(2, callers.count("summarize(timings)"))
        self.assertIn('"raw_p90": p90_of(timings)', callers)
        helpers = SCRIPT_TEXT.split("def measure_companion(", 1)[1].split(
            "def with_tolerance(", 1)[0]
        self.assertIn("summarize(candidate_timings)", helpers)
        self.assertIn("summarize(companion_timings)", helpers)

    def test_trimmed_tail_statistic_is_not_labelled_raw_p90(self) -> None:
        helper = SCRIPT_TEXT.split("def summarize(", 1)[1].split(
            "def p90_of(", 1)[0]
        self.assertIn("upper-central", helper)
        gate_output = SCRIPT_TEXT.split("absolute gates", 1)[1]
        self.assertIn("upper-central", gate_output)

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

    def test_baseline_is_cheaper_than_the_candidate_side(self) -> None:
        # The baseline measures the unfixed functions and only needs parity rows
        # plus a median for the no-regression rule; the candidate keeps full
        # precision. Guard against the two being silently coupled again.
        module = self.module
        self.assertEqual(3, module.BASELINE_SAMPLES)
        self.assertIn("min(count, samples or BASELINE_SAMPLES)", SCRIPT_TEXT)
        candidate_half = SCRIPT_TEXT.split("print(\"parity / regression:\")", 1)[1]
        self.assertIn("sample(cur, signature, preset, count)", candidate_half)
        self.assertNotIn("BASELINE_SAMPLES", candidate_half)

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
