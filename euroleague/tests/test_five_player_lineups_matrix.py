import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "benchmark_five_player_lineups_matrix.py"
SPEC = importlib.util.spec_from_file_location("benchmark_five_player_lineups_matrix", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class FivePlayerLineupsMatrixTests(unittest.TestCase):
    def test_static_matrix_covers_handoff_filters(self):
        params = {key for preset in MODULE.STATIC_PRESETS for key in preset.overrides}
        required = {
            "p_start_date",
            "p_end_date",
            "p_last_n_games",
            "p_team_ids_csv",
            "p_phase_csv",
            "p_opp_ids_csv",
            "p_home_away",
            "p_outcome",
            "p_opp_rank_side",
            "p_num_starters_off_min",
            "p_num_starters_off_max",
            "p_num_starters_def_min",
            "p_num_starters_def_max",
            "p_min_poss",
            "p_min_gn",
        }
        self.assertTrue(required <= params)
        self.assertEqual(MODULE.BASE["p_unit_size"], 5)
        self.assertEqual(MODULE.BASE["p_max_margin"], 3)
        self.assertEqual(MODULE.BASE["p_max_time_remaining"], 240)

    def test_player_membership_presets_are_defined_at_runtime(self):
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertIn('Preset("players on"', source)
        self.assertIn('Preset("players on and off"', source)
        self.assertIn("p_players_on_csv", source)
        self.assertIn("p_players_off_csv", source)

    def test_expansion_replaces_whole_parameter_names(self):
        params = dict(MODULE.BASE)
        body = "SELECT p_min_gn, p_min_gn_extra, p_competition"
        with self.assertRaisesRegex(RuntimeError, "p_min_gn_extra"):
            MODULE.expand_function_body(body, params)
        expanded = MODULE.expand_function_body(
            "SELECT p_min_gn, p_competition, p_ot_margin_filter", params
        )
        self.assertEqual(
            expanded,
            "SELECT NULL::integer, 'E'::text, false::boolean",
        )

    def test_central_sixty_summary_retains_raw_samples(self):
        values = list(range(1, 16))
        result = MODULE.summarize(values)
        self.assertEqual(result["raw_seconds"], values)
        self.assertEqual(result["central_sample_count"], 9)
        self.assertEqual(result["median_seconds"], 8)

    def test_source_enforces_read_only_transaction_and_rollback(self):
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertIn('cursor.execute("BEGIN READ ONLY")', source)
        self.assertIn("connection.rollback()", source)
        self.assertNotIn("connection.commit()", source)

    def test_timed_modes_strictly_alternate_without_pair_boundary_repeats(self):
        source = SCRIPT.read_text(encoding="utf-8")
        measured = source.split("def measure_preset(", 1)[1].split(
            "def choose_presets(", 1
        )[0]
        self.assertIn('((True, "normal"), (False, "nestloop_off"))', measured)
        self.assertNotIn("reversed(order)", measured)
        self.assertIn("strict alternation", measured)


if __name__ == "__main__":
    unittest.main()
