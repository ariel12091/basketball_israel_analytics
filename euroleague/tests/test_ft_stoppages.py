from __future__ import annotations

from pathlib import Path
import sys
import unittest

import pandas as pd
from pandas.testing import assert_frame_equal

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "euroleague/src"))
from euroleague_possessions.ft_stoppages import infer_ft_stoppages


def events(*items):
    return pd.DataFrame([dict(season=2025, gamecode=1, period=1,
                              source_event_order=i, clock=clock, play_type=code,
                              team_code=team, player_id=player)
                         for i, (code, team, player, clock) in enumerate(items)])


class StoppageTest(unittest.TestCase):
    def test_new_foul_keeps_its_basket_when_older_pending_foul_closes(self):
        for drawn_first in (False, True):
            with self.subTest(drawn_first=drawn_first):
                pair = [("CM", "A", "d2", "02:50"), ("RV", "B", "p", "02:50")]
                if drawn_first:
                    pair.reverse()
                result = infer_ft_stoppages(events(("CM", "A", "d1", "03:00"),
                    ("RV", "B", "p", "03:00"), ("2FGM", "B", "p", "02:50"),
                    ("AS", "B", "q", "02:50"), *pair, ("FTM", "B", "p", "02:50")))
                self.assertEqual(result.iloc[-1].shadow_status, "unique")
                self.assertEqual(result.iloc[-1].shadow_parent_order, 2)
                self.assertEqual(result.iloc[-1].shadow_foul_order, 5 if drawn_first else 4)

    def test_basket_after_clock_ran_belongs_to_the_new_foul(self):
        # E2026_89 Q2: p fouled at 06:48 (no FTs), play on, p scores at 06:42,
        # fouled on the shot (logged 06:40), one FT. The clock ran between the
        # old foul and the basket, so the basket is not the old foul's and-one.
        result = infer_ft_stoppages(events(("CM", "A", "d1", "06:48"),
            ("RV", "B", "p", "06:48"), ("OUT", "A", "d0", "06:48"),
            ("IN", "A", "d2", "06:48"), ("2FGM", "B", "p", "06:42"),
            ("CM", "A", "d2", "06:40"), ("RV", "B", "p", "06:40"),
            ("FTA", "B", "p", "06:40")))
        self.assertEqual(result.iloc[-1].shadow_status, "unique")
        self.assertEqual(result.iloc[-1].shadow_parent_order, 4)
        self.assertEqual(result.iloc[-1].shadow_foul_order, 5)

    # Parser 0.3.0: the model's answers overlaid on the parser's grouping.
    def test_unsportsmanlike_and_one_does_not_end_the_possession(self):
        # User, 2026-11-19: "Unsportsmanlike does not finish possession".
        from euroleague_possessions.parser import group_events
        result = group_events(events(("2FGM", "B", "p", "03:19"), ("CMU", "A", "d", "03:19"),
            ("RV", "B", "p", "03:19"), ("FTM", "B", "p", "03:19"),
            ("3FGM", "B", "q", "03:09")))
        self.assertEqual(result.final_end_poss.tolist(), [False, False, False, False, True])
        self.assertEqual(result.synthetic_parent_order.iloc[3], 0)

    def test_basket_from_an_earlier_play_keeps_its_end(self):
        # U2025_25 Q2: basket at 03:40, a foul 31 s later, two FTs.
        from euroleague_possessions.parser import group_events
        result = group_events(events(("2FGM", "B", "p", "03:40"), ("CM", "A", "d", "03:09"),
            ("RV", "B", "q", "03:09"), ("FTM", "B", "q", "03:09"), ("FTM", "B", "q", "03:09")))
        self.assertTrue(result.final_end_poss.iloc[0])
        self.assertEqual(result.synthetic_parent_order.iloc[3:].tolist(), [1, 1])
        self.assertEqual(result.final_end_poss.iloc[3:].tolist(), [False, True])

    # Replacement shooters (approved 2026-11-17): the fouled player's OUT and the
    # shooter's IN in the same stoppage let the substitute shoot.
    def test_substitute_shoots_whole_trip_for_fouled_player_who_left(self):
        for sub_after_fts in (False, True):
            with self.subTest(sub_after_fts=sub_after_fts):
                sub = [("OUT", "B", "p", "05:00"), ("IN", "B", "q", "05:00")]
                fts = [("FTM", "B", "q", "05:00"), ("FTA", "B", "q", "05:00")]
                items = [("CM", "A", "d", "05:00"), ("RV", "B", "p", "05:00")]
                items += fts + sub if sub_after_fts else sub + fts
                result = infer_ft_stoppages(events(*items))
                ft = result[result.play_type.isin({"FTM", "FTA"})]
                self.assertTrue(ft.shadow_status.eq("unique").all())
                self.assertTrue(ft.shadow_foul_order.eq(0).all())
                self.assertEqual(ft.shadow_ft_attempt.tolist(), [1, 2])
                self.assertIn("replacement_shooter_after_substitution", ft.iloc[-1].shadow_trace)

    def test_substitute_for_someone_else_cannot_shoot(self):
        result = infer_ft_stoppages(events(("CM", "A", "d", "05:00"),
            ("RV", "B", "p", "05:00"), ("OUT", "B", "r", "05:00"),
            ("IN", "B", "q", "05:00"), ("FTM", "B", "q", "05:00"),
            ("FTA", "B", "q", "05:00")))
        self.assertTrue(result.iloc[4:].shadow_status.eq("no_fit").all())
        self.assertTrue(result.iloc[4:].shadow_parent_order.isna().all())

    def test_joint_foul_drawn_pairing_constrains_two_personal_fouls(self):
        result = infer_ft_stoppages(events(("RV", "B", "p1", "05:00"),
            ("CM", "A", "d1", "05:00"), ("RV", "B", "p2", "05:00"),
            ("CM", "A", "d2", "05:00"), ("FTM", "B", "p2", "05:00"),
            ("FTM", "B", "p2", "05:00")))
        self.assertTrue(result.iloc[4:].shadow_status.eq("unique").all())
        self.assertEqual(result.iloc[-1].shadow_foul_order, 3)

    def test_one_drawn_row_cannot_identify_two_personal_fouls(self):
        result = infer_ft_stoppages(events(("CM", "A", "d1", "05:00"),
            ("RV", "B", "p", "05:00"), ("CM", "A", "d2", "05:00"),
            ("FTM", "B", "p", "05:00"), ("FTM", "B", "p", "05:00")))
        self.assertEqual(result.iloc[-1].shadow_status, "no_fit")
        self.assertIn("personal_foul_drawn_pairing_conflict", result.iloc[-1].shadow_trace)

    def test_ambiguity_exposes_two_competing_attempt_allocations(self):
        import json
        result = infer_ft_stoppages(events(("CM", "A", "d", "05:00"),
            ("RV", "B", "p", "05:00"), ("CMT", "A", "d", "05:00"),
            ("FTM", "B", "p", "05:00"), ("FTM", "B", "p", "05:00"),
            ("FTM", "B", "p", "05:00")))
        alternatives = json.loads(result.iloc[-1].shadow_alternatives)
        self.assertEqual(len(alternatives), 2)
        self.assertNotEqual(alternatives[0], alternatives[1])

    def test_unpaired_drawn_before_foul_can_cross_clock_without_losing_and_one(self):
        result = infer_ft_stoppages(events(("2FGM", "B", "p", "06:03"),
            ("RV", "B", "p", "06:00"), ("CM", "A", "d", "06:01"),
            ("OUT", "A", "d", "06:01"), ("FTM", "B", "p", "06:01")))
        self.assertEqual(result.iloc[-1].shadow_status, "unique")
        self.assertEqual(result.iloc[-1].shadow_parent_order, 0)
        self.assertEqual(result.iloc[-1].shadow_foul_order, 2)
        self.assertIn("retain_unpaired_drawn_across_clock", result.iloc[-1].shadow_trace)

    def test_measured_reordered_sequences_match_manual_labels(self):
        frame = pd.read_csv(ROOT / "euroleague/tests/fixtures/ft_stoppage_reordered.csv")
        result = infer_ft_stoppages(frame)
        ft = result[result.expected_trip.notna()]
        self.assertTrue(ft.shadow_status.eq("unique").all())
        self.assertEqual(ft.shadow_foul_order.astype(int).tolist(), ft.expected_foul.astype(int).tolist())
        self.assertEqual(ft.shadow_parent_order.astype(int).tolist(), ft.expected_root.astype(int).tolist())
        for _, part in ft.groupby(["gamecode", "period"]):
            expected, actual = part.expected_trip.tolist(), part.shadow_ft_trip_id.tolist()
            self.assertEqual([[a == b for b in expected] for a in expected],
                             [[a == b for b in actual] for a in actual])

    def test_foul_before_basket_and_clock_shift(self):
        result = infer_ft_stoppages(events(("CM", "A", "d", "05:00"),
            ("RV", "B", "p", "05:00"), ("CCH", "B", None, "05:00"),
            ("2FGM", "B", "p", "05:00"), ("FTM", "B", "p", "04:59")))
        ft = result.iloc[-1]
        self.assertEqual(ft.shadow_parent_order, 3)
        self.assertEqual(ft.shadow_foul_order, 0)
        self.assertEqual(ft.shadow_ft_awarded, 1)
        self.assertIn("clock_discrepancy", ft.shadow_trace)
        self.assertEqual(ft.shadow_grouping_status, "provisional")

    def test_interrupted_personal_trip_resumes(self):
        result = infer_ft_stoppages(events(("CM", "A", "d", "05:00"),
            ("RV", "B", "p", "05:00"), ("FTM", "B", "p", "05:00"),
            ("CMT", "A", "d", "05:00"), ("FTM", "B", "q", "05:00"),
            ("FTA", "B", "p", "05:00"), ("FTM", "B", "p", "05:00")))
        trip = result.loc[[2, 5, 6]]
        self.assertTrue(trip.shadow_status.eq("unique").all())
        self.assertEqual(trip.shadow_ft_trip_id.nunique(), 1)
        self.assertEqual(trip.shadow_ft_attempt.tolist(), [1, 2, 3])
        self.assertEqual(trip.shadow_ft_awarded.tolist(), [3, 3, 3])
        self.assertNotEqual(result.iloc[4].shadow_ft_trip_id, trip.iloc[0].shadow_ft_trip_id)

    def test_late_bench_foul_can_own_prior_ft(self):
        result = infer_ft_stoppages(events(("FTM", "B", "p", "05:00"),
                                            ("B", "A", None, "05:00")))
        self.assertEqual(result.iloc[0].shadow_foul_order, 1)
        self.assertFalse(result.iloc[0].shadow_ft_end_poss)

    def test_new_contact_foul_closes_old_nonshooting_incident_before_rv(self):
        for prefix in [(("CM", "A", "d1", "05:00"),), ()]:
            with self.subTest(orphan_rv=not prefix):
                result = infer_ft_stoppages(events(*prefix,
                    ("RV", "B", "p1", "05:00"), ("CM", "A", "d2", "04:50"),
                    ("RV", "B", "p2", "04:50"), ("FTM", "B", "p2", "04:50"),
                    ("FTM", "B", "p2", "04:50")))
                self.assertEqual(result.iloc[-1].shadow_status, "unique")
                self.assertEqual(result.iloc[-1].shadow_foul_order, len(prefix) + 1)

    def test_same_shooter_multiple_trips_keep_attempt_ambiguity(self):
        result = infer_ft_stoppages(events(("CM", "A", "d", "05:00"),
            ("RV", "B", "p", "05:00"), ("CMT", "A", "d", "05:00"),
            ("FTM", "B", "p", "05:00"), ("FTA", "B", "p", "05:00"),
            ("FTM", "B", "p", "05:00")))
        self.assertTrue(result.iloc[3:].shadow_status.eq("ambiguous").all())
        self.assertTrue(result.iloc[3:].shadow_ft_trip_id.isna().all())

    def test_substitute_who_drew_his_own_foul_shoots_for_it(self):
        # E2025_259 Q2: p fouled (no FTs), p -> q, then q fouled and shoots two.
        result = infer_ft_stoppages(events(("CM", "A", "d1", "03:50"),
            ("RV", "B", "p", "03:50"), ("IN", "B", "q", "03:50"),
            ("OUT", "B", "p", "03:50"), ("CM", "A", "d2", "03:50"),
            ("RV", "B", "q", "03:50"), ("FTA", "B", "q", "03:50"),
            ("FTM", "B", "q", "03:50")))
        ft = result.iloc[6:]
        self.assertTrue(ft.shadow_status.eq("unique").all())
        self.assertTrue(ft.shadow_foul_order.eq(4).all())
        self.assertNotIn("replacement_shooter", ft.iloc[-1].shadow_trace)

    def test_mid_trip_substitute_finishes_the_trip(self):
        result = infer_ft_stoppages(events(("CM", "A", "d", "05:00"),
            ("RV", "B", "p", "05:00"), ("FTM", "B", "p", "05:00"),
            ("OUT", "B", "p", "05:00"), ("IN", "B", "q", "05:00"),
            ("FTM", "B", "q", "05:00")))
        trip = result.loc[[2, 5]]
        self.assertTrue(trip.shadow_status.eq("unique").all())
        self.assertEqual(trip.shadow_ft_trip_id.nunique(), 1)
        self.assertEqual(trip.shadow_ft_attempt.tolist(), [1, 2])

    def test_substitute_cannot_shoot_before_fouled_player_left(self):
        result = infer_ft_stoppages(events(("CM", "A", "d", "05:00"),
            ("RV", "B", "p", "05:00"), ("FTM", "B", "q", "05:00"),
            ("OUT", "B", "p", "05:00"), ("IN", "B", "q", "05:00"),
            ("FTM", "B", "p", "05:00")))
        self.assertNotEqual(result.iloc[-1].shadow_status, "unique")

    # Competing fouls (approved 2026-11-17): equivalent candidates go to the
    # foul logged last before the trip; a substitution between them keeps the
    # incident ambiguous, because the credited lineup would differ.
    def test_equivalent_competing_fouls_resolve_to_latest_before_trip(self):
        result = infer_ft_stoppages(events(("CM", "A", "d1", "05:00"),
            ("RV", "B", "p", "05:00"), ("CM", "A", "d2", "05:00"),
            ("RV", "B", "p", "05:00"), ("FTM", "B", "p", "05:00"),
            ("FTM", "B", "p", "05:00")))
        ft = result.iloc[4:]
        self.assertTrue(ft.shadow_status.eq("unique").all())
        self.assertTrue(ft.shadow_foul_order.eq(2).all())
        self.assertIn("competing_fouls_resolved_to_latest_before_trip", ft.iloc[-1].shadow_trace)

    def test_competing_fouls_with_substitution_between_stay_ambiguous(self):
        result = infer_ft_stoppages(events(("CM", "A", "d1", "05:00"),
            ("RV", "B", "p", "05:00"), ("OUT", "A", "d1", "05:00"),
            ("IN", "A", "x", "05:00"), ("CM", "A", "d2", "05:00"),
            ("RV", "B", "p", "05:00"), ("FTM", "B", "p", "05:00"),
            ("FTM", "B", "p", "05:00")))
        self.assertTrue(result.iloc[6:].shadow_status.eq("ambiguous").all())

    def test_opposing_technicals_do_not_cancel_after_first_ft_started(self):
        result = infer_ft_stoppages(events(("CMT", "A", "d", "05:00"),
            ("FTM", "B", "p", "05:00"), ("CMT", "B", "p", "05:00"),
            ("FTM", "A", "d", "05:00")))
        ft = result.iloc[[1, 3]]
        self.assertTrue(ft.shadow_status.eq("unique").all())
        self.assertFalse(ft.shadow_trace.str.contains("cancellation").any())

    def test_live_boundary_prevents_reaching_back_to_old_foul(self):
        result = infer_ft_stoppages(events(("CM", "A", "d", "05:00"),
            ("RV", "B", "p", "05:00"), ("TO", "B", "p", "05:00"),
            ("FTM", "B", "p", "05:00")))
        self.assertEqual(result.iloc[-1].shadow_status, "no_fit")

    def test_search_limit_never_means_unique(self):
        result = infer_ft_stoppages(events(("CM", "A", "d", "05:00"),
            ("RV", "B", "p", "05:00"), ("FTM", "B", "p", "05:00"),
            ("FTM", "B", "p", "05:00")), search_budget=1)
        self.assertEqual(result.iloc[-1].shadow_status, "search_limit")

    def test_uncertain_shooter_evidence_does_not_propose_an_endpoint(self):
        result = infer_ft_stoppages(events(("CM", "A", "d", "05:00"),
            ("FTM", "B", "p", "05:00"), ("FTM", "B", "p", "05:00")))
        self.assertEqual(result.iloc[-1].shadow_status, "unique")
        self.assertIn("missing_fouled_player", result.iloc[-1].shadow_trace)
        self.assertTrue(result.iloc[1:].shadow_ft_end_poss.isna().all())

    def test_comparison_baseline_checkpoint_reuse_is_exact(self):
        import importlib.util
        from unittest.mock import patch
        spec = importlib.util.spec_from_file_location("shadow_compare_test", ROOT / "euroleague/scripts/compare_ft_stoppages.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        frame = events(("CM", "A", "d", "05:00"), ("RV", "B", "p", "05:00"),
                       ("FTM", "B", "p", "05:00"), ("FTA", "B", "p", "05:00"),
                       ("D", "A", "d", "05:00"))
        with patch.object(module, "read_cached_pbp", return_value=frame):
            initial = module.compare(Path("E2025_1.pbp.json"))
            reused = module.compare(Path("E2025_1.pbp.json"), initial)
        self.assertEqual(initial, reused)

    def test_input_order_period_boundaries_and_canonical_columns_preserved(self):
        frame = events(("CM", "A", "d", "05:00"), ("RV", "B", "p", "05:00"),
                       ("FTM", "B", "p", "05:00"), ("FTM", "B", "p", "05:00"))
        frame["grouping_status"] = "baseline"
        frame["final_end_poss"] = False
        original = frame.copy(deep=True)
        direct = infer_ft_stoppages(frame)
        shuffled = infer_ft_stoppages(frame.sample(frac=1, random_state=8))
        assert_frame_equal(frame, original)
        self.assertEqual(direct.grouping_status.tolist(), ["baseline"] * 4)
        self.assertFalse(direct.final_end_poss.any())
        assert_frame_equal(direct.sort_values("source_event_order").reset_index(drop=True),
                           shuffled.sort_values("source_event_order").reset_index(drop=True))
        frame.loc[[2, 3], "period"] = 2
        self.assertTrue(infer_ft_stoppages(frame).iloc[2:].shadow_status.eq("no_fit").all())

    def test_all_existing_labelled_trip_partitions(self):
        frame = pd.read_csv(ROOT / "etl/euroleague/fixtures/event_grouping_edge_cases.csv")
        result = infer_ft_stoppages(frame)
        for fixture, part in result.groupby("fixture_id"):
            ft = part[part.play_type.isin({"FTM", "FTA"})]
            if ft.empty:
                continue
            with self.subTest(fixture=fixture):
                self.assertTrue(ft.shadow_status.eq("unique").all())
                expected = ft.expected_ft_trip_id.tolist()
                actual = ft.shadow_ft_trip_id.tolist()
                self.assertEqual([[a == b for b in expected] for a in expected],
                                 [[a == b for b in actual] for a in actual])


if __name__ == "__main__":
    unittest.main()
