from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "euroleague" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions import group_events  # noqa: E402


FIXTURE_PATH = (
    REPO_ROOT
    / "etl"
    / "euroleague"
    / "fixtures"
    / "event_grouping_edge_cases.csv"
)
RAW_COLUMNS = [
    "season",
    "gamecode",
    "source_event_order",
    "provider_number_of_play",
    "period",
    "clock",
    "team_code",
    "play_type",
    "player_id",
    "player_name",
    "play_info",
    "score_a",
    "score_b",
    "comment",
]


class ParserFixtureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.labelled = pd.read_csv(FIXTURE_PATH)
        cls.predicted = group_events(cls.labelled[RAW_COLUMNS])

    def test_all_parents_match_manual_labels(self) -> None:
        actual = self.predicted["synthetic_parent_order"].astype(int).tolist()
        expected = self.labelled["expected_parent_order"].astype(int).tolist()
        self.assertEqual(actual, expected)

    def test_ft_trip_partition_matches_manual_labels(self) -> None:
        ft_mask = self.labelled["play_type"].isin({"FTA", "FTM"})
        expected = self.labelled.loc[ft_mask, "expected_ft_trip_id"].tolist()
        actual = self.predicted.loc[ft_mask, "synthetic_ft_trip_id"].tolist()
        expected_partition = [
            [left == right for right in expected] for left in expected
        ]
        actual_partition = [[left == right for right in actual] for left in actual]
        self.assertEqual(actual_partition, expected_partition)
        self.assertFalse(
            self.predicted.loc[ft_mask, "grouping_status"].eq("unresolved").any()
        )

    def test_endpoints_and_reasons_match_manual_labels(self) -> None:
        expected_end = self.labelled["expected_possession_end"].astype(bool)
        self.assertEqual(
            self.predicted["final_end_poss"].tolist(), expected_end.tolist()
        )
        expected_reason = self.labelled.loc[
            expected_end, "expected_end_reason"
        ].fillna("")
        actual_reason = self.predicted.loc[expected_end, "end_reason"].fillna("")
        self.assertEqual(actual_reason.tolist(), expected_reason.tolist())

    def test_decisions_are_independent_of_input_row_order(self) -> None:
        shuffled = self.labelled[RAW_COLUMNS].sample(frac=1, random_state=20260805)
        shuffled_result = group_events(shuffled)
        key = ["season", "gamecode", "period", "source_event_order"]
        decision_columns = key + [
            "synthetic_parent_order",
            "synthetic_ft_trip_id",
            "final_end_poss",
            "end_reason",
            "grouping_status",
            "grouping_confidence_pct",
        ]
        expected = self.predicted[decision_columns].sort_values(key).reset_index(
            drop=True
        )
        actual = shuffled_result[decision_columns].sort_values(key).reset_index(
            drop=True
        )
        assert_frame_equal(actual, expected)

    def test_transition_trace_explains_and_one_through_substitutions(self) -> None:
        example = self.predicted.loc[
            (self.predicted["gamecode"] == 5)
            & (self.predicted["source_event_order"] == 101)
        ]
        self.assertEqual(len(example), 1)
        trace = example.iloc[0].decision_trace
        self.assertIn("free_throw.best_pending_penalty", trace)
        self.assertIn("free_throw.partition_trip", trace)
        self.assertIn("endpoint.final_made_ft", trace)

    def test_special_penalty_cluster_spans_administrative_rows(self) -> None:
        play_types = [
            "CMU",
            "RV",
            "OUT",
            "IN",
            "OUT",
            "IN",
            "OUT",
            "IN",
            "OUT",
            "IN",
            "FTM",
            "AS",
            "FTM",
            "2FGM",
        ]
        teams = [
            "B",
            "A",
            "A",
            "A",
            "B",
            "B",
            "A",
            "A",
            "B",
            "B",
            "A",
            "A",
            "A",
            "B",
        ]
        players = [
            "b1",
            "a1",
            "a2",
            "a3",
            "b2",
            "b3",
            "a4",
            "a5",
            "b4",
            "b5",
            "a1",
            "a2",
            "a1",
            "b1",
        ]
        events = pd.DataFrame(
            {
                "season": 2025,
                "gamecode": 901,
                "source_event_order": range(1, len(play_types) + 1),
                "period": 2,
                "play_type": play_types,
                "team_code": teams,
                "player_id": players,
            }
        )

        grouped = group_events(events)
        free_throws = grouped.loc[grouped["play_type"].eq("FTM")]

        self.assertEqual(free_throws["grouping_status"].tolist(), [
            "provisional",
            "provisional",
        ])
        self.assertEqual(free_throws["synthetic_parent_order"].tolist(), [1, 1])
        self.assertEqual(free_throws["synthetic_ft_trip_id"].nunique(), 1)
        self.assertTrue(
            free_throws["decision_trace"]
            .str.contains("qa.same_penalty_cluster_as_special", regex=False)
            .all()
        )

    def test_live_boundary_separates_and_one_from_later_special_penalty(self) -> None:
        events = pd.DataFrame(
            {
                "season": 2025,
                "gamecode": 902,
                "source_event_order": range(1, 9),
                "period": 4,
                "play_type": [
                    "2FGM",
                    "CM",
                    "RV",
                    "OUT",
                    "IN",
                    "FTM",
                    "2FGA",
                    "CMU",
                ],
                "team_code": ["A", "B", "A", "B", "B", "A", "B", "A"],
                "player_id": ["a1", "b1", "a1", "b2", "b3", "a1", "b4", "a2"],
            }
        )

        grouped = group_events(events)
        free_throw = grouped.loc[grouped["source_event_order"].eq(6)].iloc[0]

        self.assertEqual(free_throw.grouping_status, "confirmed")
        self.assertEqual(free_throw.synthetic_parent_order, 1)
        self.assertTrue(free_throw.final_end_poss)
        self.assertEqual(free_throw.end_reason, "and_one_final_ft")
        self.assertNotIn("qa.same_penalty_cluster_as_special", free_throw.decision_trace)


class RenamedFoulCodesTest(unittest.TestCase):
    """2026-27 renamed the special fouls (CMT -> CMT1/CMT2, CMU -> CMU_DI/CMU_FL).

    A renamed code must group and end possessions exactly as its old code did, so
    2025-26 and earlier are unchanged (not retroactive; shared_definitions D29).
    Sequences follow the provider's shape (game 1 of 2025, games 782 and 785 of 2026).
    """

    COLUMNS = ["final_end_poss", "end_reason", "synthetic_parent_order", "synthetic_ft_trip_id"]

    @staticmethod
    def _events(rows):
        """rows: (play_type, team, player, play_info)."""
        return pd.DataFrame(
            {
                "season": 2026,
                "gamecode": 950,
                "source_event_order": range(1, len(rows) + 1),
                "period": 2,
                "clock": "05:00",
                "play_type": [r[0] for r in rows],
                "team_code": [r[1] for r in rows],
                "player_id": [r[2] for r in rows],
                "play_info": [r[3] for r in rows],
            }
        )

    def _same_as(self, old, new, rows):
        swap = lambda code: [(code if r[0] == "X" else r[0],) + tuple(r[1:]) for r in rows]
        old_rows = group_events(self._events(swap(old)))
        new_rows = group_events(self._events(swap(new)))
        assert_frame_equal(
            new_rows[self.COLUMNS].reset_index(drop=True),
            old_rows[self.COLUMNS].reset_index(drop=True),
            obj=f"{new} vs {old}",
        )
        return new_rows

    # T scores, then commits the special foul; I shoots two and keeps the ball.
    TRIP = [
        ("2FGM", "T", "t1", None),
        ("AS", "T", "t2", None),
        ("X", "T", "t3", None),
        ("RV", "I", "i1", None),
        ("IN", "I", "i2", None),
        ("OUT", "I", "i3", None),
        ("FTM", "I", "i1", "Free Throw In (1/2 -  5 pt)"),
        ("FTM", "I", "i1", "Free Throw In (2/2 -  6 pt)"),
        ("2FGM", "I", "i4", None),
    ]

    def test_renamed_unsportsmanlike_trip_keeps_the_possession(self) -> None:
        for new in ("CMU_DI", "CMU_FL"):
            rows = self._same_as("CMU", new, self.TRIP)
            self.assertFalse(rows.loc[rows["play_type"].eq("FTM"), "final_end_poss"].any(), new)
            self.assertTrue(rows["final_end_poss"].iloc[-1], new)

    def test_renamed_trip_at_the_end_of_a_period(self) -> None:
        rows = self.TRIP[:8] + [("EP", None, None, None)]
        for new in ("CMU_DI", "CMU_FL"):
            # Same as CMU, which ends no possession here (kept: not retroactive).
            self._same_as("CMU", new, rows)

    def test_renamed_technical_matches_the_old_code(self) -> None:
        rows = [
            ("2FGA", "R", "r1", None),
            ("D", "Z", "z1", None),
            ("X", "R", "r2", None),
            ("FTM", "Z", "z2", "Free Throw In (1/1 -  1 pt)"),
            ("2FGA", "Z", "z3", None),
            ("O", "Z", "z4", None),
            ("3FGA", "Z", "z3", None),
            ("D", "R", "r1", None),
        ]
        for new in ("CMT1", "CMT2"):
            out = self._same_as("CMT", new, rows)
            self.assertFalse(out.loc[out["play_type"].eq("FTM"), "final_end_poss"].any(), new)

    def test_renamed_unsportsmanlike_on_a_made_basket(self) -> None:
        rows = [
            ("2FGM", "A", "a1", None),
            ("X", "B", "b1", None),
            ("RV", "A", "a1", None),
            ("FTM", "A", "a1", "Free Throw In (1/1 -  3 pt)"),
            ("3FGA", "A", "a2", None),
            ("D", "B", "b2", None),
        ]
        for new in ("CMU_DI", "CMU_FL"):
            self._same_as("CMU", new, rows)

    def test_foul_family_predicates(self) -> None:
        from euroleague_possessions.parser import is_committed_foul, is_special_foul

        for code in ("CMT", "CMT1", "CMT2", "CMTI", "CMU", "CMU_DI", "CMU_FL", "B", "C"):
            self.assertTrue(is_special_foul(code), code)
        for code in ("CM", "CMD", "CCH", "OF", "RV", "CMTX", "CMUX", "", None):
            self.assertFalse(is_special_foul(code), code)
        for code in ("CM", "OF", "CMT2", "CMU_FL"):
            self.assertTrue(is_committed_foul(code), code)
        for code in ("CMD", "RV", "CCH"):
            self.assertFalse(is_committed_foul(code), code)


if __name__ == "__main__":
    unittest.main()
