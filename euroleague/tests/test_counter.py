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

from euroleague_possessions import count_possessions  # noqa: E402


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


class CounterFixtureTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        labelled = pd.read_csv(FIXTURE_PATH)
        cls.raw = labelled[RAW_COLUMNS]
        cls.expected_endpoints = int(labelled["expected_possession_end"].sum())

    def test_counter_is_exactly_deterministic(self) -> None:
        first = count_possessions(self.raw)
        second = count_possessions(self.raw)
        for name in (
            "events",
            "possessions",
            "team_totals",
            "reason_totals",
            "game_qa",
        ):
            assert_frame_equal(getattr(first, name), getattr(second, name))

    def test_one_row_is_emitted_for_each_labelled_endpoint(self) -> None:
        result = count_possessions(self.raw)
        self.assertEqual(len(result.possessions), self.expected_endpoints)

    def test_game_and_team_sequences_are_gap_free(self) -> None:
        result = count_possessions(self.raw)
        for _, group in result.possessions.groupby(["season", "gamecode"]):
            self.assertEqual(
                group["game_possession_number"].tolist(),
                list(range(1, len(group) + 1)),
            )
        for _, group in result.possessions.groupby(
            ["season", "gamecode", "offense_team"]
        ):
            self.assertEqual(
                group["team_possession_number"].tolist(),
                list(range(1, len(group) + 1)),
            )

    def test_fixture_games_have_no_hard_structural_failures(self) -> None:
        result = count_possessions(self.raw)
        self.assertTrue(result.game_qa["structural_status"].eq("pass").all())
        self.assertEqual(int(result.game_qa["unresolved_ft_rows"].sum()), 0)
        self.assertEqual(
            int(result.game_qa["duplicate_endpoint_incidents"].sum()), 0
        )
        self.assertEqual(int(result.game_qa["missing_parent_targets"].sum()), 0)


if __name__ == "__main__":
    unittest.main()

