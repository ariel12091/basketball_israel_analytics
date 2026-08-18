from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "euroleague" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions.analytics_validation import (  # noqa: E402
    canonical_elapsed_seconds,
    effective_period,
)


class AnalyticsValidationTest(unittest.TestCase):
    def test_canonical_clock_clamps_regression_and_uses_overtime_budget(self) -> None:
        game = pd.DataFrame(
            {
                "TRUE_NUMBEROFPLAY": [1, 2, 3, 4, 5],
                "PERIOD": [1, 1, 1, 4, 5],
                "MINUTE": [1, 1, 1, 40, 45],
                "MARKERTIME": ["10:00", "09:30", "09:35", "00:00", "00:00"],
                "PLAYTYPE": ["BP", "2FGM", "CM", "EP", "EP"],
            }
        )

        elapsed, game_end = canonical_elapsed_seconds(game)

        self.assertEqual(elapsed, {1: 0, 2: 30, 3: 30, 4: 2400, 5: 2700})
        self.assertEqual(game_end, 2700)

    def test_cumulative_minutes_expand_second_overtime(self) -> None:
        game = pd.DataFrame(
            {
                "TRUE_NUMBEROFPLAY": [1, 2, 3, 4, 5],
                "PERIOD": [5, 5, 5, 5, 5],
                "MINUTE": [41, 46, 46, 50, 51],
                "MARKERTIME": ["05:00", None, "05:00", "00:01", None],
                "PLAYTYPE": ["BP", "EP", "BP", "3FGA", "EP"],
            }
        )

        elapsed, game_end = canonical_elapsed_seconds(game)

        # Markerless EP rows retain the preceding canonical instant; the next
        # BP and the game-end budget provide the exact 2700/3000 boundaries.
        self.assertEqual(elapsed, {1: 2400, 2: 2400, 3: 2700, 4: 2999, 5: 2999})
        self.assertEqual(game_end, 3000)
        self.assertEqual(effective_period(5, 46, "EP"), 5)
        self.assertEqual(effective_period(5, 46, "BP"), 6)
        self.assertEqual(effective_period(5, 51, "EP"), 6)
        self.assertEqual(effective_period(5, 51, "BP"), 7)
        self.assertEqual(effective_period(5, 56, "EP"), 7)


if __name__ == "__main__":
    unittest.main()
