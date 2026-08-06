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
)


class AnalyticsValidationTest(unittest.TestCase):
    def test_canonical_clock_clamps_regression_and_uses_overtime_budget(self) -> None:
        game = pd.DataFrame(
            {
                "TRUE_NUMBEROFPLAY": [1, 2, 3, 4, 5],
                "PERIOD": [1, 1, 1, 4, 5],
                "MARKERTIME": ["10:00", "09:30", "09:35", "00:00", "00:00"],
            }
        )

        elapsed, game_end = canonical_elapsed_seconds(game)

        self.assertEqual(elapsed, {1: 0, 2: 30, 3: 30, 4: 2400, 5: 2700})
        self.assertEqual(game_end, 2700)


if __name__ == "__main__":
    unittest.main()
