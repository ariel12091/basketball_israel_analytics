from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "euroleague" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions.package_lineups import (  # noqa: E402
    _game_audit,
    apply_package_lineups,
)


def _boxscore() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Home": [1] * 6 + [0] * 5,
            "Team": ["AAA"] * 6 + ["BBB"] * 5,
            "Player": [
                "A1",
                "A2",
                "A3",
                "A4",
                "A5",
                "A6",
                "B1",
                "B2",
                "B3",
                "B4",
                "B5",
            ],
            "IsStarter": [1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1],
        }
    )


def _events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Season": [2025] * 5,
            "Gamecode": [1] * 5,
            "TRUE_NUMBEROFPLAY": [0, 1, 2, 3, 4],
            "PLAYTYPE": ["BP", "2FGM", "OUT", "IN", "2FGM"],
            "PLAYER": [None, "A1", "A1", "A6", "A6"],
            "CODETEAM": [None, "AAA", "AAA", "AAA", "AAA"],
            "MARKERTIME": ["10:00", "09:40", "08:00", "08:00", "07:30"],
        }
    )


class PackageLineupsTest(unittest.TestCase):
    def test_delegates_substitution_to_package_lineup_function(self) -> None:
        enriched = apply_package_lineups(_events(), _boxscore())

        self.assertEqual(enriched.iloc[1]["Lineup_A"], ["A1", "A2", "A3", "A4", "A5"])
        self.assertEqual(enriched.iloc[2]["Lineup_A"], ["A6", "A2", "A3", "A4", "A5"])
        self.assertTrue(enriched["validate_on_court_player"].all())

        audit = _game_audit(enriched, _boxscore())
        self.assertTrue(audit["starters_valid"])
        self.assertTrue(audit["lineup_structure_valid"])
        self.assertEqual(audit["invalid_actor_rows"], 0)

    def test_preserves_package_invalid_actor_flag(self) -> None:
        events = _events()
        events.loc[4, "PLAYER"] = "A1"
        enriched = apply_package_lineups(events, _boxscore())

        self.assertFalse(bool(enriched.iloc[4]["validate_on_court_player"]))
        self.assertEqual(_game_audit(enriched, _boxscore())["invalid_actor_rows"], 1)


if __name__ == "__main__":
    unittest.main()
