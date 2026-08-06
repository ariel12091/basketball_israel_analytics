from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "euroleague" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions.schema_coverage import (  # noqa: E402
    _lineup_names,
    _roster_players,
    _team_sides,
)


class SchemaCoverageTest(unittest.TestCase):
    def test_roster_excludes_blank_non_player_actor_ids(self) -> None:
        boxscore = pd.DataFrame(
            {
                "Player_ID": [" P1 ", "   ", None],
                "Player": [" PLAYER ONE ", "", None],
                "Team": [" AAA ", "AAA", "AAA"],
            }
        )

        roster = _roster_players(boxscore)

        self.assertEqual(len(roster), 1)
        self.assertEqual(roster.iloc[0]["provider_player_id"], "P1")
        self.assertEqual(roster.iloc[0]["player_name"], "PLAYER ONE")
        self.assertEqual(roster.iloc[0]["team_code"], "AAA")

    def test_roster_excludes_package_team_and_total_aggregates(self) -> None:
        boxscore = pd.DataFrame(
            {
                "Player_ID": ["P1", "Team", "Total", "team-like-id"],
                "Player": ["PLAYER ONE", "Team", "Total", "REAL TEAM"],
                "Team": ["AAA", "AAA", "AAA", "AAA"],
            }
        )

        roster = _roster_players(boxscore)

        self.assertEqual(
            roster["provider_player_id"].tolist(), ["P1", "team-like-id"]
        )

    def test_team_sides_require_one_distinct_home_and_away_team(self) -> None:
        boxscore = pd.DataFrame(
            {"Home": [1, 1, 0, 0], "Team": ["AAA", "AAA", "BBB", "BBB"]}
        )
        self.assertEqual(_team_sides(boxscore), ("AAA", "BBB"))

        invalid = pd.DataFrame({"Home": [1, 0], "Team": ["AAA", "AAA"]})
        with self.assertRaises(ValueError):
            _team_sides(invalid)

    def test_lineup_names_keep_team_context(self) -> None:
        events = pd.DataFrame(
            {
                "Lineup_A": [["A1", "A2"], ["A1", "A3"]],
                "Lineup_B": [["B1", "B2"], ["B1", "B3"]],
            }
        )

        names = _lineup_names(events, "AAA", "BBB")

        self.assertEqual(
            names,
            {
                ("AAA", "A1"),
                ("AAA", "A2"),
                ("AAA", "A3"),
                ("BBB", "B1"),
                ("BBB", "B2"),
                ("BBB", "B3"),
            },
        )


if __name__ == "__main__":
    unittest.main()
