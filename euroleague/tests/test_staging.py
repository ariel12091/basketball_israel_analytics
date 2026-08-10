from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "euroleague" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions.staging import (  # noqa: E402
    PACKAGE_EVENT_TO_ACTION_COLUMN,
    _columnar_package_event,
    _minutes_seconds,
    _package_events_by_order,
    _restore_package_home_team_marker,
    build_staged_game,
    staged_counts,
)


SAMPLE_PBP = Path("C:/tmp/euroleague_pbp_2025_100games.csv")
BOXSCORE_DIR = REPO_ROOT / "euroleague" / "data" / "raw" / "boxscores"
HAS_CACHED_GAME = SAMPLE_PBP.exists() and (BOXSCORE_DIR / "E2025_1.json").exists()


class StagingTest(unittest.TestCase):
    def test_minutes_parser_preserves_dnp_as_unknown_duration(self) -> None:
        self.assertEqual(_minutes_seconds("33:21"), 2001)
        self.assertIsNone(_minutes_seconds("DNP"))
        self.assertIsNone(_minutes_seconds(None))

    def test_complete_package_event_is_preserved_by_source_order(self) -> None:
        events = pd.DataFrame(
            {
                "TRUE_NUMBEROFPLAY": [7],
                "PLAYTYPE": ["2FGM"],
                "Lineup_A": [["A1", "A2", "A3", "A4", "A5"]],
                "Lineup_B": [["B1", "B2", "B3", "B4", "B5"]],
                "IsHomeTeam": [True],
                "validate_on_court_player": [False],
                "optional_package_field": [pd.NA],
            }
        )

        raw_event = _package_events_by_order(events)[7]

        self.assertEqual(raw_event["Lineup_A"], ["A1", "A2", "A3", "A4", "A5"])
        self.assertEqual(raw_event["Lineup_B"], ["B1", "B2", "B3", "B4", "B5"])
        self.assertTrue(raw_event["IsHomeTeam"])
        self.assertFalse(raw_event["validate_on_court_player"])
        self.assertIsNone(raw_event["optional_package_field"])

    def test_complete_package_event_becomes_typed_action_columns(self) -> None:
        event = {
            "Season": 2025,
            "Gamecode": 7,
            "TYPE": 0,
            "NUMBEROFPLAY": 42,
            "CODETEAM": "AAA",
            "PLAYER_ID": "P1",
            "PLAYTYPE": "2FGM",
            "PLAYER": "PLAYER, ONE",
            "TEAM": "Team A",
            "DORSAL": 4,
            "MINUTE": 3,
            "MARKERTIME": "07:12",
            "POINTS_A": 8,
            "POINTS_B": None,
            "COMMENT": None,
            "PLAYINFO": "Two pointer made",
            "PERIOD": 1,
            "TRUE_NUMBEROFPLAY": 9,
            "Lineup_A": ["A1", "A2", "A3", "A4", "A5"],
            "Lineup_B": ["B1", "B2", "B3", "B4", "B5"],
            "IsHomeTeam": True,
            "validate_on_court_player": True,
        }

        action = _columnar_package_event(event)

        self.assertEqual(set(action), set(PACKAGE_EVENT_TO_ACTION_COLUMN.values()))
        self.assertEqual(action["lineup_a"], event["Lineup_A"])
        self.assertEqual(action["lineup_b"], event["Lineup_B"])
        self.assertEqual(action["source_event_order"], 9)
        self.assertNotIn("raw_event", action)

    def test_home_marker_is_restored_from_boxscore_sides(self) -> None:
        events = pd.DataFrame(
            {
                "CODETEAM": ["AAA", "BBB", None],
                "IsHomeTeam": [pd.NA, pd.NA, pd.NA],
            }
        )

        restored = _restore_package_home_team_marker(events, "AAA", "BBB")

        self.assertEqual(restored["IsHomeTeam"].tolist(), [True, False, None])

    def test_contradictory_package_home_marker_is_rejected(self) -> None:
        events = pd.DataFrame(
            {"CODETEAM": ["AAA"], "IsHomeTeam": [False]}
        )

        with self.assertRaisesRegex(ValueError, "contradicts box-score sides"):
            _restore_package_home_team_marker(events, "AAA", "BBB")

    def test_unmapped_package_field_blocks_columnar_actions(self) -> None:
        event = {key: None for key in PACKAGE_EVENT_TO_ACTION_COLUMN}
        event["new_package_field"] = "new"

        with self.assertRaisesRegex(ValueError, "extra=\\['new_package_field'\\]"):
            _columnar_package_event(event)

    @unittest.skipUnless(HAS_CACHED_GAME, "cached 100-game sample is unavailable")
    def test_cached_game_one_matches_reviewed_load_plan(self) -> None:
        pbp = pd.read_csv(SAMPLE_PBP)
        staged = build_staged_game(
            pbp,
            BOXSCORE_DIR,
            season=2025,
            gamecode=1,
        )

        counts = staged_counts(staged)
        self.assertEqual(
            {
                key: counts[key]
                for key in (
                    "source_artifacts",
                    "full_rosters",
                    "team_boxscores",
                    "actions_raw",
                    "actions",
                    "reconciliation_metrics",
                    "game_qa",
                )
            },
            {
                "source_artifacts": 3,
                "full_rosters": 24,
                "team_boxscores": 2,
                "actions_raw": 546,
                "actions": 546,
                "reconciliation_metrics": 32,
                "game_qa": 1,
            },
        )
        qa = staged.snapshot.rows["game_qa"][0]
        self.assertEqual(qa["publication_status"], "review")
        self.assertTrue(qa["boxscore_metrics_exact"])
        self.assertTrue(qa["score_progression_reconciled"])
        self.assertEqual(qa["lineup_invalid_actor_rows"], 1)
        first_raw_event = staged.snapshot.rows["actions_raw"][0]["raw_event"]
        self.assertIn("Lineup_A", first_raw_event)
        self.assertIn("Lineup_B", first_raw_event)
        self.assertIn("IsHomeTeam", first_raw_event)
        self.assertIn("validate_on_court_player", first_raw_event)
        self.assertEqual(len(first_raw_event["Lineup_A"]), 5)
        self.assertEqual(len(first_raw_event["Lineup_B"]), 5)
        first_action = staged.snapshot.rows["actions"][0]
        self.assertEqual(first_action["lineup_a"], first_raw_event["Lineup_A"])
        self.assertEqual(first_action["lineup_b"], first_raw_event["Lineup_B"])
        self.assertEqual(
            sum(
                row["is_home_team"] is not None
                for row in staged.snapshot.rows["actions"]
            ),
            534,
        )
        self.assertNotIn("raw_event", first_action)
        self.assertEqual(
            sum(row["end_possession"] for row in staged.snapshot.rows["actions"]),
            140,
        )


if __name__ == "__main__":
    unittest.main()
