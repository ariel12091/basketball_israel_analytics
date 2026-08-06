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
    _minutes_seconds,
    _stints_for_team,
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

    def test_stints_use_half_open_event_boundaries(self) -> None:
        events = pd.DataFrame(
            {
                "TRUE_NUMBEROFPLAY": [1, 2, 3],
                "CODETEAM": ["AAA", "AAA", "BBB"],
                "validate_on_court_player": [True, False, True],
            }
        )
        lineups = {
            1: {"AAA": "AAA:first"},
            2: {"AAA": "AAA:first"},
            3: {"AAA": "AAA:second"},
        }

        rows, event_stints = _stints_for_team(
            events,
            "AAA",
            lineups,
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(
            (rows[0]["start_event_order"], rows[0]["end_event_order_exclusive"]),
            (1, 3),
        )
        self.assertEqual(
            (rows[1]["start_event_order"], rows[1]["end_event_order_exclusive"]),
            (3, 4),
        )
        self.assertEqual(rows[0]["invalid_actor_rows"], 1)
        self.assertEqual(event_stints, {1: "AAA:1", 2: "AAA:1", 3: "AAA:2"})

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
                    "actions_clean",
                    "possessions",
                    "lineups",
                    "lineup_players",
                    "action_lineups",
                    "stints",
                    "pws",
                    "reconciliation_metrics",
                    "game_qa",
                )
            },
            {
                "source_artifacts": 3,
                "full_rosters": 24,
                "team_boxscores": 2,
                "actions_raw": 546,
                "actions_clean": 546,
                "possessions": 140,
                "lineups": 51,
                "lineup_players": 255,
                "action_lineups": 546,
                "stints": 64,
                "pws": 140,
                "reconciliation_metrics": 32,
                "game_qa": 1,
            },
        )
        qa = staged.snapshot.rows["game_qa"][0]
        self.assertEqual(qa["publication_status"], "review")
        self.assertTrue(qa["boxscore_metrics_exact"])
        self.assertTrue(qa["score_progression_reconciled"])
        self.assertEqual(qa["lineup_invalid_actor_rows"], 1)


if __name__ == "__main__":
    unittest.main()
