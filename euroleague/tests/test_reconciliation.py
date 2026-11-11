from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "euroleague" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions.boxscore_collector import (  # noqa: E402
    GameKey,
    _retry_after_seconds,
    game_keys_from_csv,
)
from euroleague_possessions.reconciliation import (  # noqa: E402
    METRIC_TO_BOXSCORE,
    reconcile_boxscores,
)


def _total(**overrides: int) -> dict[str, int]:
    total = {source: 0 for source in METRIC_TO_BOXSCORE.values()}
    total.update(overrides)
    return total


class ReconciliationTest(unittest.TestCase):
    def test_rate_limit_backoff_uses_exponential_fallback(self) -> None:
        self.assertEqual(_retry_after_seconds(None, 1, 60), 60)
        self.assertEqual(_retry_after_seconds(None, 2, 60), 120)
        self.assertEqual(_retry_after_seconds(None, 5, 60), 300)

    def test_game_keys_are_unique_and_sorted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "games.csv"
            pd.DataFrame(
                {"Season": [2025, 2025, 2024], "Gamecode": [2, 2, 9]}
            ).to_csv(path, index=False)
            self.assertEqual(
                game_keys_from_csv(path),
                [GameKey(2024, 9), GameKey(2025, 2)],
            )

    def test_exact_pbp_totals_reconcile_to_official_totals(self) -> None:
        play_types = [
            ("AAA", "2FGM"),
            ("AAA", "2FGA"),
            ("AAA", "3FGM"),
            ("AAA", "3FGA"),
            ("AAA", "FTM"),
            ("AAA", "FTA"),
            ("AAA", "O"),
            ("AAA", "D"),
            ("AAA", "AS"),
            ("AAA", "ST"),
            ("AAA", "TO"),
            ("AAA", "FV"),
            ("AAA", "AG"),
            ("AAA", "CM"),
            ("AAA", "RV"),
            ("BBB", "2FGM"),
        ]
        events = pd.DataFrame(
            {
                "season": 2025,
                "gamecode": 1,
                "source_event_order": range(1, len(play_types) + 1),
                "period": 1,
                "play_type": [play_type for _, play_type in play_types],
                "team_code": [team for team, _ in play_types],
                "score_a": [
                    2,
                    None,
                    5,
                    None,
                    6,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    6,
                ],
                "score_b": [
                    0,
                    None,
                    0,
                    None,
                    0,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    2,
                ],
            }
        )
        payload = {
            "Stats": [
                {
                    "tmr": {"Team": "AAA"},
                    "totr": _total(
                        Points=6,
                        FieldGoalsMade2=1,
                        FieldGoalsAttempted2=2,
                        FieldGoalsMade3=1,
                        FieldGoalsAttempted3=2,
                        FreeThrowsMade=1,
                        FreeThrowsAttempted=2,
                        OffensiveRebounds=1,
                        DefensiveRebounds=1,
                        Assistances=1,
                        Steals=1,
                        Turnovers=1,
                        BlocksFavour=1,
                        BlocksAgainst=1,
                        FoulsCommited=1,
                        FoulsReceived=1,
                    ),
                },
                {
                    "tmr": {"Team": "BBB"},
                    "totr": _total(
                        Points=2,
                        FieldGoalsMade2=1,
                        FieldGoalsAttempted2=1,
                    ),
                },
            ]
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "E2025_1.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            result = reconcile_boxscores(events, Path(directory))

        self.assertTrue(result.team_metrics["matches"].all())
        self.assertTrue(result.game_summary.iloc[0]["core_exact"])
        self.assertTrue(result.game_summary.iloc[0]["all_exact"])
        self.assertTrue(result.game_summary.iloc[0]["progression_exact"])
        self.assertEqual(len(result.score_anomalies), 0)
        self.assertEqual(len(result.missing_boxscores), 0)


class RenamedFoulCountTest(unittest.TestCase):
    def test_renamed_2026_27_fouls_count_as_committed(self) -> None:
        from euroleague_possessions.reconciliation import _event_counts

        codes = ["CM", "CMU_FL", "CMU_DI", "CMT1", "CMT2", "CMTI", "OF", "B", "C", "CMD"]
        events = pd.DataFrame(
            {
                "season": 2026,
                "gamecode": 960,
                "source_event_order": range(1, len(codes) + 1),
                "period": 1,
                "play_type": codes,
                "team_code": "A",
                "player_id": [f"a{i}" for i in range(len(codes))],
            }
        )
        counts = _event_counts(events)
        # Bench/coach (B, C) are excluded; a player's CMD counts.
        self.assertEqual(int(counts["fouls_committed"].iloc[0]), 8)

    def test_disqualifying_foul_counts_only_with_a_player(self) -> None:
        from euroleague_possessions.reconciliation import _event_counts

        events = pd.DataFrame(
            {
                "season": 2025,
                "gamecode": 14,
                "source_event_order": [1, 2, 3],
                "period": 4,
                "play_type": ["CM", "CMD", "CMD"],
                "team_code": "A",
                "player_id": ["p1", "p1", None],
            }
        )
        # The player's CMD is his foul; the bench CMD (no player) is not.
        self.assertEqual(int(_event_counts(events)["fouls_committed"].iloc[0]), 2)


if __name__ == "__main__":
    unittest.main()
