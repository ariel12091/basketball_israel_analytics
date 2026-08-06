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


if __name__ == "__main__":
    unittest.main()
