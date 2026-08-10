from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "euroleague" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions.raw_event_backfill import (  # noqa: E402
    _assert_same_orders,
    _json_payload,
    parse_gamecodes,
)


class RawEventBackfillTest(unittest.TestCase):
    def test_parse_gamecodes_is_sorted_and_deduplicated(self) -> None:
        self.assertEqual(parse_gamecodes("3,1-2,2"), [1, 2, 3])
        with self.assertRaises(ValueError):
            parse_gamecodes("4-2")

    def test_payload_preserves_complete_package_fields(self) -> None:
        payload = json.loads(
            _json_payload(
                {
                    9: {
                        "TRUE_NUMBEROFPLAY": 9,
                        "Lineup_A": ["A1", "A2", "A3", "A4", "A5"],
                        "Lineup_B": ["B1", "B2", "B3", "B4", "B5"],
                        "validate_on_court_player": True,
                    }
                }
            )
        )
        self.assertEqual(payload[0]["source_event_order"], 9)
        self.assertEqual(payload[0]["raw_event"]["Lineup_A"][0], "A1")

    def test_event_key_mismatch_fails_closed(self) -> None:
        _assert_same_orders(1, [1, 2], [1, 2])
        with self.assertRaisesRegex(ValueError, "event keys differ"):
            _assert_same_orders(1, [1, 2], [1, 3])


if __name__ == "__main__":
    unittest.main()
