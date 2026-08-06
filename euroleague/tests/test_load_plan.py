from __future__ import annotations

import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "euroleague" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions.load_plan import (  # noqa: E402
    _lineup_identity,
    _run_count,
    canonical_lineup_hash,
)


class LoadPlanTest(unittest.TestCase):
    def test_lineup_hash_is_order_independent_but_preserves_duplicates(self) -> None:
        self.assertEqual(
            canonical_lineup_hash(["P3", "P1", "P2"]),
            canonical_lineup_hash(["P2", "P3", "P1"]),
        )
        self.assertNotEqual(
            canonical_lineup_hash(["P1", "P1", "P2"]),
            canonical_lineup_hash(["P1", "P2"]),
        )

    def test_run_count_counts_contiguous_stints(self) -> None:
        self.assertEqual(_run_count([]), 0)
        self.assertEqual(_run_count(["A", "A", "B", "B", "A"]), 3)

    def test_lineup_identity_resolves_package_names_to_provider_ids(self) -> None:
        mapping = {
            ("AAA", "PLAYER A"): "P1",
            ("AAA", "PLAYER B"): "P2",
        }
        lineup_hash, members = _lineup_identity(
            ["PLAYER B", "PLAYER A"], "AAA", mapping
        )

        self.assertEqual(members, ("P2", "P1"))
        self.assertEqual(lineup_hash, canonical_lineup_hash(["P1", "P2"]))

        with self.assertRaises(ValueError):
            _lineup_identity(["MISSING"], "AAA", mapping)


if __name__ == "__main__":
    unittest.main()
