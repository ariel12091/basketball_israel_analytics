import unittest
from pathlib import Path


ROOT = Path(__file__).parents[1]
ACTIONS_VERIFY = (ROOT / "scripts" / "verify_actions_schema.py").read_text(
    encoding="utf-8"
)
LINEUPS_VERIFY = (ROOT / "scripts" / "verify_lineup_units.py").read_text(
    encoding="utf-8"
)


class StorageVerificationQueryTests(unittest.TestCase):
    def test_schema_size_does_not_double_count_index_relations(self):
        self.assertIn("c.relkind IN ('r', 'm')", ACTIONS_VERIFY)

    def test_lineup_duration_uses_effective_overtime_period(self):
        self.assertIn(
            "max(euroleague.effective_period(period, minute, play_type))",
            LINEUPS_VERIFY,
        )


if __name__ == "__main__":
    unittest.main()
