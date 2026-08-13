from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class TeamLineupPerformanceMigrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.lineup_sql = (ROOT / "sql" / "029_lineup_filter_before_expand.sql").read_text(
            encoding="utf-8"
        ).upper()

    def test_lineup_identity_is_limited_by_filtered_facts_before_expansion(self) -> None:
        sql = self.lineup_sql
        self.assertIn("LINEUP_IDENTITY AS MATERIALIZED", sql)
        identity = sql.split("LINEUP_IDENTITY AS MATERIALIZED", 1)[1].split(
            "UNIT_ROWS AS", 1
        )[0]
        self.assertIn("FROM FACTS F", identity)
        self.assertIn("JOIN EUROLEAGUE.LINEUP_TOTALS_BY_GAME L", identity)
        self.assertIn("L.OWN_LINEUP = F.OWN_LINEUP", identity)

    def test_no_unproven_index_is_added(self) -> None:
        self.assertNotIn("CREATE INDEX", self.lineup_sql)


if __name__ == "__main__":
    unittest.main()
