from __future__ import annotations

import importlib.util
import sys
import unittest
from decimal import Decimal
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "audit_team_reader_contracts.py"


def load_module():
    spec = importlib.util.spec_from_file_location("team_reader_contract_audit", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FakeColumn:
    def __init__(self, name):
        self.name = name


class FakeCursor:
    def __init__(self, results):
        self.results = iter(results)

    def execute(self, _query):
        columns, rows = next(self.results)
        self.description = [FakeColumn(name) for name in columns]
        self.rows = rows

    def fetchall(self):
        return self.rows


class TeamReaderContractAuditTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.module = load_module()
        cls.source = SCRIPT.read_text(encoding="utf-8")

    def test_audit_is_read_only_and_time_bounded(self):
        self.assertIn("SET TRANSACTION READ ONLY", self.source)
        self.assertIn("statement_timeout='60s'", self.source)
        self.assertIn("connection.rollback()", self.source)
        self.assertNotIn("connection.commit", self.source)
        self.assertNotIn("pg_get_functiondef", self.source)

    def test_matrix_covers_both_schemas_and_all_app_routes(self):
        joined = " ".join(route.ratings_sql + route.factors_sql for route in self.module.ROUTES)
        for token in (
            "basketball_test.team_ppp_ratings_mv", "basketball_test.team_four_factors_mv",
            "euroleague.team_ppp_ratings_mv", "euroleague.team_four_factors_mv",
            "get_team_ratings_pergame", "get_team_four_factors_pergame",
            "get_team_ratings_dynamic", "get_team_four_factors_dynamic",
            "get_team_ratings_direct", "get_team_four_factors_direct",
            "p_last_n_games", "p_num_starters_off_min", "p_num_starters_def_max",
            "p_max_margin", "p_max_time_remaining",
        ):
            self.assertIn(token, joined)

    def test_mismatch_identifies_the_first_common_column(self):
        columns = ("team_id",) + self.module.COMMON_COLUMNS
        cursor = FakeCursor([
            (columns, [(1, Decimal("110.0"), Decimal("100.0"), Decimal("10.0"), 50, 50)]),
            (columns, [(1, Decimal("110.0"), Decimal("100.0"), Decimal("9.9"), 50, 50)]),
        ])
        detail = self.module.mismatch(cursor, self.module.ROUTES[0])
        self.assertIn("column=net_rtg", detail)

    def test_mismatch_reports_every_differing_column(self):
        columns = ("team_id",) + self.module.COMMON_COLUMNS
        cursor = FakeCursor([
            (columns, [(1, Decimal("110.0"), Decimal("100.0"), Decimal("10.0"), 50, 50)]),
            (columns, [(1, Decimal("109.9"), Decimal("100.0"), Decimal("9.9"), 49, 50)]),
        ])
        detail = self.module.mismatch(cursor, self.module.ROUTES[0])
        self.assertIn("mismatches=3", detail)
        self.assertIn("columns=off_ppp=1,net_rtg=1,off_poss=1", detail)

    def test_numeric_scale_is_not_drift(self):
        columns = ("team_id",) + self.module.COMMON_COLUMNS
        row_a = (1, Decimal("110"), Decimal("100"), Decimal("10"), 50, 50)
        row_b = (1, Decimal("110.0"), Decimal("100.0"), Decimal("10.0"), 50, 50)
        cursor = FakeCursor([(columns, [row_a]), (columns, [row_b])])
        self.assertIsNone(self.module.mismatch(cursor, self.module.ROUTES[0]))


if __name__ == "__main__":
    unittest.main()
