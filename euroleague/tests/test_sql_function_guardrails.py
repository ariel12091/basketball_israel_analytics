from __future__ import annotations

import importlib.util
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SQL = ROOT / "sql"
SCRIPTS = ROOT / "scripts"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    __import__("sys").modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class SqlFunctionGuardrailsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.contract = load_module(
            "euroleague_function_contract",
            SCRIPTS / "euroleague_function_contract.py",
        )

    def test_new_migrations_cannot_patch_catalog_function_text(self):
        historical = {
            "015_effective_overtime_periods.sql",
            "016_matchup_segment_join_order.sql",
            "017_materialize_player_refresh.sql",
        }
        found = set()
        for path in SQL.glob("[0-9][0-9][0-9]_*.sql"):
            source = path.read_text(encoding="utf-8")
            reads_definition = "pg_get_functiondef" in source or re.search(
                r"\bSELECT\s+(?:p\.)?prosrc\b", source, re.I
            )
            if reads_definition:
                found.add(path.name)
        self.assertEqual(historical, found)

    def test_reader_manifest_covers_all_router_variants(self):
        readers = self.contract.DIRECT_APP_READERS
        for base in (
            "get_team_ratings",
            "get_team_four_factors",
            "get_team_minutes",
            "fetch_lineups",
        ):
            for kind in ("pergame", "dynamic", "direct"):
                self.assertIn(f"{base}_{kind}", readers)

    def test_drop_applicator_uses_the_shared_contract(self):
        source = (SCRIPTS / "apply_047_drop_orphans.py").read_text(encoding="utf-8")
        self.assertIn("PENDING_REMOVAL_FUNCTIONS", source)
        self.assertIn("PENDING_REMOVAL_VIEWS", source)
        self.assertIn("PROTECTED_RELATIONS", source)
        self.assertIn("APP_READER_SMOKE", source)

    def test_reachability_audit_is_read_only_and_fail_closed(self):
        source = (SCRIPTS / "audit_function_reachability.py").read_text(encoding="utf-8")
        self.assertIn("SET TRANSACTION READ ONLY", source)
        self.assertIn("connection.rollback()", source)
        self.assertIn("if missing or overloaded or uncovered", source)
        self.assertIn("pg_get_function_identity_arguments", source)
        for mutation in ("CREATE FUNCTION", "DROP FUNCTION", "connection.commit"):
            self.assertNotIn(mutation, source)


if __name__ == "__main__":
    unittest.main()
