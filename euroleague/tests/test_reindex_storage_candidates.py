import ast
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "reindex_storage_candidates.py"
SOURCE = SCRIPT.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)


class ReindexStorageCandidatesTests(unittest.TestCase):
    def test_only_concurrent_reindex_is_present(self):
        self.assertIn("REINDEX INDEX CONCURRENTLY euroleague.", SOURCE)
        self.assertNotIn("REINDEX TABLE", SOURCE.upper())

    def test_script_checks_load_state_and_definition(self):
        self.assertIn("assert_no_active_load(cursor)", SOURCE)
        self.assertIn("started_at >= now() - interval '24 hours'", SOURCE)
        self.assertIn("RowExclusiveLock", SOURCE)
        self.assertIn("new_definition != old_definition", SOURCE)
        self.assertIn("not valid or not ready", SOURCE)

    def test_script_has_no_drop_or_israeli_schema_reference(self):
        executable_strings = [
            node.value
            for node in ast.walk(TREE)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        ]
        joined = "\n".join(executable_strings).upper()
        self.assertNotIn("DROP INDEX", joined)
        self.assertNotIn("BASKETBALL.", joined)
        self.assertNotIn("BASKETBALL_TEST.", joined)


if __name__ == "__main__":
    unittest.main()
