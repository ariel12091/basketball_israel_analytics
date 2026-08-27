import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "scripts" / "benchmark_storage_indexes.py"
SPEC = importlib.util.spec_from_file_location("benchmark_storage_indexes", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)


class StorageIndexBenchmarkTests(unittest.TestCase):
    def test_candidate_definition_matches_expected_index(self):
        self.assertEqual(
            MODULE.CANDIDATE,
            "euroleague_player_stats_actions_team_idx",
        )
        self.assertIn("ON euroleague.player_stats_actions_by_game", MODULE.CANDIDATE_DDL)
        self.assertNotIn("CONCURRENTLY", MODULE.CANDIDATE_DDL.upper())

    def test_probe_set_covers_all_custom_consumers(self):
        sql = "\n".join(probe.sql for probe in MODULE.PROBES)
        self.assertIn("get_team_ratings_direct", sql)
        self.assertIn("get_team_four_factors_direct", sql)
        self.assertIn("get_team_minutes_direct", sql)
        self.assertIn("fetch_lineups_direct", sql)

    def test_digest_is_order_sensitive_and_stable(self):
        rows = [(1, "a"), (2, "b")]
        self.assertEqual(MODULE.digest_rows(rows), MODULE.digest_rows(list(rows)))
        self.assertNotEqual(MODULE.digest_rows(rows), MODULE.digest_rows(list(reversed(rows))))

    def test_script_explicitly_disables_loader_autocommit(self):
        source = SCRIPT.read_text(encoding="utf-8")
        connect_at = source.index("connection = connect_from_env_file")
        cursor_at = source.index("cursor = connection.cursor()", connect_at)
        self.assertIn("connection.autocommit = False", source[connect_at:cursor_at])


if __name__ == "__main__":
    unittest.main()
