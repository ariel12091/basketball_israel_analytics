import json
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path

from scripts import apply_049_team_net_rating as applier


class Apply049SafetyTests(unittest.TestCase):
    def setUp(self):
        self.row = {
            "team_id": 1,
            "off_pts": 100,
            "off_poss": 100,
            "def_pts": 90,
            "def_poss": 100,
            "games_played": 1,
            "wins": 1,
            "losses": 0,
            "off_ppp": 100.0,
            "def_ppp": 90.0,
            "net_rtg": 10.0,
            "rank_net_rtg": 1,
            "rank_off_ppp": 1,
            "rank_def_ppp": 1,
            "off_rank": 1,
            "def_rank": 1,
            "net_rank": 1,
        }

    def _migration(self, sql):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / "migration.sql"
        path.write_text(sql, encoding="utf-8")
        return path

    def test_scope_has_one_target_and_one_read_only_reference(self):
        self.assertEqual(("team_ppp_ratings_mv",), applier.TARGET_MVS)
        self.assertEqual(("team_four_factors_mv",), applier.REFERENCE_MVS)
        for digest in (
            *applier.EXPECTED_FUNCTION_HASHES.values(),
            *applier.EXPECTED_MV_HASHES.values(),
        ):
            self.assertRegex(digest, r"^[0-9a-f]{64}$")

    def test_real_migration_drops_only_the_two_defective_mvs(self):
        statements = applier.candidate_statements(applier.MIGRATION)
        body = "\n".join(statements).upper()
        self.assertEqual(2, body.count("DROP MATERIALIZED VIEW"))
        self.assertIn("EUROLEAGUE.TEAM_PPP_RATINGS_MV", body)
        self.assertIn("BASKETBALL_TEST.TEAM_FOUR_FACTORS_MV", body)
        self.assertNotIn(
            "DROP MATERIALIZED VIEW IF EXISTS "
            "EUROLEAGUE.TEAM_FOUR_FACTORS_MV",
            body,
        )

    def test_candidate_parser_rejects_euro_companion_drop(self):
        path = self._migration(
            "DROP MATERIALIZED VIEW euroleague.team_four_factors_mv;"
        )
        with self.assertRaisesRegex(ValueError, "unexpected DROP target"):
            applier.candidate_statements(path)

    def test_candidate_parser_rejects_unexpected_create_and_destructive_dml(self):
        for sql in (
            "CREATE MATERIALIZED VIEW euroleague.surprise AS SELECT 1;",
            "DELETE FROM euroleague.actions;",
            "DROP INDEX euroleague.some_index;",
        ):
            with self.subTest(sql=sql):
                with self.assertRaises(ValueError):
                    applier.candidate_statements(self._migration(sql))

    def test_candidate_parser_rejects_drop_function(self):
        with self.assertRaises(ValueError):
            applier.candidate_statements(self._migration(
                "DROP FUNCTION euroleague.get_team_ratings_pergame;"
            ))

    def test_definition_hash_gate_fails_closed(self):
        with self.assertRaisesRegex(RuntimeError, "changed unexpectedly"):
            applier.gate_definition_hashes(
                "functions", {"f": "old"}, {"f": "new"}
            )

    def test_mv_value_gates_accept_only_the_intended_net_change(self):
        before = {1: deepcopy(self.row)}
        after = {1: deepcopy(self.row)}
        applier.gate_additive_parity(before, after)
        applier.gate_ppp_unchanged(before, after)
        applier.gate_net_rtg_delta(before, after)
        applier.gate_ranks_unchanged(before, after)

        mutated = {1: deepcopy(self.row)}
        mutated[1]["off_pts"] += 1
        with self.assertRaisesRegex(RuntimeError, "additive parity"):
            applier.gate_additive_parity(before, mutated)

        mutated = {1: deepcopy(self.row)}
        mutated[1]["off_ppp"] += 0.1
        with self.assertRaisesRegex(RuntimeError, "must not touch"):
            applier.gate_ppp_unchanged(before, mutated)

        mutated = {1: deepcopy(self.row)}
        mutated[1]["net_rtg"] = 9.9
        with self.assertRaisesRegex(RuntimeError, "canonical additive"):
            applier.gate_net_rtg_delta(before, mutated)

        mutated = {1: deepcopy(self.row)}
        mutated[1]["net_rank"] = 2
        with self.assertRaisesRegex(RuntimeError, "must not change"):
            applier.gate_ranks_unchanged(before, mutated)

    def test_reader_and_companion_gates(self):
        before = [json.dumps({"team_id": 1, "off_ppp": 100.0,
                              "def_ppp": 90.0, "net_rtg": 9.9,
                              "off_poss": 100, "def_poss": 100})]
        after = [json.dumps({"team_id": 1, "off_ppp": 100.0,
                             "def_ppp": 90.0, "net_rtg": 10.0,
                             "off_poss": 100, "def_poss": 100})]
        applier.gate_reader_change(before, after, "broad season")
        applier.gate_summary_ff_agreement(after, after, "broad season")

        disagreement = [json.dumps({"team_id": 1, "off_ppp": 100.0,
                                     "def_ppp": 90.0, "net_rtg": 9.9,
                                     "off_poss": 100, "def_poss": 100})]
        with self.assertRaisesRegex(RuntimeError, "disagree"):
            applier.gate_summary_ff_agreement(after, disagreement, "broad season")

    def test_season_mv_must_match_publication_eligible_reader(self):
        mv = {1: deepcopy(self.row)}
        reader = [json.dumps({
            "team_id": 1, "off_ppp": 100.0, "def_ppp": 90.0,
            "net_rtg": 10.0, "games_played": 1, "wins": 1, "losses": 0,
            "off_poss": 100, "def_poss": 100, "rank_net_rtg": 1,
            "rank_off_ppp": 1, "rank_def_ppp": 1,
        })]
        applier.gate_mv_matches_pergame(mv, reader)
        changed = json.loads(reader[0])
        changed["off_poss"] = 101
        with self.assertRaisesRegex(RuntimeError, "season/pergame"):
            applier.gate_mv_matches_pergame(mv, [json.dumps(changed)])
        applier.gate_mv_net_is_canonical(mv)

    def test_exact_relation_contract_gate(self):
        contract = {
            "relation": ("owner", "{reader=r/owner}", None, 0, None),
            "indexes": [("mv_pk", "CREATE UNIQUE INDEX mv_pk")],
        }
        applier.gate_relation_contract("mv", contract, deepcopy(contract))
        changed = deepcopy(contract)
        changed["indexes"].append(("extra", "CREATE INDEX extra"))
        with self.assertRaisesRegex(RuntimeError, "contract changed"):
            applier.gate_relation_contract("mv", contract, changed)


if __name__ == "__main__":
    unittest.main()
