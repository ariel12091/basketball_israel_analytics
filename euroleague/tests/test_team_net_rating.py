"""Static contract tests for migration 046."""

import unittest
from pathlib import Path


MIGRATION = (
    Path(__file__).resolve().parent.parent
    / "sql"
    / "049_team_net_rating_single_round.sql"
)
RATINGS_READERS = (
    "get_team_ratings_pergame",
    "get_team_ratings_dynamic",
    "get_team_ratings_direct",
)
ISRAELI_FACTORS = "get_team_four_factors_dynamic"


def _sql():
    return MIGRATION.read_text(encoding="utf-8")


def _squashed():
    return "".join(_sql().split())


def _function_body(name):
    sql = _sql()
    start = sql.index(f"CREATE OR REPLACE FUNCTION euroleague.{name}(")
    end = sql.index("$function$;", start)
    return "".join(sql[start:end].split())


class TeamNetRatingMigrationTest(unittest.TestCase):
    def test_migration_exists(self):
        self.assertTrue(MIGRATION.is_file(), "049 has not been written yet")

    def test_no_pre_rounded_net_rating(self):
        self.assertNotIn("round(r.off_ppp-r.def_ppp,1)", _squashed())

    def test_each_ratings_reader_rounds_once(self):
        for name in RATINGS_READERS:
            with self.subTest(reader=name):
                body = _function_body(name)
                self.assertIn(
                    "round(100.0*r.off_pts/nullif(r.off_poss,0)", body
                )
                self.assertIn(
                    "100.0*r.def_pts/nullif(r.def_poss,0),1)", body
                )
                self.assertNotIn("round(r.off_ppp-r.def_ppp,1)", body)

    def test_each_ratings_reader_keeps_rounded_ranks(self):
        """Opponent-strength ranks remain unchanged in this safe slice."""
        for name in RATINGS_READERS:
            with self.subTest(reader=name):
                self.assertIn(
                    "dense_rank()OVER(ORDERBYr.off_ppp-r.def_pppDESC)",
                    _function_body(name),
                )

    def test_functions_are_replaced_not_dropped(self):
        upper = _sql().upper()
        self.assertNotIn("DROP FUNCTION", upper)
        self.assertEqual(upper.count("CREATE OR REPLACE FUNCTION"), 4)

    def test_israeli_four_factors_rounds_once_in_both_paths(self):
        sql = _sql()
        start = sql.index(
            "CREATE OR REPLACE FUNCTION "
            "basketball_test.get_team_four_factors_dynamic("
        )
        body = "".join(sql[start:].split())
        self.assertEqual(
            body.count("100.0*p.off_pts/NULLIF(p.off_poss,0)"), 2
        )
        self.assertNotIn("ROUND(p.off_ppp-p.def_ppp,1)", body)

    def test_only_defective_mv_in_each_schema_is_recreated(self):
        sql = _sql()
        upper = sql.upper()
        self.assertEqual(upper.count("DROP MATERIALIZED VIEW"), 2)
        self.assertEqual(upper.count("CREATE MATERIALIZED VIEW"), 2)
        self.assertIn(
            "DROP MATERIALIZED VIEW IF EXISTS euroleague.team_ppp_ratings_mv",
            sql,
        )
        self.assertNotIn(
            "DROP MATERIALIZED VIEW IF EXISTS "
            "euroleague.team_four_factors_mv",
            sql,
        )
        self.assertIn(
            "DROP MATERIALIZED VIEW IF EXISTS "
            "basketball_test.team_four_factors_mv",
            sql,
        )
        self.assertIn(
            "GRANT SELECT ON euroleague.team_ppp_ratings_mv TO app_readonly",
            sql,
        )
        self.assertIn(
            "GRANT SELECT ON basketball_test.team_four_factors_mv "
            "TO app_readonly",
            sql,
        )

    def test_exact_unique_index_is_recreated(self):
        sql = _sql()
        self.assertEqual(sql.upper().count("CREATE UNIQUE INDEX"), 2)
        self.assertIn(
            "CREATE UNIQUE INDEX euroleague_team_ppp_ratings_mv_pk\n"
            "  ON euroleague.team_ppp_ratings_mv "
            "(competition, game_year, team_id)",
            sql,
        )

    def test_season_ratings_uses_published_per_game_fact(self):
        sql = _sql()
        start = sql.index(
            "CREATE MATERIALIZED VIEW euroleague.team_ppp_ratings_mv AS"
        )
        end = sql.index("WITH NO DATA;", start)
        body = sql[start:end]
        self.assertIn("euroleague.team_four_factors_by_game", body)
        self.assertNotIn("euroleague.team_game_ratings_mv", body)


if __name__ == "__main__":
    unittest.main()
