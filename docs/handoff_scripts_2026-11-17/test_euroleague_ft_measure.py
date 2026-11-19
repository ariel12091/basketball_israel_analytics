"""Checks that the experiment preserves genuine assignment uncertainty."""
import unittest
from types import SimpleNamespace

from euroleague_ft_measure import solve


def rows(*events):
    return [SimpleNamespace(source_event_order=i, play_type=kind, team_code=team,
                            player_id=player) for i, (kind, team, player) in enumerate(events)]


class EntitlementExperimentTest(unittest.TestCase):
    def test_foul_before_and_one_and_after_ft_are_order_independent(self):
        incident = rows(("CM", "A", "def"), ("RV", "B", "p"),
                        ("2FGM", "B", "p"), ("FTM", "B", "p"))
        self.assertEqual(solve(incident)[0], "unique")
        self.assertEqual(solve(list(reversed(incident))), solve(incident))

    def test_technical_and_personal_have_distinct_shooters(self):
        incident = rows(("CM", "A", "def"), ("RV", "B", "p"),
                        ("FTM", "B", "q"), ("B", "A", None),
                        ("FTM", "B", "p"), ("FTA", "B", "p"))
        self.assertEqual(solve(incident)[0], "unique")
        self.assertEqual(sorted(a[2] for a in solve(incident)[1]), [1, 2])

    def test_two_personals_are_ambiguous_without_pairing_evidence(self):
        incident = rows(("CM", "A", "d1"), ("CM", "A", "d2"),
                        ("FTM", "B", "p"), ("FTM", "B", "p"))
        self.assertEqual(solve(incident)[0], "ambiguous")

    def test_split_shooter_personal_does_not_fit(self):
        incident = rows(("CM", "A", "d"), ("RV", "B", "p"),
                        ("FTM", "B", "p"), ("FTM", "B", "q"))
        self.assertEqual(solve(incident)[0], "no_fit")

    def test_missing_foul_and_cancellation_are_not_silently_filled(self):
        self.assertEqual(solve(rows(("FTM", "B", "p")))[0], "no_fit")
        incident = rows(("CMT", "A", "d1"), ("CMT", "A", "d2"),
                        ("FTM", "B", "p"))
        self.assertEqual(solve(incident)[0], "no_fit")
        self.assertEqual(solve(incident, allow_cancel=True)[0], "ambiguous")


if __name__ == "__main__":
    unittest.main()
