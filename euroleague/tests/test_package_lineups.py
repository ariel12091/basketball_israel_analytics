from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "euroleague" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from euroleague_possessions.package_lineups import (  # noqa: E402
    _game_audit,
    apply_package_lineups,
)


def _boxscore() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Home": [1] * 6 + [0] * 5,
            "Team": ["AAA"] * 6 + ["BBB"] * 5,
            "Player": [
                "A1",
                "A2",
                "A3",
                "A4",
                "A5",
                "A6",
                "B1",
                "B2",
                "B3",
                "B4",
                "B5",
            ],
            "IsStarter": [1, 1, 1, 1, 1, 0, 1, 1, 1, 1, 1],
        }
    )


def _events() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Season": [2025] * 5,
            "Gamecode": [1] * 5,
            "TRUE_NUMBEROFPLAY": [0, 1, 2, 3, 4],
            "PLAYTYPE": ["BP", "2FGM", "OUT", "IN", "2FGM"],
            "PLAYER": [None, "A1", "A1", "A6", "A6"],
            "CODETEAM": [None, "AAA", "AAA", "AAA", "AAA"],
            "MARKERTIME": ["10:00", "09:40", "08:00", "08:00", "07:30"],
        }
    )


class PackageLineupsTest(unittest.TestCase):
    def test_delegates_substitution_to_package_lineup_function(self) -> None:
        enriched = apply_package_lineups(_events(), _boxscore())

        self.assertEqual(enriched.iloc[1]["Lineup_A"], ["A1", "A2", "A3", "A4", "A5"])
        self.assertEqual(enriched.iloc[2]["Lineup_A"], ["A6", "A2", "A3", "A4", "A5"])
        self.assertTrue(enriched["validate_on_court_player"].all())

        audit = _game_audit(enriched, _boxscore())
        self.assertTrue(audit["starters_valid"])
        self.assertTrue(audit["lineup_structure_valid"])
        self.assertEqual(audit["invalid_actor_rows"], 0)

    def test_preserves_package_invalid_actor_flag(self) -> None:
        events = _events()
        events.loc[4, "PLAYER"] = "A1"
        enriched = apply_package_lineups(events, _boxscore())

        self.assertFalse(bool(enriched.iloc[4]["validate_on_court_player"]))
        self.assertEqual(_game_audit(enriched, _boxscore())["invalid_actor_rows"], 1)


def _two_team_boxscore() -> pd.DataFrame:
    box = _boxscore()
    bench = pd.DataFrame({"Home": [0], "Team": ["BBB"], "Player": ["B6"], "IsStarter": [0]})
    return pd.concat([box, bench], ignore_index=True)


def _interleaved_events() -> pd.DataFrame:
    # Both teams substitute at one stoppage and the feed interleaves the rows
    # (shape of E 2025 game 249 and EuroCup 2026 game 79).
    return pd.DataFrame(
        {
            "Season": [2025] * 7,
            "Gamecode": [2] * 7,
            "TRUE_NUMBEROFPLAY": list(range(7)),
            "PLAYTYPE": ["BP", "OUT", "IN", "IN", "OUT", "2FGM", "2FGM"],
            "PLAYER": [None, "A1", "B6", "A6", "B1", "A6", "B6"],
            "CODETEAM": [None, "AAA", "BBB", "AAA", "BBB", "AAA", "BBB"],
            "MARKERTIME": ["10:00", "08:00", "08:00", "08:00", "08:00", "07:30", "07:00"],
        }
    )


class SameTeamPairingTest(unittest.TestCase):
    def test_interleaved_substitutions_pair_within_each_team(self) -> None:
        enriched = apply_package_lineups(_interleaved_events(), _two_team_boxscore())
        last = enriched.iloc[-1]
        self.assertEqual(sorted(last["Lineup_A"]), ["A2", "A3", "A4", "A5", "A6"])
        self.assertEqual(sorted(last["Lineup_B"]), ["B2", "B3", "B4", "B5", "B6"])
        self.assertTrue(enriched["validate_on_court_player"].all())
        self.assertTrue(_game_audit(enriched, _two_team_boxscore())["lineup_structure_valid"])

    def test_the_package_still_pairs_across_teams(self) -> None:
        # Documents why the builder exists: if the package ever pairs within a
        # team, this fails and the builder can go back to delegating.
        from euroleague_api.utils import get_pbp_lineups

        events = _interleaved_events()
        events["CODETEAM"] = events["CODETEAM"].fillna("")
        package = get_pbp_lineups(events, _two_team_boxscore(), validate=True)
        self.assertIn("B6", package.iloc[-1]["Lineup_A"])

    def test_matches_the_package_when_teams_do_not_interleave(self) -> None:
        from euroleague_api.utils import get_pbp_lineups

        events = _events()
        package = get_pbp_lineups(
            events.assign(CODETEAM=events["CODETEAM"].fillna("")), _boxscore(), validate=True
        )
        ours = apply_package_lineups(events, _boxscore())
        self.assertEqual(ours["Lineup_A"].tolist(), package["Lineup_A"].tolist())
        self.assertEqual(ours["Lineup_B"].tolist(), package["Lineup_B"].tolist())
        self.assertEqual(
            ours["validate_on_court_player"].tolist(),
            package["validate_on_court_player"].tolist(),
        )


class SubstitutionCorrectionTest(unittest.TestCase):
    def _with(self, corrections):
        from unittest import mock

        from euroleague_possessions import package_lineups

        return mock.patch.dict(
            package_lineups.KNOWN_SUBSTITUTION_CORRECTIONS, corrections, clear=True
        )

    def test_a_correction_renames_the_incoming_player(self) -> None:
        events = _events()
        events.loc[3, "PLAYER"] = "A2"  # the feed names a player already on
        with self._with({("E", 2025, 1, 3): ("IN", "A2", "A6")}):
            enriched = apply_package_lineups(events, _boxscore())
        self.assertEqual(sorted(enriched.iloc[-1]["Lineup_A"]), ["A2", "A3", "A4", "A5", "A6"])
        self.assertEqual(enriched.iloc[3]["PLAYER"], "A2")  # the event itself is not edited

    def test_an_ignored_row_takes_no_part(self) -> None:
        with self._with({("E", 2025, 1, 2): ("OUT", "A1", None), ("E", 2025, 1, 3): ("IN", "A6", None)}):
            enriched = apply_package_lineups(_events(), _boxscore())
        self.assertEqual(sorted(enriched.iloc[-1]["Lineup_A"]), ["A1", "A2", "A3", "A4", "A5"])

    def test_a_correction_that_no_longer_matches_is_skipped(self) -> None:
        with self._with({("E", 2025, 1, 3): ("IN", "SOMEONE ELSE", None)}):
            with self.assertLogs("euroleague_possessions.package_lineups", level="WARNING"):
                enriched = apply_package_lineups(_events(), _boxscore())
        self.assertIn("A6", enriched.iloc[-1]["Lineup_A"])

    def test_corrections_are_scoped_to_their_competition(self) -> None:
        with self._with({("U", 2025, 1, 3): ("IN", "A6", None)}):
            enriched = apply_package_lineups(_events(), _boxscore(), competition="E")
        self.assertIn("A6", enriched.iloc[-1]["Lineup_A"])


def _bench_boxscore() -> pd.DataFrame:
    bench = pd.DataFrame({"Home": [1], "Team": ["AAA"], "Player": ["A7"], "IsStarter": [0]})
    return pd.concat([_boxscore(), bench], ignore_index=True)


def _rows(spec) -> pd.DataFrame:
    """spec: (play_type, player, clock) rows for team AAA after a BP row."""
    rows = [("BP", None, None, "10:00")] + [(t, p, "AAA", c) for t, p, c in spec]
    return pd.DataFrame(
        {
            "Season": [2025] * len(rows),
            "Gamecode": [3] * len(rows),
            "TRUE_NUMBEROFPLAY": list(range(len(rows))),
            "PLAYTYPE": [r[0] for r in rows],
            "PLAYER": [r[1] for r in rows],
            "CODETEAM": [r[2] for r in rows],
            "MARKERTIME": [r[3] for r in rows],
        }
    )


class InferredRepairTest(unittest.TestCase):
    """D37 rules: substitutions alternate IN/OUT and players act only on the floor."""

    def _build(self, events, infer=True):
        from unittest import mock

        from euroleague_possessions import package_lineups

        with mock.patch.object(package_lineups, "INFER_SUBSTITUTION_REPAIRS", infer):
            return apply_package_lineups(events, _bench_boxscore())

    def test_wrong_outgoing_player_is_the_one_next_seen_coming_in(self) -> None:
        # Shape of EuroCup 2025-26 game 32: the OUT row names a player on the
        # bench (A6); A2 is the one who left, since he next comes IN.
        events = _rows([
            ("2FGM", "A1", "09:40"),
            ("OUT", "A6", "08:00"), ("IN", "A7", "08:00"),
            ("2FGM", "A7", "07:30"),
            ("IN", "A2", "06:00"), ("OUT", "A7", "06:00"),
            ("2FGM", "A2", "05:30"),
        ])
        stored = self._build(events, infer=False)
        self.assertFalse(stored["validate_on_court_player"].all())

        enriched = self._build(events)
        self.assertEqual(sorted(enriched.iloc[3]["Lineup_A"]), ["A1", "A3", "A4", "A5", "A7"])
        self.assertEqual(sorted(enriched.iloc[-1]["Lineup_A"]), ["A1", "A2", "A3", "A4", "A5"])
        self.assertTrue(enriched["validate_on_court_player"].all())

    def test_wrong_outgoing_player_is_left_alone_when_ambiguous(self) -> None:
        events = _rows([
            ("OUT", "A6", "08:00"), ("IN", "A7", "08:00"),
            ("IN", "A2", "06:00"), ("OUT", "A7", "06:00"),
            ("IN", "A3", "05:00"), ("OUT", "A1", "05:00"),
        ])
        self.assertEqual(
            self._build(events)["Lineup_A"].tolist(),
            self._build(events, infer=False)["Lineup_A"].tolist(),
        )

    def test_phantom_pair_is_ignored(self) -> None:
        # Shape of EuroCup 2026-27 game 71: OUT A1 / IN A6 logged early; A1
        # keeps acting and the same swap is logged again later.
        events = _rows([
            ("OUT", "A1", "09:00"), ("IN", "A6", "09:00"),
            ("2FGM", "A1", "08:30"),
            ("OUT", "A1", "08:00"), ("IN", "A6", "08:00"),
            ("2FGM", "A6", "07:30"),
        ])
        self.assertFalse(self._build(events, infer=False)["validate_on_court_player"].all())

        enriched = self._build(events)
        self.assertIn("A1", enriched.iloc[3]["Lineup_A"])
        self.assertEqual(sorted(enriched.iloc[-1]["Lineup_A"]), ["A2", "A3", "A4", "A5", "A6"])
        # Only the phantom IN row itself still names a player not on the floor.
        invalid = enriched.index[~enriched["validate_on_court_player"].astype(bool)].tolist()
        self.assertEqual(invalid, [2])

    def test_consistent_substitutions_are_unchanged(self) -> None:
        events = _rows([
            ("OUT", "A1", "09:00"), ("IN", "A6", "09:00"),
            ("2FGM", "A6", "08:30"),
            ("IN", "A1", "08:00"), ("OUT", "A6", "08:00"),
            ("2FGM", "A1", "07:30"),
        ])
        self.assertEqual(
            self._build(events)["Lineup_A"].tolist(),
            self._build(events, infer=False)["Lineup_A"].tolist(),
        )


if __name__ == "__main__":
    unittest.main()
