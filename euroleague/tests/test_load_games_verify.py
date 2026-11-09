import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import load_games as m


def test_one_parser_version_per_competition_season_passes():
    rows = [("E", 2025, "0.2.1"), ("E", 2026, "0.2.2"), ("U", 2025, "0.2.1"), ("U", 2026, "0.2.2")]
    assert m.mixed_parser_versions(rows, equivalent={}) == {}


def test_two_versions_inside_one_season_fail():
    rows = [("E", 2026, "0.2.1"), ("E", 2026, "0.2.2"), ("U", 2026, "0.2.2")]
    assert m.mixed_parser_versions(rows, equivalent={}) == {("E", 2026): ["0.2.1", "0.2.2"]}


def test_proven_equivalent_versions_pass():
    # 0.2.3 re-derived only the games whose output changed (D36, D37).
    rows = [
        ("E", 2025, "0.2.1"), ("E", 2025, "0.2.3"),
        ("U", 2025, "0.2.1"), ("U", 2025, "0.2.3"),
        ("E", 2026, "0.2.2"), ("E", 2026, "0.2.3"),
        ("U", 2026, "0.2.2"), ("U", 2026, "0.2.3"),
    ]
    assert m.mixed_parser_versions(rows) == {}


def test_equivalence_is_per_season():
    # 0.2.1 predates the renamed 2026-27 foul codes, so it is not equivalent there.
    rows = [("E", 2026, "0.2.1"), ("E", 2026, "0.2.3")]
    assert m.mixed_parser_versions(rows) == {("E", 2026): ["0.2.1", "0.2.3"]}


def test_an_unlisted_older_version_still_fails():
    rows = [("U", 2025, "0.2.0"), ("U", 2025, "0.2.3")]
    assert m.mixed_parser_versions(rows) == {("U", 2025): ["0.2.0", "0.2.3"]}
