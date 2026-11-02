import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import load_games as m


def test_one_parser_version_per_competition_season_passes():
    # 2026-27 re-derived with 0.2.2; earlier seasons keep 0.2.1 (not retroactive).
    rows = [("E", 2025, "0.2.1"), ("E", 2026, "0.2.2"), ("U", 2025, "0.2.1"), ("U", 2026, "0.2.2")]
    assert m.mixed_parser_versions(rows) == {}


def test_two_versions_inside_one_season_fail():
    rows = [("E", 2026, "0.2.1"), ("E", 2026, "0.2.2"), ("U", 2026, "0.2.2")]
    assert m.mixed_parser_versions(rows) == {("E", 2026): ["0.2.1", "0.2.2"]}
