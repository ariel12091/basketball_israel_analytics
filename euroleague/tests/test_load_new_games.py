import sys
from datetime import date, datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import load_new_games as m


NOW = datetime(2026, 9, 26, 5, 0, tzinfo=timezone.utc)


def rec(code, day, time="20:00", played=True):
    return {"gameCode": code, "date": day, "time": time, "played": played}


def test_current_provider_season_turns_over_in_august():
    assert m.current_provider_season(date(2026, 9, 23)) == 2026
    assert m.current_provider_season(date(2026, 8, 1)) == 2026
    assert m.current_provider_season(date(2027, 5, 24)) == 2026
    assert m.current_provider_season(date(2027, 7, 31)) == 2026


def test_select_played_keeps_finished_games_only():
    records = [
        rec(3, "Sep 24, 2026"),
        rec(1, "Sep 24, 2026"),
        rec(2, "Sep 26, 2026", "01:00"),              # tipped off 4h ago: may be live
        rec(4, "Sep 25, 2026", played=False),
        rec(5, None),                                  # no tip-off: never guess
    ]
    assert m.select_played(records, now=NOW) == [1, 3]


def test_new_gamecodes_never_requests_a_loaded_game():
    assert m.new_gamecodes([1, 2, 3, 4], {1, 3}) == [2, 4]
    assert m.new_gamecodes([1, 2], {1, 2}) == []
