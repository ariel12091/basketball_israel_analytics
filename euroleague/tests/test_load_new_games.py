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


def test_tipoff_is_central_european_time():
    # Dubai game E2025/4: listed 18:00, tipped off 16:00 UTC (CEST, UTC+2).
    assert m.tipoff_utc("Sep 30, 2025", "18:00") == datetime(2025, 9, 30, 16, 0, tzinfo=timezone.utc)
    # Winter: CET, UTC+1.
    assert m.tipoff_utc("Jan 13, 2027", "21:00") == datetime(2027, 1, 13, 20, 0, tzinfo=timezone.utc)
    assert m.tipoff_utc(None, "20:00") is None


def test_select_played_waits_three_hours_after_tipoff():
    # NOW = Sep 26 05:00 UTC = 07:00 CEST. The cutoff is a 04:00 CEST tip-off.
    records = [
        rec(3, "Sep 24, 2026"),
        rec(1, "Sep 24, 2026"),
        rec(2, "Sep 26, 2026", "04:00"),              # exactly 3h ago: due
        rec(6, "Sep 26, 2026", "04:01"),              # 2h59m ago: may be live
        rec(4, "Sep 25, 2026", played=False),
        rec(5, None),                                  # no tip-off: never guess
    ]
    assert m.select_played(records, now=NOW) == [1, 2, 3]


def test_new_gamecodes_never_requests_a_loaded_game():
    assert m.new_gamecodes([1, 2, 3, 4], {1, 3}) == [2, 4]
    assert m.new_gamecodes([1, 2], {1, 2}) == []
