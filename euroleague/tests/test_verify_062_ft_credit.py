import importlib.util
import sys
from pathlib import Path

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "verify_062_ft_credit.py"
spec = importlib.util.spec_from_file_location("verify_062", SCRIPT)
v = importlib.util.module_from_spec(spec)
sys.modules["verify_062"] = v  # dataclasses resolve their module through sys.modules
spec.loader.exec_module(v)


def test_team_totals_sum_events_by_team_keys():
    s = v.Spec("x", "euroleague.t", ["game_id", "team_id"], ["points", "a + b AS ab"])
    sql = v.team_sql(s)
    assert "SELECT game_id, team_id, sum(points) AS points, sum(a + b) AS ab" in sql
    assert "FROM (SELECT * FROM euroleague.t) s GROUP BY game_id, team_id" in sql


def test_time_drops_zero_rows_and_is_absent_without_time():
    s = v.Spec("x", "euroleague.t", ["team_id"], [], ["team_id", "lineup_key"], ["seconds"])
    assert v.time_sql(s).endswith("GROUP BY team_id, lineup_key HAVING sum(seconds) <> 0")
    assert v.time_sql(v.Spec("y", "euroleague.t", ["team_id"], ["points"])) is None


def test_readers_are_called_with_named_arguments_on_the_largest_season():
    sql = v.reader("fetch_lineups_dynamic", p_unit_size="5")
    assert sql == ("SELECT * FROM euroleague.fetch_lineups_dynamic(p_competition => 'E', "
                   "p_game_year => 2025, p_unit_size => 5)")
    assert "season = 2025" in v.game_reader("select_team_game_facts")


def test_diff_compares_both_ways_against_the_baseline_schema():
    sql = v.diff_sql("SELECT 1", "lineup_totals_by_game__team")
    assert "euroleague_ftc_baseline.lineup_totals_by_game__team" in sql
    assert "live EXCEPT SELECT * FROM base" in sql and "base EXCEPT SELECT * FROM live" in sql


def test_every_spec_has_unique_name_and_starters_presets_are_reported_only():
    names = [s.name for s in v.SPECS]
    assert len(names) == len(set(names))
    assert all(s.expect_change for s in v.SPECS if s.name.endswith("_starters"))
    assert not any(s.expect_change for s in v.SPECS if not s.name.endswith("_starters"))
