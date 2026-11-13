import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("apply_062", ROOT / "scripts" / "apply_062_ft_credit_columns.py")
apply_062 = importlib.util.module_from_spec(_spec)
sys.modules["apply_062"] = apply_062
_spec.loader.exec_module(apply_062)

LIVE_SHAPE = """CREATE OR REPLACE FUNCTION euroleague.refresh_actions_consumer_candidates(game_ids bigint[])
AS $function$
BEGIN
  INSERT INTO euroleague.action_team_context_actions (
    own_team_score, opp_team_score, load_run_id, derivation_version
  )
  SELECT
    'actions-v2'
  FROM sided sd;
  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$
"""


def test_the_fact_refresh_writes_and_recredits_the_credit_columns():
    out = apply_062.edit_refresh(LIVE_SHAPE)
    assert "credit_opp_starters, credit_segment_id\n  )" in out
    assert "    sd.own_lineup,\n    sd.opp_lineup,\n    sd.own_starters,\n    sd.opp_starters,\n    ms.segment_id\n  FROM sided sd" in out
    assert out.index("refresh_ft_credit_fields_for_games(game_ids)") < out.index("RETURN inserted_count")
    assert out.endswith("$function$;\n")


def test_an_edit_that_no_longer_matches_once_stops_the_apply():
    with pytest.raises(RuntimeError, match="expected one match"):
        apply_062.edit_refresh(LIVE_SHAPE.replace("'actions-v2'", "'actions-v3'"))


def test_the_migration_is_additive_and_adds_five_credit_columns():
    ddl = (ROOT / "sql" / "062_ft_credit_columns.sql").read_text(encoding="utf-8")
    statements = apply_062.validate_ddl(ddl)
    assert any("refresh_ft_credit_fields_for_games" in s for s in statements)
    for column in apply_062.CREDIT:
        assert f"ADD COLUMN {column}" in ddl
