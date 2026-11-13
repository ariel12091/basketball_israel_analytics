#!/usr/bin/env python
"""Switch EuroLeague readers to the free-throw credit columns, one object at a
time (docs/superpowers/plans/2026-11-13-euroleague-ft-foul-credit.md, the
reader switch procedure).

  apply_062_readers.py --object NAME          dry run: edit, refresh, gates, ROLLBACK
  CONFIRM_EL_062_READER=NAME apply_062_readers.py --object NAME   commit

For each object: read its LIVE definition, apply exact edits (each must match
once), write sql/062_<object>.sql and the untouched sql/062_<object>_before.sql
(the rollback), CREATE OR REPLACE it, refresh what it feeds, then run the named
verify_062_ft_credit.py checks inside the same transaction. Any FAIL rolls back.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "src"))

from euroleague_possessions.postgres_backend import connect_from_env_file, inspect_target  # noqa: E402

ENV_PATH = REPO.parent / "etl" / ".Renviron"
BATCH = 100

_spec = importlib.util.spec_from_file_location("verify_062", REPO / "scripts" / "verify_062_ft_credit.py")
verify = importlib.util.module_from_spec(_spec)
sys.modules["verify_062"] = verify
_spec.loader.exec_module(verify)


@dataclass(frozen=True)
class ReaderSwitch:
    signature: str                 # regprocedure text of the function to edit
    edits: list[tuple[str, str]]   # exact (old, new) pairs on the live definition
    refresh: str | None = None     # per-batch call, %s = bigint[] of game ids
    pre: list[str] = field(default_factory=list)     # statements before the function is replaced
    after: list[str] = field(default_factory=list)   # statements once the refresh is done
    checks: list[str] = field(default_factory=list)  # verify_062 spec names
    # (relation, key columns, value columns): counted before and after, to show
    # that values moved, which the totals-unchanged gates cannot.
    moved: tuple[str, list[str], list[str]] | None = None
    expect_moved: bool = True      # False: the listed values must NOT change
    asserts: list[tuple[str, str]] = field(default_factory=list)  # (label, SQL returning a count that must be 0)


OBJECTS: dict[str, ReaderSwitch] = {
    # Events (the counts CTE) group on the credited lineup; segment time and
    # event spans stay physical. Output names are kept, so the join to the
    # segment rows is unchanged; a credited row always matches one, because its
    # foul's own row sat in that lineup and starters combination.
    "lineup_totals_by_game": ReaderSwitch(
        signature="euroleague.refresh_lineup_totals_by_game(bigint[])",
        edits=[
            ("    SELECT atc.game_id, atc.team_id, atc.own_lineup, atc.type_lineup,\n"
             "      atc.opp_starters,\n      sum(atc.possession_flag)",
             "    SELECT atc.game_id, atc.team_id, atc.credit_own_lineup AS own_lineup, atc.type_lineup,\n"
             "      atc.credit_opp_starters AS opp_starters,\n      sum(atc.possession_flag)"),
            ("    GROUP BY atc.game_id, atc.team_id, atc.own_lineup,\n      atc.type_lineup, atc.opp_starters",
             "    GROUP BY atc.game_id, atc.team_id, atc.credit_own_lineup,\n"
             "      atc.type_lineup, atc.credit_opp_starters"),
        ],
        refresh="SELECT euroleague.refresh_lineup_totals_by_game(%s::bigint[])",
        after=["REFRESH MATERIALIZED VIEW euroleague.sub_lineups_stats_mv"],
        checks=["lineup_totals_by_game", "sub_lineups_stats_mv", "fetch_lineups_pergame_season"],
        moved=("euroleague.lineup_totals_by_game",
               ["game_id", "team_id", "lineup_key", "type_lineup", "opp_starters"],
               ["points", "possessions", "ts_possessions"]),
    ),
    # The per-action player fact carries the credited lineup and starters next
    # to the physical ones (the Israeli player_stats_actions_by_game gained
    # credit_lineup_hash the same way). Nothing reads them until the clutch and
    # direct lineup readers switch; every value it already had is unchanged.
    "player_stats_actions_by_game": ReaderSwitch(
        signature="euroleague.refresh_player_stats_actions_for_games(bigint[])",
        pre=["ALTER TABLE euroleague.player_stats_actions_by_game\n"
             "  ADD COLUMN credit_own_lineup text[],\n"
             "  ADD COLUMN credit_own_starters smallint,\n"
             "  ADD COLUMN credit_opp_starters smallint"],
        edits=[
            ("  own_starters,opp_starters,fg2_made,fg2_att,orebounds,oreb_opportunities,steals)",
             "  own_starters,opp_starters,fg2_made,fg2_att,orebounds,oreb_opportunities,steals,\n"
             "  credit_own_lineup,credit_own_starters,credit_opp_starters)"),
            ("  atc.orebounds,atc.oreb_opportunities,atc.steals\n FROM euroleague.action_team_context_actions atc",
             "  atc.orebounds,atc.oreb_opportunities,atc.steals,\n"
             "  atc.credit_own_lineup,atc.credit_own_starters,atc.credit_opp_starters\n"
             " FROM euroleague.action_team_context_actions atc"),
        ],
        refresh="SELECT euroleague.refresh_player_stats_actions_for_games(%s::bigint[])",
        after=["ALTER TABLE euroleague.player_stats_actions_by_game\n"
               "  ALTER COLUMN credit_own_lineup SET NOT NULL,\n"
               "  ALTER COLUMN credit_own_starters SET NOT NULL,\n"
               "  ALTER COLUMN credit_opp_starters SET NOT NULL"],
        checks=["player_stats_actions_by_game"],
        moved=("euroleague.player_stats_actions_by_game",
               ["game_id", "team_id", "source_event_order"],
               ["points", "own_lineup", "segment_id", "possession_flag"]),
        expect_moved=False,
        asserts=[("action rows whose credit columns differ from the event fact's", """
            SELECT count(*) FROM euroleague.player_stats_actions_by_game a
            JOIN euroleague.action_team_context_actions f
              USING (game_id, team_id, source_event_order)
            WHERE a.credit_own_lineup IS DISTINCT FROM f.credit_own_lineup
               OR a.credit_own_starters IS DISTINCT FROM f.credit_own_starters
               OR a.credit_opp_starters IS DISTINCT FROM f.credit_opp_starters"""),
                 ("action rows without a fact row", """
            SELECT count(*) FROM euroleague.player_stats_actions_by_game a
            WHERE NOT EXISTS (SELECT 1 FROM euroleague.action_team_context_actions f
              WHERE f.game_id = a.game_id AND f.team_id = a.team_id
                AND f.source_event_order = a.source_event_order)""")],
    ),
    # Player on/off and Four Factors: the counts CTE decides ON/OFF from the
    # event's lineup and groups on its starters -- both move to credit. Player
    # minutes (segments) stay physical. A credited event always lands on an
    # (is_on, starters) cell that has minutes: its foul's own segment gives it.
    "player_four_factors_by_game": ReaderSwitch(
        signature="euroleague.refresh_player_four_factors_by_game_for_games(bigint[])",
        edits=[
            ("      CASE WHEN rr.source_player_name = ANY(atc.own_lineup)\n"
             "           THEN 1 ELSE 0 END::smallint AS is_on_key,\n"
             "      atc.type_lineup,\n      atc.own_starters,\n      atc.opp_starters,\n",
             "      CASE WHEN rr.source_player_name = ANY(atc.credit_own_lineup)\n"
             "           THEN 1 ELSE 0 END::smallint AS is_on_key,\n"
             "      atc.type_lineup,\n      atc.credit_own_starters AS own_starters,\n"
             "      atc.credit_opp_starters AS opp_starters,\n"),
            ("   GROUP BY atc.game_id, atc.team_id, rr.player_id,\n"
             "            CASE WHEN rr.source_player_name = ANY(atc.own_lineup)\n"
             "                 THEN 1 ELSE 0 END,\n"
             "            atc.type_lineup, atc.own_starters, atc.opp_starters\n",
             "   GROUP BY atc.game_id, atc.team_id, rr.player_id,\n"
             "            CASE WHEN rr.source_player_name = ANY(atc.credit_own_lineup)\n"
             "                 THEN 1 ELSE 0 END,\n"
             "            atc.type_lineup, atc.credit_own_starters, atc.credit_opp_starters\n"),
        ],
        refresh="SELECT euroleague.refresh_player_four_factors_by_game_for_games(%s::bigint[])",
        after=["REFRESH MATERIALIZED VIEW euroleague.player_onoff_default_mv",
               "REFRESH MATERIALIZED VIEW euroleague.player_advanced_stats_mv",
               "REFRESH MATERIALIZED VIEW euroleague.player_traditional_stats_mv"],
        checks=["player_four_factors_by_game", "player_onoff_default_mv", "player_advanced_stats_mv",
                "player_traditional_stats_mv"],
        moved=("euroleague.player_four_factors_by_game",
               ["game_id", "team_id", "player_id", "is_on_key", "type_lineup", "own_starters", "opp_starters"],
               ["total_points", "total_poss", "ts_poss_count"]),
        asserts=[("credited counts with no minutes cell (would be dropped by the LEFT JOIN)", """
            SELECT count(*) FROM (
              SELECT DISTINCT atc.game_id, atc.team_id, rr.player_id,
                     (rr.source_player_name = ANY(atc.credit_own_lineup)) AS is_on,
                     atc.credit_own_starters, atc.credit_opp_starters
                FROM euroleague.action_team_context_actions atc
                JOIN euroleague.full_rosters rr ON rr.game_id = atc.game_id AND rr.team_id = atc.team_id
               WHERE atc.type_lineup IS NOT NULL AND atc.credit_own_lineup <> atc.own_lineup) c
            WHERE NOT EXISTS (
              SELECT 1 FROM euroleague.matchup_segments_actions ms
               WHERE ms.game_id = c.game_id AND ms.team_id = c.team_id
                 AND ms.own_starters = c.credit_own_starters AND ms.opp_starters = c.credit_opp_starters
                 AND (c.player_id IN (SELECT fr.player_id FROM euroleague.full_rosters fr
                                       WHERE fr.game_id = ms.game_id AND fr.team_id = ms.team_id
                                         AND fr.source_player_name = ANY(ms.own_lineup))) = c.is_on)""")],
    ),
    # Player stats inside a custom clutch window. Possessions on the floor
    # follow the credited lineup; games played and minutes follow the floor
    # (the Israeli Task 7 lost five players' games played by switching both).
    # The view over the per-action fact gains credit_own_lineup, appended last.
    "get_player_traditional_custom_clutch": ReaderSwitch(
        signature=("euroleague.get_player_traditional_custom_clutch(text, integer, date, date, text, text, "
                   "text, text, text, text, integer, text, integer, text, integer, boolean, integer, integer, integer)"),
        pre=["CREATE OR REPLACE VIEW euroleague.player_stats_action_context AS SELECT game_id, team_id, "
             "own_lineup, segment_id, source_event_order, event_elapsed_seconds, type_lineup, possession_flag, "
             "action_player_id, points, play_type, turnovers, fgm, fga, fg3_made, fg3_att, ft_attempts, "
             "ts_possessions, is_overtime, regulation_seconds_remaining, pre_margin, pre_abs_margin, pre_status, "
             "credit_own_lineup FROM euroleague.player_stats_actions_by_game"],
        edits=[
            ("    a.ft_attempts,a.ts_possessions\n  FROM euroleague.player_stats_action_context a\n"
             "  JOIN games_filtered gf USING(game_id,team_id)\n  WHERE NOT a.is_overtime",
             "    a.ft_attempts,a.ts_possessions,a.credit_own_lineup\n  FROM euroleague.player_stats_action_context a\n"
             "  JOIN games_filtered gf USING(game_id,team_id)\n  WHERE NOT a.is_overtime"),
            ("    a.ft_attempts,a.ts_possessions\n  FROM euroleague.player_stats_action_context a\n"
             "  JOIN games_filtered gf USING(game_id,team_id)\n  WHERE a.is_overtime",
             "    a.ft_attempts,a.ts_possessions,a.credit_own_lineup\n  FROM euroleague.player_stats_action_context a\n"
             "  JOIN games_filtered gf USING(game_id,team_id)\n  WHERE a.is_overtime"),
            ("  SELECT DISTINCT game_id,team_id,own_lineup FROM acts WHERE own_lineup IS NOT NULL\n),",
             "  SELECT game_id,team_id,own_lineup FROM acts WHERE own_lineup IS NOT NULL\n"
             "  UNION SELECT game_id,team_id,credit_own_lineup FROM acts WHERE credit_own_lineup IS NOT NULL\n),"),
            ("  SELECT DISTINCT game_id,team_id,own_lineup,source_event_order poss_end_id\n",
             "  SELECT DISTINCT game_id,team_id,own_lineup,credit_own_lineup,source_event_order poss_end_id\n"),
            ("  SELECT lm.player_id,pe.team_id,count(DISTINCT pe.game_id)::integer gp,\n"
             "    count(DISTINCT (pe.game_id,pe.team_id,pe.poss_end_id))::numeric poss_on_floor\n"
             "  FROM poss_end pe JOIN lineup_map lm USING(game_id,team_id,own_lineup)\n"
             "  GROUP BY lm.player_id,pe.team_id\n),",
             "  -- Migration 062: games played follow the floor lineup, possessions on\n"
             "  -- the floor the credited one.\n"
             "  SELECT player_id,team_id,coalesce(g.gp,0)::integer gp,coalesce(p.poss_on_floor,0)::numeric poss_on_floor\n"
             "  FROM (SELECT lm.player_id,pe.team_id,count(DISTINCT pe.game_id)::integer gp\n"
             "          FROM poss_end pe JOIN lineup_map lm USING(game_id,team_id,own_lineup)\n"
             "         GROUP BY lm.player_id,pe.team_id) g\n"
             "  FULL JOIN (SELECT lm.player_id,pe.team_id,\n"
             "                    count(DISTINCT (pe.game_id,pe.team_id,pe.poss_end_id))::numeric poss_on_floor\n"
             "               FROM poss_end pe JOIN lineup_map lm\n"
             "                 ON lm.game_id=pe.game_id AND lm.team_id=pe.team_id AND lm.own_lineup=pe.credit_own_lineup\n"
             "              GROUP BY lm.player_id,pe.team_id) p USING (player_id,team_id)\n),"),
        ],
        checks=["get_player_traditional_custom_clutch_clutch"],
    ),
    # The player traditional table reads tables 062 already switched
    # (lineup_totals_by_game, player/team Four Factors), so its possessions on
    # the floor follow credit. Games played must follow the floor: a player
    # subbed in for a free throw and out before the clock ran had 0 seconds and
    # his only possession was that free throw's -- now credited away. He still
    # played: a row is kept when he was on the floor (physically) at any
    # possession end, which is what poss_on_floor > 0 meant before 062.
    "player_traditional_by_game": ReaderSwitch(
        signature="euroleague.refresh_player_traditional_by_game_for_games(bigint[])",
        edits=[
            ("  ), player_usage AS MATERIALIZED (\n    SELECT pf.game_id",
             "  ), floor_poss AS MATERIALIZED (\n"
             "    -- Migration 062: on the floor at a possession end = played the\n"
             "    -- game, whichever lineup the possession is credited to.\n"
             "    SELECT DISTINCT a.game_id, a.team_id, u.player_id\n"
             "    FROM euroleague.player_stats_actions_by_game a\n"
             "    JOIN euroleague.lineup_totals_by_game l\n"
             "      ON l.game_id = a.game_id AND l.team_id = a.team_id\n"
             "     AND l.own_lineup = a.own_lineup AND l.type_lineup = 'offense'\n"
             "    CROSS JOIN LATERAL unnest(l.player_ids) AS u(player_id)\n"
             "    WHERE a.type_lineup = 'offense' AND a.possession_flag = 1\n"
             "      AND (p_game_ids IS NULL OR a.game_id = ANY(p_game_ids))\n"
             "  ), player_usage AS MATERIALIZED (\n    SELECT pf.game_id"),
            ("      coalesce(t.team_poss, 0)::numeric AS team_poss\n    FROM roster r",
             "      coalesce(t.team_poss, 0)::numeric AS team_poss,\n"
             "      (fp.player_id IS NOT NULL) AS on_floor_at_poss\n    FROM roster r"),
            ("    LEFT JOIN team_usage t\n      ON t.game_id = r.game_id AND t.team_id = r.team_id\n  )",
             "    LEFT JOIN team_usage t\n      ON t.game_id = r.game_id AND t.team_id = r.team_id\n"
             "    LEFT JOIN floor_poss fp\n"
             "      ON fp.game_id = r.game_id AND fp.team_id = r.team_id AND fp.player_id = r.player_id\n  )"),
            ("  WHERE minutes > 0 OR poss_on_floor > 0\n",
             "  WHERE minutes > 0 OR poss_on_floor > 0 OR on_floor_at_poss\n"),
        ],
        refresh="SELECT euroleague.refresh_player_traditional_by_game_for_games(%s::bigint[])",
        checks=["player_traditional_by_game"],
        moved=("euroleague.player_traditional_by_game", ["game_id", "team_id", "player_id"],
               ["poss_on_floor", "gp", "minutes"]),
    ),
    # Team Four Factors per (game, team, starters on the floor): events, so the
    # starters split follows credit. Team totals are unchanged; only a
    # starters-filtered team reading moves. get_team_minutes_pergame (segment
    # time) and get_team_traditional_pergame (no starters filter, switched
    # sources) need no edit.
    "team_four_factors_by_game": ReaderSwitch(
        signature="euroleague.refresh_team_four_factors_by_game_for_games(bigint[])",
        edits=[
            ("    s.season::smallint,\n    atc.own_starters,\n    atc.opp_starters,\n",
             "    s.season::smallint,\n    atc.credit_own_starters,\n    atc.credit_opp_starters,\n"),
            (" GROUP BY atc.game_id, atc.team_id, s.season,\n          atc.own_starters, atc.opp_starters;",
             " GROUP BY atc.game_id, atc.team_id, s.season,\n          atc.credit_own_starters, atc.credit_opp_starters;"),
        ],
        refresh="SELECT euroleague.refresh_team_four_factors_by_game_for_games(%s::bigint[])",
        after=["REFRESH MATERIALIZED VIEW euroleague.team_four_factors_mv",
               "REFRESH MATERIALIZED VIEW euroleague.team_ppp_ratings_mv",
               "REFRESH MATERIALIZED VIEW euroleague.player_traditional_stats_mv"],
        checks=["team_four_factors_by_game", "team_four_factors_mv", "team_ppp_ratings_mv",
                "player_traditional_stats_mv", "get_team_traditional_pergame_season",
                "get_team_minutes_pergame_season", "filtered_team_game_facts_season",
                "select_team_game_facts_season", "filtered_team_game_facts_starters",
                "get_team_minutes_pergame_starters"],
        moved=("euroleague.team_four_factors_by_game", ["game_id", "team_id", "own_starters", "opp_starters"],
               ["off_pts", "off_poss", "def_pts"]),
    ),
    # Gameflow events carry each event's credited five (the web's stint
    # ratings read them), closing D39; bars, +/- and score come from
    # ribbon_segments_v / ribbon_margin_v, which stay physical.
    "ribbon_events_v": ReaderSwitch(
        signature="view:euroleague.ribbon_events_v",
        edits=[("l.own_lineup = a.own_lineup", "l.own_lineup = a.credit_own_lineup")],
        asserts=[
            ("event rows without a credited five", """
                SELECT count(*) FROM euroleague.ribbon_events_v WHERE player_ids IS NULL"""),
            ("event perspectives listed twice", """
                SELECT count(*) FROM (SELECT 1 FROM euroleague.ribbon_events_v
                  GROUP BY game_id, team_id, source_event_order HAVING count(*) > 1) d"""),
            ("events whose five is not their credited lineup's", """
                SELECT count(*) FROM euroleague.ribbon_events_v r
                JOIN euroleague.action_team_context_actions a USING (game_id, team_id, source_event_order)
                WHERE NOT EXISTS (SELECT 1 FROM euroleague.lineup_totals_by_game l
                  WHERE l.game_id = a.game_id AND l.team_id = a.team_id
                    AND l.own_lineup = a.credit_own_lineup AND l.player_ids = r.player_ids)"""),
        ],
        moved=("euroleague.ribbon_events_v", ["game_id", "team_id", "source_event_order"],
               ["points", "player_ids"]),  # first value is summed: must be numeric
    ),
    # Direct lineups read the per-action fact for both events and event-span
    # time. Starters filters leave the shared acts CTE: events filter on the
    # credited starters, time on the physical ones. Events group on the
    # credited lineup; the lineup list takes credited lineups too; facts is a
    # FULL JOIN, as in clutch_team_game_facts, so no event is dropped.
    "fetch_lineups_direct": ReaderSwitch(
        signature=("euroleague.fetch_lineups_direct(text, integer, date, date, text, text, text, text, text, "
                   "text, integer, text, integer, text, integer, boolean, integer, integer, integer, integer, "
                   "integer, integer, integer, integer, text, text, integer)"),
        edits=[
            ("  a.own_starters\n FROM euroleague.player_stats_actions_by_game a",
             "  a.own_starters,a.opp_starters,a.credit_own_lineup,a.credit_own_starters,a.credit_opp_starters\n"
             " FROM euroleague.player_stats_actions_by_game a"),
            ("  AND (p_num_starters_off_min IS NULL OR a.own_starters>=p_num_starters_off_min)\n"
             "  AND (p_num_starters_off_max IS NULL OR a.own_starters<=p_num_starters_off_max)\n"
             "  AND (p_num_starters_def_min IS NULL OR a.opp_starters>=p_num_starters_def_min)\n"
             "  AND (p_num_starters_def_max IS NULL OR a.opp_starters<=p_num_starters_def_max)\n"
             "),lineup_identity",
             "),lineup_identity"),
            (" FROM (SELECT DISTINCT game_id,team_id,own_lineup FROM acts) a",
             " FROM (SELECT game_id,team_id,own_lineup FROM acts\n"
             "       UNION SELECT game_id,team_id,credit_own_lineup FROM acts) a"),
            (" SELECT game_id,team_id,own_lineup,type_lineup,sum(possession_flag)::bigint possessions",
             " SELECT game_id,team_id,credit_own_lineup AS own_lineup,type_lineup,sum(possession_flag)::bigint possessions"),
            ("max(own_starters)::smallint own_starters\n"
             " FROM acts WHERE type_lineup IS NOT NULL GROUP BY game_id,team_id,own_lineup,type_lineup",
             "max(credit_own_starters)::smallint own_starters\n"
             " FROM acts WHERE type_lineup IS NOT NULL\n"
             "  AND (p_num_starters_off_min IS NULL OR credit_own_starters>=p_num_starters_off_min)\n"
             "  AND (p_num_starters_off_max IS NULL OR credit_own_starters<=p_num_starters_off_max)\n"
             "  AND (p_num_starters_def_min IS NULL OR credit_opp_starters>=p_num_starters_def_min)\n"
             "  AND (p_num_starters_def_max IS NULL OR credit_opp_starters<=p_num_starters_def_max)\n"
             " GROUP BY game_id,team_id,credit_own_lineup,type_lineup"),
            (" FROM acts GROUP BY game_id,team_id,own_lineup,segment_id",
             " FROM acts\n"
             " WHERE (p_num_starters_off_min IS NULL OR own_starters>=p_num_starters_off_min)\n"
             "  AND (p_num_starters_off_max IS NULL OR own_starters<=p_num_starters_off_max)\n"
             "  AND (p_num_starters_def_min IS NULL OR opp_starters>=p_num_starters_def_min)\n"
             "  AND (p_num_starters_def_max IS NULL OR opp_starters<=p_num_starters_def_max)\n"
             " GROUP BY game_id,team_id,own_lineup,segment_id"),
            (" SELECT d.game_id,d.team_id,d.own_lineup,s.type_lineup,",
             " SELECT game_id,team_id,own_lineup,type_lineup,"),
            ("CASE WHEN s.type_lineup='offense' THEN d.seconds END seconds\n"
             " FROM duration d CROSS JOIN(VALUES('offense'::text),('defense'::text))s(type_lineup)\n"
             " LEFT JOIN event_counts e USING(game_id,team_id,own_lineup,type_lineup)\n"
             " WHERE d.seconds>0 OR e.game_id IS NOT NULL",
             "CASE WHEN type_lineup='offense' THEN coalesce(d.seconds,0) END seconds\n"
             " FROM (SELECT x.*,s.type_lineup FROM duration x\n"
             "       CROSS JOIN(VALUES('offense'::text),('defense'::text))s(type_lineup)) d\n"
             " FULL JOIN event_counts e USING(game_id,team_id,own_lineup,type_lineup)\n"
             " WHERE d.seconds>0 OR e.game_id IS NOT NULL"),
        ],
        checks=["fetch_lineups_direct_season"],
        asserts=[("units where direct and per-game lineup event counts differ (two teams, E 2025-26)",
                  None)],  # filled in by _direct_vs_pergame() below
    ),
    # Not a switch: default_clutch_lineup_totals_by_game was stale against its
    # own refresh function before 062 (2026-11-13: rebuilding every game with
    # the unchanged function moved 1,480 floor-time rows and 22 team totals).
    # Rebuilt first, so the clutch switch is measured on its own.
    "default_clutch_rebuild": ReaderSwitch(
        signature="euroleague.refresh_default_clutch_for_games(bigint[])",
        edits=[],
        refresh="SELECT euroleague.refresh_default_clutch_for_games(%s::bigint[])",
        asserts=[("default-clutch rows that differ from clutch_team_game_facts", """
            WITH g AS (SELECT array_agg(DISTINCT game_id) ids FROM euroleague.default_clutch_lineup_totals_by_game),
            f AS (SELECT game_id, team_id, own_lineup, own_starters, opp_starters, type_lineup,
                         possessions, points, ts_possessions, seconds
                    FROM g, euroleague.clutch_team_game_facts(g.ids, 5, 'all', 300, false)),
            t AS (SELECT game_id, team_id, own_lineup, own_starters, opp_starters, type_lineup,
                         possessions, points, ts_possessions, seconds
                    FROM euroleague.default_clutch_lineup_totals_by_game)
            SELECT count(*) FROM ((SELECT * FROM f EXCEPT SELECT * FROM t)
                                  UNION ALL (SELECT * FROM t EXCEPT SELECT * FROM f)) d""")],
    ),
    # Clutch lineups: event_counts groups on the credited lineup and starters;
    # clutch time (clutch_segment_durations joined to segments) stays physical.
    # A free throw inside the window whose foul's lineup has no clutch time is
    # not matched by the LEFT JOIN from duration -- the Israeli accepted
    # deviation (Review Focus 5); the clutch G2 check measures it.
    "clutch_team_game_facts": ReaderSwitch(
        signature="euroleague.clutch_team_game_facts(bigint[], integer, text, integer, boolean)",
        edits=[
            (" SELECT game_id,team_id,own_lineup,own_starters,opp_starters,type_lineup,\n  sum(possession_flag)",
             " SELECT game_id,team_id,credit_own_lineup AS own_lineup,credit_own_starters AS own_starters,\n"
             "  credit_opp_starters AS opp_starters,type_lineup,\n  sum(possession_flag)"),
            (" GROUP BY game_id,team_id,own_lineup,own_starters,opp_starters,type_lineup\n), duration AS (",
             " GROUP BY game_id,team_id,credit_own_lineup,credit_own_starters,credit_opp_starters,type_lineup\n"
             "), duration AS ("),
            # Keep events whose credited lineup has no clutch time (a free throw
            # inside the window whose foul came just before it): FULL JOIN, so
            # clutch team totals are exactly the events in the window. The
            # Israeli readers accepted this loss; here it is closed.
            ("SELECT i.game_id,i.team_id,i.own_lineup,i.own_starters,i.opp_starters,s.type_lineup,",
             "SELECT game_id,team_id,own_lineup,own_starters,opp_starters,type_lineup,"),
            (" CASE WHEN s.type_lineup = 'offense' THEN i.seconds END\n"
             "FROM duration i CROSS JOIN (VALUES ('offense'::text),('defense'::text)) s(type_lineup)\n"
             "LEFT JOIN event_counts e USING(game_id,team_id,own_lineup,own_starters,opp_starters,type_lineup)\n"
             "WHERE i.seconds > 0 OR e.game_id IS NOT NULL;",
             # An event-only row's lineup had no clutch time: 0, not NULL (the
             # table requires seconds on every offense row).
             " CASE WHEN type_lineup = 'offense' THEN coalesce(d.seconds, 0) END\n"
             "FROM (SELECT i.*, s.type_lineup\n"
             "        FROM duration i CROSS JOIN (VALUES ('offense'::text),('defense'::text)) s(type_lineup)) d\n"
             "FULL JOIN event_counts e USING(game_id,team_id,own_lineup,own_starters,opp_starters,type_lineup)\n"
             "WHERE d.seconds > 0 OR e.game_id IS NOT NULL;"),
        ],
        refresh="SELECT euroleague.refresh_default_clutch_for_games(%s::bigint[])",
        checks=["default_clutch_lineup_totals_by_game", "clutch_team_game_facts_clutch",
                "fetch_lineups_dynamic_clutch"],
        moved=("euroleague.default_clutch_lineup_totals_by_game",
               ["game_id", "team_id", "own_lineup", "own_starters", "opp_starters", "type_lineup"],
               ["points", "possessions", "ts_possessions"]),
    ),
}


def _direct_vs_pergame() -> str:
    """Both lineup readers count the same events per unit once both credit the
    same way. Units with no event on either side are listed differently (floor
    time only), so both sides keep only units with an event; a missing side
    counts as zero."""
    events = ["off_poss", "off_pts", "off_ts_poss", "off_fga", "off_fta",
              "def_poss", "def_pts", "def_ts_poss", "def_fga", "def_fta"]
    cols = ", ".join(f"coalesce({c}, 0) AS {c}" for c in events)
    has_event = " + ".join(f"coalesce({c}, 0)" for c in events) + " > 0"
    side = lambda src: f"SELECT unit_key, {cols} FROM ({src}) q WHERE {has_event}"
    d = side(verify.SPEC_BY_NAME["fetch_lineups_direct_season"].source)
    p = side(verify.SPEC_BY_NAME["fetch_lineups_pergame_season"].source)
    return (f"WITH d AS ({d}), p AS ({p}) SELECT count(*) FROM "
            "((SELECT * FROM d EXCEPT SELECT * FROM p) UNION ALL (SELECT * FROM p EXCEPT SELECT * FROM d)) x")


OBJECTS["fetch_lineups_direct"] = ReaderSwitch(
    **{**OBJECTS["fetch_lineups_direct"].__dict__,
       "asserts": [(OBJECTS["fetch_lineups_direct"].asserts[0][0], _direct_vs_pergame())]})


def apply_edits(definition: str, edits: list[tuple[str, str]]) -> str:
    out = definition.replace("\r\n", "\n")
    for old, new in edits:
        hits = out.count(old)
        if hits != 1:
            raise RuntimeError(f"expected one match, found {hits}: {old[:70]!r}")
        out = out.replace(old, new)
    return out.rstrip().rstrip(";") + ";\n"


def header(name: str, what: str) -> str:
    return (f"-- {what} of {name}, EuroLeague migration 062 (EUROLEAGUE SHADOW SCHEMA),\n"
            "-- written by scripts/apply_062_readers.py from the live definition.\n\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--object", required=True, choices=sorted(OBJECTS))
    a = ap.parse_args()
    sw = OBJECTS[a.object]
    apply = os.environ.get("CONFIRM_EL_062_READER") == a.object
    conn = connect_from_env_file(ENV_PATH, direct_port=5432)
    cur = conn.cursor()
    try:
        if inspect_target(conn)["euroleague_schema"] != "euroleague":
            raise RuntimeError("EuroLeague schema unavailable")
        print(f"{a.object}: " + ("EXECUTE" if apply else f"DRY RUN (CONFIRM_EL_062_READER={a.object} to commit)"), flush=True)
        cur.execute("BEGIN")
        cur.execute("SET LOCAL lock_timeout = '10s'")
        cur.execute("SET LOCAL statement_timeout = '600s'")
        if sw.signature.startswith("view:"):
            view = sw.signature[len("view:"):]
            cur.execute("SELECT pg_get_viewdef(%s::regclass, true)", (view,))
            live = f"CREATE OR REPLACE VIEW {view} AS {cur.fetchone()[0]}"
        else:
            cur.execute("SELECT pg_get_functiondef(%s::regprocedure)", (sw.signature,))
            live = cur.fetchone()[0]
        new = apply_edits(live, sw.edits)
        (REPO / "sql" / f"062_{a.object}_before.sql").write_text(header(a.object, "Definition before") + live.rstrip() + ";\n", encoding="utf-8")
        (REPO / "sql" / f"062_{a.object}.sql").write_text(header(a.object, "Credit-attribution definition") + new, encoding="utf-8")
        if sw.moved:
            rel, keys, vals = sw.moved
            cur.execute(f"CREATE TEMP TABLE moved_before ON COMMIT DROP AS SELECT {', '.join(keys + vals)} FROM {rel}")
        for s in sw.pre:
            cur.execute(s)
            print(f"  {s.splitlines()[0][:90]}", flush=True)
        cur.execute(new)
        print(f"  {sw.signature} replaced ({len(sw.edits)} edits)", flush=True)
        if sw.refresh:
            cur.execute("SELECT DISTINCT game_id FROM euroleague.action_team_context_actions ORDER BY 1")
            games = [r[0] for r in cur.fetchall()]
            t0 = time.perf_counter()
            slowest = 0.0
            for i in range(0, len(games), BATCH):
                b0 = time.perf_counter()
                cur.execute(sw.refresh, (games[i:i + BATCH],))
                slowest = max(slowest, time.perf_counter() - b0)
            print(f"  refreshed {len(games)} games in {time.perf_counter() - t0:.1f}s "
                  f"(slowest batch of {BATCH}: {slowest:.1f}s)", flush=True)
        for s in sw.after:
            t0 = time.perf_counter()
            cur.execute(s)
            print(f"  {s} ({time.perf_counter() - t0:.1f}s)", flush=True)
        if sw.moved:
            rel, keys, vals = sw.moved
            on = " AND ".join(f"b.{k} = a.{k}" for k in keys)  # key columns are never NULL
            differs = " OR ".join(f"b.{v} IS DISTINCT FROM a.{v}" for v in vals)
            cur.execute(f"""SELECT count(*) FILTER (WHERE {differs}),
                                   coalesce(sum(abs(coalesce(a.{vals[0]}, 0) - coalesce(b.{vals[0]}, 0))), 0) / 2,
                                   count(*)
                              FROM moved_before b FULL JOIN {rel} a ON {on}""")
            rows_changed, half_abs, total = cur.fetchone()
            print(f"  moved: {rows_changed} of {total} rows changed; {vals[0]} moved between rows: {half_abs}", flush=True)
            if sw.expect_moved and not rows_changed:
                raise RuntimeError("nothing moved: the switch did not take effect")
            if not sw.expect_moved and rows_changed:
                raise RuntimeError("existing values changed, which this step must not do")
        for label, sql in sw.asserts:
            cur.execute(sql)
            n = cur.fetchone()[0]
            print(f"  {'ok  ' if n == 0 else 'FAIL'} {label}: {n}", flush=True)
            if n:
                raise RuntimeError(f"{label}: {n}")
        failed = verify.check(conn, sw.checks)
        if failed:
            raise RuntimeError(f"{failed} gate(s) failed")
        if apply:
            cur.execute("COMMIT")
            print(f"{a.object}: applied")
        else:
            cur.execute("ROLLBACK")
            print(f"{a.object}: DRY RUN passed and rolled back")
        return 0
    except Exception:
        try:
            cur.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
