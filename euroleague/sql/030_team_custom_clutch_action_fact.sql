-- EuroLeague migration 030: reuse the narrow action fact for custom Team and
-- Lineup clutch reads.  This keeps the Israeli-style action grain while
-- avoiding the generic PL/pgSQL selector for custom requests.

BEGIN;
SET LOCAL search_path TO euroleague, public;

ALTER TABLE euroleague.player_stats_actions_by_game
  ADD COLUMN IF NOT EXISTS own_starters SMALLINT,
  ADD COLUMN IF NOT EXISTS opp_starters SMALLINT,
  ADD COLUMN IF NOT EXISTS fg2_made INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS fg2_att INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS orebounds INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS oreb_opportunities INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS steals INTEGER NOT NULL DEFAULT 0;

CREATE OR REPLACE FUNCTION euroleague.refresh_player_stats_actions_for_games(game_ids BIGINT[])
RETURNS BIGINT LANGUAGE plpgsql AS $function$
DECLARE target_ids BIGINT[]; inserted_count BIGINT:=0;
BEGIN
 IF game_ids IS NULL OR array_length(game_ids,1) IS NULL THEN
  SELECT array_agg(game_id ORDER BY game_id) INTO target_ids FROM euroleague.schedule;
  DELETE FROM euroleague.player_stats_actions_by_game;
 ELSE
  SELECT array_agg(DISTINCT x ORDER BY x) INTO target_ids FROM unnest(game_ids) x;
  DELETE FROM euroleague.player_stats_actions_by_game WHERE game_id=ANY(target_ids);
 END IF;
 IF target_ids IS NULL THEN RETURN 0; END IF;
 INSERT INTO euroleague.player_stats_actions_by_game(
  game_id,team_id,source_event_order,own_lineup,segment_id,event_elapsed_seconds,
  type_lineup,possession_flag,action_player_id,points,play_type,turnovers,fgm,fga,
  fg3_made,fg3_att,ft_attempts,ts_possessions,is_overtime,
  regulation_seconds_remaining,pre_margin,pre_abs_margin,pre_status,
  own_starters,opp_starters,fg2_made,fg2_att,orebounds,oreb_opportunities,steals)
 SELECT atc.game_id,atc.team_id,atc.source_event_order,atc.own_lineup,atc.segment_id,
  atc.event_elapsed_seconds,atc.type_lineup,atc.possession_flag,atc.action_player_id,
  atc.points,atc.play_type,atc.turnovers,atc.fgm,atc.fga,atc.fg3_made,atc.fg3_att,
  atc.ft_attempts,atc.ts_possessions,(atc.period>4),
  greatest(2400-atc.event_elapsed_seconds,0)::numeric,
  (atc.own_team_score-CASE WHEN atc.event_team_id=atc.team_id THEN atc.points ELSE 0 END)
    -(atc.opp_team_score-CASE WHEN atc.event_team_id=atc.opponent_team_id THEN atc.points ELSE 0 END),
  abs((atc.own_team_score-CASE WHEN atc.event_team_id=atc.team_id THEN atc.points ELSE 0 END)
    -(atc.opp_team_score-CASE WHEN atc.event_team_id=atc.opponent_team_id THEN atc.points ELSE 0 END)),
  sign((atc.own_team_score-CASE WHEN atc.event_team_id=atc.team_id THEN atc.points ELSE 0 END)
    -(atc.opp_team_score-CASE WHEN atc.event_team_id=atc.opponent_team_id THEN atc.points ELSE 0 END))::smallint,
  atc.own_starters,atc.opp_starters,atc.fg2_made,atc.fg2_att,
  atc.orebounds,atc.oreb_opportunities,atc.steals
 FROM euroleague.action_team_context_actions atc
 WHERE atc.game_id=ANY(target_ids);
 GET DIAGNOSTICS inserted_count=ROW_COUNT; RETURN inserted_count;
END;
$function$;

-- Populate the additive columns once; future game publication refreshes only
-- the changed game IDs through the same function.
SELECT euroleague.refresh_player_stats_actions_for_games(NULL::bigint[]);

-- Custom Team/Lineup facts use the already-materialized action grain. The
-- standard preset remains on the existing additive clutch cache.
CREATE OR REPLACE FUNCTION euroleague.clutch_team_game_facts(
    p_game_ids BIGINT[], p_max_margin INTEGER, p_margin_status TEXT,
    p_max_time_remaining INTEGER, p_ot_margin_filter BOOLEAN)
RETURNS TABLE (
 game_id BIGINT, team_id BIGINT, own_lineup TEXT[], own_starters SMALLINT,
 opp_starters SMALLINT, type_lineup TEXT, possessions BIGINT, points BIGINT,
 fg2_made BIGINT, fg2_att BIGINT, fg3_made BIGINT, fg3_att BIGINT,
 ts_possessions BIGINT, fgm BIGINT, fga BIGINT, ft_attempts BIGINT,
 orebounds BIGINT, oreb_opportunities BIGINT, turnovers BIGINT, steals BIGINT,
 seconds NUMERIC)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
SET plan_cache_mode = force_custom_plan
AS $function$
WITH eligible AS (
 SELECT a.*
 FROM euroleague.player_stats_actions_by_game a
 WHERE a.game_id = ANY(coalesce(p_game_ids, ARRAY[]::bigint[]))
   AND (a.is_overtime OR p_max_time_remaining IS NULL
        OR a.regulation_seconds_remaining <= p_max_time_remaining)
   AND (a.is_overtime AND NOT coalesce(p_ot_margin_filter,false)
        OR (p_max_margin IS NULL OR a.pre_abs_margin <= p_max_margin)
        AND (coalesce(nullif(btrim(p_margin_status),''),'all')='all'
          OR (p_margin_status='leading' AND a.pre_status>0)
          OR (p_margin_status='trailing' AND a.pre_status<0)
          OR (p_margin_status='tied' AND a.pre_status=0)))
), event_counts AS (
 SELECT game_id,team_id,own_lineup,own_starters,opp_starters,type_lineup,
  sum(possession_flag)::bigint possessions,sum(points)::bigint points,
  sum(fg2_made)::bigint fg2_made,sum(fg2_att)::bigint fg2_att,
  sum(fg3_made)::bigint fg3_made,sum(fg3_att)::bigint fg3_att,
  sum(ts_possessions)::bigint ts_possessions,sum(fgm)::bigint fgm,sum(fga)::bigint fga,
  sum(ft_attempts)::bigint ft_attempts,sum(orebounds)::bigint orebounds,
  sum(oreb_opportunities)::bigint oreb_opportunities,sum(turnovers)::bigint turnovers,
  sum(steals)::bigint steals
 FROM eligible WHERE type_lineup IS NOT NULL
 GROUP BY game_id,team_id,own_lineup,own_starters,opp_starters,type_lineup
), duration AS (
 SELECT d.game_id,d.team_id,ms.own_lineup,ms.own_starters,ms.opp_starters,
  sum(d.seconds)::numeric seconds
 FROM euroleague.clutch_segment_durations(
   p_game_ids,p_max_margin,p_margin_status,p_max_time_remaining,p_ot_margin_filter) d
 JOIN euroleague.matchup_segments_actions ms USING(game_id,team_id,segment_id)
 GROUP BY d.game_id,d.team_id,ms.own_lineup,ms.own_starters,ms.opp_starters
), identities AS (
 SELECT DISTINCT game_id,team_id,own_lineup,own_starters,opp_starters FROM duration
)
SELECT i.game_id,i.team_id,i.own_lineup,i.own_starters,i.opp_starters,s.type_lineup,
 coalesce(e.possessions,0),coalesce(e.points,0),coalesce(e.fg2_made,0),coalesce(e.fg2_att,0),
 coalesce(e.fg3_made,0),coalesce(e.fg3_att,0),coalesce(e.ts_possessions,0),coalesce(e.fgm,0),
 coalesce(e.fga,0),coalesce(e.ft_attempts,0),coalesce(e.orebounds,0),coalesce(e.oreb_opportunities,0),
 coalesce(e.turnovers,0),coalesce(e.steals,0),i.seconds
FROM duration i CROSS JOIN (VALUES ('offense'::text),('defense'::text)) s(type_lineup)
LEFT JOIN event_counts e USING(game_id,team_id,own_lineup,own_starters,opp_starters,type_lineup)
WHERE i.seconds > 0 OR e.game_id IS NOT NULL;
$function$;

REVOKE ALL ON FUNCTION euroleague.clutch_team_game_facts(bigint[],integer,text,integer,boolean) FROM PUBLIC;
COMMIT;
