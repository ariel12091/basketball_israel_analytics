-- Definition before of clutch_team_game_facts, EuroLeague migration 062 (EUROLEAGUE SHADOW SCHEMA),
-- written by scripts/apply_062_readers.py from the live definition.

CREATE OR REPLACE FUNCTION euroleague.clutch_team_game_facts(p_game_ids bigint[], p_max_margin integer, p_margin_status text, p_max_time_remaining integer, p_ot_margin_filter boolean)
 RETURNS TABLE(game_id bigint, team_id bigint, own_lineup text[], own_starters smallint, opp_starters smallint, type_lineup text, possessions bigint, points bigint, fg2_made bigint, fg2_att bigint, fg3_made bigint, fg3_att bigint, ts_possessions bigint, fgm bigint, fga bigint, ft_attempts bigint, orebounds bigint, oreb_opportunities bigint, turnovers bigint, steals bigint, seconds numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'euroleague', 'public'
 SET plan_cache_mode TO 'force_custom_plan'
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
 coalesce(e.turnovers,0),coalesce(e.steals,0),
 CASE WHEN s.type_lineup = 'offense' THEN i.seconds END
FROM duration i CROSS JOIN (VALUES ('offense'::text),('defense'::text)) s(type_lineup)
LEFT JOIN event_counts e USING(game_id,team_id,own_lineup,own_starters,opp_starters,type_lineup)
WHERE i.seconds > 0 OR e.game_id IS NOT NULL;
$function$;
