-- EuroLeague shadow schema -- migration 040: restore the offense-only seconds
-- guard in clutch_team_game_facts.
--
-- Migration 019 built this function with the guard:
--
--     CASE WHEN side.type_lineup = 'offense' THEN st.seconds END
--
-- because both perspectives of a lineup fan out from ONE segment duration.
-- The same five players are on the floor for their offense row and their
-- defense row, so crediting seconds to both double-counts floor time. The
-- offense perspective owns it; the defense row is deliberately NULL. That is
-- the Israeli floor-time rule, and default_clutch_lineup_totals_by_game
-- encodes it as a validated CHECK:
--
--     CHECK ((type_lineup = 'offense') = (seconds IS NOT NULL))
--
-- Migration 030 re-implemented the function over the narrow action fact and
-- dropped the CASE, projecting i.seconds onto both perspectives. Every
-- refresh_default_clutch_for_games() call for a game with any clutch segment
-- has failed on that CHECK since. Because refresh_derived_for_games() runs all
-- eight refresh functions in ONE transaction, that failure also rolled back
-- the actions-consumer, four-factors, lineup and player facts for the same
-- game -- 53 of the 106 games in load run 17, plus gamecode 249.
--
-- This migration is CREATE OR REPLACE with an unchanged signature and return
-- type, so EXECUTE grants are preserved and no security re-apply is required.
-- The body below is byte-identical to migration 030 except for the single
-- projected expression on the seconds column.

BEGIN;
SET LOCAL search_path TO euroleague, public;

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
 coalesce(e.turnovers,0),coalesce(e.steals,0),
 CASE WHEN s.type_lineup = 'offense' THEN i.seconds END
FROM duration i CROSS JOIN (VALUES ('offense'::text),('defense'::text)) s(type_lineup)
LEFT JOIN event_counts e USING(game_id,team_id,own_lineup,own_starters,opp_starters,type_lineup)
WHERE i.seconds > 0 OR e.game_id IS NOT NULL;
$function$;

REVOKE ALL ON FUNCTION euroleague.clutch_team_game_facts(bigint[],integer,text,integer,boolean) FROM PUBLIC;
COMMIT;
