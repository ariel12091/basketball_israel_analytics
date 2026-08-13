-- EuroLeague migration 026: direct adaptation of the Israeli custom-clutch
-- Player Stats function. One filtered action set feeds every downstream CTE.

BEGIN;
SET LOCAL search_path TO euroleague, public;

-- Migration 027 repoints this stable interface to a narrow physical fact.
CREATE OR REPLACE VIEW euroleague.player_stats_action_context AS
SELECT atc.game_id,atc.team_id,atc.own_lineup,atc.segment_id,
  atc.source_event_order,atc.event_elapsed_seconds,atc.type_lineup,
  atc.possession_flag,atc.action_player_id,atc.points,atc.play_type,
  atc.turnovers,atc.fgm,atc.fga,atc.fg3_made,atc.fg3_att,
  atc.ft_attempts,atc.ts_possessions,
  (atc.period>4) AS is_overtime,
  greatest(2400-atc.event_elapsed_seconds,0)::numeric AS regulation_seconds_remaining,
  (atc.own_team_score-CASE WHEN atc.event_team_id=atc.team_id THEN atc.points ELSE 0 END)
    -(atc.opp_team_score-CASE WHEN atc.event_team_id=atc.opponent_team_id THEN atc.points ELSE 0 END) AS pre_margin,
  abs((atc.own_team_score-CASE WHEN atc.event_team_id=atc.team_id THEN atc.points ELSE 0 END)
    -(atc.opp_team_score-CASE WHEN atc.event_team_id=atc.opponent_team_id THEN atc.points ELSE 0 END)) AS pre_abs_margin,
  sign((atc.own_team_score-CASE WHEN atc.event_team_id=atc.team_id THEN atc.points ELSE 0 END)
    -(atc.opp_team_score-CASE WHEN atc.event_team_id=atc.opponent_team_id THEN atc.points ELSE 0 END))::smallint AS pre_status
FROM euroleague.action_team_context_actions atc;

CREATE OR REPLACE FUNCTION euroleague.get_player_traditional_custom_clutch(
    p_competition TEXT, p_game_year INTEGER,
    p_start_date DATE DEFAULT NULL, p_end_date DATE DEFAULT NULL,
    p_team_ids_csv TEXT DEFAULT NULL, p_phase_csv TEXT DEFAULT NULL,
    p_opp_ids_csv TEXT DEFAULT NULL, p_home_away TEXT DEFAULT 'all',
    p_outcome TEXT DEFAULT 'all', p_opp_rank_side TEXT DEFAULT NULL,
    p_opp_rank_n INTEGER DEFAULT NULL, p_opp_rank_metric TEXT DEFAULT NULL,
    p_max_margin INTEGER DEFAULT NULL, p_margin_status TEXT DEFAULT NULL,
    p_max_time_remaining INTEGER DEFAULT NULL, p_ot_margin_filter BOOLEAN DEFAULT FALSE,
    p_min_gn INTEGER DEFAULT NULL, p_max_gn INTEGER DEFAULT NULL,
    p_last_n_games INTEGER DEFAULT NULL
)
RETURNS TABLE (
    team_id BIGINT, player_id BIGINT, team_name TEXT, "Player" TEXT,
    gp INTEGER, poss_on_floor NUMERIC, minutes NUMERIC,
    pts NUMERIC, reb NUMERIC, oreb NUMERIC, dreb NUMERIC,
    ast NUMERIC, stl NUMERIC, blk NUMERIC, dfl NUMERIC, tov NUMERIC,
    fgm NUMERIC, fga NUMERIC, fg_pct NUMERIC,
    "3pm" NUMERIC, "3pa" NUMERIC, tp_pct NUMERIC,
    ftm NUMERIC, fta NUMERIC, ft_pct NUMERIC, efg NUMERIC,
    ts NUMERIC, usg_pct NUMERIC
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
SET plan_cache_mode = force_custom_plan
AS $function$
WITH normalized AS (
  SELECT coalesce(nullif(btrim(p_competition),''),'E') competition,
    CASE WHEN nullif(btrim(p_team_ids_csv),'') IS NULL THEN NULL::bigint[] ELSE string_to_array(regexp_replace(p_team_ids_csv,'\s+','','g'),',')::bigint[] END team_ids,
    CASE WHEN nullif(btrim(p_phase_csv),'') IS NULL THEN NULL::text[] ELSE string_to_array(p_phase_csv,',') END phases,
    CASE WHEN nullif(btrim(p_opp_ids_csv),'') IS NULL THEN NULL::bigint[] ELSE string_to_array(regexp_replace(p_opp_ids_csv,'\s+','','g'),',')::bigint[] END opp_ids,
    coalesce(nullif(btrim(p_home_away),''),'all') home_away,
    coalesce(nullif(btrim(p_outcome),''),'all') outcome,
    nullif(btrim(p_opp_rank_side),'') rank_side,
    coalesce(nullif(btrim(p_opp_rank_metric),''),'net') rank_metric
),
schedule_ranked AS (
  SELECT fs.*,row_number() OVER(PARTITION BY fs.team_id ORDER BY fs.game_date DESC,fs.game_id DESC) team_game_rank
  FROM euroleague.final_schedule_mv fs CROSS JOIN normalized n
  WHERE fs.competition=n.competition AND fs.game_year=p_game_year
),
opponent_ranks AS (
  SELECT r.team_id,r.off_rank,r.def_rank,r.net_rank,count(*) OVER() team_count
  FROM euroleague.team_ppp_ratings_mv r CROSS JOIN normalized n
  WHERE r.competition=n.competition AND r.game_year=p_game_year
),
games_filtered AS MATERIALIZED (
  SELECT sr.game_id,sr.team_id,sr.team_name
  FROM schedule_ranked sr CROSS JOIN normalized n
  LEFT JOIN opponent_ranks r ON r.team_id=sr.opp_team_id
  WHERE (p_start_date IS NULL OR sr.game_date>=p_start_date)
    AND (p_end_date IS NULL OR sr.game_date<=p_end_date)
    AND (n.team_ids IS NULL OR sr.team_id=ANY(n.team_ids))
    AND (n.phases IS NULL OR sr.phase=ANY(n.phases))
    AND (n.opp_ids IS NULL OR sr.opp_team_id=ANY(n.opp_ids))
    AND (n.home_away='all' OR (n.home_away='home' AND sr.is_home) OR (n.home_away='away' AND NOT sr.is_home))
    AND (n.outcome='all' OR (n.outcome='win' AND sr.has_won) OR (n.outcome='loss' AND NOT sr.has_won))
    AND (n.rank_side IS NULL OR p_opp_rank_n IS NULL
      OR (n.rank_side='top' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank WHEN 'def' THEN r.def_rank ELSE r.net_rank END<=p_opp_rank_n)
      OR (n.rank_side='bottom' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank WHEN 'def' THEN r.def_rank ELSE r.net_rank END>r.team_count-p_opp_rank_n))
    AND (p_min_gn IS NULL OR sr.round_number>=p_min_gn)
    AND (p_max_gn IS NULL OR sr.round_number<=p_max_gn)
    AND (p_last_n_games IS NULL OR sr.team_game_rank<=p_last_n_games)
),
acts AS MATERIALIZED (
  SELECT a.game_id,a.team_id,a.own_lineup,a.segment_id,a.source_event_order,
    a.event_elapsed_seconds,a.type_lineup,a.possession_flag,a.action_player_id,
    a.points,a.play_type,a.turnovers,a.fgm,a.fga,a.fg3_made,a.fg3_att,
    a.ft_attempts,a.ts_possessions
  FROM euroleague.player_stats_action_context a
  JOIN games_filtered gf USING(game_id,team_id)
  WHERE NOT a.is_overtime
    AND (p_max_time_remaining IS NULL
         OR a.regulation_seconds_remaining<=p_max_time_remaining)
    AND (p_max_margin IS NULL OR a.pre_abs_margin<=p_max_margin)
    AND (coalesce(nullif(btrim(p_margin_status),''),'all')='all'
      OR (p_margin_status='leading' AND a.pre_status>0)
      OR (p_margin_status='trailing' AND a.pre_status<0)
      OR (p_margin_status='tied' AND a.pre_status=0))
  UNION ALL
  SELECT a.game_id,a.team_id,a.own_lineup,a.segment_id,a.source_event_order,
    a.event_elapsed_seconds,a.type_lineup,a.possession_flag,a.action_player_id,
    a.points,a.play_type,a.turnovers,a.fgm,a.fga,a.fg3_made,a.fg3_att,
    a.ft_attempts,a.ts_possessions
  FROM euroleague.player_stats_action_context a
  JOIN games_filtered gf USING(game_id,team_id)
  WHERE a.is_overtime
    AND (NOT coalesce(p_ot_margin_filter,false) OR (
      (p_max_margin IS NULL OR a.pre_abs_margin<=p_max_margin)
      AND (coalesce(nullif(btrim(p_margin_status),''),'all')='all'
        OR (p_margin_status='leading' AND a.pre_status>0)
        OR (p_margin_status='trailing' AND a.pre_status<0)
        OR (p_margin_status='tied' AND a.pre_status=0))))
),
real_roster AS MATERIALIZED (
  SELECT gf.game_id,gf.team_id,fr.player_id,fr.source_player_name
  FROM games_filtered gf JOIN euroleague.full_rosters fr USING(game_id,team_id)
  JOIN euroleague.players p USING(player_id)
  WHERE lower(p.provider_player_id) NOT IN ('team','total')
),
observed_lineups AS (
  SELECT DISTINCT game_id,team_id,own_lineup FROM acts WHERE own_lineup IS NOT NULL
),
lineup_map AS MATERIALIZED (
  SELECT ol.game_id,ol.team_id,ol.own_lineup,rr.player_id
  FROM observed_lineups ol JOIN real_roster rr USING(game_id,team_id)
  WHERE rr.source_player_name=ANY(ol.own_lineup)
),
poss_end AS MATERIALIZED (
  SELECT DISTINCT game_id,team_id,own_lineup,source_event_order poss_end_id
  FROM acts WHERE type_lineup='offense' AND possession_flag=1 AND own_lineup IS NOT NULL
),
player_usage AS MATERIALIZED (
  SELECT lm.player_id,pe.team_id,count(DISTINCT pe.game_id)::integer gp,
    count(DISTINCT (pe.game_id,pe.team_id,pe.poss_end_id))::numeric poss_on_floor
  FROM poss_end pe JOIN lineup_map lm USING(game_id,team_id,own_lineup)
  GROUP BY lm.player_id,pe.team_id
),
team_possession_totals AS MATERIALIZED (
  SELECT team_id,count(DISTINCT (game_id,team_id,poss_end_id))::numeric team_poss
  FROM poss_end GROUP BY team_id
),
seg_times AS MATERIALIZED (
  SELECT game_id,team_id,own_lineup,segment_id,
    greatest(max(event_elapsed_seconds)-min(event_elapsed_seconds),0)::numeric seg_seconds
  FROM acts WHERE own_lineup IS NOT NULL AND segment_id IS NOT NULL AND event_elapsed_seconds IS NOT NULL
  GROUP BY game_id,team_id,own_lineup,segment_id
),
player_minutes AS MATERIALIZED (
  SELECT lm.player_id,st.team_id,round(sum(st.seg_seconds)::numeric/60.0,1) minutes
  FROM seg_times st JOIN lineup_map lm USING(game_id,team_id,own_lineup)
  GROUP BY lm.player_id,st.team_id
),
stats AS MATERIALIZED (
  SELECT a.action_player_id player_id,a.team_id,
    sum(CASE WHEN a.type_lineup='offense' THEN a.points ELSE 0 END)::numeric pts,
    sum(CASE WHEN a.play_type='O' AND a.type_lineup='offense' THEN 1 ELSE 0 END
      + CASE WHEN a.play_type='D' AND a.type_lineup='defense' THEN 1 ELSE 0 END)::numeric reb,
    sum(CASE WHEN a.play_type='O' AND a.type_lineup='offense' THEN 1 ELSE 0 END)::numeric oreb,
    sum(CASE WHEN a.play_type='D' AND a.type_lineup='defense' THEN 1 ELSE 0 END)::numeric dreb,
    sum(CASE WHEN a.play_type='AS' AND a.type_lineup='offense' THEN 1 ELSE 0 END)::numeric ast,
    sum(CASE WHEN a.play_type='ST' AND a.type_lineup='defense' THEN 1 ELSE 0 END)::numeric stl,
    sum(CASE WHEN a.play_type='FV' AND a.type_lineup='defense' THEN 1 ELSE 0 END)::numeric blk,
    sum(CASE WHEN a.type_lineup='offense' THEN a.turnovers ELSE 0 END)::numeric tov,
    sum(CASE WHEN a.type_lineup='offense' THEN a.fgm ELSE 0 END)::numeric fgm,
    sum(CASE WHEN a.type_lineup='offense' THEN a.fga ELSE 0 END)::numeric fga,
    sum(CASE WHEN a.type_lineup='offense' THEN a.fg3_made ELSE 0 END)::numeric "3pm",
    sum(CASE WHEN a.type_lineup='offense' THEN a.fg3_att ELSE 0 END)::numeric "3pa",
    sum(CASE WHEN a.play_type='FTM' AND a.type_lineup='offense' THEN 1 ELSE 0 END)::numeric ftm,
    sum(CASE WHEN a.type_lineup='offense' THEN a.ft_attempts ELSE 0 END)::numeric fta,
    sum(CASE WHEN a.type_lineup='offense' THEN a.ts_possessions ELSE 0 END)::numeric ts_poss_count
  -- Match the Israeli function: type_lineup already selects the actor's team
  -- perspective. names_df removes the zero-valued opposite-perspective row.
  -- A roster join here forces millions of redundant action/roster comparisons.
  FROM acts a
  WHERE a.action_player_id IS NOT NULL
  GROUP BY a.action_player_id,a.team_id
),
team_usage_totals AS MATERIALIZED (
  SELECT team_id,sum(ts_possessions)::numeric team_ts_poss_count,
    sum(turnovers)::numeric team_tov
  FROM acts WHERE type_lineup='offense' GROUP BY team_id
),
names_df AS MATERIALIZED (
  SELECT fr.player_id,fr.team_id,min(gf.team_name) team_name,
    min(euroleague.person_display_name(p.display_name)) player_name
  FROM games_filtered gf JOIN euroleague.full_rosters fr USING(game_id,team_id)
  JOIN euroleague.players p USING(player_id)
  WHERE lower(p.provider_player_id) NOT IN ('team','total')
  GROUP BY fr.player_id,fr.team_id
),
final_rows AS (
  SELECT s.team_id,s.player_id,nd.team_name,nd.player_name,coalesce(pu.gp,0)::integer gp,
    coalesce(pu.poss_on_floor,0)::numeric poss_on_floor,coalesce(pm.minutes,0)::numeric minutes,
    s.pts,s.reb,s.oreb,s.dreb,s.ast,s.stl,s.blk,NULL::numeric dfl,s.tov,s.fgm,s.fga,
    CASE WHEN s.fga>0 THEN round(100*s.fgm/s.fga,1) END fg_pct,
    s."3pm",s."3pa",CASE WHEN s."3pa">0 THEN round(100*s."3pm"/s."3pa",1) END tp_pct,
    s.ftm,s.fta,CASE WHEN s.fta>0 THEN round(100*s.ftm/s.fta,1) END ft_pct,
    CASE WHEN s.fga>0 THEN round(100*(s.fgm+0.5*s."3pm")/s.fga,1) END efg,
    CASE WHEN s.ts_poss_count>0 THEN round(100*s.pts/(2*s.ts_poss_count),1) END ts,
    CASE WHEN (s.ts_poss_count+s.tov)>0 AND (tut.team_ts_poss_count+tut.team_tov)>0
      AND coalesce(pu.poss_on_floor,0)>0 AND coalesce(tpt.team_poss,0)>0
      THEN round(100*(s.ts_poss_count+s.tov)*tpt.team_poss/
        nullif((tut.team_ts_poss_count+tut.team_tov)*pu.poss_on_floor,0),1) END usg_pct
  FROM stats s LEFT JOIN team_usage_totals tut USING(team_id)
  LEFT JOIN player_usage pu USING(player_id,team_id)
  LEFT JOIN team_possession_totals tpt USING(team_id)
  LEFT JOIN player_minutes pm USING(player_id,team_id)
  LEFT JOIN names_df nd USING(player_id,team_id)
)
SELECT team_id,player_id,team_name,player_name "Player",gp,poss_on_floor,minutes,
 pts,reb,oreb,dreb,ast,stl,blk,dfl,tov,fgm,fga,fg_pct,"3pm","3pa",tp_pct,
 ftm,fta,ft_pct,efg,ts,usg_pct
FROM final_rows
WHERE player_name IS NOT NULL AND player_name<>'' AND team_name IS NOT NULL AND team_name<>''
  AND (gp>0 OR poss_on_floor>0 OR minutes>0)
$function$;

CREATE OR REPLACE FUNCTION euroleague.get_player_traditional_clutch(
    p_competition TEXT,p_game_year INTEGER,p_start_date DATE DEFAULT NULL,p_end_date DATE DEFAULT NULL,
    p_team_ids_csv TEXT DEFAULT NULL,p_phase_csv TEXT DEFAULT NULL,p_opp_ids_csv TEXT DEFAULT NULL,
    p_home_away TEXT DEFAULT 'all',p_outcome TEXT DEFAULT 'all',p_opp_rank_side TEXT DEFAULT NULL,
    p_opp_rank_n INTEGER DEFAULT NULL,p_opp_rank_metric TEXT DEFAULT NULL,p_max_margin INTEGER DEFAULT NULL,
    p_margin_status TEXT DEFAULT NULL,p_max_time_remaining INTEGER DEFAULT NULL,p_ot_margin_filter BOOLEAN DEFAULT FALSE,
    p_min_gn INTEGER DEFAULT NULL,p_max_gn INTEGER DEFAULT NULL,p_last_n_games INTEGER DEFAULT NULL
)
RETURNS TABLE (team_id BIGINT,player_id BIGINT,team_name TEXT,"Player" TEXT,gp INTEGER,
 poss_on_floor NUMERIC,minutes NUMERIC,pts NUMERIC,reb NUMERIC,oreb NUMERIC,dreb NUMERIC,
 ast NUMERIC,stl NUMERIC,blk NUMERIC,dfl NUMERIC,tov NUMERIC,fgm NUMERIC,fga NUMERIC,
 fg_pct NUMERIC,"3pm" NUMERIC,"3pa" NUMERIC,tp_pct NUMERIC,ftm NUMERIC,fta NUMERIC,
 ft_pct NUMERIC,efg NUMERIC,ts NUMERIC,usg_pct NUMERIC)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path=pg_catalog,euroleague,public SET plan_cache_mode=force_custom_plan
AS $function$
BEGIN
 IF p_max_margin=5 AND coalesce(nullif(btrim(p_margin_status),''),'all')='all'
    AND p_max_time_remaining=300 AND NOT coalesce(p_ot_margin_filter,false) THEN
  RETURN QUERY SELECT * FROM euroleague.get_player_traditional_standard_clutch(
    p_competition,p_game_year,p_start_date,p_end_date,p_team_ids_csv,p_phase_csv,p_opp_ids_csv,
    p_home_away,p_outcome,p_opp_rank_side,p_opp_rank_n,p_opp_rank_metric,p_min_gn,p_max_gn,p_last_n_games);
 ELSE
  RETURN QUERY SELECT * FROM euroleague.get_player_traditional_custom_clutch(
    p_competition,p_game_year,p_start_date,p_end_date,p_team_ids_csv,p_phase_csv,p_opp_ids_csv,
    p_home_away,p_outcome,p_opp_rank_side,p_opp_rank_n,p_opp_rank_metric,p_max_margin,p_margin_status,
    p_max_time_remaining,p_ot_margin_filter,p_min_gn,p_max_gn,p_last_n_games);
 END IF;
END;
$function$;

REVOKE ALL ON FUNCTION euroleague.get_player_traditional_custom_clutch(
 text,int4,date,date,text,text,text,text,text,text,int4,text,int4,text,int4,bool,int4,int4,int4) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_player_traditional_custom_clutch(
 text,int4,date,date,text,text,text,text,text,text,int4,text,int4,text,int4,bool,int4,int4,int4) TO app_readonly;
COMMIT;
