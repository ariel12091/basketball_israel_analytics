-- EuroLeague migration 033: Israeli-shaped direct custom Team Minutes.
-- Schedule filters and the clutch predicate live in the same SQL statement;
-- qualifying duration is max-minus-min per canonical segment.

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.get_team_minutes_direct(
 p_competition TEXT,p_game_year INTEGER,p_start_date DATE DEFAULT NULL,p_end_date DATE DEFAULT NULL,
 p_team_ids_csv TEXT DEFAULT NULL,p_phase_csv TEXT DEFAULT NULL,p_opp_ids_csv TEXT DEFAULT NULL,
 p_home_away TEXT DEFAULT 'all',p_outcome TEXT DEFAULT 'all',p_opp_rank_side TEXT DEFAULT NULL,
 p_opp_rank_n INTEGER DEFAULT NULL,p_opp_rank_metric TEXT DEFAULT NULL,p_max_margin INTEGER DEFAULT NULL,
 p_margin_status TEXT DEFAULT NULL,p_max_time_remaining INTEGER DEFAULT NULL,p_ot_margin_filter BOOLEAN DEFAULT FALSE,
 p_min_gn INTEGER DEFAULT NULL,p_max_gn INTEGER DEFAULT NULL,p_last_n_games INTEGER DEFAULT NULL,
 p_num_starters_off_min INTEGER DEFAULT NULL,p_num_starters_off_max INTEGER DEFAULT NULL,
 p_num_starters_def_min INTEGER DEFAULT NULL,p_num_starters_def_max INTEGER DEFAULT NULL)
RETURNS TABLE(team_id BIGINT,minutes NUMERIC)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path=pg_catalog,euroleague,public SET plan_cache_mode=force_custom_plan
AS $function$
WITH n AS (
 SELECT coalesce(nullif(btrim(p_competition),''),'E') competition,
  CASE WHEN nullif(btrim(p_team_ids_csv),'') IS NULL THEN NULL::bigint[] ELSE string_to_array(regexp_replace(p_team_ids_csv,'\s+','','g'),',')::bigint[] END team_ids,
  CASE WHEN nullif(btrim(p_phase_csv),'') IS NULL THEN NULL::text[] ELSE string_to_array(p_phase_csv,',') END phases,
  CASE WHEN nullif(btrim(p_opp_ids_csv),'') IS NULL THEN NULL::bigint[] ELSE string_to_array(regexp_replace(p_opp_ids_csv,'\s+','','g'),',')::bigint[] END opp_ids,
  coalesce(nullif(btrim(p_home_away),''),'all') home_away,coalesce(nullif(btrim(p_outcome),''),'all') outcome,
  nullif(btrim(p_opp_rank_side),'') rank_side,coalesce(nullif(btrim(p_opp_rank_metric),''),'net') rank_metric,
  coalesce(nullif(btrim(p_margin_status),''),'all') margin_status
),sr AS (
 SELECT fs.*,row_number() OVER(PARTITION BY fs.team_id ORDER BY fs.game_date DESC,fs.game_id DESC) recent
 FROM euroleague.final_schedule_mv fs CROSS JOIN n
 WHERE fs.competition=n.competition AND fs.game_year=p_game_year
),ranks AS (
 SELECT r.team_id,r.off_rank,r.def_rank,r.net_rank,count(*) OVER() team_count
 FROM euroleague.team_ppp_ratings_mv r CROSS JOIN n WHERE r.competition=n.competition AND r.game_year=p_game_year
),games AS MATERIALIZED (
 SELECT sr.game_id,sr.team_id FROM sr CROSS JOIN n LEFT JOIN ranks r ON r.team_id=sr.opp_team_id
 WHERE (p_start_date IS NULL OR sr.game_date>=p_start_date) AND (p_end_date IS NULL OR sr.game_date<=p_end_date)
  AND (n.team_ids IS NULL OR sr.team_id=ANY(n.team_ids)) AND (n.phases IS NULL OR sr.phase=ANY(n.phases))
  AND (n.opp_ids IS NULL OR sr.opp_team_id=ANY(n.opp_ids))
  AND (n.home_away='all' OR n.home_away='home' AND sr.is_home OR n.home_away='away' AND NOT sr.is_home)
  AND (n.outcome='all' OR n.outcome='win' AND sr.has_won OR n.outcome='loss' AND NOT sr.has_won)
  AND (n.rank_side IS NULL OR p_opp_rank_n IS NULL
    OR n.rank_side='top' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank WHEN 'def' THEN r.def_rank ELSE r.net_rank END<=p_opp_rank_n
    OR n.rank_side='bottom' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank WHEN 'def' THEN r.def_rank ELSE r.net_rank END>r.team_count-p_opp_rank_n)
  AND (p_min_gn IS NULL OR sr.round_number>=p_min_gn) AND (p_max_gn IS NULL OR sr.round_number<=p_max_gn)
  AND (p_last_n_games IS NULL OR sr.recent<=p_last_n_games)
),filtered AS MATERIALIZED (
 SELECT a.game_id,a.team_id,a.segment_id,a.event_elapsed_seconds
 FROM euroleague.player_stats_actions_by_game a JOIN games g USING(game_id,team_id) CROSS JOIN n
 WHERE a.segment_id IS NOT NULL AND a.event_elapsed_seconds IS NOT NULL
  AND (p_max_time_remaining IS NULL OR a.regulation_seconds_remaining<=p_max_time_remaining OR a.is_overtime)
  AND (p_max_margin IS NULL OR a.pre_abs_margin<=p_max_margin OR a.is_overtime AND NOT coalesce(p_ot_margin_filter,false))
  AND (n.margin_status='all' OR n.margin_status='leading' AND a.pre_status>0
    OR n.margin_status='trailing' AND a.pre_status<0 OR n.margin_status='tied' AND a.pre_status=0
    OR a.is_overtime AND NOT coalesce(p_ot_margin_filter,false))
  AND (p_num_starters_off_min IS NULL OR a.own_starters>=p_num_starters_off_min)
  AND (p_num_starters_off_max IS NULL OR a.own_starters<=p_num_starters_off_max)
  AND (p_num_starters_def_min IS NULL OR a.opp_starters>=p_num_starters_def_min)
  AND (p_num_starters_def_max IS NULL OR a.opp_starters<=p_num_starters_def_max)
),segments AS (
 SELECT game_id,team_id,segment_id,greatest(max(event_elapsed_seconds)-min(event_elapsed_seconds),0::numeric) seconds
 FROM filtered GROUP BY game_id,team_id,segment_id)
SELECT team_id,round(sum(seconds)/60.0,3)::numeric FROM segments GROUP BY team_id
$function$;

REVOKE ALL ON FUNCTION euroleague.get_team_minutes_direct(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_team_minutes_direct(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) TO app_readonly;
COMMIT;
