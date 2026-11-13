-- Definition before of fetch_lineups_direct, EuroLeague migration 062 (EUROLEAGUE SHADOW SCHEMA),
-- written by scripts/apply_062_readers.py from the live definition.

CREATE OR REPLACE FUNCTION euroleague.fetch_lineups_direct(p_competition text, p_game_year integer, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_team_ids_csv text DEFAULT NULL::text, p_phase_csv text DEFAULT NULL::text, p_opp_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT NULL::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT NULL::text, p_max_margin integer DEFAULT NULL::integer, p_margin_status text DEFAULT NULL::text, p_max_time_remaining integer DEFAULT NULL::integer, p_ot_margin_filter boolean DEFAULT false, p_min_gn integer DEFAULT NULL::integer, p_max_gn integer DEFAULT NULL::integer, p_last_n_games integer DEFAULT NULL::integer, p_num_starters_off_min integer DEFAULT NULL::integer, p_num_starters_off_max integer DEFAULT NULL::integer, p_num_starters_def_min integer DEFAULT NULL::integer, p_num_starters_def_max integer DEFAULT NULL::integer, p_unit_size integer DEFAULT 5, p_players_on_csv text DEFAULT NULL::text, p_players_off_csv text DEFAULT NULL::text, p_min_poss integer DEFAULT 0)
 RETURNS TABLE(team_id bigint, unit_key text, unit_size smallint, player_ids bigint[], player_names_str text, off_poss bigint, off_pts bigint, off_fg2_made bigint, off_fg2_att bigint, off_fg3_made bigint, off_fg3_att bigint, off_ts_poss bigint, off_fgm bigint, off_fga bigint, off_fta bigint, off_oreb bigint, off_oreb_opp bigint, off_tov bigint, off_steals bigint, def_poss bigint, def_pts bigint, def_fg2_made bigint, def_fg2_att bigint, def_fg3_made bigint, def_fg3_att bigint, def_ts_poss bigint, def_fgm bigint, def_fga bigint, def_fta bigint, def_oreb bigint, def_oreb_opp bigint, def_tov bigint, def_steals bigint, minutes numeric, starters_poss_num numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'euroleague', 'public'
 SET plan_cache_mode TO 'force_custom_plan'
AS $function$
WITH n AS (
 SELECT coalesce(nullif(btrim(p_competition),''),'E') competition,
  CASE WHEN nullif(btrim(p_team_ids_csv),'') IS NULL THEN NULL::bigint[] ELSE string_to_array(regexp_replace(p_team_ids_csv,'\s+','','g'),',')::bigint[] END team_ids,
  CASE WHEN nullif(btrim(p_phase_csv),'') IS NULL THEN NULL::text[] ELSE string_to_array(p_phase_csv,',') END phases,
  CASE WHEN nullif(btrim(p_opp_ids_csv),'') IS NULL THEN NULL::bigint[] ELSE string_to_array(regexp_replace(p_opp_ids_csv,'\s+','','g'),',')::bigint[] END opp_ids,
  CASE WHEN nullif(btrim(p_players_on_csv),'') IS NULL THEN NULL::bigint[] ELSE string_to_array(regexp_replace(p_players_on_csv,'\s+','','g'),',')::bigint[] END players_on,
  CASE WHEN nullif(btrim(p_players_off_csv),'') IS NULL THEN NULL::bigint[] ELSE string_to_array(regexp_replace(p_players_off_csv,'\s+','','g'),',')::bigint[] END players_off,
  coalesce(nullif(btrim(p_home_away),''),'all') home_away,coalesce(nullif(btrim(p_outcome),''),'all') outcome,
  nullif(btrim(p_opp_rank_side),'') rank_side,coalesce(nullif(btrim(p_opp_rank_metric),''),'net') rank_metric,
  coalesce(nullif(btrim(p_margin_status),''),'all') margin_status
),sr AS (
 SELECT fs.*,row_number() OVER(PARTITION BY fs.team_id ORDER BY fs.game_date DESC,fs.game_id DESC) recent
 FROM euroleague.final_schedule_mv fs CROSS JOIN n WHERE fs.competition=n.competition AND fs.game_year=p_game_year
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
),acts AS MATERIALIZED (
 SELECT a.game_id,a.team_id,a.own_lineup,a.segment_id,a.event_elapsed_seconds,a.type_lineup,
  a.possession_flag,a.points,a.fg2_made,a.fg2_att,a.fg3_made,a.fg3_att,a.ts_possessions,
  a.fgm,a.fga,a.ft_attempts,a.orebounds,a.oreb_opportunities,a.turnovers,a.steals,
  a.own_starters
 FROM euroleague.player_stats_actions_by_game a JOIN games g USING(game_id,team_id) CROSS JOIN n
 WHERE a.own_lineup IS NOT NULL AND a.segment_id IS NOT NULL AND a.event_elapsed_seconds IS NOT NULL
  AND (p_max_time_remaining IS NULL OR a.regulation_seconds_remaining<=p_max_time_remaining OR a.is_overtime)
  AND (p_max_margin IS NULL OR a.pre_abs_margin<=p_max_margin OR a.is_overtime AND NOT coalesce(p_ot_margin_filter,false))
  AND (n.margin_status='all' OR n.margin_status='leading' AND a.pre_status>0 OR n.margin_status='trailing' AND a.pre_status<0
   OR n.margin_status='tied' AND a.pre_status=0 OR a.is_overtime AND NOT coalesce(p_ot_margin_filter,false))
  AND (p_num_starters_off_min IS NULL OR a.own_starters>=p_num_starters_off_min)
  AND (p_num_starters_off_max IS NULL OR a.own_starters<=p_num_starters_off_max)
  AND (p_num_starters_def_min IS NULL OR a.opp_starters>=p_num_starters_def_min)
  AND (p_num_starters_def_max IS NULL OR a.opp_starters<=p_num_starters_def_max)
),lineup_identity AS MATERIALIZED (
 SELECT DISTINCT l.game_id,l.team_id,l.own_lineup,l.lineup_key,l.player_ids,gs.starter_ids
 FROM (SELECT DISTINCT game_id,team_id,own_lineup FROM acts) a
 JOIN euroleague.lineup_totals_by_game l USING(game_id,team_id,own_lineup)
 LEFT JOIN LATERAL (
   SELECT array_agg(fr.player_id) AS starter_ids
   FROM euroleague.full_rosters fr
   WHERE fr.game_id = l.game_id AND fr.team_id = l.team_id AND fr.is_starter
 ) gs ON true
 WHERE l.competition=(SELECT competition FROM n) AND l.game_year=p_game_year
),units AS MATERIALIZED (
 SELECT li.game_id,li.team_id,li.own_lineup,li.lineup_key unit_key,5::smallint unit_size,li.player_ids,li.starter_ids
 FROM lineup_identity li CROSS JOIN n WHERE p_unit_size=5
  AND (n.players_on IS NULL OR li.player_ids@>n.players_on) AND (n.players_off IS NULL OR NOT(li.player_ids&&n.players_off))
 UNION ALL
 SELECT li.game_id,li.team_id,li.own_lineup,sl.unit_key,sl.unit_size,sl.player_ids,li.starter_ids
 FROM lineup_identity li JOIN euroleague.sub_lineups sl
  ON sl.competition=(SELECT competition FROM n) AND sl.game_year=p_game_year AND sl.team_id=li.team_id AND sl.lineup_key=li.lineup_key
 CROSS JOIN n WHERE p_unit_size BETWEEN 2 AND 4 AND sl.unit_size=p_unit_size::smallint
  AND (n.players_on IS NULL OR sl.player_ids@>n.players_on) AND (n.players_off IS NULL OR NOT(sl.player_ids&&n.players_off))
),event_counts AS (
 SELECT game_id,team_id,own_lineup,type_lineup,sum(possession_flag)::bigint possessions,sum(points)::bigint points,
  sum(fg2_made)::bigint fg2_made,sum(fg2_att)::bigint fg2_att,sum(fg3_made)::bigint fg3_made,sum(fg3_att)::bigint fg3_att,
  sum(ts_possessions)::bigint ts_possessions,sum(fgm)::bigint fgm,sum(fga)::bigint fga,sum(ft_attempts)::bigint ft_attempts,
  sum(orebounds)::bigint orebounds,sum(oreb_opportunities)::bigint oreb_opportunities,sum(turnovers)::bigint turnovers,sum(steals)::bigint steals,max(own_starters)::smallint own_starters
 FROM acts WHERE type_lineup IS NOT NULL GROUP BY game_id,team_id,own_lineup,type_lineup
),segment_duration AS (
 SELECT game_id,team_id,own_lineup,segment_id,greatest(max(event_elapsed_seconds)-min(event_elapsed_seconds),0::numeric) seconds
 FROM acts GROUP BY game_id,team_id,own_lineup,segment_id
),duration AS (
 SELECT game_id,team_id,own_lineup,sum(seconds)::numeric seconds FROM segment_duration GROUP BY game_id,team_id,own_lineup
),facts AS (
 SELECT d.game_id,d.team_id,d.own_lineup,s.type_lineup,
  coalesce(e.possessions,0)::bigint possessions,coalesce(e.points,0)::bigint points,
  coalesce(e.fg2_made,0)::bigint fg2_made,coalesce(e.fg2_att,0)::bigint fg2_att,
  coalesce(e.fg3_made,0)::bigint fg3_made,coalesce(e.fg3_att,0)::bigint fg3_att,
  coalesce(e.ts_possessions,0)::bigint ts_possessions,coalesce(e.fgm,0)::bigint fgm,coalesce(e.fga,0)::bigint fga,
  coalesce(e.ft_attempts,0)::bigint ft_attempts,coalesce(e.orebounds,0)::bigint orebounds,
  coalesce(e.oreb_opportunities,0)::bigint oreb_opportunities,coalesce(e.turnovers,0)::bigint turnovers,
  coalesce(e.steals,0)::bigint steals,coalesce(e.own_starters,0)::smallint own_starters,CASE WHEN s.type_lineup='offense' THEN d.seconds END seconds
 FROM duration d CROSS JOIN(VALUES('offense'::text),('defense'::text))s(type_lineup)
 LEFT JOIN event_counts e USING(game_id,team_id,own_lineup,type_lineup)
 WHERE d.seconds>0 OR e.game_id IS NOT NULL
),unit_rows AS (
 SELECT u.team_id,u.unit_key,u.unit_size,u.player_ids,f.type_lineup,f.possessions,f.points,f.fg2_made,f.fg2_att,
  f.fg3_made,f.fg3_att,f.ts_possessions,f.fgm,f.fga,f.ft_attempts,f.orebounds,f.oreb_opportunities,f.turnovers,f.steals,f.seconds,
  (SELECT count(*) FROM unnest(u.player_ids) AS us(pid) WHERE us.pid = ANY(u.starter_ids)) unit_starters
 FROM facts f JOIN units u USING(game_id,team_id,own_lineup)
),agg AS (
 SELECT team_id,unit_key,unit_size,player_ids,
  sum(possessions) FILTER(WHERE type_lineup='offense') off_poss,sum(points) FILTER(WHERE type_lineup='offense') off_pts,
  sum(fg2_made) FILTER(WHERE type_lineup='offense') off_fg2_made,sum(fg2_att) FILTER(WHERE type_lineup='offense') off_fg2_att,
  sum(fg3_made) FILTER(WHERE type_lineup='offense') off_fg3_made,sum(fg3_att) FILTER(WHERE type_lineup='offense') off_fg3_att,
  sum(ts_possessions) FILTER(WHERE type_lineup='offense') off_ts_poss,sum(fgm) FILTER(WHERE type_lineup='offense') off_fgm,
  sum(fga) FILTER(WHERE type_lineup='offense') off_fga,sum(ft_attempts) FILTER(WHERE type_lineup='offense') off_fta,
  sum(orebounds) FILTER(WHERE type_lineup='offense') off_oreb,sum(oreb_opportunities) FILTER(WHERE type_lineup='offense') off_oreb_opp,
  sum(turnovers) FILTER(WHERE type_lineup='offense') off_tov,sum(steals) FILTER(WHERE type_lineup='offense') off_steals,
  sum(possessions) FILTER(WHERE type_lineup='defense') def_poss,sum(points) FILTER(WHERE type_lineup='defense') def_pts,
  sum(fg2_made) FILTER(WHERE type_lineup='defense') def_fg2_made,sum(fg2_att) FILTER(WHERE type_lineup='defense') def_fg2_att,
  sum(fg3_made) FILTER(WHERE type_lineup='defense') def_fg3_made,sum(fg3_att) FILTER(WHERE type_lineup='defense') def_fg3_att,
  sum(ts_possessions) FILTER(WHERE type_lineup='defense') def_ts_poss,sum(fgm) FILTER(WHERE type_lineup='defense') def_fgm,
  sum(fga) FILTER(WHERE type_lineup='defense') def_fga,sum(ft_attempts) FILTER(WHERE type_lineup='defense') def_fta,
  sum(orebounds) FILTER(WHERE type_lineup='defense') def_oreb,sum(oreb_opportunities) FILTER(WHERE type_lineup='defense') def_oreb_opp,
  sum(turnovers) FILTER(WHERE type_lineup='defense') def_tov,sum(steals) FILTER(WHERE type_lineup='defense') def_steals,sum(seconds) seconds,
  -- unfiltered by type_lineup on purpose; see migration header
  sum(unit_starters*possessions) starters_poss_num
 FROM unit_rows GROUP BY team_id,unit_key,unit_size,player_ids)
SELECT a.team_id,a.unit_key,a.unit_size,a.player_ids,names.player_names_str,
 a.off_poss,a.off_pts,a.off_fg2_made,a.off_fg2_att,a.off_fg3_made,a.off_fg3_att,a.off_ts_poss,a.off_fgm,a.off_fga,a.off_fta,a.off_oreb,a.off_oreb_opp,a.off_tov,a.off_steals,
 a.def_poss,a.def_pts,a.def_fg2_made,a.def_fg2_att,a.def_fg3_made,a.def_fg3_att,a.def_ts_poss,a.def_fgm,a.def_fga,a.def_fta,a.def_oreb,a.def_oreb_opp,a.def_tov,a.def_steals,
 round(coalesce(a.seconds,0)/60.0,1),a.starters_poss_num::numeric
FROM agg a CROSS JOIN LATERAL(SELECT string_agg(coalesce(euroleague.person_display_name(p.display_name),'#'||x.pid::text),', ' ORDER BY x.ord) player_names_str
 FROM unnest(a.player_ids) WITH ORDINALITY x(pid,ord) LEFT JOIN euroleague.players p ON p.player_id=x.pid) names
WHERE coalesce(a.off_poss,0)+coalesce(a.def_poss,0)>=coalesce(p_min_poss,0)
$function$;
