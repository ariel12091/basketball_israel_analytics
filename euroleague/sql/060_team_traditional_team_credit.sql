-- EuroLeague migration 060: Team Ratings Traditional counts team rebounds and
-- team turnovers, for the EUROLEAGUE SHADOW SCHEMA.
--
-- Migration 059 counted player-credited events only, because the box score
-- (full_rosters.boxscore_stats) has no team row. Checked against RealGM's
-- 2025-26 team totals the same day, every column matched except REB, OREB,
-- DREB and TOV, which were low by the team-credited events: about 1.8 OREB,
-- 1.9 DREB and 0.6 TOV per game in EuroLeague (1,460 / 1,493 / 533 rows over
-- 804 team-games). A team's official totals include them, and so does the
-- Israeli reader, which counts every rebound and turnover action.
--
-- The play-by-play records them as actions with no player. Both readers now
-- add them:
--   pergame  box score (players) + the team-credited rebounds and turnovers
--            of the same games, read through the partial index in
--            060a_team_credit_action_index.sql (apply that first);
--   direct   no longer filters out player-less rows. Those rows carry only
--            rebounds, turnovers and team fouls (zero points, shots, assists,
--            steals, blocks: checked on E 2025), so no other count changes.
--
-- Same signatures as 059, so CREATE OR REPLACE keeps the grants. Additive.

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.get_team_traditional_pergame(
    p_competition TEXT, p_game_year INTEGER,
    p_start_date DATE DEFAULT NULL, p_end_date DATE DEFAULT NULL,
    p_team_ids_csv TEXT DEFAULT NULL, p_phase_csv TEXT DEFAULT NULL,
    p_opp_ids_csv TEXT DEFAULT NULL, p_home_away TEXT DEFAULT 'all',
    p_outcome TEXT DEFAULT 'all', p_opp_rank_side TEXT DEFAULT NULL,
    p_opp_rank_n INTEGER DEFAULT NULL, p_opp_rank_metric TEXT DEFAULT NULL,
    p_min_gn INTEGER DEFAULT NULL, p_max_gn INTEGER DEFAULT NULL,
    p_last_n_games INTEGER DEFAULT NULL,
    p_side TEXT DEFAULT 'offense'
)
RETURNS TABLE (
    team_id BIGINT, team_name TEXT, gp INTEGER, poss_on_floor INTEGER,
    minutes NUMERIC, pts INTEGER, reb INTEGER, oreb INTEGER, dreb INTEGER,
    ast INTEGER, stl INTEGER, blk INTEGER, dfl INTEGER, tov INTEGER,
    fgm INTEGER, fga INTEGER, "2pm" INTEGER, "2pa" INTEGER,
    "3pm" INTEGER, "3pa" INTEGER, ftm INTEGER, fta INTEGER,
    fg_pct NUMERIC, two_pct NUMERIC, tp_pct NUMERIC, ft_pct NUMERIC,
    efg NUMERIC, ts NUMERIC
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
SET plan_cache_mode = force_custom_plan
AS $function$
WITH normalized AS (
  SELECT coalesce(nullif(btrim(p_competition),''),'E') competition,
    CASE WHEN nullif(btrim(p_team_ids_csv),'') IS NULL THEN NULL::bigint[]
      ELSE string_to_array(regexp_replace(p_team_ids_csv,'\s+','','g'),',')::bigint[] END team_ids,
    CASE WHEN nullif(btrim(p_phase_csv),'') IS NULL THEN NULL::text[]
      ELSE string_to_array(p_phase_csv,',') END phases,
    CASE WHEN nullif(btrim(p_opp_ids_csv),'') IS NULL THEN NULL::bigint[]
      ELSE string_to_array(regexp_replace(p_opp_ids_csv,'\s+','','g'),',')::bigint[] END opp_ids,
    coalesce(nullif(btrim(p_home_away),''),'all') home_away,
    coalesce(nullif(btrim(p_outcome),''),'all') outcome,
    nullif(btrim(p_opp_rank_side),'') rank_side,
    coalesce(nullif(btrim(p_opp_rank_metric),''),'net') rank_metric,
    p_side = 'defense' AS defense
), schedule_ranked AS (
  SELECT fs.*,row_number() OVER(
    PARTITION BY fs.team_id ORDER BY fs.game_date DESC,fs.game_id DESC) team_game_rank
  FROM euroleague.final_schedule_mv fs CROSS JOIN normalized n
  WHERE fs.competition=n.competition AND fs.game_year=p_game_year
), opponent_ranks AS (
  SELECT r.team_id,r.off_rank,r.def_rank,r.net_rank,count(*) OVER() team_count
  FROM euroleague.team_ppp_ratings_mv r CROSS JOIN normalized n
  WHERE r.competition=n.competition AND r.game_year=p_game_year
), games_filtered AS MATERIALIZED (
  -- box_team_id is whose box line this game contributes: the team's own on
  -- the offence side, the opponent's on the defence side.
  SELECT sr.game_id,sr.team_id,sr.team_name,
    CASE WHEN n.defense THEN sr.opp_team_id ELSE sr.team_id END box_team_id
  FROM schedule_ranked sr CROSS JOIN normalized n
  LEFT JOIN opponent_ranks r ON r.team_id=sr.opp_team_id
  WHERE (p_start_date IS NULL OR sr.game_date>=p_start_date)
    AND (p_end_date IS NULL OR sr.game_date<=p_end_date)
    AND (n.team_ids IS NULL OR sr.team_id=ANY(n.team_ids))
    AND (n.phases IS NULL OR sr.phase=ANY(n.phases))
    AND (n.opp_ids IS NULL OR sr.opp_team_id=ANY(n.opp_ids))
    AND (n.home_away='all' OR (n.home_away='home' AND sr.is_home)
      OR (n.home_away='away' AND NOT sr.is_home))
    AND (n.outcome='all' OR (n.outcome='win' AND sr.has_won)
      OR (n.outcome='loss' AND NOT sr.has_won))
    AND (n.rank_side IS NULL OR p_opp_rank_n IS NULL
      OR (n.rank_side='top' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank
        WHEN 'def' THEN r.def_rank ELSE r.net_rank END<=p_opp_rank_n)
      OR (n.rank_side='bottom' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank
        WHEN 'def' THEN r.def_rank ELSE r.net_rank END>r.team_count-p_opp_rank_n))
    AND (p_min_gn IS NULL OR sr.round_number>=p_min_gn)
    AND (p_max_gn IS NULL OR sr.round_number<=p_max_gn)
    AND (p_last_n_games IS NULL OR sr.team_game_rank<=p_last_n_games)
    -- Only games with play-by-play facts, as get_team_ratings_pergame and
    -- get_team_minutes_pergame count them: a game whose play-by-play failed
    -- QA has a box score but no possessions or floor time, and keeping its box
    -- would inflate per-75 and per-40. (E 2025 game 246, U 2025 493/549/650.)
    AND EXISTS (SELECT 1 FROM euroleague.team_four_factors_by_game t
                WHERE t.game_id=sr.game_id AND t.team_id=sr.team_id)
), box AS (
  SELECT g.team_id,max(g.team_name) team_name,count(DISTINCT g.game_id)::integer gp,
    sum(f.pts) pts,sum(f.reb) reb,sum(f.oreb) oreb,sum(f.dreb) dreb,
    sum(f.ast) ast,sum(f.stl) stl,sum(f.blk) blk,sum(f.tov) tov,
    sum(f.fg2m) fg2m,sum(f.fg2a) fg2a,sum(f.fg3m) fg3m,sum(f.fg3a) fg3a,
    sum(f.ftm) ftm,sum(f.fta) fta
  FROM games_filtered g
  JOIN euroleague.player_traditional_by_game f
    ON f.game_id=g.game_id AND f.team_id=g.box_team_id
  WHERE f.competition=(SELECT competition FROM normalized)
    AND f.game_year=p_game_year
  GROUP BY g.team_id
), team_credit AS (
  -- Team rebounds and team turnovers: play-by-play rows with no player, on
  -- the box team's own perspective (offensive rebounds and turnovers on its
  -- offense rows, defensive rebounds on its defense rows), as in
  -- get_team_traditional_direct.
  SELECT g.team_id,
    count(*) FILTER(WHERE a.play_type='O' AND a.type_lineup='offense') oreb,
    count(*) FILTER(WHERE a.play_type='D' AND a.type_lineup='defense') dreb,
    coalesce(sum(a.turnovers) FILTER(WHERE a.type_lineup='offense'),0) tov
  FROM games_filtered g
  JOIN euroleague.player_stats_actions_by_game a
    ON a.game_id=g.game_id AND a.team_id=g.box_team_id
  WHERE a.action_player_id IS NULL
  GROUP BY g.team_id
), usage AS (
  SELECT g.team_id,
    sum(CASE WHEN n.defense THEN f.def_poss ELSE f.off_poss END) poss,
    sum(CASE WHEN n.defense THEN f.def_ts_poss ELSE f.off_ts_poss END) ts_poss
  FROM games_filtered g
  JOIN euroleague.team_four_factors_by_game f
    ON f.game_id=g.game_id AND f.team_id=g.team_id
  CROSS JOIN normalized n
  WHERE f.game_year=p_game_year
  GROUP BY g.team_id
), floor_time AS (
  SELECT g.team_id,round(sum(l.action_span_seconds)/60.0,1) minutes
  FROM games_filtered g
  JOIN euroleague.lineup_totals_by_game l
    ON l.game_id=g.game_id AND l.team_id=g.team_id
  WHERE l.competition=(SELECT competition FROM normalized)
    AND l.game_year=p_game_year
    AND l.type_lineup='offense'
  GROUP BY g.team_id
)
SELECT b.team_id,b.team_name,b.gp,
  coalesce(u.poss,0)::integer,coalesce(ft.minutes,0)::numeric,
  b.pts::integer,
  (b.reb+coalesce(tc.oreb,0)+coalesce(tc.dreb,0))::integer,
  (b.oreb+coalesce(tc.oreb,0))::integer,(b.dreb+coalesce(tc.dreb,0))::integer,
  b.ast::integer,b.stl::integer,b.blk::integer,NULL::integer,
  (b.tov+coalesce(tc.tov,0))::integer,
  (b.fg2m+b.fg3m)::integer,(b.fg2a+b.fg3a)::integer,
  b.fg2m::integer,b.fg2a::integer,b.fg3m::integer,b.fg3a::integer,
  b.ftm::integer,b.fta::integer,
  round(100*(b.fg2m+b.fg3m)/nullif(b.fg2a+b.fg3a,0),1),
  round(100*b.fg2m/nullif(b.fg2a,0),1),
  round(100*b.fg3m/nullif(b.fg3a,0),1),
  round(100*b.ftm/nullif(b.fta,0),1),
  round(100*(b.fg2m+1.5*b.fg3m)/nullif(b.fg2a+b.fg3a,0),1),
  round(100*b.pts/nullif(2*u.ts_poss,0),1)
FROM box b
LEFT JOIN team_credit tc ON tc.team_id=b.team_id
LEFT JOIN usage u ON u.team_id=b.team_id
LEFT JOIN floor_time ft ON ft.team_id=b.team_id
ORDER BY b.pts DESC,coalesce(ft.minutes,0) DESC,b.team_name
$function$;

CREATE OR REPLACE FUNCTION euroleague.get_team_traditional_direct(
    p_competition TEXT, p_game_year INTEGER,
    p_start_date DATE DEFAULT NULL, p_end_date DATE DEFAULT NULL,
    p_team_ids_csv TEXT DEFAULT NULL, p_phase_csv TEXT DEFAULT NULL,
    p_opp_ids_csv TEXT DEFAULT NULL, p_home_away TEXT DEFAULT 'all',
    p_outcome TEXT DEFAULT 'all', p_opp_rank_side TEXT DEFAULT NULL,
    p_opp_rank_n INTEGER DEFAULT NULL, p_opp_rank_metric TEXT DEFAULT NULL,
    p_max_margin INTEGER DEFAULT NULL, p_margin_status TEXT DEFAULT NULL,
    p_max_time_remaining INTEGER DEFAULT NULL, p_ot_margin_filter BOOLEAN DEFAULT FALSE,
    p_min_gn INTEGER DEFAULT NULL, p_max_gn INTEGER DEFAULT NULL,
    p_last_n_games INTEGER DEFAULT NULL,
    p_side TEXT DEFAULT 'offense'
)
RETURNS TABLE (
    team_id BIGINT, team_name TEXT, gp INTEGER, poss_on_floor INTEGER,
    minutes NUMERIC, pts INTEGER, reb INTEGER, oreb INTEGER, dreb INTEGER,
    ast INTEGER, stl INTEGER, blk INTEGER, dfl INTEGER, tov INTEGER,
    fgm INTEGER, fga INTEGER, "2pm" INTEGER, "2pa" INTEGER,
    "3pm" INTEGER, "3pa" INTEGER, ftm INTEGER, fta INTEGER,
    fg_pct NUMERIC, two_pct NUMERIC, tp_pct NUMERIC, ft_pct NUMERIC,
    efg NUMERIC, ts NUMERIC
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
SET plan_cache_mode = force_custom_plan
AS $function$
WITH normalized AS (
  SELECT coalesce(nullif(btrim(p_competition),''),'E') competition,
    CASE WHEN nullif(btrim(p_team_ids_csv),'') IS NULL THEN NULL::bigint[]
      ELSE string_to_array(regexp_replace(p_team_ids_csv,'\s+','','g'),',')::bigint[] END team_ids,
    CASE WHEN nullif(btrim(p_phase_csv),'') IS NULL THEN NULL::text[]
      ELSE string_to_array(p_phase_csv,',') END phases,
    CASE WHEN nullif(btrim(p_opp_ids_csv),'') IS NULL THEN NULL::bigint[]
      ELSE string_to_array(regexp_replace(p_opp_ids_csv,'\s+','','g'),',')::bigint[] END opp_ids,
    coalesce(nullif(btrim(p_home_away),''),'all') home_away,
    coalesce(nullif(btrim(p_outcome),''),'all') outcome,
    nullif(btrim(p_opp_rank_side),'') rank_side,
    coalesce(nullif(btrim(p_opp_rank_metric),''),'net') rank_metric,
    coalesce(nullif(btrim(p_margin_status),''),'all') margin_status,
    -- own: the perspective that holds this side's shots, assists, offensive
    -- rebounds and turnovers; other: its defensive rebounds, steals, blocks.
    CASE WHEN p_side='defense' THEN 'defense' ELSE 'offense' END own_tl,
    CASE WHEN p_side='defense' THEN 'offense' ELSE 'defense' END other_tl
), schedule_ranked AS (
  SELECT fs.*,row_number() OVER(
    PARTITION BY fs.team_id ORDER BY fs.game_date DESC,fs.game_id DESC) team_game_rank
  FROM euroleague.final_schedule_mv fs CROSS JOIN normalized n
  WHERE fs.competition=n.competition AND fs.game_year=p_game_year
), opponent_ranks AS (
  SELECT r.team_id,r.off_rank,r.def_rank,r.net_rank,count(*) OVER() team_count
  FROM euroleague.team_ppp_ratings_mv r CROSS JOIN normalized n
  WHERE r.competition=n.competition AND r.game_year=p_game_year
), games_filtered AS MATERIALIZED (
  SELECT sr.game_id,sr.team_id,sr.team_name
  FROM schedule_ranked sr CROSS JOIN normalized n
  LEFT JOIN opponent_ranks r ON r.team_id=sr.opp_team_id
  WHERE (p_start_date IS NULL OR sr.game_date>=p_start_date)
    AND (p_end_date IS NULL OR sr.game_date<=p_end_date)
    AND (n.team_ids IS NULL OR sr.team_id=ANY(n.team_ids))
    AND (n.phases IS NULL OR sr.phase=ANY(n.phases))
    AND (n.opp_ids IS NULL OR sr.opp_team_id=ANY(n.opp_ids))
    AND (n.home_away='all' OR (n.home_away='home' AND sr.is_home)
      OR (n.home_away='away' AND NOT sr.is_home))
    AND (n.outcome='all' OR (n.outcome='win' AND sr.has_won)
      OR (n.outcome='loss' AND NOT sr.has_won))
    AND (n.rank_side IS NULL OR p_opp_rank_n IS NULL
      OR (n.rank_side='top' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank
        WHEN 'def' THEN r.def_rank ELSE r.net_rank END<=p_opp_rank_n)
      OR (n.rank_side='bottom' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank
        WHEN 'def' THEN r.def_rank ELSE r.net_rank END>r.team_count-p_opp_rank_n))
    AND (p_min_gn IS NULL OR sr.round_number>=p_min_gn)
    AND (p_max_gn IS NULL OR sr.round_number<=p_max_gn)
    AND (p_last_n_games IS NULL OR sr.team_game_rank<=p_last_n_games)
), acts AS MATERIALIZED (
  -- The clutch predicate of get_team_ratings_direct / get_team_minutes_direct.
  SELECT a.game_id,a.team_id,a.segment_id,a.event_elapsed_seconds,
    a.type_lineup,a.possession_flag,a.action_player_id,a.points,a.play_type,
    a.turnovers,a.fgm,a.fga,a.fg3_made,a.fg3_att,a.ft_attempts,a.ts_possessions
  FROM euroleague.player_stats_actions_by_game a
  JOIN games_filtered gf USING(game_id,team_id) CROSS JOIN normalized n
  WHERE (p_max_time_remaining IS NULL OR a.regulation_seconds_remaining<=p_max_time_remaining OR a.is_overtime)
    AND (p_max_margin IS NULL OR a.pre_abs_margin<=p_max_margin
      OR (a.is_overtime AND NOT coalesce(p_ot_margin_filter,false)))
    AND (n.margin_status='all' OR (n.margin_status='leading' AND a.pre_status>0)
      OR (n.margin_status='trailing' AND a.pre_status<0)
      OR (n.margin_status='tied' AND a.pre_status=0)
      OR (a.is_overtime AND NOT coalesce(p_ot_margin_filter,false)))
), box AS (
  -- Box counters of get_player_traditional_custom_clutch, by team and side,
  -- over every row: team rebounds and team turnovers have no player.
  SELECT a.team_id,
    sum(a.points) FILTER(WHERE a.type_lineup=n.own_tl) pts,
    sum((a.play_type='O')::int) FILTER(WHERE a.type_lineup=n.own_tl) oreb,
    sum((a.play_type='D')::int) FILTER(WHERE a.type_lineup=n.other_tl) dreb,
    sum((a.play_type='AS')::int) FILTER(WHERE a.type_lineup=n.own_tl) ast,
    sum((a.play_type='ST')::int) FILTER(WHERE a.type_lineup=n.other_tl) stl,
    sum((a.play_type='FV')::int) FILTER(WHERE a.type_lineup=n.other_tl) blk,
    sum(a.turnovers) FILTER(WHERE a.type_lineup=n.own_tl) tov,
    sum(a.fgm) FILTER(WHERE a.type_lineup=n.own_tl) fgm,
    sum(a.fga) FILTER(WHERE a.type_lineup=n.own_tl) fga,
    sum(a.fg3_made) FILTER(WHERE a.type_lineup=n.own_tl) fg3m,
    sum(a.fg3_att) FILTER(WHERE a.type_lineup=n.own_tl) fg3a,
    sum((a.play_type='FTM')::int) FILTER(WHERE a.type_lineup=n.own_tl) ftm,
    sum(a.ft_attempts) FILTER(WHERE a.type_lineup=n.own_tl) fta
  FROM acts a CROSS JOIN normalized n
  GROUP BY a.team_id
), usage AS (
  -- Possessions and true-shooting attempts as the ratings/four-factors
  -- readers count them: every row of the side, credited or not. A game counts
  -- when an offensive or defensive action survives the filters, as
  -- get_team_ratings_direct's games_played does (a substitution alone does not).
  SELECT a.team_id,count(DISTINCT a.game_id) FILTER(WHERE a.type_lineup IS NOT NULL)::integer gp,
    sum(a.possession_flag) FILTER(WHERE a.type_lineup=n.own_tl) poss,
    sum(a.ts_possessions) FILTER(WHERE a.type_lineup=n.own_tl) ts_poss
  FROM acts a CROSS JOIN normalized n
  GROUP BY a.team_id
), segments AS (
  -- Floor time inside a window: the event span per segment, as in
  -- get_team_minutes_direct, rounded once below.
  SELECT game_id,team_id,segment_id,
    greatest(max(event_elapsed_seconds)-min(event_elapsed_seconds),0::numeric) seconds
  FROM acts WHERE segment_id IS NOT NULL AND event_elapsed_seconds IS NOT NULL
  GROUP BY game_id,team_id,segment_id
), floor_time AS (
  SELECT team_id,round(sum(seconds)/60.0,1) minutes FROM segments GROUP BY team_id
), names AS (
  SELECT team_id,max(team_name) team_name FROM games_filtered GROUP BY team_id
)
SELECT u.team_id,nm.team_name,u.gp,
  coalesce(u.poss,0)::integer,coalesce(ft.minutes,0)::numeric,
  coalesce(b.pts,0)::integer,(coalesce(b.oreb,0)+coalesce(b.dreb,0))::integer,
  coalesce(b.oreb,0)::integer,coalesce(b.dreb,0)::integer,
  coalesce(b.ast,0)::integer,coalesce(b.stl,0)::integer,coalesce(b.blk,0)::integer,
  NULL::integer,coalesce(b.tov,0)::integer,
  coalesce(b.fgm,0)::integer,coalesce(b.fga,0)::integer,
  (coalesce(b.fgm,0)-coalesce(b.fg3m,0))::integer,(coalesce(b.fga,0)-coalesce(b.fg3a,0))::integer,
  coalesce(b.fg3m,0)::integer,coalesce(b.fg3a,0)::integer,
  coalesce(b.ftm,0)::integer,coalesce(b.fta,0)::integer,
  round(100.0*b.fgm/nullif(b.fga,0),1),
  round(100.0*(b.fgm-b.fg3m)/nullif(b.fga-b.fg3a,0),1),
  round(100.0*b.fg3m/nullif(b.fg3a,0),1),
  round(100.0*b.ftm/nullif(b.fta,0),1),
  round(100.0*(b.fgm+0.5*b.fg3m)/nullif(b.fga,0),1),
  round(100.0*b.pts/nullif(2*u.ts_poss,0),1)
FROM usage u
JOIN names nm ON nm.team_id=u.team_id
LEFT JOIN box b ON b.team_id=u.team_id
LEFT JOIN floor_time ft ON ft.team_id=u.team_id
WHERE u.gp>0
ORDER BY coalesce(b.pts,0) DESC,coalesce(ft.minutes,0) DESC,nm.team_name
$function$;

REVOKE ALL ON FUNCTION euroleague.get_team_traditional_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION euroleague.get_team_traditional_direct(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_team_traditional_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,text) TO app_readonly;
GRANT EXECUTE ON FUNCTION euroleague.get_team_traditional_direct(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,text) TO app_readonly;

COMMIT;
