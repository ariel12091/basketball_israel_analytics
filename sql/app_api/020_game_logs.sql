-- Additive Game Logs readers. R/TypeScript owns derived rates and percentiles.
-- Each season result is one snapshot; the summary and FF facts retain their
-- own additive counts and minutes. Preaggregate BEFORE joining the two facts.
CREATE OR REPLACE FUNCTION app_api.game_logs_il(p_season integer)
RETURNS jsonb LANGUAGE sql STABLE
SET search_path = '' SET plan_cache_mode = force_custom_plan
AS $fn$
WITH schedule AS MATERIALIZED (
  SELECT * FROM basketball_test.final_schedule_mv WHERE game_year = p_season
), summary AS MATERIALIZED (
  SELECT game_id,team_id,type_lineup,num_starters,
    sum(total_poss) AS total_poss, sum(total_pts) AS total_pts,
    sum(fg2_made) AS fg2_made, sum(fg2_att) AS fg2_att,
    sum(fg3_made) AS fg3_made, sum(fg3_att) AS fg3_att, sum(minutes) AS minutes
  FROM basketball_test.mv_lineup_totals_by_day WHERE game_year = p_season
  GROUP BY game_id,team_id,type_lineup,num_starters
), ff AS MATERIALIZED (
  SELECT game_id,team_id,type_lineup,num_starters,
    sum(total_poss) AS ff_total_poss, sum(total_points) AS total_points,
    sum(ts_poss_count) AS ts_poss_count, sum(oreb_count) AS oreb_count,
    sum(oreb_opportunities) AS oreb_opportunities, sum(tov_count) AS tov_count,
    sum(total_ft_attempts) AS total_ft_attempts, sum(total_fga) AS total_fga,
    sum(total_fgm) AS total_fgm, sum(total_fg3_made) AS total_fg3_made,
    sum(minutes) AS ff_minutes
  FROM basketball_test.lineup_four_factors_by_game WHERE game_year = p_season
  GROUP BY game_id,team_id,type_lineup,num_starters
), rows AS MATERIALIZED (
  SELECT coalesce(a.game_id,b.game_id) AS game_id,
    coalesce(a.team_id,b.team_id) AS team_id,
    coalesce(a.type_lineup,b.type_lineup) AS type_lineup,
    coalesce(a.num_starters,b.num_starters) AS num_starters,
    a.total_poss,a.total_pts,a.fg2_made,a.fg2_att,a.fg3_made,a.fg3_att,a.minutes,
    b.ff_total_poss,b.total_points,b.ts_poss_count,b.oreb_count,b.oreb_opportunities,
    b.tov_count,b.total_ft_attempts,b.total_fga,b.total_fgm,b.total_fg3_made,b.ff_minutes
  FROM summary a FULL JOIN ff b ON a.game_id=b.game_id AND a.team_id=b.team_id
    AND a.type_lineup=b.type_lineup AND a.num_starters IS NOT DISTINCT FROM b.num_starters
), scoreless AS (
  SELECT game_id,team_id FROM basketball_test.df_pts_poss_lineups_longer_mv
  WHERE game_year=p_season GROUP BY game_id,team_id HAVING count(own_team_score)=0
), aggregate_rows AS (
  SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY game_id,team_id,type_lineup,num_starters),'[]'::jsonb) AS rows,
    count(*) AS row_count, coalesce(sum(total_poss),0) AS ctl_poss FROM rows r
)
SELECT jsonb_build_object('rows',a.rows,'row_count',a.row_count,'ctl_poss',a.ctl_poss,
  'schedule',(SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY game_id,team_id),'[]'::jsonb) FROM schedule s),
  'scoreless',(SELECT coalesce(jsonb_agg(to_jsonb(z)),'[]'::jsonb) FROM scoreless z),
  'state',p.state,'version',p.version)
FROM aggregate_rows a CROSS JOIN basketball_test.publication_status('il') p
$fn$;

CREATE OR REPLACE FUNCTION app_api.game_logs_euro(p_competition text,p_season integer)
RETURNS jsonb LANGUAGE sql STABLE
SET search_path = '' SET plan_cache_mode = force_custom_plan
AS $fn$
WITH rows AS MATERIALIZED (
  SELECT f.*,s.round_number,s.phase,s.game_date,s.opp_team_id,
    s.is_home,s.has_won,s.team_points,s.opp_points,s.team_name,s.opp_team_name,
    m.seconds/60.0 AS minutes
  FROM euroleague.team_four_factors_by_game f
  JOIN euroleague.final_schedule_mv s ON s.game_id=f.game_id AND s.team_id=f.team_id
  LEFT JOIN (
    SELECT game_id,team_id,own_starters,opp_starters,sum(seconds) AS seconds
    FROM euroleague.lineup_totals_by_game
    WHERE type_lineup='offense' AND competition=p_competition AND game_year=p_season
    GROUP BY game_id,team_id,own_starters,opp_starters
  ) m ON m.game_id=f.game_id AND m.team_id=f.team_id
    AND m.own_starters=f.own_starters AND m.opp_starters=f.opp_starters
  WHERE s.competition=p_competition AND s.game_year=p_season AND p_competition IN ('E','U')
), aggregate_rows AS (
  SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY team_id,game_date DESC,game_id DESC,own_starters,opp_starters),'[]'::jsonb) AS rows,
    count(*) AS row_count,coalesce(sum(off_poss),0) AS ctl_poss FROM rows r
)
SELECT jsonb_build_object('rows',a.rows,'row_count',a.row_count,'ctl_poss',a.ctl_poss,
  'scoreless','[]'::jsonb,'state',p.state,'version',p.version)
FROM aggregate_rows a CROSS JOIN basketball_test.publication_status(
  CASE p_competition WHEN 'E' THEN 'euroleague' WHEN 'U' THEN 'eurocup' ELSE '' END) p
$fn$;

CREATE OR REPLACE FUNCTION app_api.scoreless_games_il(p_season integer)
RETURNS TABLE(game_id integer,team_id integer) LANGUAGE sql STABLE SET search_path=''
AS $fn$
  SELECT game_id,team_id FROM basketball_test.df_pts_poss_lineups_longer_mv
  WHERE game_year=p_season GROUP BY game_id,team_id HAVING count(own_team_score)=0
$fn$;
REVOKE ALL ON FUNCTION app_api.game_logs_il(integer),app_api.game_logs_euro(text,integer),
  app_api.scoreless_games_il(integer) FROM PUBLIC,anon,authenticated;
