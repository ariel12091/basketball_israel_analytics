-- compute_mv_lineup_totals_by_day(p_game_ids): the rows of basketball_test.mv_lineup_totals_by_day.
--
-- The single definition of that table (refreshed by refresh_mv_lineup_totals_by_day_for_games()).
-- Rows depend only on their own game (every window partitions by game_id), so one
-- game's rows can be rebuilt alone. NULL = every game.
-- The query is the materialized view's definition as it stood on 2026-11-14
-- (pg_get_viewdef) with only the game filter added;
-- scripts/apply_il_per_game_tables.R proved it returns the view's rows exactly.
CREATE OR REPLACE FUNCTION basketball_test.compute_mv_lineup_totals_by_day(p_game_ids int4[])
RETURNS TABLE (team_id integer, lineup_hash text, type_lineup text, g_date date, game_id integer, game_year integer, opp_starters integer, total_poss bigint, total_pts numeric, fg2_made bigint, fg2_att bigint, fg3_made bigint, fg3_att bigint, num_starters integer, minutes numeric)
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = force_custom_plan
AS $function$
#variable_conflict use_column
BEGIN
  RETURN QUERY
   WITH base AS (
           SELECT d.team_id,
              d.lineup_hash,
              d.type_lineup,
              d.game_id,
              d.segment_id,
              d.opp_starters,
              d.num_starters,
              d.credit_lineup_hash,
              d.credit_opp_starters,
              d.credit_num_starters,
              d.id,
              d.event_elapsed_seconds,
              d.segment_start_elapsed_seconds,
              d.segment_seconds,
              d.final_end_poss,
              d.team_score,
              d.type,
              d.parameters_points,
              d.parameters_made,
              row_number() OVER (PARTITION BY d.team_id, d.lineup_hash, d.game_id, d.segment_id ORDER BY d.id) - row_number() OVER (PARTITION BY d.team_id, d.lineup_hash, d.game_id, d.segment_id, d.opp_starters ORDER BY d.id) AS opp_island
             FROM basketball_test.df_pts_poss_lineups_longer_mv d
            WHERE p_game_ids IS NULL OR d.game_id = ANY (p_game_ids)
          ), window_bounds AS (
           SELECT b.team_id,
              b.lineup_hash,
              b.game_id,
              b.segment_id,
              b.opp_starters,
              b.opp_island,
              min(b.id) AS first_id,
              (array_agg(b.event_elapsed_seconds ORDER BY b.id) FILTER (WHERE b.event_elapsed_seconds IS NOT NULL))[1] AS first_event_elapsed_seconds,
              COALESCE(max(b.segment_start_elapsed_seconds), min(b.event_elapsed_seconds), 0::numeric) AS segment_start_elapsed_seconds,
              max(b.segment_seconds) AS segment_seconds
             FROM base b
            WHERE b.segment_seconds IS NOT NULL
            GROUP BY b.team_id, b.lineup_hash, b.game_id, b.segment_id, b.opp_starters, b.opp_island
          ), ordered_windows AS (
           SELECT wb.team_id,
              wb.lineup_hash,
              wb.game_id,
              wb.segment_id,
              wb.opp_starters,
              wb.opp_island,
              wb.first_id,
              wb.first_event_elapsed_seconds,
              wb.segment_start_elapsed_seconds,
              wb.segment_seconds,
              row_number() OVER (PARTITION BY wb.team_id, wb.lineup_hash, wb.game_id, wb.segment_id ORDER BY wb.first_id) AS window_number
             FROM window_bounds wb
          ), window_start_candidates AS (
           SELECT ow.team_id,
              ow.lineup_hash,
              ow.game_id,
              ow.segment_id,
              ow.opp_starters,
              ow.opp_island,
              ow.first_id,
              ow.first_event_elapsed_seconds,
              ow.segment_start_elapsed_seconds,
              ow.segment_seconds,
              ow.window_number,
                  CASE
                      WHEN ow.window_number = 1 THEN ow.segment_start_elapsed_seconds
                      ELSE GREATEST(ow.segment_start_elapsed_seconds, LEAST(ow.segment_start_elapsed_seconds + ow.segment_seconds, COALESCE(ow.first_event_elapsed_seconds, ow.segment_start_elapsed_seconds)))
                  END AS window_start_candidate
             FROM ordered_windows ow
          ), normalized_window_starts AS (
           SELECT wsc.team_id,
              wsc.lineup_hash,
              wsc.game_id,
              wsc.segment_id,
              wsc.opp_starters,
              wsc.opp_island,
              wsc.first_id,
              wsc.first_event_elapsed_seconds,
              wsc.segment_start_elapsed_seconds,
              wsc.segment_seconds,
              wsc.window_number,
              wsc.window_start_candidate,
              max(wsc.window_start_candidate) OVER (PARTITION BY wsc.team_id, wsc.lineup_hash, wsc.game_id, wsc.segment_id ORDER BY wsc.first_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS window_start
             FROM window_start_candidates wsc
          ), window_times AS (
           SELECT nws.team_id,
              nws.lineup_hash,
              nws.game_id,
              nws.segment_id,
              nws.opp_starters,
              nws.opp_island,
              GREATEST(COALESCE(lead(nws.window_start) OVER (PARTITION BY nws.team_id, nws.lineup_hash, nws.game_id, nws.segment_id ORDER BY nws.first_id), nws.segment_start_elapsed_seconds + nws.segment_seconds) - nws.window_start, 0::numeric) AS window_seconds
             FROM normalized_window_starts nws
          ), window_minutes AS (
           SELECT wt.team_id,
              wt.lineup_hash,
              wt.game_id,
              wt.opp_starters,
              sum(wt.window_seconds) / 60.0 AS minutes
             FROM window_times wt
            GROUP BY wt.team_id, wt.lineup_hash, wt.game_id, wt.opp_starters
          ), day_stats AS (
           SELECT b.team_id,
              b.credit_lineup_hash AS lineup_hash,
              b.type_lineup,
              s.game_date AS g_date,
              b.game_id,
              s.game_year,
              b.credit_opp_starters AS opp_starters,
              max(b.credit_num_starters) AS num_starters,
              sum(
                  CASE
                      WHEN COALESCE(b.final_end_poss, false) THEN 1
                      ELSE 0
                  END) AS total_poss,
              COALESCE(sum(b.team_score), 0::numeric) AS total_pts,
              sum(
                  CASE
                      WHEN b.type = 'shot'::text AND b.parameters_points = 2::numeric AND b.parameters_made = 'made'::text THEN 1
                      ELSE 0
                  END) AS fg2_made,
              sum(
                  CASE
                      WHEN b.type = 'shot'::text AND b.parameters_points = 2::numeric THEN 1
                      ELSE 0
                  END) AS fg2_att,
              sum(
                  CASE
                      WHEN b.type = 'shot'::text AND b.parameters_points = 3::numeric AND b.parameters_made = 'made'::text THEN 1
                      ELSE 0
                  END) AS fg3_made,
              sum(
                  CASE
                      WHEN b.type = 'shot'::text AND b.parameters_points = 3::numeric THEN 1
                      ELSE 0
                  END) AS fg3_att
             FROM base b
               JOIN basketball_test.schedule s USING (game_id)
            GROUP BY b.team_id, b.credit_lineup_hash, b.type_lineup, s.game_date, b.game_id, s.game_year, b.credit_opp_starters
          ), slices AS (
           SELECT ds.team_id,
              ds.lineup_hash,
              ds.type_lineup,
              ds.g_date,
              ds.game_id,
              ds.game_year,
              ds.opp_starters,
              ds.num_starters,
              ds.total_poss,
              ds.total_pts,
              ds.fg2_made,
              ds.fg2_att,
              ds.fg3_made,
              ds.fg3_att,
              wm.minutes AS slice_minutes,
              wm.team_id IS NOT NULL AS has_window,
              count(*) FILTER (WHERE ds.type_lineup = 'offense'::text) OVER (PARTITION BY ds.team_id, ds.lineup_hash, ds.game_id, ds.opp_starters) AS offense_rows
             FROM day_stats ds
               LEFT JOIN window_minutes wm ON wm.team_id = ds.team_id AND wm.lineup_hash = ds.lineup_hash AND wm.game_id = ds.game_id AND NOT wm.opp_starters IS DISTINCT FROM ds.opp_starters
          ), slices_without_offense AS (
           SELECT sl.team_id,
              sl.lineup_hash,
              sl.game_id,
              sl.opp_starters,
              sl.slice_minutes AS minutes,
              max(sl.g_date) AS g_date,
              max(sl.game_year) AS game_year,
              max(sl.num_starters) AS num_starters
             FROM slices sl
            WHERE sl.has_window AND sl.offense_rows = 0
            GROUP BY sl.team_id, sl.lineup_hash, sl.game_id, sl.opp_starters, sl.slice_minutes
          )
   SELECT ds.team_id,
      ds.lineup_hash,
      ds.type_lineup,
      ds.g_date,
      ds.game_id,
      ds.game_year,
      ds.opp_starters,
      ds.total_poss,
      ds.total_pts,
      ds.fg2_made,
      ds.fg2_att,
      ds.fg3_made,
      ds.fg3_att,
      ds.num_starters,
          CASE
              WHEN ds.type_lineup = 'offense'::text THEN ds.slice_minutes
              ELSE NULL::numeric
          END AS minutes
     FROM slices ds
  UNION ALL
   SELECT swo.team_id,
      swo.lineup_hash,
      'offense'::text AS type_lineup,
      swo.g_date,
      swo.game_id,
      swo.game_year,
      swo.opp_starters,
      0::bigint AS total_poss,
      0::numeric AS total_pts,
      0::bigint AS fg2_made,
      0::bigint AS fg2_att,
      0::bigint AS fg3_made,
      0::bigint AS fg3_att,
      swo.num_starters,
      swo.minutes
     FROM slices_without_offense swo;
END;
$function$;
