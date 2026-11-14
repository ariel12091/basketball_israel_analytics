-- compute_lineup_four_factors_by_game(p_game_ids): the rows of basketball_test.lineup_four_factors_by_game.
--
-- The single definition of that table (refreshed by refresh_lineup_four_factors_by_game_for_games()).
-- Rows depend only on their own game (every window partitions by game_id), so one
-- game's rows can be rebuilt alone. NULL = every game.
-- The query is the materialized view's definition as it stood on 2026-11-14
-- (pg_get_viewdef) with only the game filter added;
-- scripts/apply_il_per_game_tables.R proved it returns the view's rows exactly.
CREATE OR REPLACE FUNCTION basketball_test.compute_lineup_four_factors_by_game(p_game_ids int4[])
RETURNS TABLE (lineup_hash text, team_id integer, game_id integer, game_year integer, type_lineup text, num_starters integer, opp_starters integer, total_points numeric, total_poss bigint, ts_poss_count bigint, oreb_count bigint, oreb_opportunities bigint, tov_count bigint, total_ft_attempts bigint, total_fga bigint, total_fgm bigint, total_fg3_made bigint, minutes numeric)
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = force_custom_plan
AS $function$
#variable_conflict use_column
BEGIN
  RETURN QUERY
   WITH base AS (
           SELECT d.id,
              d.game_id,
              d.lineup_hash,
              d.team_id,
              d.team_score,
              d.type,
              d.parameters_type,
              d.parameters_made,
              d.parameters_points,
              d.pct_ft,
              d.parent_action_id,
              d.type_lineup,
              d.num_starters,
              d.opp_starters,
              d.segment_id,
              d.credit_lineup_hash,
              d.credit_num_starters,
              d.credit_opp_starters,
              d.credit_segment_id,
              d.end_game_seconds_remaining,
              d.event_elapsed_seconds,
              d.segment_start_elapsed_seconds,
              d.segment_seconds,
                  CASE
                      WHEN d.final_end_poss IS TRUE THEN 1
                      ELSE 0
                  END AS final_end_flag,
              row_number() OVER (PARTITION BY d.team_id, d.lineup_hash, d.game_id, d.segment_id ORDER BY d.id) - row_number() OVER (PARTITION BY d.team_id, d.lineup_hash, d.game_id, d.segment_id, d.opp_starters ORDER BY d.id) AS opp_island
             FROM basketball_test.df_pts_poss_lineups_longer_mv d
            WHERE p_game_ids IS NULL OR d.game_id = ANY (p_game_ids)
          ), complex_flags AS (
           SELECT DISTINCT ON (d.id) d.id AS main_id,
              t2.type AS parent_type,
              t2.parameters_type AS parent_param
             FROM basketball_test.df_pts_poss_lineups_longer_mv d
               JOIN basketball_test.df_pts_poss_lineups_longer_mv t2 ON t2.id = d.parent_action_id AND t2.game_id = d.game_id AND t2.type = 'foul'::text
            WHERE d.parent_action_id IS NOT NULL AND (p_game_ids IS NULL OR d.game_id = ANY (p_game_ids))
            ORDER BY d.id
          ), combined_data AS (
           SELECT b.credit_lineup_hash AS lineup_hash,
              b.team_id,
              b.game_id,
              s.game_year,
              b.type_lineup,
              b.credit_num_starters AS num_starters,
              b.credit_opp_starters AS opp_starters,
              b.credit_segment_id AS segment_id,
              b.end_game_seconds_remaining,
              b.team_score,
              b.final_end_flag,
              b.type,
              b.parameters_type,
              b.parameters_made,
              b.parameters_points,
              b.pct_ft,
              b.parent_action_id,
              cf.parent_type,
              cf.parent_param
             FROM base b
               JOIN basketball_test.schedule s ON b.game_id = s.game_id
               LEFT JOIN complex_flags cf ON b.id = cf.main_id
          ), window_bounds AS (
           SELECT b.lineup_hash,
              b.team_id,
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
            GROUP BY b.lineup_hash, b.team_id, b.game_id, b.segment_id, b.opp_starters, b.opp_island
          ), ordered_windows AS (
           SELECT wb.lineup_hash,
              wb.team_id,
              wb.game_id,
              wb.segment_id,
              wb.opp_starters,
              wb.opp_island,
              wb.first_id,
              wb.first_event_elapsed_seconds,
              wb.segment_start_elapsed_seconds,
              wb.segment_seconds,
              row_number() OVER (PARTITION BY wb.lineup_hash, wb.team_id, wb.game_id, wb.segment_id ORDER BY wb.first_id) AS window_number
             FROM window_bounds wb
          ), window_start_candidates AS (
           SELECT ow.lineup_hash,
              ow.team_id,
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
           SELECT wsc.lineup_hash,
              wsc.team_id,
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
              max(wsc.window_start_candidate) OVER (PARTITION BY wsc.lineup_hash, wsc.team_id, wsc.game_id, wsc.segment_id ORDER BY wsc.first_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS window_start
             FROM window_start_candidates wsc
          ), window_times AS (
           SELECT nws.lineup_hash,
              nws.team_id,
              nws.game_id,
              nws.segment_id,
              nws.opp_starters,
              nws.opp_island,
              GREATEST(COALESCE(lead(nws.window_start) OVER (PARTITION BY nws.lineup_hash, nws.team_id, nws.game_id, nws.segment_id ORDER BY nws.first_id), nws.segment_start_elapsed_seconds + nws.segment_seconds) - nws.window_start, 0::numeric) AS window_seconds
             FROM normalized_window_starts nws
          ), window_minutes AS (
           SELECT wt.lineup_hash,
              wt.team_id,
              wt.game_id,
              wt.opp_starters,
              sum(wt.window_seconds) / 60.0 AS minutes
             FROM window_times wt
            GROUP BY wt.lineup_hash, wt.team_id, wt.game_id, wt.opp_starters
          ), segment_stats AS (
           SELECT cd.lineup_hash,
              cd.team_id,
              cd.game_id,
              cd.game_year,
              cd.type_lineup,
              cd.num_starters,
              cd.opp_starters,
              cd.segment_id,
              sum(cd.team_score) AS total_points,
              sum(cd.final_end_flag) AS total_poss,
              count(
                  CASE
                      WHEN cd.type = 'shot'::text THEN 1
                      ELSE NULL::integer
                  END) + count(DISTINCT
                  CASE
                      WHEN cd.type = 'freeThrow'::text AND cd.parent_type = 'foul'::text AND cd.parent_param = 'personal'::text THEN cd.parent_action_id
                      ELSE NULL::integer
                  END) AS ts_poss_count,
              count(
                  CASE
                      WHEN cd.type = 'rebound'::text AND cd.parameters_type = 'offensive'::text THEN 1
                      ELSE NULL::integer
                  END) AS oreb_count,
              count(
                  CASE
                      WHEN cd.type = 'shot'::text AND (cd.parameters_made = ANY (ARRAY['missed'::text, 'blocked'::text])) THEN 1
                      WHEN cd.type = 'freeThrow'::text AND cd.parameters_made = 'missed'::text AND cd.pct_ft = 1::numeric AND cd.parent_type = 'foul'::text AND cd.parent_param = 'personal'::text THEN 1
                      ELSE NULL::integer
                  END) AS oreb_opportunities,
              count(
                  CASE
                      WHEN cd.type = 'turnover'::text THEN 1
                      ELSE NULL::integer
                  END) AS tov_count,
              count(
                  CASE
                      WHEN cd.type = 'freeThrow'::text THEN 1
                      ELSE NULL::integer
                  END) AS total_ft_attempts,
              count(
                  CASE
                      WHEN cd.type = 'shot'::text THEN 1
                      ELSE NULL::integer
                  END) AS total_fga,
              count(
                  CASE
                      WHEN cd.type = 'shot'::text AND cd.parameters_made = 'made'::text THEN 1
                      ELSE NULL::integer
                  END) AS total_fgm,
              count(
                  CASE
                      WHEN cd.type = 'shot'::text AND cd.parameters_made = 'made'::text AND cd.parameters_points = 3::numeric THEN 1
                      ELSE NULL::integer
                  END) AS total_fg3_made
             FROM combined_data cd
            GROUP BY cd.lineup_hash, cd.team_id, cd.game_id, cd.game_year, cd.type_lineup, cd.num_starters, cd.opp_starters, cd.segment_id
          ), slices_without_offense AS (
           SELECT wm.team_id,
              wm.lineup_hash,
              wm.game_id,
              wm.opp_starters,
              wm.minutes,
              max(ss.game_year) AS game_year,
              max(ss.num_starters) AS num_starters
             FROM window_minutes wm
               JOIN segment_stats ss ON ss.team_id = wm.team_id AND ss.lineup_hash = wm.lineup_hash AND ss.game_id = wm.game_id AND NOT ss.opp_starters IS DISTINCT FROM wm.opp_starters
            WHERE NOT (EXISTS ( SELECT 1
                     FROM segment_stats s2
                    WHERE s2.team_id = wm.team_id AND s2.lineup_hash = wm.lineup_hash AND s2.game_id = wm.game_id AND NOT s2.opp_starters IS DISTINCT FROM wm.opp_starters AND s2.type_lineup = 'offense'::text))
            GROUP BY wm.team_id, wm.lineup_hash, wm.game_id, wm.opp_starters, wm.minutes
          )
   SELECT ss.lineup_hash,
      ss.team_id,
      ss.game_id,
      ss.game_year,
      ss.type_lineup,
      ss.num_starters,
      ss.opp_starters,
      sum(ss.total_points) AS total_points,
      sum(ss.total_poss)::bigint AS total_poss,
      sum(ss.ts_poss_count)::bigint AS ts_poss_count,
      sum(ss.oreb_count)::bigint AS oreb_count,
      sum(ss.oreb_opportunities)::bigint AS oreb_opportunities,
      sum(ss.tov_count)::bigint AS tov_count,
      sum(ss.total_ft_attempts)::bigint AS total_ft_attempts,
      sum(ss.total_fga)::bigint AS total_fga,
      sum(ss.total_fgm)::bigint AS total_fgm,
      sum(ss.total_fg3_made)::bigint AS total_fg3_made,
          CASE
              WHEN ss.type_lineup = 'offense'::text THEN wm.minutes
              ELSE NULL::numeric
          END AS minutes
     FROM segment_stats ss
       LEFT JOIN window_minutes wm ON wm.lineup_hash = ss.lineup_hash AND wm.team_id = ss.team_id AND wm.game_id = ss.game_id AND NOT wm.opp_starters IS DISTINCT FROM ss.opp_starters
    GROUP BY ss.lineup_hash, ss.team_id, ss.game_id, ss.game_year, ss.type_lineup, ss.num_starters, ss.opp_starters, wm.minutes
  UNION ALL
   SELECT swo.lineup_hash,
      swo.team_id,
      swo.game_id,
      swo.game_year,
      'offense'::text AS type_lineup,
      swo.num_starters,
      swo.opp_starters,
      0::numeric AS total_points,
      0::bigint AS total_poss,
      0::bigint AS ts_poss_count,
      0::bigint AS oreb_count,
      0::bigint AS oreb_opportunities,
      0::bigint AS tov_count,
      0::bigint AS total_ft_attempts,
      0::bigint AS total_fga,
      0::bigint AS total_fgm,
      0::bigint AS total_fg3_made,
      swo.minutes
     FROM slices_without_offense swo;
END;
$function$;
