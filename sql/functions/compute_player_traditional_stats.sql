-- compute_player_traditional_stats(p_game_years): the rows of basketball_test.player_traditional_stats_mv.
--
-- The single definition of that table (refreshed by refresh_player_traditional_stats_for_games()).
-- Rows are per player-season (every aggregate groups by game_year), so one
-- season's rows can be rebuilt alone. NULL = every season.
-- The query is the materialized view's definition as it stood on 2026-11-14
-- (pg_get_viewdef) with only the season filter added;
-- scripts/apply_il_per_game_tables.R proved it returns the view's rows exactly.
CREATE OR REPLACE FUNCTION basketball_test.compute_player_traditional_stats(p_game_years int4[])
RETURNS TABLE (game_year integer, team_id integer, player_id integer, team_name text, firstname text, lastname text, player_name text, gp integer, poss_on_floor integer, minutes numeric(10,1), pts integer, reb integer, oreb integer, dreb integer, ast integer, stl integer, blk integer, dfl integer, tov integer, fgm integer, fga integer, "3pm" integer, "3pa" integer, ftm integer, fta integer, fg_pct numeric, tp_pct numeric, ft_pct numeric, efg numeric, ts numeric, usg_pct numeric)
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = force_custom_plan
AS $function$
#variable_conflict use_column
BEGIN
  RETURN QUERY
   WITH lineup_map AS (
           SELECT DISTINCT ll.game_id,
              ll.team_id,
              ll.lineup_hash,
              ll.player_id
             FROM basketball_test.lineups_lookup ll
            WHERE ll.game_year IS NOT NULL AND COALESCE(ll.is_on_verdict, 0::numeric)::integer = 1
              AND (p_game_years IS NULL OR ll.game_id IN ( SELECT fs0.game_id FROM basketball_test.final_schedule_mv fs0 WHERE fs0.game_year = ANY (p_game_years)))
          ), sched_year AS (
           SELECT DISTINCT fs.game_id,
              fs.game_year
             FROM basketball_test.final_schedule_mv fs
            WHERE p_game_years IS NULL OR fs.game_year = ANY (p_game_years)
          ), actions_with_year AS (
           SELECT d.id,
              d.game_id,
              d.team_id,
              d.lineup_hash,
              d.credit_lineup_hash,
              d.segment_id,
              d.end_game_seconds_remaining,
              d.segment_seconds,
              d.type,
              d.parameters_type,
              d.parameters_made,
              d.parameters_points,
              d.player_id,
              d.event_owner_side,
              d.type_lineup,
              d.final_end_poss,
              d.final_end_id,
              d.parent_action_id,
              d.pct_ft,
              sy.game_year
             FROM basketball_test.df_pts_poss_lineups_longer_mv d
               JOIN sched_year sy ON sy.game_id = d.game_id
          ), complex_flags AS (
           SELECT DISTINCT ON (awy.id, awy.game_id) awy.id AS main_id,
              awy.game_id,
              t2.type AS parent_type,
              t2.parameters_type AS parent_param
             FROM actions_with_year awy
               JOIN basketball_test.df_pts_poss_lineups_longer_mv t2 ON t2.id = awy.parent_action_id AND t2.game_id = awy.game_id AND t2.type = 'foul'::text
            WHERE awy.parent_action_id IS NOT NULL
            ORDER BY awy.id, awy.game_id
          ), actions_enriched AS (
           SELECT awy.id,
              awy.game_id,
              awy.team_id,
              awy.lineup_hash,
              awy.credit_lineup_hash,
              awy.segment_id,
              awy.end_game_seconds_remaining,
              awy.segment_seconds,
              awy.type,
              awy.parameters_type,
              awy.parameters_made,
              awy.parameters_points,
              awy.player_id,
              awy.event_owner_side,
              awy.type_lineup,
              awy.final_end_poss,
              awy.final_end_id,
              awy.parent_action_id,
              awy.pct_ft,
              awy.game_year,
              cf.parent_type,
              cf.parent_param
             FROM actions_with_year awy
               LEFT JOIN complex_flags cf ON cf.main_id = awy.id AND cf.game_id = awy.game_id
          ), box_totals AS (
           SELECT awy.game_year,
              awy.team_id,
              awy.player_id,
              sum(
                  CASE
                      WHEN awy.type = 'shot'::text AND awy.parameters_made = 'made'::text AND awy.type_lineup = 'offense'::text THEN COALESCE(awy.parameters_points, 0::numeric)
                      ELSE 0::numeric
                  END) + sum(
                  CASE
                      WHEN awy.type = 'freeThrow'::text AND awy.parameters_made = 'made'::text AND awy.type_lineup = 'offense'::text THEN 1
                      ELSE 0
                  END)::numeric AS pts,
              sum(
                  CASE
                      WHEN awy.type = 'rebound'::text AND awy.type_lineup = 'offense'::text AND awy.parameters_type = 'offensive'::text THEN 1
                      ELSE 0
                  END) AS oreb,
              sum(
                  CASE
                      WHEN awy.type = 'rebound'::text AND awy.type_lineup = 'defense'::text AND awy.parameters_type = 'defensive'::text THEN 1
                      ELSE 0
                  END) AS dreb,
              sum(
                  CASE
                      WHEN awy.type = 'assist'::text AND awy.type_lineup = 'offense'::text THEN 1
                      ELSE 0
                  END) AS ast,
              sum(
                  CASE
                      WHEN awy.type = 'steal'::text AND awy.type_lineup = 'defense'::text THEN 1
                      ELSE 0
                  END) AS stl,
              sum(
                  CASE
                      WHEN awy.type = 'block'::text AND awy.type_lineup = 'defense'::text THEN 1
                      ELSE 0
                  END) AS blk,
              sum(
                  CASE
                      WHEN awy.type = 'deflection'::text AND awy.type_lineup = 'defense'::text THEN 1
                      ELSE 0
                  END) AS dfl,
              sum(
                  CASE
                      WHEN awy.type = 'turnover'::text AND awy.type_lineup = 'offense'::text THEN 1
                      ELSE 0
                  END) AS tov,
              sum(
                  CASE
                      WHEN awy.type = 'shot'::text AND awy.parameters_made = 'made'::text AND awy.type_lineup = 'offense'::text THEN 1
                      ELSE 0
                  END) AS fgm,
              sum(
                  CASE
                      WHEN awy.type = 'shot'::text AND awy.type_lineup = 'offense'::text THEN 1
                      ELSE 0
                  END) AS fga,
              sum(
                  CASE
                      WHEN awy.type = 'shot'::text AND awy.parameters_made = 'made'::text AND awy.parameters_points = 3::numeric AND awy.type_lineup = 'offense'::text THEN 1
                      ELSE 0
                  END) AS "3pm",
              sum(
                  CASE
                      WHEN awy.type = 'shot'::text AND awy.parameters_points = 3::numeric AND awy.type_lineup = 'offense'::text THEN 1
                      ELSE 0
                  END) AS "3pa",
              sum(
                  CASE
                      WHEN awy.type = 'freeThrow'::text AND awy.parameters_made = 'made'::text AND awy.type_lineup = 'offense'::text THEN 1
                      ELSE 0
                  END) AS ftm,
              sum(
                  CASE
                      WHEN awy.type = 'freeThrow'::text AND awy.type_lineup = 'offense'::text THEN 1
                      ELSE 0
                  END) AS fta,
              count(
                  CASE
                      WHEN awy.type = 'shot'::text AND awy.type_lineup = 'offense'::text THEN 1
                      ELSE NULL::integer
                  END) + count(DISTINCT
                  CASE
                      WHEN awy.type = 'freeThrow'::text AND awy.type_lineup = 'offense'::text AND awy.parent_type = 'foul'::text AND awy.parent_param = 'personal'::text THEN awy.parent_action_id
                      ELSE NULL::integer
                  END) AS ts_poss_count
             FROM actions_enriched awy
            WHERE awy.player_id IS NOT NULL AND awy.player_id > 0
            GROUP BY awy.game_year, awy.team_id, awy.player_id
          ), team_usage_totals AS (
           SELECT awy.game_year,
              awy.team_id,
              count(
                  CASE
                      WHEN awy.type = 'shot'::text AND awy.type_lineup = 'offense'::text THEN 1
                      ELSE NULL::integer
                  END) + count(DISTINCT
                  CASE
                      WHEN awy.type = 'freeThrow'::text AND awy.type_lineup = 'offense'::text AND awy.parent_type = 'foul'::text AND awy.parent_param = 'personal'::text THEN awy.parent_action_id
                      ELSE NULL::integer
                  END) AS team_ts_poss_count,
              sum(
                  CASE
                      WHEN awy.type = 'turnover'::text AND awy.type_lineup = 'offense'::text THEN 1
                      ELSE 0
                  END) AS team_tov
             FROM actions_enriched awy
            GROUP BY awy.game_year, awy.team_id
          ), poss_end AS (
           SELECT DISTINCT awy.game_year,
              awy.game_id,
              awy.team_id,
              awy.lineup_hash AS floor_lineup_hash,
              awy.credit_lineup_hash AS lineup_hash,
              awy.id AS poss_end_id
             FROM actions_with_year awy
            WHERE awy.type_lineup = 'offense'::text AND awy.final_end_poss AND awy.id IS NOT NULL AND awy.credit_lineup_hash IS NOT NULL
          ), usage_totals AS (
           SELECT game_year,
              team_id,
              player_id,
              COALESCE(f.gp, 0::bigint) AS gp,
              COALESCE(c.poss_on_floor, 0::bigint) AS poss_on_floor
             FROM ( SELECT pe.game_year,
                      lm.team_id,
                      lm.player_id,
                      count(DISTINCT ROW(pe.game_id, pe.team_id, pe.poss_end_id)) AS poss_on_floor
                     FROM poss_end pe
                       JOIN lineup_map lm ON lm.game_id = pe.game_id AND lm.team_id = pe.team_id AND lm.lineup_hash = pe.lineup_hash
                    GROUP BY pe.game_year, lm.team_id, lm.player_id) c
               FULL JOIN ( SELECT pe.game_year,
                      lm.team_id,
                      lm.player_id,
                      count(DISTINCT pe.game_id) AS gp
                     FROM poss_end pe
                       JOIN lineup_map lm ON lm.game_id = pe.game_id AND lm.team_id = pe.team_id AND lm.lineup_hash = pe.floor_lineup_hash
                    GROUP BY pe.game_year, lm.team_id, lm.player_id) f USING (game_year, team_id, player_id)
          ), team_possession_totals AS (
           SELECT pe.game_year,
              pe.team_id,
              count(DISTINCT ROW(pe.game_id, pe.team_id, pe.poss_end_id)) AS team_poss
             FROM poss_end pe
            GROUP BY pe.game_year, pe.team_id
          ), segment_times AS (
           SELECT awy.game_year,
              awy.game_id,
              awy.team_id,
              awy.lineup_hash,
              awy.segment_id,
              max(awy.segment_seconds) AS seg_seconds
             FROM actions_with_year awy
            WHERE awy.lineup_hash IS NOT NULL AND awy.segment_id IS NOT NULL AND awy.segment_seconds IS NOT NULL
            GROUP BY awy.game_year, awy.game_id, awy.team_id, awy.lineup_hash, awy.segment_id
          ), minutes_totals AS (
           SELECT st.game_year,
              lm.team_id,
              lm.player_id,
              round(sum(st.seg_seconds) / 60.0, 1) AS minutes
             FROM segment_times st
               JOIN lineup_map lm ON lm.game_id = st.game_id AND lm.team_id = st.team_id AND lm.lineup_hash = st.lineup_hash
            GROUP BY st.game_year, lm.team_id, lm.player_id
          ), roster_names AS (
           SELECT fr.game_year,
              fr.team_id,
              fr.player_id,
              max(fr.team_name) AS team_name,
              min(NULLIF(regexp_replace(TRIM(BOTH FROM COALESCE(fr.firstname, ''::text)), '\\s*\\.\\s*'::text, '.'::text, 'g'::text), ''::text)) AS firstname,
              min(NULLIF(regexp_replace(TRIM(BOTH FROM COALESCE(fr.lastname, ''::text)), '\\s*\\.\\s*'::text, '.'::text, 'g'::text), ''::text)) AS lastname,
              TRIM(BOTH FROM (COALESCE(min(NULLIF(regexp_replace(TRIM(BOTH FROM COALESCE(fr.firstname, ''::text)), '\\s*\\.\\s*'::text, '.'::text, 'g'::text), ''::text)), ''::text) || ' '::text) || COALESCE(min(NULLIF(regexp_replace(TRIM(BOTH FROM COALESCE(fr.lastname, ''::text)), '\\s*\\.\\s*'::text, '.'::text, 'g'::text), ''::text)), ''::text)) AS player_name
             FROM basketball_test.full_rosters fr
            WHERE p_game_years IS NULL OR fr.game_year = ANY (p_game_years)
            GROUP BY fr.game_year, fr.team_id, fr.player_id
          )
   SELECT bt.game_year,
      bt.team_id,
      bt.player_id,
      rn.team_name,
      rn.firstname,
      rn.lastname,
      rn.player_name,
      COALESCE(ut.gp, 0::bigint)::integer AS gp,
      COALESCE(ut.poss_on_floor, 0::bigint)::integer AS poss_on_floor,
      COALESCE(mt.minutes, 0::numeric)::numeric(10,1) AS minutes,
      bt.pts::integer AS pts,
      (bt.oreb + bt.dreb)::integer AS reb,
      bt.oreb::integer AS oreb,
      bt.dreb::integer AS dreb,
      bt.ast::integer AS ast,
      bt.stl::integer AS stl,
      bt.blk::integer AS blk,
      bt.dfl::integer AS dfl,
      bt.tov::integer AS tov,
      bt.fgm::integer AS fgm,
      bt.fga::integer AS fga,
      bt."3pm"::integer AS "3pm",
      bt."3pa"::integer AS "3pa",
      bt.ftm::integer AS ftm,
      bt.fta::integer AS fta,
          CASE
              WHEN bt.fga > 0 THEN round(bt.fgm::numeric / bt.fga::numeric * 100::numeric, 1)
              ELSE NULL::numeric
          END AS fg_pct,
          CASE
              WHEN bt."3pa" > 0 THEN round(bt."3pm"::numeric / bt."3pa"::numeric * 100::numeric, 1)
              ELSE NULL::numeric
          END AS tp_pct,
          CASE
              WHEN bt.fta > 0 THEN round(bt.ftm::numeric / bt.fta::numeric * 100::numeric, 1)
              ELSE NULL::numeric
          END AS ft_pct,
          CASE
              WHEN bt.fga > 0 THEN round((bt.fgm::numeric + 0.5 * bt."3pm"::numeric) / bt.fga::numeric * 100::numeric, 1)
              ELSE NULL::numeric
          END AS efg,
          CASE
              WHEN bt.ts_poss_count > 0 THEN round(bt.pts / (2 * bt.ts_poss_count)::numeric * 100::numeric, 1)
              ELSE NULL::numeric
          END AS ts,
          CASE
              WHEN (bt.ts_poss_count + bt.tov) > 0 AND (tut.team_ts_poss_count + tut.team_tov) > 0 AND COALESCE(ut.poss_on_floor, 0::bigint) > 0 AND COALESCE(tpt.team_poss, 0::bigint) > 0 THEN round(100.0 * (bt.ts_poss_count + bt.tov)::numeric * tpt.team_poss::numeric / NULLIF((tut.team_ts_poss_count + tut.team_tov)::numeric * ut.poss_on_floor::numeric, 0::numeric), 1)
              ELSE NULL::numeric
          END AS usg_pct
     FROM box_totals bt
       LEFT JOIN team_usage_totals tut ON tut.game_year = bt.game_year AND tut.team_id = bt.team_id
       LEFT JOIN usage_totals ut ON ut.game_year = bt.game_year AND ut.team_id = bt.team_id AND ut.player_id = bt.player_id
       LEFT JOIN team_possession_totals tpt ON tpt.game_year = bt.game_year AND tpt.team_id = bt.team_id
       LEFT JOIN minutes_totals mt ON mt.game_year = bt.game_year AND mt.team_id = bt.team_id AND mt.player_id = bt.player_id
       LEFT JOIN roster_names rn ON rn.game_year = bt.game_year AND rn.team_id = bt.team_id AND rn.player_id = bt.player_id
    WHERE COALESCE(ut.gp, 0::bigint) > 0 OR COALESCE(ut.poss_on_floor, 0::bigint) > 0 OR COALESCE(mt.minutes, 0::numeric) > 0::numeric
    ORDER BY bt.game_year DESC, bt.pts DESC, rn.team_name, rn.lastname, rn.firstname;
END;
$function$;
