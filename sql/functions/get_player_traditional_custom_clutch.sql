DROP FUNCTION IF EXISTS basketball_test.get_player_traditional_custom_clutch(
  INT, DATE, DATE, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, INT, TEXT,
  INT, TEXT, INT, BOOLEAN, INT, INT, INT
);

CREATE OR REPLACE FUNCTION basketball_test.get_player_traditional_custom_clutch(
  p_game_year int,
  p_start_date date DEFAULT NULL,
  p_end_date date DEFAULT NULL,
  p_team_ids_csv text DEFAULT NULL,
  p_game_type_csv text DEFAULT NULL,
  p_opp_team_ids_csv text DEFAULT NULL,
  p_home_away text DEFAULT 'all',
  p_outcome text DEFAULT 'all',
  p_opp_rank_side text DEFAULT 'all',
  p_opp_rank_n int DEFAULT NULL,
  p_opp_rank_metric text DEFAULT 'net',
  p_max_margin int DEFAULT NULL,
  p_margin_status text DEFAULT 'all',
  p_max_time_remaining int DEFAULT NULL,
  p_ot_margin_filter boolean DEFAULT FALSE,
  p_min_gn int DEFAULT NULL,
  p_max_gn int DEFAULT NULL,
  p_last_n_games int DEFAULT NULL
)
RETURNS TABLE (
  player_id int, team_id int, team_name text, player_name text,
  gp int, poss_on_floor int, minutes numeric,
  pts int, reb int, oreb int, dreb int, ast int, stl int, blk int, dfl int, tov int,
  fgm int, fga int, "3pm" int, "3pa" int, ftm int, fta int,
  fg_pct numeric, tp_pct numeric, ft_pct numeric, efg numeric, ts numeric, usg_pct numeric
)
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = force_custom_plan
AS $$
DECLARE
  v_team_ids int4[];
  v_game_types int4[];
  v_opp_ids int4[];
  v_home_away text := COALESCE(NULLIF(btrim(p_home_away), ''), 'all');
  v_outcome text := COALESCE(NULLIF(btrim(p_outcome), ''), 'all');
  v_opp_rank_side text := COALESCE(NULLIF(btrim(p_opp_rank_side), ''), 'all');
  v_opp_rank_metric text := COALESCE(NULLIF(btrim(p_opp_rank_metric), ''), 'net');
  v_margin_status text := COALESCE(NULLIF(btrim(p_margin_status), ''), 'all');
BEGIN
  IF p_max_margin IS NULL AND p_max_time_remaining IS NULL
     AND v_margin_status = 'all' THEN
    RAISE EXCEPTION 'get_player_traditional_custom_clutch requires a clutch predicate';
  END IF;

  IF p_team_ids_csv IS NOT NULL AND length(btrim(p_team_ids_csv)) > 0 THEN
    v_team_ids := ARRAY(
      SELECT DISTINCT x::int4
      FROM unnest(string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> '' ORDER BY 1
    );
  END IF;
  IF p_game_type_csv IS NOT NULL AND length(btrim(p_game_type_csv)) > 0 THEN
    v_game_types := ARRAY(
      SELECT DISTINCT x::int4
      FROM unnest(string_to_array(regexp_replace(p_game_type_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> '' ORDER BY 1
    );
  END IF;
  IF p_opp_team_ids_csv IS NOT NULL AND length(btrim(p_opp_team_ids_csv)) > 0 THEN
    v_opp_ids := ARRAY(
      SELECT DISTINCT x::int4
      FROM unnest(string_to_array(regexp_replace(p_opp_team_ids_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> '' ORDER BY 1
    );
  END IF;

  RETURN QUERY
  WITH schedule_ranked AS (
    SELECT fs.game_id, fs.team_id, fs.game_year,
           ROW_NUMBER() OVER (
             PARTITION BY fs.team_id, fs.game_year
             ORDER BY fs.game_date DESC NULLS LAST, fs.game_id DESC
           ) AS rn_recent
    FROM basketball_test.final_schedule_mv fs
    WHERE fs.game_year = p_game_year
  ),
  games_base AS (
    SELECT fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id
    FROM basketball_test.final_schedule_mv fs
    JOIN schedule_ranked sr
      ON sr.game_id = fs.game_id AND sr.team_id = fs.team_id AND sr.game_year = fs.game_year
    WHERE fs.game_year = p_game_year
      AND (p_start_date IS NULL OR fs.game_date >= p_start_date)
      AND (p_end_date IS NULL OR fs.game_date <= p_end_date)
      AND (v_team_ids IS NULL OR fs.team_id = ANY(v_team_ids))
      AND (v_game_types IS NULL OR fs.game_type = ANY(v_game_types))
      AND (v_opp_ids IS NULL OR fs.opp_team_id = ANY(v_opp_ids))
      AND (v_home_away = 'all' OR (v_home_away = 'home' AND fs.is_home)
           OR (v_home_away = 'away' AND NOT fs.is_home))
      AND (v_outcome = 'all' OR (v_outcome = 'win' AND fs.has_won IS TRUE)
           OR (v_outcome = 'loss' AND fs.has_won IS FALSE))
      AND (p_min_gn IS NULL OR fs.gn >= p_min_gn)
      AND (p_max_gn IS NULL OR fs.gn <= p_max_gn)
      AND (p_last_n_games IS NULL OR sr.rn_recent <= p_last_n_games)
  ),
  games_ranked AS (
    SELECT gb.*,
      CASE WHEN v_opp_rank_side IN ('top', 'bottom') THEN
        CASE v_opp_rank_metric WHEN 'off' THEN r.rank_off_ppp
             WHEN 'def' THEN r.rank_def_ppp ELSE r.rank_net_rtg END
      END AS opp_rank,
      CASE WHEN v_opp_rank_side = 'bottom' THEN
        MAX(CASE v_opp_rank_metric WHEN 'off' THEN r.rank_off_ppp
                 WHEN 'def' THEN r.rank_def_ppp ELSE r.rank_net_rtg END)
        OVER (PARTITION BY gb.game_year)
      END AS max_rank
    FROM games_base gb
    LEFT JOIN basketball_test.team_ppp_ratings_mv r
      ON r.game_year::int4 = gb.game_year AND r.team_id::int4 = gb.opp_team_id
     AND v_opp_rank_side IN ('top', 'bottom')
  ),
  games_filtered AS MATERIALIZED (
    SELECT gr.game_id, gr.team_id, gr.game_year
    FROM games_ranked gr
    WHERE v_opp_rank_side = 'all' OR p_opp_rank_n IS NULL
       OR (v_opp_rank_side = 'top' AND gr.opp_rank <= p_opp_rank_n)
       OR (v_opp_rank_side = 'bottom' AND gr.opp_rank >= gr.max_rank - p_opp_rank_n + 1)
  ),
  acts AS MATERIALIZED (
    SELECT a.*
    FROM basketball_test.player_stats_actions_by_game a
    JOIN games_filtered gf
      ON gf.game_year = a.game_year AND gf.game_id = a.game_id AND gf.team_id = a.team_id
    WHERE (a.is_overtime OR p_max_time_remaining IS NULL
           OR a.regulation_seconds_remaining <= p_max_time_remaining)
      AND (
        (a.is_overtime AND NOT COALESCE(p_ot_margin_filter, FALSE))
        OR (p_max_margin IS NULL OR a.pre_abs_margin <= p_max_margin)
           AND (v_margin_status = 'all'
             OR (v_margin_status = 'leading' AND a.pre_status > 0)
             OR (v_margin_status = 'trailing' AND a.pre_status < 0)
             OR (v_margin_status = 'tied' AND a.pre_status = 0))
      )
  ),
  lineup_map AS MATERIALIZED (
    SELECT DISTINCT ll.game_id, ll.team_id, ll.lineup_hash, ll.player_id
    FROM basketball_test.lineups_lookup ll
    JOIN games_filtered gf
      ON gf.game_id = ll.game_id AND gf.team_id = ll.team_id
    WHERE ll.game_year = p_game_year
      AND COALESCE(ll.is_on_verdict, 0)::int = 1
  ),
  poss_end AS MATERIALIZED (
    SELECT DISTINCT a.game_id, a.team_id, a.lineup_hash, a.action_id AS poss_end_id
    FROM acts a
    WHERE a.type_lineup = 'offense' AND a.possession_flag = 1
      AND a.lineup_hash IS NOT NULL
  ),
  player_usage AS MATERIALIZED (
    SELECT lm.player_id, pe.team_id,
           COUNT(DISTINCT pe.game_id)::int AS gp,
           COUNT(DISTINCT (pe.game_id, pe.team_id, pe.poss_end_id))::int AS poss_on_floor
    FROM poss_end pe
    JOIN lineup_map lm
      ON lm.game_id = pe.game_id AND lm.team_id = pe.team_id AND lm.lineup_hash = pe.lineup_hash
    GROUP BY lm.player_id, pe.team_id
  ),
  team_possession_totals AS MATERIALIZED (
    SELECT pe.team_id,
           COUNT(DISTINCT (pe.game_id, pe.team_id, pe.poss_end_id))::numeric AS team_poss
    FROM poss_end pe
    GROUP BY pe.team_id
  ),
  seg_times AS MATERIALIZED (
    SELECT a.game_id, a.team_id, a.lineup_hash, a.segment_id,
           GREATEST(
             (array_agg(a.event_elapsed_seconds ORDER BY a.action_id DESC))[1] -
             (array_agg(a.event_elapsed_seconds ORDER BY a.action_id))[1], 0
           )::numeric AS seg_seconds
    FROM acts a
    WHERE a.lineup_hash IS NOT NULL AND a.segment_id IS NOT NULL
      AND a.event_elapsed_seconds IS NOT NULL
    GROUP BY a.game_id, a.team_id, a.lineup_hash, a.segment_id
  ),
  player_minutes AS MATERIALIZED (
    SELECT lm.player_id, st.team_id,
           ROUND(SUM(COALESCE(st.seg_seconds, 0))::numeric / 60.0, 1) AS minutes
    FROM seg_times st
    JOIN lineup_map lm
      ON lm.game_id = st.game_id AND lm.team_id = st.team_id AND lm.lineup_hash = st.lineup_hash
    GROUP BY lm.player_id, st.team_id
  ),
  stats AS MATERIALIZED (
    SELECT a.action_player_id AS player_id, a.team_id,
      SUM(a.points)::int AS pts,
      SUM(a.oreb)::int AS oreb,
      SUM(a.dreb)::int AS dreb,
      SUM(a.assists)::int AS ast,
      SUM(a.steals)::int AS stl,
      SUM(a.blocks)::int AS blk,
      SUM(a.deflections)::int AS dfl,
      SUM(a.turnovers)::int AS tov,
      SUM(a.fgm)::int AS fgm,
      SUM(a.fga)::int AS fga,
      SUM(a.fg3_made)::int AS "3pm",
      SUM(a.fg3_att)::int AS "3pa",
      SUM(a.ftm)::int AS ftm,
      SUM(a.ft_attempts)::int AS fta,
      (SUM(a.shot_ts_possession)
       + COUNT(DISTINCT a.personal_ft_trip_id))::int AS ts_poss_count
    FROM acts a
    WHERE a.action_player_id IS NOT NULL AND a.action_player_id > 0
    GROUP BY a.action_player_id, a.team_id
  ),
  team_usage_totals AS MATERIALIZED (
    SELECT a.team_id,
      (SUM(a.shot_ts_possession)
       + COUNT(DISTINCT a.personal_ft_trip_id))::numeric AS team_ts_poss_count,
      SUM(a.turnovers)::numeric AS team_tov
    FROM acts a
    GROUP BY a.team_id
  ),
  names_df AS MATERIALIZED (
    SELECT fr.player_id, fr.team_id,
           MIN(btrim(fr.team_name)) AS team_name,
           MIN(btrim(CONCAT_WS(' ', fr.firstname, fr.lastname))) AS player_name
    FROM basketball_test.full_rosters fr
    WHERE fr.game_year = p_game_year
    GROUP BY fr.player_id, fr.team_id
  ),
  final_rows AS (
    SELECT s.player_id, s.team_id, nd.team_name, nd.player_name,
      COALESCE(pu.gp, 0)::int AS gp,
      COALESCE(pu.poss_on_floor, 0)::int AS poss_on_floor,
      COALESCE(pm.minutes, 0)::numeric AS minutes,
      s.pts, (s.oreb + s.dreb)::int AS reb, s.oreb, s.dreb,
      s.ast, s.stl, s.blk, s.dfl, s.tov, s.fgm, s.fga,
      s."3pm", s."3pa", s.ftm, s.fta,
      CASE WHEN s.fga > 0 THEN ROUND(100.0 * s.fgm / s.fga, 1) END AS fg_pct,
      CASE WHEN s."3pa" > 0 THEN ROUND(100.0 * s."3pm" / s."3pa", 1) END AS tp_pct,
      CASE WHEN s.fta > 0 THEN ROUND(100.0 * s.ftm / s.fta, 1) END AS ft_pct,
      CASE WHEN s.fga > 0 THEN ROUND(100.0 * (s.fgm + 0.5 * s."3pm") / s.fga, 1) END AS efg,
      CASE WHEN s.fga + 0.44 * s.fta > 0
           THEN ROUND(100.0 * s.pts / (2.0 * (s.fga + 0.44 * s.fta)), 1) END AS ts,
      CASE WHEN s.ts_poss_count + s.tov > 0
             AND tut.team_ts_poss_count + tut.team_tov > 0
             AND COALESCE(pu.poss_on_floor, 0) > 0 AND COALESCE(tpt.team_poss, 0) > 0
           THEN ROUND(100.0 * (s.ts_poss_count + s.tov) * tpt.team_poss
                      / ((tut.team_ts_poss_count + tut.team_tov) * pu.poss_on_floor), 1) END AS usg_pct
    FROM stats s
    LEFT JOIN team_usage_totals tut ON tut.team_id = s.team_id
    LEFT JOIN player_usage pu ON pu.player_id = s.player_id AND pu.team_id = s.team_id
    LEFT JOIN team_possession_totals tpt ON tpt.team_id = s.team_id
    LEFT JOIN player_minutes pm ON pm.player_id = s.player_id AND pm.team_id = s.team_id
    LEFT JOIN names_df nd ON nd.player_id = s.player_id AND nd.team_id = s.team_id
  )
  SELECT fr.player_id, fr.team_id, fr.team_name, fr.player_name,
    fr.gp, fr.poss_on_floor, fr.minutes,
    fr.pts, fr.reb, fr.oreb, fr.dreb, fr.ast, fr.stl, fr.blk, fr.dfl, fr.tov,
    fr.fgm, fr.fga, fr."3pm", fr."3pa", fr.ftm, fr.fta,
    fr.fg_pct, fr.tp_pct, fr.ft_pct, fr.efg, fr.ts, fr.usg_pct
  FROM final_rows fr
  WHERE fr.player_name IS NOT NULL AND fr.player_name <> ''
    AND fr.team_name IS NOT NULL AND fr.team_name <> ''
    AND (fr.gp > 0 OR fr.poss_on_floor > 0 OR fr.minutes > 0)
  ORDER BY fr.pts DESC, fr.minutes DESC, fr.team_name, fr.player_name;
END;
$$;
