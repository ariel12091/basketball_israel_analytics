DROP FUNCTION IF EXISTS basketball_test.get_player_traditional_from_games(
  INT, DATE, DATE, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, INT, TEXT,
  INT, TEXT, INT, BOOLEAN, INT, INT, INT
);

CREATE OR REPLACE FUNCTION basketball_test.get_player_traditional_from_games(
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
  IF p_max_margin IS NOT NULL OR p_max_time_remaining IS NOT NULL
     OR v_margin_status <> 'all' OR COALESCE(p_ot_margin_filter, FALSE) THEN
    RAISE EXCEPTION 'get_player_traditional_from_games does not accept clutch filters';
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
  games_filtered AS (
    SELECT gr.game_id, gr.team_id, gr.game_year
    FROM games_ranked gr
    WHERE v_opp_rank_side = 'all' OR p_opp_rank_n IS NULL
       OR (v_opp_rank_side = 'top' AND gr.opp_rank <= p_opp_rank_n)
       OR (v_opp_rank_side = 'bottom' AND gr.opp_rank >= gr.max_rank - p_opp_rank_n + 1)
  ),
  facts AS (
    SELECT f.*
    FROM basketball_test.player_traditional_by_game f
    JOIN games_filtered g
      ON g.game_year = f.game_year AND g.game_id = f.game_id AND g.team_id = f.team_id
  ),
  team_game AS (
    SELECT f.game_year, f.game_id, f.team_id,
           MAX(f.team_ts_poss_count)::numeric AS team_ts_poss_count,
           MAX(f.team_tov)::numeric AS team_tov,
           MAX(f.team_poss)::numeric AS team_poss
    FROM facts f
    GROUP BY f.game_year, f.game_id, f.team_id
  ),
  team_totals AS (
    SELECT tg.game_year, tg.team_id,
           SUM(tg.team_ts_poss_count) AS team_ts_poss_count,
           SUM(tg.team_tov) AS team_tov,
           SUM(tg.team_poss) AS team_poss
    FROM team_game tg
    GROUP BY tg.game_year, tg.team_id
  ),
  player_totals AS (
    SELECT f.game_year, f.team_id, f.player_id,
           SUM(f.gp)::int4 AS gp,
           SUM(f.poss_on_floor)::int4 AS poss_on_floor,
           ROUND(SUM(f.seconds_on_floor) / 60.0, 1) AS minutes,
           SUM(f.pts)::int4 AS pts, SUM(f.reb)::int4 AS reb,
           SUM(f.oreb)::int4 AS oreb, SUM(f.dreb)::int4 AS dreb,
           SUM(f.ast)::int4 AS ast, SUM(f.stl)::int4 AS stl,
           SUM(f.blk)::int4 AS blk, SUM(f.dfl)::int4 AS dfl,
           SUM(f.tov)::int4 AS tov, SUM(f.fgm)::int4 AS fgm,
           SUM(f.fga)::int4 AS fga, SUM(f."3pm")::int4 AS "3pm",
           SUM(f."3pa")::int4 AS "3pa", SUM(f.ftm)::int4 AS ftm,
           SUM(f.fta)::int4 AS fta,
           SUM(f.player_ts_poss_count)::numeric AS player_ts_poss_count
    FROM facts f
    GROUP BY f.game_year, f.team_id, f.player_id
    HAVING bool_or(f.has_actor_stats)
  ),
  names_df AS (
    SELECT fr.game_year, fr.team_id, fr.player_id,
           MIN(btrim(fr.team_name)) AS team_name,
           MIN(btrim(CONCAT_WS(' ', fr.firstname, fr.lastname))) AS player_name
    FROM basketball_test.full_rosters fr
    WHERE fr.game_year = p_game_year
    GROUP BY fr.game_year, fr.team_id, fr.player_id
  )
  SELECT
    p.player_id, p.team_id, n.team_name, n.player_name,
    p.gp, p.poss_on_floor, p.minutes,
    p.pts, p.reb, p.oreb, p.dreb, p.ast, p.stl, p.blk, p.dfl, p.tov,
    p.fgm, p.fga, p."3pm", p."3pa", p.ftm, p.fta,
    CASE WHEN p.fga > 0 THEN ROUND(100.0 * p.fgm / p.fga, 1) END,
    CASE WHEN p."3pa" > 0 THEN ROUND(100.0 * p."3pm" / p."3pa", 1) END,
    CASE WHEN p.fta > 0 THEN ROUND(100.0 * p.ftm / p.fta, 1) END,
    CASE WHEN p.fga > 0 THEN ROUND(100.0 * (p.fgm + 0.5 * p."3pm") / p.fga, 1) END,
    CASE WHEN p.fga + 0.44 * p.fta > 0
         THEN ROUND(100.0 * p.pts / (2.0 * (p.fga + 0.44 * p.fta)), 1) END,
    CASE WHEN p.player_ts_poss_count + p.tov > 0
           AND t.team_ts_poss_count + t.team_tov > 0
           AND p.poss_on_floor > 0 AND t.team_poss > 0
         THEN ROUND(100.0 * (p.player_ts_poss_count + p.tov) * t.team_poss
                    / ((t.team_ts_poss_count + t.team_tov) * p.poss_on_floor), 1) END
  FROM player_totals p
  JOIN team_totals t ON t.game_year = p.game_year AND t.team_id = p.team_id
  LEFT JOIN names_df n
    ON n.game_year = p.game_year AND n.team_id = p.team_id AND n.player_id = p.player_id
  WHERE n.player_name IS NOT NULL AND n.player_name <> ''
    AND n.team_name IS NOT NULL AND n.team_name <> ''
    AND (p.gp > 0 OR p.poss_on_floor > 0 OR p.minutes > 0)
  ORDER BY p.pts DESC, p.minutes DESC, n.team_name, n.player_name;
END;
$$;
