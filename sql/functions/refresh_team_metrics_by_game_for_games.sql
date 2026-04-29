CREATE OR REPLACE FUNCTION basketball_test.refresh_team_metrics_by_game_for_games(game_ids int4[])
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM basketball_test.team_metrics_by_game_mv;
  ELSE
    DELETE FROM basketball_test.team_metrics_by_game_mv
    WHERE game_id = ANY(game_ids);
  END IF;

  INSERT INTO basketball_test.team_metrics_by_game_mv (
    game_year, game_id, game_date, gn, game_type, team_id, team_name,
    opp_team_id, opp_team_name, is_home, has_won, team_score, opp_score,
    margin, off_ppp, def_ppp, net_rtg, off_poss, def_poss, off_points_raw,
    def_points_raw, off_poss_raw, def_poss_raw, off_ts_poss_raw,
    def_ts_poss_raw, off_oreb_count_raw, def_oreb_count_raw,
    off_oreb_opp_raw, def_oreb_opp_raw, off_tov_raw, def_tov_raw,
    off_fta_raw, def_fta_raw, off_fga_raw, def_fga_raw, off_fgm_raw, def_fgm_raw,
    off_fg3m_raw, def_fg3m_raw, off_ts, off_efg, off_oreb,
    off_tov, off_ftr, def_ts, def_efg, def_oreb, def_tov, def_ftr, off_minutes,
    def_minutes, pts, reb, ast, stl, blk, tov, fgm, fga, "3pm", "3pa",
    ftm, fta, fg_pct, tp_pct, ft_pct, efg, ts
  )
  WITH lffg_team_game AS (
    SELECT
      lf.team_id,
      lf.game_id,
      lf.game_year,
      lf.type_lineup,
      SUM(lf.total_points) AS total_points,
      SUM(lf.total_poss) AS total_poss,
      SUM(lf.ts_poss_count) AS ts_poss_count,
      SUM(lf.oreb_count) AS oreb_count,
      SUM(lf.oreb_opportunities) AS oreb_opportunities,
      SUM(lf.tov_count) AS tov_count,
      SUM(lf.total_ft_attempts) AS total_ft_attempts,
      SUM(lf.total_fga) AS total_fga,
      SUM(lf.total_fgm) AS total_fgm,
      SUM(lf.total_fg3_made) AS total_fg3_made,
      SUM(lf.minutes) FILTER (WHERE lf.type_lineup = 'offense') AS off_minutes,
      SUM(lf.minutes) FILTER (WHERE lf.type_lineup = 'defense') AS def_minutes
    FROM basketball_test.lineup_four_factors_by_game lf
    WHERE game_ids IS NULL OR lf.game_id = ANY(game_ids)
    GROUP BY lf.team_id, lf.game_id, lf.game_year, lf.type_lineup
  ),
  pivot_ff AS (
    SELECT
      t.team_id,
      t.game_id,
      t.game_year,
      COALESCE(SUM(t.total_points) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric AS off_points_raw,
      COALESCE(SUM(t.total_points) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric AS def_points_raw,
      COALESCE(SUM(t.total_poss) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric AS off_poss_raw,
      COALESCE(SUM(t.total_poss) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric AS def_poss_raw,
      COALESCE(SUM(t.ts_poss_count) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric AS off_ts_poss_raw,
      COALESCE(SUM(t.ts_poss_count) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric AS def_ts_poss_raw,
      COALESCE(SUM(t.oreb_count) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric AS off_oreb_count_raw,
      COALESCE(SUM(t.oreb_count) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric AS def_oreb_count_raw,
      COALESCE(SUM(t.oreb_opportunities) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric AS off_oreb_opp_raw,
      COALESCE(SUM(t.oreb_opportunities) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric AS def_oreb_opp_raw,
      COALESCE(SUM(t.tov_count) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric AS off_tov_raw,
      COALESCE(SUM(t.tov_count) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric AS def_tov_raw,
      COALESCE(SUM(t.total_ft_attempts) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric AS off_fta_raw,
      COALESCE(SUM(t.total_ft_attempts) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric AS def_fta_raw,
      COALESCE(SUM(t.total_fga) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric AS off_fga_raw,
      COALESCE(SUM(t.total_fga) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric AS def_fga_raw,
      COALESCE(SUM(t.total_fgm) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric AS off_fgm_raw,
      COALESCE(SUM(t.total_fgm) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric AS def_fgm_raw,
      COALESCE(SUM(t.total_fg3_made) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric AS off_fg3m_raw,
      COALESCE(SUM(t.total_fg3_made) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric AS def_fg3m_raw,
      ROUND(
        SUM(t.total_points) FILTER (WHERE t.type_lineup = 'offense')::numeric
        / NULLIF(SUM(t.total_poss) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric
        * 100, 1
      ) AS off_ppp,
      ROUND(
        SUM(t.total_points) FILTER (WHERE t.type_lineup = 'defense')::numeric
        / NULLIF(SUM(t.total_poss) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric
        * 100, 1
      ) AS def_ppp,
      ROUND(
        (
          SUM(t.total_points) FILTER (WHERE t.type_lineup = 'offense')::numeric
          / NULLIF(SUM(t.total_poss) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric
        ) - (
          SUM(t.total_points) FILTER (WHERE t.type_lineup = 'defense')::numeric
          / NULLIF(SUM(t.total_poss) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric
        )
        * 100, 1
      ) AS net_rtg,
      COALESCE(SUM(t.total_poss) FILTER (WHERE t.type_lineup = 'offense'), 0)::int4 AS off_poss,
      COALESCE(SUM(t.total_poss) FILTER (WHERE t.type_lineup = 'defense'), 0)::int4 AS def_poss,

      ROUND(
        SUM(t.total_points) FILTER (WHERE t.type_lineup = 'offense')::numeric
        / (2.0 * NULLIF(SUM(t.ts_poss_count) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric)
        * 100, 1
      ) AS off_ts,
      ROUND(
        (
          SUM(t.total_fgm) FILTER (WHERE t.type_lineup = 'offense')::numeric
          + 0.5 * SUM(t.total_fg3_made) FILTER (WHERE t.type_lineup = 'offense')::numeric
        )
        / NULLIF(SUM(t.total_fga) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric
        * 100, 1
      ) AS off_efg,
      ROUND(
        SUM(t.oreb_count) FILTER (WHERE t.type_lineup = 'offense')::numeric
        / NULLIF(SUM(t.oreb_opportunities) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric
        * 100, 1
      ) AS off_oreb,
      ROUND(
        SUM(t.tov_count) FILTER (WHERE t.type_lineup = 'offense')::numeric
        / NULLIF(SUM(t.total_poss) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric
        * 100, 1
      ) AS off_tov,
      ROUND(
        SUM(t.total_ft_attempts) FILTER (WHERE t.type_lineup = 'offense')::numeric
        / NULLIF(SUM(t.total_fga) FILTER (WHERE t.type_lineup = 'offense'), 0)::numeric
        * 100, 1
      ) AS off_ftr,

      ROUND(
        SUM(t.total_points) FILTER (WHERE t.type_lineup = 'defense')::numeric
        / (2.0 * NULLIF(SUM(t.ts_poss_count) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric)
        * 100, 1
      ) AS def_ts,
      ROUND(
        (
          SUM(t.total_fgm) FILTER (WHERE t.type_lineup = 'defense')::numeric
          + 0.5 * SUM(t.total_fg3_made) FILTER (WHERE t.type_lineup = 'defense')::numeric
        )
        / NULLIF(SUM(t.total_fga) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric
        * 100, 1
      ) AS def_efg,
      ROUND(
        SUM(t.oreb_count) FILTER (WHERE t.type_lineup = 'defense')::numeric
        / NULLIF(SUM(t.oreb_opportunities) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric
        * 100, 1
      ) AS def_oreb,
      ROUND(
        SUM(t.tov_count) FILTER (WHERE t.type_lineup = 'defense')::numeric
        / NULLIF(SUM(t.total_poss) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric
        * 100, 1
      ) AS def_tov,
      ROUND(
        SUM(t.total_ft_attempts) FILTER (WHERE t.type_lineup = 'defense')::numeric
        / NULLIF(SUM(t.total_fga) FILTER (WHERE t.type_lineup = 'defense'), 0)::numeric
        * 100, 1
      ) AS def_ftr,

      COALESCE(SUM(t.off_minutes), 0)::numeric(10, 1) AS off_minutes,
      COALESCE(SUM(t.def_minutes), 0)::numeric(10, 1) AS def_minutes
    FROM lffg_team_game t
    GROUP BY t.team_id, t.game_id, t.game_year
  ),
  traditional_game AS (
    SELECT
      d.game_id,
      d.team_id,
      (
        SUM(CASE WHEN d.type = 'shot' AND d.parameters_made = 'made' AND d.type_lineup = 'offense' THEN COALESCE(d.parameters_points, 0) ELSE 0 END)
        + SUM(CASE WHEN d.type = 'freeThrow' AND d.parameters_made = 'made' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)
      )::int4 AS pts,
      SUM(CASE WHEN d.type = 'rebound' AND (
                (d.type_lineup = 'offense' AND d.parameters_type = 'offensive')
                OR (d.type_lineup = 'defense' AND d.parameters_type = 'defensive')
              ) THEN 1 ELSE 0 END)::int4 AS reb,
      SUM(CASE WHEN d.type = 'assist'   AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS ast,
      SUM(CASE WHEN d.type = 'steal'    AND d.type_lineup = 'defense' THEN 1 ELSE 0 END)::int4 AS stl,
      SUM(CASE WHEN d.type = 'block'    AND d.type_lineup = 'defense' THEN 1 ELSE 0 END)::int4 AS blk,
      SUM(CASE WHEN d.type = 'turnover' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS tov,
      SUM(CASE WHEN d.type = 'shot' AND d.parameters_made = 'made' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS fgm,
      SUM(CASE WHEN d.type = 'shot' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS fga,
      SUM(CASE WHEN d.type = 'shot' AND d.parameters_made = 'made' AND d.parameters_points = 3 AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS "3pm",
      SUM(CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS "3pa",
      SUM(CASE WHEN d.type = 'freeThrow' AND d.parameters_made = 'made' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS ftm,
      SUM(CASE WHEN d.type = 'freeThrow' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS fta
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    WHERE game_ids IS NULL OR d.game_id = ANY(game_ids)
    GROUP BY d.game_id, d.team_id
  )
  SELECT
    fs.game_year,
    fs.game_id,
    fs.game_date,
    fs.gn,
    fs.game_type,
    fs.team_id,
    fs.team_name,
    fs.opp_team_id,
    fs.opp_team_name,
    fs.is_home,
    fs.has_won,
    fs.team_score,
    fs.opp_score,
    fs.margin,

    ff.off_ppp,
    ff.def_ppp,
    ff.net_rtg,
    ff.off_poss,
    ff.def_poss,
    ff.off_points_raw,
    ff.def_points_raw,
    ff.off_poss_raw,
    ff.def_poss_raw,
    ff.off_ts_poss_raw,
    ff.def_ts_poss_raw,
    ff.off_oreb_count_raw,
    ff.def_oreb_count_raw,
    ff.off_oreb_opp_raw,
    ff.def_oreb_opp_raw,
    ff.off_tov_raw,
    ff.def_tov_raw,
    ff.off_fta_raw,
    ff.def_fta_raw,
    ff.off_fga_raw,
    ff.def_fga_raw,
    ff.off_fgm_raw,
    ff.def_fgm_raw,
    ff.off_fg3m_raw,
    ff.def_fg3m_raw,
    ff.off_ts,
    ff.off_efg,
    ff.off_oreb,
    ff.off_tov,
    ff.off_ftr,
    ff.def_ts,
    ff.def_efg,
    ff.def_oreb,
    ff.def_tov,
    ff.def_ftr,
    ff.off_minutes,
    ff.def_minutes,

    tg.pts,
    tg.reb,
    tg.ast,
    tg.stl,
    tg.blk,
    tg.tov,
    tg.fgm,
    tg.fga,
    tg."3pm",
    tg."3pa",
    tg.ftm,
    tg.fta,
    CASE WHEN tg.fga > 0 THEN ROUND(tg.fgm::numeric / tg.fga::numeric * 100, 1) ELSE NULL END AS fg_pct,
    CASE WHEN tg."3pa" > 0 THEN ROUND(tg."3pm"::numeric / tg."3pa"::numeric * 100, 1) ELSE NULL END AS tp_pct,
    CASE WHEN tg.fta > 0 THEN ROUND(tg.ftm::numeric / tg.fta::numeric * 100, 1) ELSE NULL END AS ft_pct,
    CASE WHEN tg.fga > 0 THEN ROUND((tg.fgm + 0.5 * tg."3pm")::numeric / tg.fga::numeric * 100, 1) ELSE NULL END AS efg,
    CASE WHEN (tg.fga + 0.44 * tg.fta) > 0 THEN ROUND(tg.pts::numeric / (2.0 * (tg.fga + 0.44 * tg.fta)::numeric) * 100, 1) ELSE NULL END AS ts
  FROM basketball_test.final_schedule_mv fs
  LEFT JOIN pivot_ff ff
    ON ff.game_year = fs.game_year
   AND ff.game_id = fs.game_id
   AND ff.team_id = fs.team_id
  LEFT JOIN traditional_game tg
    ON tg.game_id = fs.game_id
   AND tg.team_id = fs.team_id
  WHERE game_ids IS NULL OR fs.game_id = ANY(game_ids);

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$$;
