-- Team-level four-factor rates aggregated from lineup_four_factors_by_game.
-- Each row in the source MV is unique per (lineup_hash, team_id, game_id, type_lineup),
-- so summing by (team_id, game_year, type_lineup) avoids double-counting.
-- Pivots offense/defense into a single row per (team_id, game_year).

CREATE MATERIALIZED VIEW basketball_test.team_four_factors_mv
TABLESPACE pg_default
AS
WITH team_agg AS (
  SELECT
    lf.team_id,
    lf.game_year,
    lf.type_lineup,
    SUM(lf.total_points)       AS total_points,
    SUM(lf.total_poss)         AS total_poss,
    SUM(lf.ts_poss_count)      AS ts_poss_count,
    SUM(lf.oreb_count)         AS oreb_count,
    SUM(lf.oreb_opportunities) AS oreb_opportunities,
    SUM(lf.tov_count)          AS tov_count,
    SUM(lf.total_ft_attempts)  AS total_ft_attempts,
    SUM(lf.total_fga)          AS total_fga,
    SUM(lf.total_fgm)          AS total_fgm,
    SUM(lf.total_fg3_made)     AS total_fg3_made
  FROM basketball_test.lineup_four_factors_by_game lf
  GROUP BY lf.team_id, lf.game_year, lf.type_lineup
),
pivoted AS (
  SELECT
    ta.team_id,
    ta.game_year,
    -- Offense rates
    ROUND(
      SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense')::numeric
      / (2.0 * NULLIF(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric)
    * 100, 1) AS off_ts,
    ROUND(
      (
        SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'offense')::numeric
        + 0.5 * SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'offense')::numeric
      )
      / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric
    * 100, 1) AS off_efg,
    ROUND(
      SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'offense')::numeric
      / NULLIF(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric
    * 100, 1) AS off_oreb,
    ROUND(
      SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'offense')::numeric
      / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric
    * 100, 1) AS off_tov,
    ROUND(
      SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'offense')::numeric
      / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric
    * 100, 1) AS off_ftr,
    ROUND(
      SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense')::numeric
      / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0) * 100
    , 1) AS off_ppp,
    COALESCE(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_poss,
    -- Offense raw counts
    COALESCE(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_pts,
    COALESCE(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_ts_poss,
    COALESCE(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_oreb_cnt,
    COALESCE(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_oreb_opps,
    COALESCE(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_tov_cnt,
    COALESCE(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fta,
    COALESCE(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fga_cnt,
    COALESCE(SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fgm_cnt,
    COALESCE(SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fg3m_cnt,
    -- Defense rates
    ROUND(
      SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense')::numeric
      / (2.0 * NULLIF(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric)
    * 100, 1) AS def_ts,
    ROUND(
      (
        SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'defense')::numeric
        + 0.5 * SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'defense')::numeric
      )
      / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric
    * 100, 1) AS def_efg,
    ROUND(
      SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'defense')::numeric
      / NULLIF(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric
    * 100, 1) AS def_oreb,
    ROUND(
      SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'defense')::numeric
      / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric
    * 100, 1) AS def_tov,
    ROUND(
      SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'defense')::numeric
      / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric
    * 100, 1) AS def_ftr,
    ROUND(
      SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense')::numeric
      / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0) * 100
    , 1) AS def_ppp,
    COALESCE(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_poss,
    -- Defense raw counts
    COALESCE(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_pts,
    COALESCE(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_ts_poss,
    COALESCE(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_oreb_cnt,
    COALESCE(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_oreb_opps,
    COALESCE(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_tov_cnt,
    COALESCE(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fta,
    COALESCE(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fga_cnt,
    COALESCE(SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fgm_cnt,
    COALESCE(SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fg3m_cnt
  FROM team_agg ta
  GROUP BY ta.team_id, ta.game_year
),
teams AS (
  SELECT DISTINCT full_rosters.game_year, full_rosters.team_id, full_rosters.team_name
  FROM basketball_test.full_rosters
)
SELECT
  p.team_id,
  p.game_year,
  t.team_name,
  p.off_ts, p.off_efg, p.off_oreb, p.off_tov, p.off_ftr, p.off_ppp, p.off_poss,
  p.off_pts, p.off_ts_poss, p.off_oreb_cnt, p.off_oreb_opps, p.off_tov_cnt, p.off_fta, p.off_fga_cnt, p.off_fgm_cnt, p.off_fg3m_cnt,
  p.def_ts, p.def_efg, p.def_oreb, p.def_tov, p.def_ftr, p.def_ppp, p.def_poss,
  p.def_pts, p.def_ts_poss, p.def_oreb_cnt, p.def_oreb_opps, p.def_tov_cnt, p.def_fta, p.def_fga_cnt, p.def_fgm_cnt, p.def_fg3m_cnt,
  -- Round once, from the additive counts. Subtracting two values that were
  -- each already rounded to 1dp disagreed with Ratings by 0.1 for 4 of 14
  -- teams in the 2026-08-29 audit.
  ROUND(
    100.0 * p.off_pts / NULLIF(p.off_poss, 0)
    - 100.0 * p.def_pts / NULLIF(p.def_poss, 0)
  , 1) AS net_rtg
FROM pivoted p
JOIN teams t ON t.game_year = p.game_year AND t.team_id = p.team_id
WITH DATA;

-- Indexes
CREATE INDEX idx_tffmv_gy ON basketball_test.team_four_factors_mv USING btree (game_year);
CREATE UNIQUE INDEX idx_tffmv_pk ON basketball_test.team_four_factors_mv USING btree (team_id, game_year);
