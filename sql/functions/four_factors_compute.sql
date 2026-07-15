DROP FUNCTION IF EXISTS basketball_test.four_factors_compute;

CREATE OR REPLACE FUNCTION basketball_test.four_factors_compute(
    p_game_year       INT,
    p_start_date      DATE DEFAULT NULL,
    p_end_date        DATE DEFAULT NULL,
    p_team_ids_csv    TEXT DEFAULT NULL,
    p_game_type_csv   TEXT DEFAULT NULL,
    p_opp_ids_csv     TEXT DEFAULT NULL,
    p_home_away       TEXT DEFAULT 'all',
    p_outcome         TEXT DEFAULT 'all',
    p_opp_rank_side   TEXT DEFAULT 'all',
    p_opp_rank_n      INT  DEFAULT NULL,
    p_opp_rank_metric TEXT DEFAULT 'net',
    p_min_gn           INT DEFAULT NULL,
    p_max_gn           INT DEFAULT NULL,
    p_last_n_games     INT DEFAULT NULL,
    p_num_starters_off INT DEFAULT NULL,
    p_num_starters_def INT DEFAULT NULL,
    p_num_starters_off_min INT DEFAULT NULL,
    p_num_starters_off_max INT DEFAULT NULL,
    p_num_starters_def_min INT DEFAULT NULL,
    p_num_starters_def_max INT DEFAULT NULL
)
RETURNS TABLE (
    player_id       INT,
    team_id         INT,
    firstname       TEXT,
    lastname        TEXT,
    team_name       TEXT,
    game_year       INT,
    off_on_ts       NUMERIC,
    off_off_ts      NUMERIC,
    def_on_ts       NUMERIC,
    def_off_ts      NUMERIC,
    off_on_efg      NUMERIC,
    off_off_efg     NUMERIC,
    def_on_efg      NUMERIC,
    def_off_efg     NUMERIC,
    off_on_oreb     NUMERIC,
    off_off_oreb    NUMERIC,
    def_on_oreb     NUMERIC,
    def_off_oreb    NUMERIC,
    off_on_tov      NUMERIC,
    off_off_tov     NUMERIC,
    def_on_tov      NUMERIC,
    def_off_tov     NUMERIC,
    def_on_disruptions  NUMERIC,
    def_off_disruptions NUMERIC,
    off_on_ftr      NUMERIC,
    off_off_ftr     NUMERIC,
    def_on_ftr      NUMERIC,
    def_off_ftr     NUMERIC,
    off_on_poss     BIGINT,
    off_off_poss    BIGINT,
    def_on_poss     BIGINT,
    def_off_poss    BIGINT,
    "Off eFG% Diff"   NUMERIC,
    "Off TS% Diff"    NUMERIC,
    "Off OREB% Diff"  NUMERIC,
    "Off TOV% Diff"   NUMERIC,
    "Off FTR Diff"    NUMERIC,
    "Def eFG% Diff"   NUMERIC,
    "Def TS% Diff"    NUMERIC,
    "Def OREB% Diff"  NUMERIC,
    "Def TOV% Diff"   NUMERIC,
    "Def FTR Diff"    NUMERIC,
    "Def Disruptions/100 Diff" NUMERIC
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_game_types      int4[];
  v_opp_ids         int4[];
  v_team_ids        int4[];
  v_home_away       text;
  v_outcome         text;
  v_opp_rank_side   text;
  v_opp_rank_metric text;
BEGIN
  -- [Input Normalization]
  v_home_away       := COALESCE(NULLIF(btrim(p_home_away), ''), 'all');
  v_outcome         := COALESCE(NULLIF(btrim(p_outcome), ''), 'all');
  v_opp_rank_side   := COALESCE(NULLIF(btrim(p_opp_rank_side), ''), 'all');
  v_opp_rank_metric := COALESCE(NULLIF(btrim(p_opp_rank_metric), ''), 'net');

  -- Parse CSVs
  IF p_game_type_csv IS NOT NULL AND length(btrim(p_game_type_csv)) > 0 THEN
      v_game_types := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_game_type_csv, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_opp_ids_csv IS NOT NULL AND length(btrim(p_opp_ids_csv)) > 0 THEN
      v_opp_ids := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_opp_ids_csv, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_team_ids_csv IS NOT NULL AND length(btrim(p_team_ids_csv)) > 0 THEN
      v_team_ids := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;

  RETURN QUERY
  WITH
  -- CTE 1: Games Base (Filter Schedule)
  games_base AS (
    SELECT fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id
    FROM basketball_test.final_schedule_mv fs
    WHERE fs.game_year = p_game_year
      AND (p_start_date IS NULL OR fs.game_date >= p_start_date)
      AND (p_end_date   IS NULL OR fs.game_date <= p_end_date)
      AND (v_game_types IS NULL OR fs.game_type = ANY(v_game_types))
      AND (v_opp_ids    IS NULL OR fs.opp_team_id = ANY(v_opp_ids))
      AND (v_team_ids   IS NULL OR fs.team_id = ANY(v_team_ids))
      AND (v_home_away = 'all' OR (v_home_away = 'home' AND fs.is_home) OR (v_home_away = 'away' AND NOT fs.is_home))
      AND (v_outcome = 'all'   OR (v_outcome = 'win' AND fs.has_won IS TRUE) OR (v_outcome = 'loss' AND fs.has_won IS FALSE))
      AND (p_min_gn IS NULL OR fs.gn >= p_min_gn)
      AND (p_max_gn IS NULL OR fs.gn <= p_max_gn)
      AND (p_last_n_games IS NULL
           OR COALESCE((
                SELECT fsr.rn_recent
                FROM (
                  SELECT fs2.game_id,
                         ROW_NUMBER() OVER (
                           PARTITION BY fs2.team_id, fs2.game_year
                           ORDER BY fs2.game_date DESC NULLS LAST, fs2.game_id DESC
                         ) AS rn_recent
                  FROM basketball_test.final_schedule_mv fs2
                  WHERE fs2.team_id = fs.team_id
                    AND fs2.game_year = fs.game_year
                ) fsr
                WHERE fsr.game_id = fs.game_id
              ), 2147483647) <= p_last_n_games)
  ),

  -- CTE 2: Games Ranked (Join Ratings MV to get Opponent Ranks)
  games_ranked AS (
    SELECT gb.game_id, gb.team_id, gb.game_year,
           CASE WHEN v_opp_rank_side IN ('top', 'bottom') THEN
             CASE v_opp_rank_metric
               WHEN 'off' THEN r.rank_off_ppp
               WHEN 'def' THEN r.rank_def_ppp
               ELSE r.rank_net_rtg
             END
           ELSE NULL END AS opp_rank,
           CASE WHEN v_opp_rank_side = 'bottom' THEN
             MAX(CASE v_opp_rank_metric
                   WHEN 'off' THEN r.rank_off_ppp
                   WHEN 'def' THEN r.rank_def_ppp
                   ELSE r.rank_net_rtg
                 END) OVER (PARTITION BY gb.game_year)
           ELSE NULL END AS max_rank
    FROM games_base gb
    LEFT JOIN basketball_test.team_ppp_ratings_mv r
      ON r.game_year::integer = gb.game_year
      AND r.team_id::integer  = gb.opp_team_id
      AND v_opp_rank_side IN ('top', 'bottom')
  ),

  -- CTE 3: Games Filtered (Apply Rank Filter)
  games_filtered AS (
    SELECT gr.game_id, gr.team_id, gr.game_year
    FROM games_ranked gr
    WHERE v_opp_rank_side = 'all' OR p_opp_rank_n IS NULL
       OR (v_opp_rank_side = 'top'    AND gr.opp_rank <= p_opp_rank_n)
       OR (v_opp_rank_side = 'bottom' AND gr.opp_rank >= (gr.max_rank - p_opp_rank_n + 1))
  ),

  -- CTE 4: Sum pre-aggregated counts from the MV, filtered by game
  agg AS (
    SELECT
      pf.player_id,
      pf.team_id,
      gf.game_year,
      pf.is_on_key,
      pf.type_lineup,
      sum(pf.total_points)       AS total_points,
      sum(pf.total_poss)         AS total_poss,
      sum(pf.ts_poss_count)      AS ts_poss_count,
      sum(pf.oreb_count)         AS oreb_count,
      sum(pf.oreb_opportunities) AS oreb_opportunities,
      sum(pf.tov_count)          AS tov_count,
      sum(pf.steal_count)        AS steal_count,
      sum(pf.deflection_count)   AS deflection_count,
      sum(pf.total_ft_attempts)  AS total_ft_attempts,
      sum(pf.total_fga)          AS total_fga,
      sum(pf.total_fgm)          AS total_fgm,
      sum(pf.total_fg3_made)     AS total_fg3_made
    FROM basketball_test.player_four_factors_by_game pf
    JOIN games_filtered gf
      ON gf.game_id = pf.game_id
      AND gf.team_id = pf.team_id
    WHERE (COALESCE(p_num_starters_off_min, p_num_starters_off) IS NULL OR pf.own_starters >= COALESCE(p_num_starters_off_min, p_num_starters_off))
      AND (COALESCE(p_num_starters_off_max, p_num_starters_off) IS NULL OR pf.own_starters <= COALESCE(p_num_starters_off_max, p_num_starters_off))
      AND (COALESCE(p_num_starters_def_min, p_num_starters_def) IS NULL OR pf.opp_starters >= COALESCE(p_num_starters_def_min, p_num_starters_def))
      AND (COALESCE(p_num_starters_def_max, p_num_starters_def) IS NULL OR pf.opp_starters <= COALESCE(p_num_starters_def_max, p_num_starters_def))
    GROUP BY pf.player_id, pf.team_id, gf.game_year, pf.is_on_key, pf.type_lineup
  ),

  -- CTE 5: Calculate rates (TS%, OREB%, TOV%, FTR)
  calc_rates AS (
    SELECT
      a.player_id,
      a.team_id,
      a.game_year,
      a.is_on_key,
      a.type_lineup,
      a.total_poss,
      a.total_points / (2.0 * NULLIF(a.ts_poss_count, 0)::numeric) AS ts_pct,
      (a.total_fgm + 0.5 * a.total_fg3_made)::numeric / NULLIF(a.total_fga, 0)::numeric AS efg_pct,
      a.oreb_count::numeric / NULLIF(a.oreb_opportunities, 0)::numeric AS oreb_pct,
      a.tov_count::numeric / NULLIF(a.total_poss, 0)::numeric AS tov_pct,
      (a.steal_count + a.deflection_count)::numeric / NULLIF(a.total_poss, 0)::numeric AS disruption_rate,
      a.total_ft_attempts::numeric / NULLIF(a.total_fga, 0)::numeric AS ft_rate
    FROM agg a
  ),

  -- CTE 6: Pivot to one row per (player, team, year)
  pivoted AS (
    SELECT
      cr.player_id,
      cr.team_id,
      cr.game_year,
      max(CASE WHEN cr.type_lineup = 'offense' AND cr.is_on_key = 1 THEN cr.ts_pct END) AS off_on_ts,
      max(CASE WHEN cr.type_lineup = 'offense' AND cr.is_on_key = 0 THEN cr.ts_pct END) AS off_off_ts,
      max(CASE WHEN cr.type_lineup = 'defense' AND cr.is_on_key = 1 THEN cr.ts_pct END) AS def_on_ts,
      max(CASE WHEN cr.type_lineup = 'defense' AND cr.is_on_key = 0 THEN cr.ts_pct END) AS def_off_ts,
      max(CASE WHEN cr.type_lineup = 'offense' AND cr.is_on_key = 1 THEN cr.efg_pct END) AS off_on_efg,
      max(CASE WHEN cr.type_lineup = 'offense' AND cr.is_on_key = 0 THEN cr.efg_pct END) AS off_off_efg,
      max(CASE WHEN cr.type_lineup = 'defense' AND cr.is_on_key = 1 THEN cr.efg_pct END) AS def_on_efg,
      max(CASE WHEN cr.type_lineup = 'defense' AND cr.is_on_key = 0 THEN cr.efg_pct END) AS def_off_efg,
      max(CASE WHEN cr.type_lineup = 'offense' AND cr.is_on_key = 1 THEN cr.oreb_pct END) AS off_on_oreb,
      max(CASE WHEN cr.type_lineup = 'offense' AND cr.is_on_key = 0 THEN cr.oreb_pct END) AS off_off_oreb,
      max(CASE WHEN cr.type_lineup = 'defense' AND cr.is_on_key = 1 THEN cr.oreb_pct END) AS def_on_oreb,
      max(CASE WHEN cr.type_lineup = 'defense' AND cr.is_on_key = 0 THEN cr.oreb_pct END) AS def_off_oreb,
      max(CASE WHEN cr.type_lineup = 'offense' AND cr.is_on_key = 1 THEN cr.tov_pct END) AS off_on_tov,
      max(CASE WHEN cr.type_lineup = 'offense' AND cr.is_on_key = 0 THEN cr.tov_pct END) AS off_off_tov,
      max(CASE WHEN cr.type_lineup = 'defense' AND cr.is_on_key = 1 THEN cr.tov_pct END) AS def_on_tov,
      max(CASE WHEN cr.type_lineup = 'defense' AND cr.is_on_key = 0 THEN cr.tov_pct END) AS def_off_tov,
      max(CASE WHEN cr.type_lineup = 'defense' AND cr.is_on_key = 1 THEN cr.disruption_rate END) AS def_on_disruptions,
      max(CASE WHEN cr.type_lineup = 'defense' AND cr.is_on_key = 0 THEN cr.disruption_rate END) AS def_off_disruptions,
      max(CASE WHEN cr.type_lineup = 'offense' AND cr.is_on_key = 1 THEN cr.ft_rate END) AS off_on_ftr,
      max(CASE WHEN cr.type_lineup = 'offense' AND cr.is_on_key = 0 THEN cr.ft_rate END) AS off_off_ftr,
      max(CASE WHEN cr.type_lineup = 'defense' AND cr.is_on_key = 1 THEN cr.ft_rate END) AS def_on_ftr,
      max(CASE WHEN cr.type_lineup = 'defense' AND cr.is_on_key = 0 THEN cr.ft_rate END) AS def_off_ftr,
      max(CASE WHEN cr.type_lineup = 'offense' AND cr.is_on_key = 1 THEN cr.total_poss END)::bigint AS off_on_poss,
      max(CASE WHEN cr.type_lineup = 'offense' AND cr.is_on_key = 0 THEN cr.total_poss END)::bigint AS off_off_poss,
      max(CASE WHEN cr.type_lineup = 'defense' AND cr.is_on_key = 1 THEN cr.total_poss END)::bigint AS def_on_poss,
      max(CASE WHEN cr.type_lineup = 'defense' AND cr.is_on_key = 0 THEN cr.total_poss END)::bigint AS def_off_poss
    FROM calc_rates cr
    GROUP BY cr.player_id, cr.team_id, cr.game_year
  ),

  -- CTE 7: Compute diffs + join names
  final_rows AS (
    SELECT
      p.player_id,
      p.team_id,
      r.firstname,
      r.lastname,
      r.team_name,
      p.game_year,
      p.off_on_ts,
      p.off_off_ts,
      p.def_on_ts,
      p.def_off_ts,
      p.off_on_efg,
      p.off_off_efg,
      p.def_on_efg,
      p.def_off_efg,
      p.off_on_oreb,
      p.off_off_oreb,
      p.def_on_oreb,
      p.def_off_oreb,
      p.off_on_tov,
      p.off_off_tov,
      p.def_on_tov,
      p.def_off_tov,
      p.def_on_disruptions,
      p.def_off_disruptions,
      p.off_on_ftr,
      p.off_off_ftr,
      p.def_on_ftr,
      p.def_off_ftr,
      p.off_on_poss,
      p.off_off_poss,
      p.def_on_poss,
      p.def_off_poss,
      round((p.off_on_efg  - p.off_off_efg)  * 100::numeric, 1) AS "Off eFG% Diff",
      round((p.off_on_ts   - p.off_off_ts)   * 100::numeric, 1) AS "Off TS% Diff",
      round((p.off_on_oreb - p.off_off_oreb) * 100::numeric, 1) AS "Off OREB% Diff",
      round((p.off_on_tov  - p.off_off_tov)  * 100::numeric, 1) AS "Off TOV% Diff",
      round((p.off_on_ftr  - p.off_off_ftr)  * 100::numeric, 1) AS "Off FTR Diff",
      round((p.def_on_efg  - p.def_off_efg)  * 100::numeric, 1) AS "Def eFG% Diff",
      round((p.def_on_ts   - p.def_off_ts)   * 100::numeric, 1) AS "Def TS% Diff",
      round((p.def_on_oreb - p.def_off_oreb) * 100::numeric, 1) AS "Def OREB% Diff",
      round((p.def_on_tov  - p.def_off_tov)  * 100::numeric, 1) AS "Def TOV% Diff",
      round((p.def_on_ftr  - p.def_off_ftr)  * 100::numeric, 1) AS "Def FTR Diff",
      round((p.def_on_disruptions - p.def_off_disruptions) * 100::numeric, 1) AS "Def Disruptions/100 Diff"
    FROM pivoted p
    JOIN (
      SELECT DISTINCT
        full_rosters.player_id,
        full_rosters.team_id,
        full_rosters.firstname,
        full_rosters.lastname,
        full_rosters.team_name
      FROM basketball_test.full_rosters
    ) r ON p.player_id = r.player_id AND p.team_id = r.team_id
  )

  SELECT
    fr.player_id,
    fr.team_id,
    fr.firstname,
    fr.lastname,
    fr.team_name,
    fr.game_year,
    fr.off_on_ts,
    fr.off_off_ts,
    fr.def_on_ts,
    fr.def_off_ts,
    fr.off_on_efg,
    fr.off_off_efg,
    fr.def_on_efg,
    fr.def_off_efg,
    fr.off_on_oreb,
    fr.off_off_oreb,
    fr.def_on_oreb,
    fr.def_off_oreb,
    fr.off_on_tov,
    fr.off_off_tov,
    fr.def_on_tov,
    fr.def_off_tov,
    fr.def_on_disruptions,
    fr.def_off_disruptions,
    fr.off_on_ftr,
    fr.off_off_ftr,
    fr.def_on_ftr,
    fr.def_off_ftr,
    fr.off_on_poss,
    fr.off_off_poss,
    fr.def_on_poss,
    fr.def_off_poss,
    fr."Off eFG% Diff",
    fr."Off TS% Diff",
    fr."Off OREB% Diff",
    fr."Off TOV% Diff",
    fr."Off FTR Diff",
    fr."Def eFG% Diff",
    fr."Def TS% Diff",
    fr."Def OREB% Diff",
    fr."Def TOV% Diff",
    fr."Def FTR Diff",
    fr."Def Disruptions/100 Diff"
  FROM final_rows fr
  ORDER BY fr."Off TS% Diff" DESC NULLS LAST;
END;
$$;
