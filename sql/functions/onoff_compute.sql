DROP FUNCTION IF EXISTS basketball_test.onoff_compute(date, date, text, int4, int4, numeric, text, text, text, text, text, text, int4, text, int4, int4, int4, int4, int4, int4, int4, int4, int4);

CREATE OR REPLACE FUNCTION basketball_test.onoff_compute(
    p_start_date      DATE,
    p_end_date        DATE,
    p_team_ids        TEXT,
    p_min_all         INTEGER,
    p_min_on          INTEGER,
    p_min_net         NUMERIC,
    p_game_year       TEXT,
    p_game_type_csv   TEXT DEFAULT NULL,
    p_opp_ids_csv     TEXT DEFAULT NULL,
    p_home_away       TEXT DEFAULT 'all',
    p_outcome         TEXT DEFAULT 'all',
    p_opp_rank_side   TEXT DEFAULT NULL,
    p_opp_rank_n      INTEGER DEFAULT NULL,
    p_opp_rank_metric TEXT DEFAULT NULL,
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
    "Team" text, "First Name" text, "Last Name" text,
    "Net RTG Diff" numeric, "Off ON Diff" numeric, "Def ON Diff" numeric,
    "Off ON PPP" numeric, "Def ON PPP" numeric, "On Net RTG" numeric,
    "Off OFF PPP" numeric, "Def OFF PPP" numeric, "Off Net RTG" numeric,
    "ON Poss" numeric, "OFF Poss" numeric, minutes numeric,
    pr_net double precision, pr_off_on double precision, pr_off_off double precision,
    pr_def_on_inv double precision, pr_def_off_inv double precision,
    pr_off_on_d double precision, pr_def_on_d double precision, pr_def_on_d_inv double precision,
    pr_on_net double precision, pr_off_net double precision,
    player_id integer, team_id integer,
    off_on_fg2_made bigint, off_on_fg2_att bigint, off_on_fg3_made bigint, off_on_fg3_att bigint,
    off_off_fg2_made bigint, off_off_fg2_att bigint, off_off_fg3_made bigint, off_off_fg3_att bigint,
    def_on_fg2_made bigint, def_on_fg2_att bigint, def_on_fg3_made bigint, def_on_fg3_att bigint,
    def_off_fg2_made bigint, def_off_fg2_att bigint, def_off_fg3_made bigint, def_off_fg3_att bigint
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_team_ids        int4[];
  v_game_types      int4[];
  v_opp_ids         int4[];
  v_home_away       text;
  v_outcome         text;
  v_opp_rank_side   text;
  v_opp_rank_metric text;
  v_game_year       text;
BEGIN
  -- [Input Normalization]
  v_home_away       := COALESCE(NULLIF(btrim(p_home_away), ''), 'all');
  v_outcome         := COALESCE(NULLIF(btrim(p_outcome), ''), 'all');
  v_opp_rank_side   := NULLIF(btrim(p_opp_rank_side), '');
  v_opp_rank_metric := NULLIF(btrim(p_opp_rank_metric), '');
  v_game_year       := NULLIF(btrim(p_game_year), '');

  -- Parse CSVs
  IF p_team_ids IS NOT NULL AND length(btrim(p_team_ids)) > 0 THEN
    v_team_ids := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_team_ids, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_game_type_csv IS NOT NULL AND length(btrim(p_game_type_csv)) > 0 THEN
    v_game_types := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_game_type_csv, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_opp_ids_csv IS NOT NULL AND length(btrim(p_opp_ids_csv)) > 0 THEN
    v_opp_ids := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_opp_ids_csv, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;

  RETURN QUERY
  WITH
  /* ------------------------------------------------------------
     Opponent strength ranking — uses mv_lineup_totals_by_day
     (pre-aggregated) instead of df_pts_poss_lineups_longer_mv
  ------------------------------------------------------------ */
  sched_base_for_ranks AS (
    SELECT DISTINCT
      fs.game_id,
      fs.game_year,
      fs.game_date,
      fs.game_type
    FROM basketball_test.final_schedule_mv fs
    WHERE fs.game_date BETWEEN p_start_date AND p_end_date
      AND (v_game_year IS NULL OR fs.game_year::text = v_game_year)
      AND (v_game_types IS NULL OR fs.game_type = ANY(v_game_types))
  ),
  team_game_raw AS (
    SELECT
      d.game_id,
      d.team_id,
      d.type_lineup,
      SUM(d.team_score) AS pts,
      SUM(CASE WHEN d.final_end_poss IS TRUE THEN 1 ELSE 0 END) AS poss
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN sched_base_for_ranks sb
      ON sb.game_id = d.game_id
    WHERE (COALESCE(p_num_starters_off_min, p_num_starters_off) IS NULL OR d.own_starters >= COALESCE(p_num_starters_off_min, p_num_starters_off))
      AND (COALESCE(p_num_starters_off_max, p_num_starters_off) IS NULL OR d.own_starters <= COALESCE(p_num_starters_off_max, p_num_starters_off))
      AND (COALESCE(p_num_starters_def_min, p_num_starters_def) IS NULL OR d.opp_starters >= COALESCE(p_num_starters_def_min, p_num_starters_def))
      AND (COALESCE(p_num_starters_def_max, p_num_starters_def) IS NULL OR d.opp_starters <= COALESCE(p_num_starters_def_max, p_num_starters_def))
    GROUP BY d.game_id, d.team_id, d.type_lineup
  ),
  team_game_ppp AS (
    SELECT
      tgr.game_id,
      tgr.team_id,
      MAX(CASE WHEN tgr.type_lineup = 'offense'
               THEN ROUND(tgr.pts / NULLIF(tgr.poss,0)::numeric * 100, 2) END) AS off_ppp,
      MAX(CASE WHEN tgr.type_lineup = 'defense'
               THEN ROUND(tgr.pts / NULLIF(tgr.poss,0)::numeric * 100, 2) END) AS def_ppp
    FROM team_game_raw tgr
    GROUP BY tgr.game_id, tgr.team_id
  ),
  opp_season_metrics AS (
    SELECT
      tgp.team_id,
      ROUND(AVG(tgp.off_ppp), 2) AS off_ppp,
      ROUND(AVG(tgp.def_ppp), 2) AS def_ppp,
      ROUND(AVG(tgp.off_ppp - tgp.def_ppp), 2) AS net_ppp
    FROM team_game_ppp tgp
    GROUP BY tgp.team_id
  ),
  opp_ranked AS (
    SELECT
      osm.*,
      CASE
        WHEN COALESCE(v_opp_rank_metric,'') = 'off' THEN osm.off_ppp
        WHEN COALESCE(v_opp_rank_metric,'') = 'def' THEN -osm.def_ppp
        ELSE osm.net_ppp
      END AS metric_for_rank
    FROM opp_season_metrics osm
  ),
  opp_topbottom AS (
    SELECT opp_ranked.team_id
    FROM opp_ranked
    WHERE v_opp_rank_side IS NOT NULL
      AND p_opp_rank_n IS NOT NULL
      AND v_opp_rank_metric IS NOT NULL
    ORDER BY
      CASE WHEN v_opp_rank_side = 'bottom' THEN metric_for_rank END ASC NULLS LAST,
      CASE WHEN v_opp_rank_side = 'top'    THEN metric_for_rank END DESC NULLS LAST
    LIMIT CASE WHEN p_opp_rank_n IS NULL OR p_opp_rank_n <= 0 THEN 0 ELSE p_opp_rank_n END
  ),

  -- schedule filtered by date/year + all game filters
  sched AS (
    SELECT DISTINCT
      fs.game_id,
      fs.team_id,
      fs.opp_team_id,
      fs.game_date,
      fs.game_year,
      fs.game_type,
      fs.is_home,
      fs.has_won
    FROM basketball_test.final_schedule_mv fs
    WHERE fs.game_date BETWEEN p_start_date AND p_end_date
      AND (v_game_year IS NULL OR fs.game_year::text = v_game_year)
      -- team filter
      AND (v_team_ids IS NULL OR fs.team_id = ANY(v_team_ids))
      -- game type filter
      AND (v_game_types IS NULL OR fs.game_type = ANY(v_game_types))
      -- explicit opponent filter
      AND (v_opp_ids IS NULL OR fs.opp_team_id = ANY(v_opp_ids))
      -- opponent rank filter (top/bottom N)
      AND (
        v_opp_rank_side IS NULL OR p_opp_rank_n IS NULL OR v_opp_rank_metric IS NULL
        OR fs.opp_team_id IN (SELECT opp_topbottom.team_id FROM opp_topbottom)
      )
      -- home/away filter
      AND (
        v_home_away = 'all'
        OR (v_home_away = 'home' AND fs.is_home IS TRUE)
        OR (v_home_away = 'away' AND fs.is_home IS FALSE)
      )
      -- outcome filter
      AND (
        v_outcome = 'all'
        OR (v_outcome = 'win'  AND fs.has_won IS TRUE)
        OR (v_outcome = 'loss' AND fs.has_won IS FALSE)
      )
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

  /* ------------------------------------------------------------
     Core aggregation - reads from merged player_four_factors_by_game
     instead of joining lineups_lookup x df_pts_poss_lineups_longer_mv
  ------------------------------------------------------------ */
  agg AS (
    SELECT
      p.player_id,
      p.team_id,
      p.is_on_key,
      p.type_lineup,
      p.game_year,
      SUM(p.total_points)  AS total_pts,
      SUM(p.total_poss) AS total_poss,
      ROUND(
        SUM(p.total_points) / NULLIF(SUM(p.total_poss), 0)::numeric * 100, 1
      ) AS ppp_calc,
      SUM(p.fg2_made)::bigint AS fg2_made,
      SUM(p.fg2_att)::bigint  AS fg2_att,
      SUM(p.fg3_made)::bigint AS fg3_made,
      SUM(p.fg3_att)::bigint  AS fg3_att,
      SUM(COALESCE(p.onoff_minutes, 0))::numeric AS minutes
    FROM basketball_test.player_four_factors_by_game p
    JOIN sched s ON s.game_id = p.game_id AND s.team_id = p.team_id
    WHERE (COALESCE(p_num_starters_off_min, p_num_starters_off) IS NULL OR p.own_starters >= COALESCE(p_num_starters_off_min, p_num_starters_off))
      AND (COALESCE(p_num_starters_off_max, p_num_starters_off) IS NULL OR p.own_starters <= COALESCE(p_num_starters_off_max, p_num_starters_off))
      AND (COALESCE(p_num_starters_def_min, p_num_starters_def) IS NULL OR p.opp_starters >= COALESCE(p_num_starters_def_min, p_num_starters_def))
      AND (COALESCE(p_num_starters_def_max, p_num_starters_def) IS NULL OR p.opp_starters <= COALESCE(p_num_starters_def_max, p_num_starters_def))
    GROUP BY p.player_id, p.team_id, p.is_on_key, p.type_lineup, p.game_year
  ),

  -- PPP percentile per (type_lineup, game_year)
  ppp_rank_base AS (
    SELECT
      a.*,
      PERCENT_RANK() OVER (
        PARTITION BY a.type_lineup, a.game_year
        ORDER BY a.ppp_calc
      ) AS pr_ppp_raw
    FROM agg a
  ),
  ppp_ranked AS (
    SELECT
      p.*,
      CASE
        WHEN p.type_lineup = 'defense' THEN 1 - p.pr_ppp_raw
        ELSE p.pr_ppp_raw
      END AS pr_ppp_better
    FROM ppp_rank_base p
  ),

  -- pre-scope roster names to filtered schedule only (avoids broad full_rosters x schedule DISTINCT)
  roster_names AS (
    SELECT
      fr.player_id,
      fr.team_id,
      s.game_year,
      MIN(fr.firstname) AS firstname,
      MIN(fr.lastname)  AS lastname,
      MIN(fr.team_name) AS team_name
    FROM basketball_test.full_rosters fr
    JOIN sched s
      ON s.game_id = fr.game_id
     AND s.team_id = fr.team_id
    GROUP BY fr.player_id, fr.team_id, s.game_year
  ),

  -- attach names + team_name + game_year
  with_names AS (
    SELECT
      a.player_id,
      a.team_id,
      a.game_year,
      a.is_on_key,
      a.type_lineup,
      a.total_pts,
      a.total_poss,
      a.ppp_calc,
      a.pr_ppp_raw,
      a.pr_ppp_better,
      a.fg2_made,
      a.fg2_att,
      a.fg3_made,
      a.fg3_att,
      a.minutes,
      r.firstname,
      r.lastname,
      r.team_name
    FROM ppp_ranked a
    JOIN roster_names r USING (player_id, team_id, game_year)
  ),

  -- eligibility per player-team-year
  elig AS (
    SELECT
      wn.player_id,
      wn.team_id,
      wn.game_year,
      MIN(wn.total_poss) AS min_poss_all,
      MAX(CASE WHEN wn.is_on_key = 1 THEN wn.total_poss ELSE 0 END) AS max_poss_on
    FROM with_names wn
    GROUP BY wn.player_id, wn.team_id, wn.game_year
  ),

  -- apply min_all/min_on filters
  filtered AS (
    SELECT
      wn.player_id,
      wn.team_id,
      wn.game_year,
      wn.is_on_key,
      wn.type_lineup,
      wn.total_pts,
      wn.total_poss,
      wn.ppp_calc,
      wn.pr_ppp_raw,
      wn.pr_ppp_better,
      wn.fg2_made,
      wn.fg2_att,
      wn.fg3_made,
      wn.fg3_att,
      wn.minutes,
      wn.firstname,
      wn.lastname,
      wn.team_name
    FROM with_names wn
    JOIN elig e USING (player_id, team_id, game_year)
    WHERE e.min_poss_all >= p_min_all
      AND e.max_poss_on  >= p_min_on
  ),

  -- Pivot to one row per (player, team, year, type_lineup) with ON/OFF stats
  type_level AS (
    SELECT
      f.player_id,
      f.team_id,
      f.game_year,
      f.type_lineup,
      MIN(f.team_name) AS team_name,
      MIN(f.firstname) AS firstname,
      MIN(f.lastname) AS lastname,

      MAX(CASE WHEN f.is_on_key = 1 THEN f.ppp_calc END) AS ppp_on,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.ppp_calc END) AS ppp_off,
      MAX(CASE WHEN f.is_on_key = 1 THEN f.pr_ppp_better END) AS pr_ppp_on,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.pr_ppp_better END) AS pr_ppp_off,
      MAX(CASE WHEN f.is_on_key = 1 THEN f.total_poss END) AS poss_on,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.total_poss END) AS poss_off,
      MAX(CASE WHEN f.is_on_key = 1 THEN f.minutes END) AS minutes_on,

      MAX(CASE WHEN f.is_on_key = 1 THEN f.fg2_made END) AS fg2_on_made,
      MAX(CASE WHEN f.is_on_key = 1 THEN f.fg2_att END)  AS fg2_on_att,
      MAX(CASE WHEN f.is_on_key = 1 THEN f.fg3_made END) AS fg3_on_made,
      MAX(CASE WHEN f.is_on_key = 1 THEN f.fg3_att END)  AS fg3_on_att,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.fg2_made END) AS fg2_off_made,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.fg2_att END)  AS fg2_off_att,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.fg3_made END) AS fg3_off_made,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.fg3_att END)  AS fg3_off_att
    FROM filtered f
    GROUP BY f.player_id, f.team_id, f.game_year, f.type_lineup
  ),

  type_ranked AS (
    SELECT
      tl.*,
      (tl.ppp_on - tl.ppp_off) AS net_rtg,
      PERCENT_RANK() OVER (
        PARTITION BY tl.type_lineup, tl.game_year
        ORDER BY (tl.ppp_on - tl.ppp_off)
      ) AS pr_net_rtg_raw,
      CASE
        WHEN tl.type_lineup = 'defense' THEN
          1 - PERCENT_RANK() OVER (
                PARTITION BY tl.type_lineup, tl.game_year
                ORDER BY (tl.ppp_on - tl.ppp_off)
              )
        ELSE
          PERCENT_RANK() OVER (
            PARTITION BY tl.type_lineup, tl.game_year
            ORDER BY (tl.ppp_on - tl.ppp_off)
          )
      END AS pr_net_rtg_better
    FROM type_level tl
    WHERE (tl.ppp_on - tl.ppp_off) IS NOT NULL
  ),

  -- collapse to one row per (player, team, year)
  final_rows AS (
    SELECT
      tr.player_id,
      tr.team_id,
      tr.game_year,
      MIN(tr.team_name) AS team_name,
      MIN(tr.firstname) AS firstname,
      MIN(tr.lastname) AS lastname,

      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.ppp_on END)  AS offense_on_ppp,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.ppp_off END) AS offense_off_ppp,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.ppp_on END)  AS defense_on_ppp,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.ppp_off END) AS defense_off_ppp,

      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.pr_ppp_on END)  AS pr_off_on,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.pr_ppp_off END) AS pr_off_off,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.pr_ppp_on END)  AS pr_def_on_inv,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.pr_ppp_off END) AS pr_def_off_inv,

      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.net_rtg END) AS offense_on_diff,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.net_rtg END) AS defense_on_diff,

      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.pr_net_rtg_better END) AS pr_off_on_d,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.pr_net_rtg_raw END)    AS pr_def_on_d,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.pr_net_rtg_better END) AS pr_def_on_d_inv,

      ROUND(
        MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.net_rtg END)
        - MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.net_rtg END),
        2
      ) AS total_net_rtg,

      GREATEST(
        COALESCE(MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.poss_on END), 0),
        COALESCE(MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.poss_on END), 0)
      ) AS on_poss,
      GREATEST(
        COALESCE(MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.poss_off END), 0),
        COALESCE(MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.poss_off END), 0)
      ) AS off_poss,
      COALESCE(
        MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.minutes_on END),
        MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.minutes_on END),
        0
      )::numeric AS minutes,
      -- Shooting splits (16 columns) carried from agg through pipeline
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.fg2_on_made END) AS off_on_fg2_made,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.fg2_on_att END)  AS off_on_fg2_att,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.fg3_on_made END) AS off_on_fg3_made,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.fg3_on_att END)  AS off_on_fg3_att,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.fg2_off_made END) AS off_off_fg2_made,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.fg2_off_att END)  AS off_off_fg2_att,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.fg3_off_made END) AS off_off_fg3_made,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.fg3_off_att END)  AS off_off_fg3_att,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg2_on_made END) AS def_on_fg2_made,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg2_on_att END)  AS def_on_fg2_att,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg3_on_made END) AS def_on_fg3_made,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg3_on_att END)  AS def_on_fg3_att,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg2_off_made END) AS def_off_fg2_made,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg2_off_att END)  AS def_off_fg2_att,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg3_off_made END) AS def_off_fg3_made,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg3_off_att END)  AS def_off_fg3_att
    FROM type_ranked tr
    GROUP BY
      tr.player_id,
      tr.team_id,
      tr.game_year
  ),

  final_net_rank AS (
    SELECT
      fr.player_id,
      fr.team_id,
      fr.game_year,
      PERCENT_RANK() OVER (
        PARTITION BY fr.game_year
        ORDER BY fr.total_net_rtg
      ) AS pr_net
    FROM final_rows fr
    WHERE fr.total_net_rtg IS NOT NULL
  ),

  final_scored AS (
    SELECT
      fr.player_id,
      fr.team_id,
      fr.game_year,
      fr.team_name,
      fr.firstname,
      fr.lastname,
      fr.offense_on_ppp,
      fr.offense_off_ppp,
      fr.defense_on_ppp,
      fr.defense_off_ppp,
      fr.pr_off_on,
      fr.pr_off_off,
      fr.pr_def_on_inv,
      fr.pr_def_off_inv,
      fr.offense_on_diff,
      fr.defense_on_diff,
      fr.pr_off_on_d,
      fr.pr_def_on_d,
      fr.pr_def_on_d_inv,
      fr.total_net_rtg,
      fnr.pr_net,
      fr.on_poss,
      fr.off_poss,
      fr.minutes,
      fr.off_on_fg2_made, fr.off_on_fg2_att, fr.off_on_fg3_made, fr.off_on_fg3_att,
      fr.off_off_fg2_made, fr.off_off_fg2_att, fr.off_off_fg3_made, fr.off_off_fg3_att,
      fr.def_on_fg2_made, fr.def_on_fg2_att, fr.def_on_fg3_made, fr.def_on_fg3_att,
      fr.def_off_fg2_made, fr.def_off_fg2_att, fr.def_off_fg3_made, fr.def_off_fg3_att,
      fr.offense_on_ppp  - fr.defense_on_ppp  AS on_net_rtg,
      fr.offense_off_ppp - fr.defense_off_ppp AS off_net_rtg,
      PERCENT_RANK() OVER (
        PARTITION BY fr.game_year
        ORDER BY (fr.offense_on_ppp - fr.defense_on_ppp)
      ) AS pr_on_net,
      PERCENT_RANK() OVER (
        PARTITION BY fr.game_year
        ORDER BY (fr.offense_off_ppp - fr.defense_off_ppp)
      ) AS pr_off_net
    FROM final_rows fr
    LEFT JOIN final_net_rank fnr
      ON fnr.player_id = fr.player_id
     AND fnr.team_id = fr.team_id
     AND fnr.game_year = fr.game_year
  )
  SELECT
    fs.team_name   AS "Team",
    fs.firstname   AS "First Name",
    fs.lastname    AS "Last Name",
    fs.total_net_rtg AS "Net RTG Diff",
    fs.offense_on_diff AS "Off ON Diff",
    fs.defense_on_diff AS "Def ON Diff",
    fs.offense_on_ppp  AS "Off ON PPP",
    fs.defense_on_ppp  AS "Def ON PPP",
    fs.on_net_rtg      AS "On Net RTG",
    fs.offense_off_ppp AS "Off OFF PPP",
    fs.defense_off_ppp AS "Def OFF PPP",
    fs.off_net_rtg     AS "Off Net RTG",
    fs.on_poss         AS "ON Poss",
    fs.off_poss        AS "OFF Poss",
    fs.minutes,
    fs.pr_net,
    fs.pr_off_on,
    fs.pr_off_off,
    fs.pr_def_on_inv,
    fs.pr_def_off_inv,
    fs.pr_off_on_d,
    fs.pr_def_on_d,
    fs.pr_def_on_d_inv,
    fs.pr_on_net,
    fs.pr_off_net,
    fs.player_id,
    fs.team_id,
    fs.off_on_fg2_made, fs.off_on_fg2_att, fs.off_on_fg3_made, fs.off_on_fg3_att,
    fs.off_off_fg2_made, fs.off_off_fg2_att, fs.off_off_fg3_made, fs.off_off_fg3_att,
    fs.def_on_fg2_made, fs.def_on_fg2_att, fs.def_on_fg3_made, fs.def_on_fg3_att,
    fs.def_off_fg2_made, fs.def_off_fg2_att, fs.def_off_fg3_made, fs.def_off_fg3_att
  FROM final_scored fs
  WHERE fs.total_net_rtg >= p_min_net
  ORDER BY "Net RTG Diff" DESC, "Team", "Last Name", "First Name";
END;
$$;
