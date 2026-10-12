-- Team floor time under the shared game-context, clutch and starters filters.
-- The single owner of the minutes behind Tab 3's pace columns
-- (docs/shared_definitions.md, "Minutes").
--
-- Outside a clutch window: MAX(segment_seconds) once per physical segment,
-- summed over every segment. Inside one: last minus first canonical event time
-- among the segment's in-window rows, substitution and timeout rows included.
-- A starters filter tests the PHYSICAL starters: floor time never follows
-- credit. Same parameters, in the same order, as get_team_ratings_dynamic.
--
-- Moved 2026-10-12 from an inline query in app/R/server_tab3.R
-- (fetch_team_game_minutes), which outside a clutch or starters filter did not
-- measure time at all: it returned the nominal game length, 40 + 5 per
-- overtime, and so overstated the time behind a game with truncated
-- play-by-play. For the game filters NULL and '' both mean "no filter".
DROP FUNCTION IF EXISTS basketball_test.get_team_minutes_dynamic;

CREATE OR REPLACE FUNCTION basketball_test.get_team_minutes_dynamic(
    p_game_year            INT,
    p_start_date           DATE DEFAULT NULL,
    p_end_date             DATE DEFAULT NULL,
    p_game_type_csv        TEXT DEFAULT NULL,
    p_opp_team_ids_csv     TEXT DEFAULT NULL,
    p_home_away            TEXT DEFAULT 'all',
    p_outcome              TEXT DEFAULT 'all',
    p_opp_rank_side        TEXT DEFAULT 'all',
    p_opp_rank_n           INT  DEFAULT NULL,
    p_opp_rank_metric      TEXT DEFAULT 'net',
    p_max_margin           INT  DEFAULT NULL,
    p_margin_status        TEXT DEFAULT 'all',
    p_max_time_remaining   INT  DEFAULT NULL,
    p_ot_margin_filter     BOOLEAN DEFAULT FALSE,
    p_min_gn               INT DEFAULT NULL,
    p_max_gn               INT DEFAULT NULL,
    p_last_n_games         INT DEFAULT NULL,
    p_num_starters_off     INT DEFAULT NULL,
    p_num_starters_def     INT DEFAULT NULL,
    p_num_starters_off_min INT DEFAULT NULL,
    p_num_starters_off_max INT DEFAULT NULL,
    p_num_starters_def_min INT DEFAULT NULL,
    p_num_starters_def_max INT DEFAULT NULL
)
RETURNS TABLE (
    team_id INT,
    minutes NUMERIC
)
LANGUAGE sql
STABLE
SET plan_cache_mode = force_custom_plan
AS $$
  WITH params AS (
    SELECT
      CASE WHEN p_game_type_csv IS NULL OR btrim(p_game_type_csv) = '' THEN NULL::int4[]
           ELSE string_to_array(regexp_replace(p_game_type_csv, '\s+', '', 'g'), ',')::int4[] END AS game_types,
      CASE WHEN p_opp_team_ids_csv IS NULL OR btrim(p_opp_team_ids_csv) = '' THEN NULL::int4[]
           ELSE string_to_array(regexp_replace(p_opp_team_ids_csv, '\s+', '', 'g'), ',')::int4[] END AS opp_ids,
      COALESCE(NULLIF(btrim(p_home_away), ''), 'all') AS home_away,
      COALESCE(NULLIF(btrim(p_outcome), ''), 'all') AS outcome,
      COALESCE(NULLIF(btrim(p_opp_rank_side), ''), 'all') AS rank_side,
      COALESCE(NULLIF(btrim(p_opp_rank_metric), ''), 'net') AS rank_metric,
      COALESCE(NULLIF(btrim(p_margin_status), ''), 'all') AS margin_status,
      (p_max_margin IS NOT NULL
        OR COALESCE(NULLIF(btrim(p_margin_status), ''), 'all') <> 'all'
        OR p_max_time_remaining IS NOT NULL) AS clutch_active,
      COALESCE(p_num_starters_off_min, p_num_starters_off) AS own_min,
      COALESCE(p_num_starters_off_max, p_num_starters_off) AS own_max,
      COALESCE(p_num_starters_def_min, p_num_starters_def) AS opp_min,
      COALESCE(p_num_starters_def_max, p_num_starters_def) AS opp_max
  ),
  -- Last N: rank each team's whole season, then apply the other filters.
  schedule_ranked AS (
    SELECT fsr.game_id, fsr.team_id, fsr.game_year,
           ROW_NUMBER() OVER (
             PARTITION BY fsr.team_id, fsr.game_year
             ORDER BY fsr.game_date DESC NULLS LAST, fsr.game_id DESC
           ) AS rn_recent
    FROM basketball_test.final_schedule_mv fsr
    WHERE fsr.game_year = p_game_year
  ),
  games_base AS (
    SELECT fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id
    FROM basketball_test.final_schedule_mv fs
    JOIN schedule_ranked sr
      ON sr.game_id = fs.game_id AND sr.team_id = fs.team_id AND sr.game_year = fs.game_year
    CROSS JOIN params p0
    WHERE fs.game_year = p_game_year
      AND (p_start_date IS NULL OR fs.game_date >= p_start_date)
      AND (p_end_date   IS NULL OR fs.game_date <= p_end_date)
      AND (p0.game_types IS NULL OR fs.game_type = ANY(p0.game_types))
      AND (p0.opp_ids    IS NULL OR fs.opp_team_id = ANY(p0.opp_ids))
      AND (p0.home_away = 'all' OR (p0.home_away = 'home' AND fs.is_home) OR (p0.home_away = 'away' AND NOT fs.is_home))
      AND (p0.outcome = 'all' OR (p0.outcome = 'win' AND fs.has_won IS TRUE) OR (p0.outcome = 'loss' AND fs.has_won IS FALSE))
      AND (p_min_gn IS NULL OR fs.gn >= p_min_gn)
      AND (p_max_gn IS NULL OR fs.gn <= p_max_gn)
      AND (p_last_n_games IS NULL OR sr.rn_recent <= p_last_n_games)
  ),
  games_ranked AS (
    SELECT gb.game_id, gb.team_id,
           CASE p0.rank_metric WHEN 'off' THEN r.rank_off_ppp
                               WHEN 'def' THEN r.rank_def_ppp
                               ELSE r.rank_net_rtg END AS opp_rank,
           MAX(CASE p0.rank_metric WHEN 'off' THEN r.rank_off_ppp
                                   WHEN 'def' THEN r.rank_def_ppp
                                   ELSE r.rank_net_rtg END) OVER (PARTITION BY gb.game_year) AS max_rank
    FROM games_base gb
    CROSS JOIN params p0
    LEFT JOIN basketball_test.team_ppp_ratings_mv r
      ON r.game_year::integer = gb.game_year
     AND r.team_id::integer = gb.opp_team_id
     AND p0.rank_side IN ('top', 'bottom')
  ),
  games_filtered AS (
    SELECT gr.game_id, gr.team_id
    FROM games_ranked gr
    CROSS JOIN params p0
    WHERE p0.rank_side = 'all' OR p_opp_rank_n IS NULL
       OR (p0.rank_side = 'top'    AND gr.opp_rank <= p_opp_rank_n)
       OR (p0.rank_side = 'bottom' AND gr.opp_rank >= (gr.max_rank - p_opp_rank_n + 1))
  ),
  -- NOTE: pre-event margin (subtract the row's own points from the score).
  rows_in_scope AS (
    SELECT d.team_id, d.game_id, d.lineup_hash, d.segment_id, d.id,
           d.event_elapsed_seconds, d.segment_seconds
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN games_filtered gf ON gf.game_id = d.game_id AND gf.team_id = d.team_id
    CROSS JOIN params p0
    WHERE d.lineup_hash IS NOT NULL
      AND d.segment_id IS NOT NULL
      AND (p_max_margin IS NULL
           OR ABS(CASE WHEN d.type_lineup = 'offense'
                       THEN (d.own_team_score - COALESCE(d.team_score, 0)) - d.opp_team_score
                       ELSE d.own_team_score - (d.opp_team_score - COALESCE(d.team_score, 0))
                  END) <= p_max_margin
           OR (d.quarter > 4 AND NOT COALESCE(p_ot_margin_filter, FALSE)))
      AND (p0.margin_status = 'all'
           OR (p0.margin_status = 'leading' AND
               CASE WHEN d.type_lineup = 'offense'
                    THEN (d.own_team_score - COALESCE(d.team_score, 0)) > d.opp_team_score
                    ELSE d.own_team_score > (d.opp_team_score - COALESCE(d.team_score, 0))
               END)
           OR (p0.margin_status = 'trailing' AND
               CASE WHEN d.type_lineup = 'offense'
                    THEN (d.own_team_score - COALESCE(d.team_score, 0)) < d.opp_team_score
                    ELSE d.own_team_score < (d.opp_team_score - COALESCE(d.team_score, 0))
               END)
           OR (p0.margin_status = 'tied' AND
               CASE WHEN d.type_lineup = 'offense'
                    THEN (d.own_team_score - COALESCE(d.team_score, 0)) = d.opp_team_score
                    ELSE d.own_team_score = (d.opp_team_score - COALESCE(d.team_score, 0))
               END)
           OR (d.quarter > 4 AND NOT COALESCE(p_ot_margin_filter, FALSE)))
      AND (p_max_time_remaining IS NULL OR d.end_game_seconds_remaining <= p_max_time_remaining OR d.quarter > 4)
      AND (p0.own_min IS NULL OR d.own_starters >= p0.own_min)
      AND (p0.own_max IS NULL OR d.own_starters <= p0.own_max)
      AND (p0.opp_min IS NULL OR d.opp_starters >= p0.opp_min)
      AND (p0.opp_max IS NULL OR d.opp_starters <= p0.opp_max)
  ),
  seg_times AS (
    SELECT r.team_id, r.game_id, r.lineup_hash, r.segment_id,
           CASE WHEN p0.clutch_active THEN
             GREATEST(
               (array_agg(r.event_elapsed_seconds ORDER BY r.id DESC) FILTER (WHERE r.event_elapsed_seconds IS NOT NULL))[1] -
               (array_agg(r.event_elapsed_seconds ORDER BY r.id) FILTER (WHERE r.event_elapsed_seconds IS NOT NULL))[1],
               0)::numeric
           ELSE MAX(r.segment_seconds)::numeric
           END AS seg_seconds
    FROM rows_in_scope r
    CROSS JOIN params p0
    GROUP BY r.team_id, r.game_id, r.lineup_hash, r.segment_id, p0.clutch_active
  )
  SELECT st.team_id, ROUND(SUM(COALESCE(st.seg_seconds, 0)) / 60.0, 3) AS minutes
  FROM seg_times st
  GROUP BY st.team_id
$$;
