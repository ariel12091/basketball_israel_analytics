-- EUROLEAGUE SHADOW SCHEMA -- candidate A for migration 045.
--
-- Single change against the live definitions (byte-identical to
-- sql/004_app_read_layer.sql): the analytical aggregation reads the base fact
-- euroleague.player_four_factors_by_game instead of the
-- euroleague.player_game_context view.
--
-- The view added, per fact row, a schedule primary-key probe and a CROSS JOIN
-- LATERAL VALUES two-perspective expansion whose columns the aggregation never
-- consumes. The `games` CTE already resolves every schedule filter, so joining
-- the fact to `games` on (game_id, team_id) is the same restriction.
--
-- The relation alias `c` is retained so no other token in either body changes.
-- Signatures, volatility, SECURITY mode, defaults, return columns, ordering and
-- plan_cache_mode are unchanged, and no DROP FUNCTION is issued (that would
-- wipe the app_readonly EXECUTE grants).

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.onoff_compute(
    p_competition          TEXT,
    p_game_year            INTEGER,
    p_start_date           DATE    DEFAULT NULL,
    p_end_date             DATE    DEFAULT NULL,
    p_team_ids_csv         TEXT    DEFAULT NULL,
    p_phase_csv            TEXT    DEFAULT NULL,
    p_opp_ids_csv          TEXT    DEFAULT NULL,
    p_home_away            TEXT    DEFAULT 'all',
    p_outcome              TEXT    DEFAULT 'all',
    p_opp_rank_side        TEXT    DEFAULT NULL,
    p_opp_rank_n           INTEGER DEFAULT NULL,
    p_opp_rank_metric      TEXT    DEFAULT NULL,
    p_min_gn               INTEGER DEFAULT NULL,
    p_max_gn               INTEGER DEFAULT NULL,
    p_last_n_games         INTEGER DEFAULT NULL,
    p_num_starters_off_min INTEGER DEFAULT NULL,
    p_num_starters_off_max INTEGER DEFAULT NULL,
    p_num_starters_def_min INTEGER DEFAULT NULL,
    p_num_starters_def_max INTEGER DEFAULT NULL,
    p_min_net              NUMERIC DEFAULT NULL,
    p_min_all              INTEGER DEFAULT 0,
    p_min_on               INTEGER DEFAULT 0
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
    player_id bigint, team_id bigint,
    off_on_fg2_made bigint, off_on_fg2_att bigint, off_on_fg3_made bigint, off_on_fg3_att bigint,
    off_off_fg2_made bigint, off_off_fg2_att bigint, off_off_fg3_made bigint, off_off_fg3_att bigint,
    def_on_fg2_made bigint, def_on_fg2_att bigint, def_on_fg3_made bigint, def_on_fg3_att bigint,
    def_off_fg2_made bigint, def_off_fg2_att bigint, def_off_fg3_made bigint, def_off_fg3_att bigint
)
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = force_custom_plan
AS $function$
DECLARE
  v_competition   text;
  v_team_ids      bigint[];
  v_opp_ids       bigint[];
  v_phases        text[];
  v_home_away     text;
  v_outcome       text;
  v_rank_side     text;
  v_rank_metric   text;
BEGIN
  v_competition := COALESCE(NULLIF(btrim(p_competition), ''), 'E');
  v_home_away   := COALESCE(NULLIF(btrim(p_home_away), ''), 'all');
  v_outcome     := COALESCE(NULLIF(btrim(p_outcome), ''), 'all');
  v_rank_side   := NULLIF(btrim(p_opp_rank_side), '');
  v_rank_metric := COALESCE(NULLIF(btrim(p_opp_rank_metric), ''), 'net');

  IF p_team_ids_csv IS NOT NULL AND length(btrim(p_team_ids_csv)) > 0 THEN
    v_team_ids := ARRAY(
      SELECT DISTINCT x::bigint
      FROM unnest(string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_opp_ids_csv IS NOT NULL AND length(btrim(p_opp_ids_csv)) > 0 THEN
    v_opp_ids := ARRAY(
      SELECT DISTINCT x::bigint
      FROM unnest(string_to_array(regexp_replace(p_opp_ids_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_phase_csv IS NOT NULL AND length(btrim(p_phase_csv)) > 0 THEN
    v_phases := ARRAY(
      SELECT DISTINCT btrim(x)
      FROM unnest(string_to_array(p_phase_csv, ',')) x
      WHERE btrim(x) <> '' ORDER BY 1);
  END IF;

  RETURN QUERY
  WITH schedule_ranked AS (
    -- Windowed last-N per team within the season (never a correlated subquery).
    SELECT
      fs.game_id, fs.team_id, fs.opp_team_id, fs.is_home, fs.has_won,
      s.round_number, s.phase, s.scheduled_at::date AS game_date,
      row_number() OVER (
        PARTITION BY fs.team_id
        ORDER BY s.scheduled_at DESC, fs.game_id DESC
      ) AS team_game_rank
    FROM euroleague.final_schedule fs
    JOIN euroleague.schedule s ON s.game_id = fs.game_id
    WHERE s.competition = v_competition
      AND s.season = p_game_year
  ),
  -- Opponent strength is SEASON-WIDE, read straight from
  -- euroleague.team_ppp_ratings_mv (migration 005) -- the same ranks the Team
  -- Ratings surface shows. "Top 3 defenses" therefore means top 3 over the
  -- whole season, which is what the phrase conventionally means, and it stays
  -- stable as the user narrows the date range instead of re-ranking underneath
  -- them. Combining a date range with an opponent-rank filter is rare, and the
  -- ranks are the same object the user would see on the ratings page.
  --
  -- Note this is a deliberate difference from the Israeli onoff_compute, which
  -- re-ranks opponents from the selected window (and averages per-game ratios
  -- while doing it).
  team_ranked AS (
    SELECT r.team_id, r.off_rank, r.def_rank, r.net_rank
    FROM euroleague.team_ppp_ratings_mv r
    WHERE r.competition = v_competition
      AND r.game_year   = p_game_year
  ),
  games AS (
    SELECT sr.*
    FROM schedule_ranked sr
    LEFT JOIN team_ranked tr ON tr.team_id = sr.opp_team_id
    WHERE (p_start_date IS NULL OR sr.game_date >= p_start_date)
      AND (p_end_date   IS NULL OR sr.game_date <= p_end_date)
      AND (v_phases   IS NULL OR sr.phase = ANY(v_phases))
      AND (v_opp_ids  IS NULL OR sr.opp_team_id = ANY(v_opp_ids))
      AND (p_min_gn   IS NULL OR sr.round_number >= p_min_gn)
      AND (p_max_gn   IS NULL OR sr.round_number <= p_max_gn)
      AND (p_last_n_games IS NULL OR sr.team_game_rank <= p_last_n_games)
      AND (v_home_away = 'all'
           OR (v_home_away = 'home' AND sr.is_home)
           OR (v_home_away = 'away' AND NOT sr.is_home))
      AND (v_outcome = 'all'
           OR (v_outcome = 'win'  AND sr.has_won)
           OR (v_outcome = 'loss' AND NOT sr.has_won))
      AND (
        v_rank_side IS NULL OR p_opp_rank_n IS NULL
        OR (v_rank_side = 'top' AND
            CASE v_rank_metric WHEN 'off' THEN tr.off_rank
                               WHEN 'def' THEN tr.def_rank
                               ELSE tr.net_rank END <= p_opp_rank_n)
        OR (v_rank_side = 'bottom' AND
            CASE v_rank_metric WHEN 'off' THEN tr.off_rank
                               WHEN 'def' THEN tr.def_rank
                               ELSE tr.net_rank END
            > (SELECT count(*) FROM team_ranked) - p_opp_rank_n)
      )
  ),
  agg AS (
    SELECT
      c.player_id, c.team_id, c.is_on_key, c.type_lineup,
      sum(c.total_points)::numeric AS pts,
      sum(c.total_poss)::numeric   AS poss,
      sum(c.onoff_minutes)::numeric AS mins,
      sum(c.fg2_made)::bigint AS fg2_made,
      sum(c.fg2_att)::bigint  AS fg2_att,
      sum(c.fg3_made)::bigint AS fg3_made,
      sum(c.fg3_att)::bigint  AS fg3_att
    FROM euroleague.player_four_factors_by_game c
    JOIN games g ON g.game_id = c.game_id AND g.team_id = c.team_id
    WHERE (p_num_starters_off_min IS NULL OR c.own_starters >= p_num_starters_off_min)
      AND (p_num_starters_off_max IS NULL OR c.own_starters <= p_num_starters_off_max)
      AND (p_num_starters_def_min IS NULL OR c.opp_starters >= p_num_starters_def_min)
      AND (p_num_starters_def_max IS NULL OR c.opp_starters <= p_num_starters_def_max)
      AND (v_team_ids IS NULL OR c.team_id = ANY(v_team_ids))
    GROUP BY c.player_id, c.team_id, c.is_on_key, c.type_lineup
  ),
  pivoted AS (
    SELECT
      a.player_id, a.team_id,
      max(a.pts)  FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_pts,
      max(a.poss) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_poss,
      max(a.pts)  FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 0) AS off_off_pts,
      max(a.poss) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 0) AS off_off_poss,
      max(a.pts)  FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 1) AS def_on_pts,
      max(a.poss) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 1) AS def_on_poss,
      max(a.pts)  FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 0) AS def_off_pts,
      max(a.poss) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 0) AS def_off_poss,
      max(a.mins) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS mins_on,
      max(a.fg2_made) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_fg2_made,
      max(a.fg2_att)  FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_fg2_att,
      max(a.fg3_made) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_fg3_made,
      max(a.fg3_att)  FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_fg3_att,
      max(a.fg2_made) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 0) AS off_off_fg2_made,
      max(a.fg2_att)  FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 0) AS off_off_fg2_att,
      max(a.fg3_made) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 0) AS off_off_fg3_made,
      max(a.fg3_att)  FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 0) AS off_off_fg3_att,
      max(a.fg2_made) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 1) AS def_on_fg2_made,
      max(a.fg2_att)  FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 1) AS def_on_fg2_att,
      max(a.fg3_made) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 1) AS def_on_fg3_made,
      max(a.fg3_att)  FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 1) AS def_on_fg3_att,
      max(a.fg2_made) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 0) AS def_off_fg2_made,
      max(a.fg2_att)  FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 0) AS def_off_fg2_att,
      max(a.fg3_made) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 0) AS def_off_fg3_made,
      max(a.fg3_att)  FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 0) AS def_off_fg3_att
    FROM agg a
    GROUP BY a.player_id, a.team_id
  ),
  rated AS (
    SELECT
      p.*,
      round(100.0 * p.off_on_pts  / NULLIF(p.off_on_poss, 0), 1)  AS off_on_ppp,
      round(100.0 * p.off_off_pts / NULLIF(p.off_off_poss, 0), 1) AS off_off_ppp,
      round(100.0 * p.def_on_pts  / NULLIF(p.def_on_poss, 0), 1)  AS def_on_ppp,
      round(100.0 * p.def_off_pts / NULLIF(p.def_off_poss, 0), 1) AS def_off_ppp
    FROM pivoted p
    WHERE least(
            COALESCE(p.off_on_poss, 0), COALESCE(p.off_off_poss, 0),
            COALESCE(p.def_on_poss, 0), COALESCE(p.def_off_poss, 0)
          ) >= COALESCE(p_min_all, 0)
      AND COALESCE(p.off_on_poss, 0) >= COALESCE(p_min_on, 0)
  ),
  diffed AS (
    SELECT
      r.*,
      r.off_on_ppp - r.def_on_ppp   AS on_net,
      r.off_off_ppp - r.def_off_ppp AS off_net,
      r.off_on_ppp - r.off_off_ppp  AS off_diff,
      r.def_on_ppp - r.def_off_ppp  AS def_diff,
      (r.off_on_ppp - r.off_off_ppp) - (r.def_on_ppp - r.def_off_ppp) AS net_diff
    FROM rated r
  ),
  ranked AS (
  SELECT
    t.display_name AS "Team",
    btrim(split_part(pl.display_name, ',', 2)) AS "First Name",
    btrim(split_part(pl.display_name, ',', 1)) AS "Last Name",
    -- Aliased to the RETURNS TABLE names: those apply to the function result,
    -- not to an inner CTE, so the final WHERE/ORDER BY needs them here.
    d.net_diff            AS "Net RTG Diff",
    d.off_diff            AS "Off ON Diff",
    d.def_diff            AS "Def ON Diff",
    d.off_on_ppp          AS "Off ON PPP",
    d.def_on_ppp          AS "Def ON PPP",
    d.on_net              AS "On Net RTG",
    d.off_off_ppp         AS "Off OFF PPP",
    d.def_off_ppp         AS "Def OFF PPP",
    d.off_net             AS "Off Net RTG",
    d.off_on_poss         AS "ON Poss",
    d.off_off_poss        AS "OFF Poss",
    round(d.mins_on, 1)   AS minutes,
    percent_rank() OVER (ORDER BY d.net_diff)     AS pr_net,
    percent_rank() OVER (ORDER BY d.off_on_ppp)   AS pr_off_on,
    percent_rank() OVER (ORDER BY d.off_off_ppp)  AS pr_off_off,
    percent_rank() OVER (ORDER BY d.def_on_ppp DESC)  AS pr_def_on_inv,
    percent_rank() OVER (ORDER BY d.def_off_ppp DESC) AS pr_def_off_inv,
    percent_rank() OVER (ORDER BY d.off_diff)     AS pr_off_on_d,
    percent_rank() OVER (ORDER BY d.def_diff)     AS pr_def_on_d,
    percent_rank() OVER (ORDER BY d.def_diff DESC) AS pr_def_on_d_inv,
    percent_rank() OVER (ORDER BY d.on_net)       AS pr_on_net,
    percent_rank() OVER (ORDER BY d.off_net)      AS pr_off_net,
    d.player_id, d.team_id,
    d.off_on_fg2_made, d.off_on_fg2_att, d.off_on_fg3_made, d.off_on_fg3_att,
    d.off_off_fg2_made, d.off_off_fg2_att, d.off_off_fg3_made, d.off_off_fg3_att,
    d.def_on_fg2_made, d.def_on_fg2_att, d.def_on_fg3_made, d.def_on_fg3_att,
    d.def_off_fg2_made, d.def_off_fg2_att, d.def_off_fg3_made, d.def_off_fg3_att
  FROM diffed d
  JOIN euroleague.players pl ON pl.player_id = d.player_id
  JOIN euroleague.teams   t  ON t.team_id    = d.team_id
  )
  -- min_net is enforced AFTER the percentile window, matching the Israeli
  -- pipeline order: percentiles describe the full eligible population.
  SELECT * FROM ranked r
  WHERE p_min_net IS NULL OR r."Net RTG Diff" >= p_min_net
  ORDER BY r."Net RTG Diff" DESC NULLS LAST, r."Team", r."Last Name", r."First Name";
END;
$function$;

CREATE OR REPLACE FUNCTION euroleague.four_factors_compute(
    p_competition          TEXT,
    p_game_year            INTEGER,
    p_start_date           DATE    DEFAULT NULL,
    p_end_date             DATE    DEFAULT NULL,
    p_team_ids_csv         TEXT    DEFAULT NULL,
    p_phase_csv            TEXT    DEFAULT NULL,
    p_opp_ids_csv          TEXT    DEFAULT NULL,
    p_home_away            TEXT    DEFAULT 'all',
    p_outcome              TEXT    DEFAULT 'all',
    p_opp_rank_side        TEXT    DEFAULT NULL,
    p_opp_rank_n           INTEGER DEFAULT NULL,
    p_opp_rank_metric      TEXT    DEFAULT NULL,
    p_min_gn               INTEGER DEFAULT NULL,
    p_max_gn               INTEGER DEFAULT NULL,
    p_last_n_games         INTEGER DEFAULT NULL,
    p_num_starters_off_min INTEGER DEFAULT NULL,
    p_num_starters_off_max INTEGER DEFAULT NULL,
    p_num_starters_def_min INTEGER DEFAULT NULL,
    p_num_starters_def_max INTEGER DEFAULT NULL
)
RETURNS TABLE (
    player_id BIGINT, team_id BIGINT,
    firstname TEXT, lastname TEXT, team_name TEXT, game_year INT,
    off_on_ts NUMERIC, off_off_ts NUMERIC, def_on_ts NUMERIC, def_off_ts NUMERIC,
    off_on_efg NUMERIC, off_off_efg NUMERIC, def_on_efg NUMERIC, def_off_efg NUMERIC,
    off_on_oreb NUMERIC, off_off_oreb NUMERIC, def_on_oreb NUMERIC, def_off_oreb NUMERIC,
    off_on_tov NUMERIC, off_off_tov NUMERIC, def_on_tov NUMERIC, def_off_tov NUMERIC,
    def_on_disruptions NUMERIC, def_off_disruptions NUMERIC,
    off_on_ftr NUMERIC, off_off_ftr NUMERIC, def_on_ftr NUMERIC, def_off_ftr NUMERIC,
    off_on_poss BIGINT, off_off_poss BIGINT, def_on_poss BIGINT, def_off_poss BIGINT,
    "Off eFG% Diff" NUMERIC, "Off TS% Diff" NUMERIC, "Off OREB% Diff" NUMERIC,
    "Off TOV% Diff" NUMERIC, "Off FTR Diff" NUMERIC,
    "Def eFG% Diff" NUMERIC, "Def TS% Diff" NUMERIC, "Def OREB% Diff" NUMERIC,
    "Def TOV% Diff" NUMERIC, "Def FTR Diff" NUMERIC,
    "Def Disruptions/100 Diff" NUMERIC
)
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = force_custom_plan
AS $function$
DECLARE
  v_competition text;
  v_team_ids    bigint[];
  v_opp_ids     bigint[];
  v_phases      text[];
  v_home_away   text;
  v_outcome     text;
  v_rank_side   text;
  v_rank_metric text;
BEGIN
  v_competition := COALESCE(NULLIF(btrim(p_competition), ''), 'E');
  v_home_away   := COALESCE(NULLIF(btrim(p_home_away), ''), 'all');
  v_outcome     := COALESCE(NULLIF(btrim(p_outcome), ''), 'all');
  v_rank_side   := NULLIF(btrim(p_opp_rank_side), '');
  v_rank_metric := COALESCE(NULLIF(btrim(p_opp_rank_metric), ''), 'net');

  IF p_team_ids_csv IS NOT NULL AND length(btrim(p_team_ids_csv)) > 0 THEN
    v_team_ids := ARRAY(
      SELECT DISTINCT x::bigint
      FROM unnest(string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_opp_ids_csv IS NOT NULL AND length(btrim(p_opp_ids_csv)) > 0 THEN
    v_opp_ids := ARRAY(
      SELECT DISTINCT x::bigint
      FROM unnest(string_to_array(regexp_replace(p_opp_ids_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_phase_csv IS NOT NULL AND length(btrim(p_phase_csv)) > 0 THEN
    v_phases := ARRAY(
      SELECT DISTINCT btrim(x)
      FROM unnest(string_to_array(p_phase_csv, ',')) x
      WHERE btrim(x) <> '' ORDER BY 1);
  END IF;

  RETURN QUERY
  WITH schedule_ranked AS (
    SELECT
      fs.game_id, fs.team_id, fs.opp_team_id, fs.is_home, fs.has_won,
      s.round_number, s.phase, s.scheduled_at::date AS game_date,
      row_number() OVER (
        PARTITION BY fs.team_id
        ORDER BY s.scheduled_at DESC, fs.game_id DESC
      ) AS team_game_rank
    FROM euroleague.final_schedule fs
    JOIN euroleague.schedule s ON s.game_id = fs.game_id
    WHERE s.competition = v_competition
      AND s.season = p_game_year
  ),
  -- Season-wide ranks from the same source as onoff_compute; see the note there.
  team_ranked AS (
    SELECT r.team_id, r.off_rank, r.def_rank, r.net_rank
    FROM euroleague.team_ppp_ratings_mv r
    WHERE r.competition = v_competition
      AND r.game_year   = p_game_year
  ),
  games AS (
    SELECT sr.*
    FROM schedule_ranked sr
    LEFT JOIN team_ranked tr ON tr.team_id = sr.opp_team_id
    WHERE (p_start_date IS NULL OR sr.game_date >= p_start_date)
      AND (p_end_date   IS NULL OR sr.game_date <= p_end_date)
      AND (v_phases   IS NULL OR sr.phase = ANY(v_phases))
      AND (v_opp_ids  IS NULL OR sr.opp_team_id = ANY(v_opp_ids))
      AND (p_min_gn   IS NULL OR sr.round_number >= p_min_gn)
      AND (p_max_gn   IS NULL OR sr.round_number <= p_max_gn)
      AND (p_last_n_games IS NULL OR sr.team_game_rank <= p_last_n_games)
      AND (v_home_away = 'all'
           OR (v_home_away = 'home' AND sr.is_home)
           OR (v_home_away = 'away' AND NOT sr.is_home))
      AND (v_outcome = 'all'
           OR (v_outcome = 'win'  AND sr.has_won)
           OR (v_outcome = 'loss' AND NOT sr.has_won))
      AND (
        v_rank_side IS NULL OR p_opp_rank_n IS NULL
        OR (v_rank_side = 'top' AND
            CASE v_rank_metric WHEN 'off' THEN tr.off_rank
                               WHEN 'def' THEN tr.def_rank
                               ELSE tr.net_rank END <= p_opp_rank_n)
        OR (v_rank_side = 'bottom' AND
            CASE v_rank_metric WHEN 'off' THEN tr.off_rank
                               WHEN 'def' THEN tr.def_rank
                               ELSE tr.net_rank END
            > (SELECT count(*) FROM team_ranked) - p_opp_rank_n)
      )
  ),
  agg AS (
    SELECT
      c.player_id, c.team_id, c.is_on_key, c.type_lineup,
      sum(c.total_points)::numeric      AS pts,
      sum(c.total_poss)::bigint         AS poss,
      sum(c.ts_poss_count)::bigint      AS ts_poss,
      sum(c.oreb_count)::bigint         AS oreb,
      sum(c.oreb_opportunities)::bigint AS oreb_opp,
      sum(c.tov_count)::bigint          AS tov,
      sum(c.steal_count)::bigint        AS steals,
      sum(c.deflection_count)::bigint   AS deflections,
      sum(c.total_ft_attempts)::bigint  AS fta,
      sum(c.total_fga)::bigint          AS fga,
      sum(c.total_fgm)::bigint          AS fgm,
      sum(c.total_fg3_made)::bigint     AS fg3m
    FROM euroleague.player_four_factors_by_game c
    JOIN games g ON g.game_id = c.game_id AND g.team_id = c.team_id
    WHERE (p_num_starters_off_min IS NULL OR c.own_starters >= p_num_starters_off_min)
      AND (p_num_starters_off_max IS NULL OR c.own_starters <= p_num_starters_off_max)
      AND (p_num_starters_def_min IS NULL OR c.opp_starters >= p_num_starters_def_min)
      AND (p_num_starters_def_max IS NULL OR c.opp_starters <= p_num_starters_def_max)
      AND (v_team_ids IS NULL OR c.team_id = ANY(v_team_ids))
    GROUP BY c.player_id, c.team_id, c.is_on_key, c.type_lineup
  ),
  rates AS (
    SELECT
      a.player_id, a.team_id, a.is_on_key, a.type_lineup, a.poss,
      a.pts / NULLIF(2 * a.ts_poss, 0)::numeric                     AS ts_pct,
      (a.fgm + 0.5 * a.fg3m) / NULLIF(a.fga, 0)::numeric            AS efg_pct,
      a.oreb / NULLIF(a.oreb_opp, 0)::numeric                       AS oreb_pct,
      a.tov / NULLIF(a.poss, 0)::numeric                            AS tov_pct,
      (a.steals + a.deflections) / NULLIF(a.poss, 0)::numeric       AS disruption_rate,
      a.fta / NULLIF(a.fga, 0)::numeric                             AS ftr
    FROM agg a
  ),
  pivoted AS (
    SELECT
      r.player_id, r.team_id,
      max(r.ts_pct)  FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_ts,
      max(r.ts_pct)  FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_ts,
      max(r.ts_pct)  FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_ts,
      max(r.ts_pct)  FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_ts,
      max(r.efg_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_efg,
      max(r.efg_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_efg,
      max(r.efg_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_efg,
      max(r.efg_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_efg,
      max(r.oreb_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_oreb,
      max(r.oreb_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_oreb,
      max(r.oreb_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_oreb,
      max(r.oreb_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_oreb,
      max(r.tov_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_tov,
      max(r.tov_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_tov,
      max(r.tov_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_tov,
      max(r.tov_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_tov,
      max(r.disruption_rate) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_disr,
      max(r.disruption_rate) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_disr,
      max(r.ftr) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_ftr,
      max(r.ftr) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_ftr,
      max(r.ftr) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_ftr,
      max(r.ftr) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_ftr,
      max(r.poss) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_poss,
      max(r.poss) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_poss,
      max(r.poss) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_poss,
      max(r.poss) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_poss
    FROM rates r
    GROUP BY r.player_id, r.team_id
  )
  SELECT
    p.player_id, p.team_id,
    btrim(split_part(pl.display_name, ',', 2)) AS firstname,
    btrim(split_part(pl.display_name, ',', 1)) AS lastname,
    t.display_name AS team_name,
    p_game_year AS game_year,
    p.off_on_ts, p.off_off_ts, p.def_on_ts, p.def_off_ts,
    p.off_on_efg, p.off_off_efg, p.def_on_efg, p.def_off_efg,
    p.off_on_oreb, p.off_off_oreb, p.def_on_oreb, p.def_off_oreb,
    p.off_on_tov, p.off_off_tov, p.def_on_tov, p.def_off_tov,
    p.def_on_disr, p.def_off_disr,
    p.off_on_ftr, p.off_off_ftr, p.def_on_ftr, p.def_off_ftr,
    p.off_on_poss, p.off_off_poss, p.def_on_poss, p.def_off_poss,
    round(100 * (p.off_on_efg  - p.off_off_efg), 1),
    round(100 * (p.off_on_ts   - p.off_off_ts), 1),
    round(100 * (p.off_on_oreb - p.off_off_oreb), 1),
    round(100 * (p.off_on_tov  - p.off_off_tov), 1),
    round(100 * (p.off_on_ftr  - p.off_off_ftr), 1),
    round(100 * (p.def_on_efg  - p.def_off_efg), 1),
    round(100 * (p.def_on_ts   - p.def_off_ts), 1),
    round(100 * (p.def_on_oreb - p.def_off_oreb), 1),
    round(100 * (p.def_on_tov  - p.def_off_tov), 1),
    round(100 * (p.def_on_ftr  - p.def_off_ftr), 1),
    round(100 * (p.def_on_disr - p.def_off_disr), 1)
  FROM pivoted p
  JOIN euroleague.players pl ON pl.player_id = p.player_id
  JOIN euroleague.teams   t  ON t.team_id    = p.team_id;
END;
$function$;

COMMIT;
