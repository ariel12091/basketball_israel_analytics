-- EuroLeague shadow schema -- migration 018: filtered team minutes read layer.
--
-- Mirrors the Israeli Team Ratings architecture: ratings and minutes are
-- fetched independently under the same filters, then pace is calculated from
-- summed possessions and summed minutes in the app. EuroLeague duration comes
-- from the canonical one-row-per-segment fact, not inferred quarter lengths.

BEGIN;

SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.get_team_minutes_dynamic(
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
RETURNS TABLE (team_id BIGINT, minutes NUMERIC)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
SET plan_cache_mode = force_custom_plan
AS $function$
  WITH normalized AS (
    SELECT
      COALESCE(NULLIF(btrim(p_competition), ''), 'E') AS competition,
      CASE WHEN NULLIF(btrim(p_team_ids_csv), '') IS NULL THEN NULL::bigint[]
           ELSE string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')::bigint[] END AS team_ids,
      CASE WHEN NULLIF(btrim(p_phase_csv), '') IS NULL THEN NULL::text[]
           ELSE string_to_array(p_phase_csv, ',') END AS phases,
      CASE WHEN NULLIF(btrim(p_opp_ids_csv), '') IS NULL THEN NULL::bigint[]
           ELSE string_to_array(regexp_replace(p_opp_ids_csv, '\s+', '', 'g'), ',')::bigint[] END AS opp_ids,
      COALESCE(NULLIF(btrim(p_home_away), ''), 'all') AS home_away,
      COALESCE(NULLIF(btrim(p_outcome), ''), 'all') AS outcome,
      NULLIF(btrim(p_opp_rank_side), '') AS rank_side,
      COALESCE(NULLIF(btrim(p_opp_rank_metric), ''), 'net') AS rank_metric
  ),
  schedule_ranked AS (
    SELECT fs.*,
           row_number() OVER (
             PARTITION BY fs.team_id
             ORDER BY fs.game_date DESC, fs.game_id DESC
           ) AS team_game_rank
    FROM euroleague.final_schedule_mv fs
    CROSS JOIN normalized n
    WHERE fs.competition = n.competition
      AND fs.game_year = p_game_year
  ),
  opponent_ranks AS (
    SELECT r.team_id, r.off_rank, r.def_rank, r.net_rank,
           count(*) OVER () AS team_count
    FROM euroleague.team_ppp_ratings_mv r
    CROSS JOIN normalized n
    WHERE r.competition = n.competition
      AND r.game_year = p_game_year
  ),
  games AS (
    SELECT sr.game_id, sr.team_id
    FROM schedule_ranked sr
    CROSS JOIN normalized n
    LEFT JOIN opponent_ranks r ON r.team_id = sr.opp_team_id
    WHERE (p_start_date IS NULL OR sr.game_date >= p_start_date)
      AND (p_end_date IS NULL OR sr.game_date <= p_end_date)
      AND (n.team_ids IS NULL OR sr.team_id = ANY(n.team_ids))
      AND (n.phases IS NULL OR sr.phase = ANY(n.phases))
      AND (n.opp_ids IS NULL OR sr.opp_team_id = ANY(n.opp_ids))
      AND (n.home_away = 'all'
           OR (n.home_away = 'home' AND sr.is_home)
           OR (n.home_away = 'away' AND NOT sr.is_home))
      AND (n.outcome = 'all'
           OR (n.outcome = 'win' AND sr.has_won)
           OR (n.outcome = 'loss' AND NOT sr.has_won))
      AND (n.rank_side IS NULL OR p_opp_rank_n IS NULL
           OR (n.rank_side = 'top' AND
               CASE n.rank_metric WHEN 'off' THEN r.off_rank
                                  WHEN 'def' THEN r.def_rank
                                  ELSE r.net_rank END <= p_opp_rank_n)
           OR (n.rank_side = 'bottom' AND
               CASE n.rank_metric WHEN 'off' THEN r.off_rank
                                  WHEN 'def' THEN r.def_rank
                                  ELSE r.net_rank END > r.team_count - p_opp_rank_n))
      AND (p_min_gn IS NULL OR sr.round_number >= p_min_gn)
      AND (p_max_gn IS NULL OR sr.round_number <= p_max_gn)
      AND (p_last_n_games IS NULL OR sr.team_game_rank <= p_last_n_games)
  ),
  game_minutes AS (
    SELECT ms.game_id, ms.team_id, sum(ms.segment_seconds) / 60.0 AS minutes
    FROM euroleague.matchup_segments_actions ms
    JOIN games g ON g.game_id = ms.game_id AND g.team_id = ms.team_id
    WHERE (p_num_starters_off_min IS NULL OR ms.own_starters >= p_num_starters_off_min)
      AND (p_num_starters_off_max IS NULL OR ms.own_starters <= p_num_starters_off_max)
      AND (p_num_starters_def_min IS NULL OR ms.opp_starters >= p_num_starters_def_min)
      AND (p_num_starters_def_max IS NULL OR ms.opp_starters <= p_num_starters_def_max)
    GROUP BY ms.game_id, ms.team_id
  )
  SELECT gm.team_id, round(sum(gm.minutes), 3)::numeric AS minutes
  FROM game_minutes gm
  GROUP BY gm.team_id
$function$;

GRANT EXECUTE ON FUNCTION euroleague.get_team_minutes_dynamic(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, integer, integer, integer, integer, integer, integer
) TO app_readonly;

COMMIT;
