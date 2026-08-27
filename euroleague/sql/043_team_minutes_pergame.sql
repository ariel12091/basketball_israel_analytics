-- EuroLeague shadow schema -- migration 043: non-clutch Team Minutes from
-- the existing per-game lineup fact.
--
-- A non-clutch request needs neither event grain nor an arbitrary time/margin
-- predicate. The app nevertheless preserves the Israeli interactive duration
-- convention: within each segment, minutes are the last qualifying action minus
-- the first. That is shorter than canonical wall-clock segment time, so this
-- migration adds the action span to the existing per-game lineup fact and keeps
-- it current in that fact's existing per-game refresh. It adds no table and no
-- unit expansion. Custom clutch remains on get_team_minutes_direct and the
-- standard preset remains on its cache.

BEGIN;
SET LOCAL search_path TO euroleague, public;

ALTER TABLE euroleague.lineup_totals_by_game
  ADD COLUMN action_span_seconds numeric;

ALTER TABLE euroleague.lineup_totals_by_game
  ADD CONSTRAINT lineup_totals_action_span_nonnegative
  CHECK (action_span_seconds IS NULL OR action_span_seconds >= 0);

CREATE OR REPLACE FUNCTION euroleague.refresh_lineup_totals_by_game(
  game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $refresh$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.lineup_totals_by_game;
  ELSE
    DELETE FROM euroleague.lineup_totals_by_game WHERE game_id = ANY(game_ids);
  END IF;

  INSERT INTO euroleague.lineup_totals_by_game (
    game_id, team_id, lineup_key, type_lineup, opp_starters,
    competition, game_year, own_starters, own_lineup, player_ids,
    possessions, points, fg2_made, fg2_att, fg3_made, fg3_att,
    ts_possessions, fgm, fga, ft_attempts,
    orebounds, oreb_opportunities, turnovers, steals, seconds,
    load_run_id, derivation_version, action_span_seconds
  )
  WITH real_roster AS (
    SELECT fr.game_id, fr.team_id, fr.player_id, fr.source_player_name
    FROM euroleague.full_rosters fr
    JOIN euroleague.players p ON p.player_id = fr.player_id
    WHERE (game_ids IS NULL OR fr.game_id = ANY(game_ids))
      AND lower(p.provider_player_id) NOT IN ('team', 'total')
      AND lower(btrim(p.display_name)) NOT IN ('team', 'total')
  ), segment_totals AS (
    SELECT ms.game_id, ms.team_id, ms.own_lineup, ms.opp_starters,
      max(ms.own_starters) AS own_starters,
      sum(ms.segment_seconds) AS seconds
    FROM euroleague.matchup_segments_actions ms
    WHERE game_ids IS NULL OR ms.game_id = ANY(game_ids)
    GROUP BY ms.game_id, ms.team_id, ms.own_lineup, ms.opp_starters
  ), action_segment_spans AS (
    SELECT a.game_id, a.team_id, a.segment_id,
      greatest(max(a.event_elapsed_seconds) - min(a.event_elapsed_seconds), 0::numeric) AS seconds
    FROM euroleague.player_stats_actions_by_game a
    WHERE (game_ids IS NULL OR a.game_id = ANY(game_ids))
      AND a.segment_id IS NOT NULL
      AND a.event_elapsed_seconds IS NOT NULL
    GROUP BY a.game_id, a.team_id, a.segment_id
  ), action_spans AS (
    SELECT x.game_id, x.team_id, ms.own_lineup, ms.opp_starters,
      sum(x.seconds) AS seconds
    FROM action_segment_spans x
    JOIN euroleague.matchup_segments_actions ms
      USING (game_id, team_id, segment_id)
    GROUP BY x.game_id, x.team_id, ms.own_lineup, ms.opp_starters
  ), distinct_lineups AS (
    SELECT DISTINCT s.game_id, s.team_id, s.own_lineup FROM segment_totals s
  ), keyed AS (
    SELECT d.game_id, d.team_id, d.own_lineup, ids.player_ids,
      md5(array_to_string(ids.player_ids, '_')) AS lineup_key
    FROM distinct_lineups d
    CROSS JOIN LATERAL (
      SELECT ARRAY(
        SELECT rr.player_id
        FROM real_roster rr
        WHERE rr.game_id = d.game_id
          AND rr.team_id = d.team_id
          AND rr.source_player_name = ANY(d.own_lineup)
        ORDER BY rr.player_id
      ) AS player_ids
    ) ids
  ), counts AS (
    SELECT atc.game_id, atc.team_id, atc.own_lineup, atc.type_lineup,
      atc.opp_starters,
      sum(atc.possession_flag)::integer AS possessions,
      sum(atc.points)::integer AS points,
      sum(atc.fg2_made)::integer AS fg2_made,
      sum(atc.fg2_att)::integer AS fg2_att,
      sum(atc.fg3_made)::integer AS fg3_made,
      sum(atc.fg3_att)::integer AS fg3_att,
      sum(atc.ts_possessions)::integer AS ts_possessions,
      sum(atc.fgm)::integer AS fgm,
      sum(atc.fga)::integer AS fga,
      sum(atc.ft_attempts)::integer AS ft_attempts,
      sum(atc.orebounds)::integer AS orebounds,
      sum(atc.oreb_opportunities)::integer AS oreb_opportunities,
      sum(atc.turnovers)::integer AS turnovers,
      sum(atc.steals)::integer AS steals
    FROM euroleague.action_team_context_actions atc
    WHERE (game_ids IS NULL OR atc.game_id = ANY(game_ids))
      AND atc.type_lineup IS NOT NULL
    GROUP BY atc.game_id, atc.team_id, atc.own_lineup,
      atc.type_lineup, atc.opp_starters
  ), game_run AS (
    SELECT a.game_id, max(a.load_run_id) AS load_run_id
    FROM euroleague.actions a
    WHERE game_ids IS NULL OR a.game_id = ANY(game_ids)
    GROUP BY a.game_id
  )
  SELECT s.game_id, s.team_id, k.lineup_key, side.type_lineup,
    s.opp_starters, sch.competition, sch.season, s.own_starters,
    s.own_lineup, k.player_ids,
    coalesce(c.possessions, 0), coalesce(c.points, 0),
    coalesce(c.fg2_made, 0), coalesce(c.fg2_att, 0),
    coalesce(c.fg3_made, 0), coalesce(c.fg3_att, 0),
    coalesce(c.ts_possessions, 0), coalesce(c.fgm, 0),
    coalesce(c.fga, 0), coalesce(c.ft_attempts, 0),
    coalesce(c.orebounds, 0), coalesce(c.oreb_opportunities, 0),
    coalesce(c.turnovers, 0), coalesce(c.steals, 0),
    CASE WHEN side.type_lineup = 'offense' THEN s.seconds END,
    gr.load_run_id, 'units-v2',
    CASE WHEN side.type_lineup = 'offense' THEN coalesce(span.seconds, 0) END
  FROM segment_totals s
  JOIN keyed k USING (game_id, team_id, own_lineup)
  JOIN euroleague.schedule sch ON sch.game_id = s.game_id
  LEFT JOIN game_run gr ON gr.game_id = s.game_id
  CROSS JOIN (VALUES ('offense'), ('defense')) AS side(type_lineup)
  LEFT JOIN counts c
    ON c.game_id = s.game_id AND c.team_id = s.team_id
   AND c.own_lineup = s.own_lineup
   AND c.opp_starters = s.opp_starters
   AND c.type_lineup = side.type_lineup
  LEFT JOIN action_spans span
    ON span.game_id = s.game_id AND span.team_id = s.team_id
   AND span.own_lineup = s.own_lineup
   AND span.opp_starters = s.opp_starters;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$refresh$;

-- Initialize only the new column. Rebuilding the other lineup metrics here
-- would add installation work without changing their values.
UPDATE euroleague.lineup_totals_by_game
SET action_span_seconds = 0
WHERE type_lineup = 'offense';

WITH action_segment_spans AS (
  SELECT a.game_id, a.team_id, a.segment_id,
    greatest(max(a.event_elapsed_seconds) - min(a.event_elapsed_seconds), 0::numeric) AS seconds
  FROM euroleague.player_stats_actions_by_game a
  WHERE a.segment_id IS NOT NULL AND a.event_elapsed_seconds IS NOT NULL
  GROUP BY a.game_id, a.team_id, a.segment_id
), action_spans AS (
  SELECT x.game_id, x.team_id, ms.own_lineup, ms.opp_starters,
    sum(x.seconds) AS seconds
  FROM action_segment_spans x
  JOIN euroleague.matchup_segments_actions ms
    USING (game_id, team_id, segment_id)
  GROUP BY x.game_id, x.team_id, ms.own_lineup, ms.opp_starters
)
UPDATE euroleague.lineup_totals_by_game l
SET action_span_seconds = span.seconds
FROM action_spans span
WHERE l.type_lineup = 'offense'
  AND l.game_id = span.game_id
  AND l.team_id = span.team_id
  AND l.own_lineup = span.own_lineup
  AND l.opp_starters = span.opp_starters;

ALTER TABLE euroleague.lineup_totals_by_game
  ADD CONSTRAINT lineup_totals_action_span_offense_only
  CHECK ((type_lineup = 'offense') = (action_span_seconds IS NOT NULL));

CREATE OR REPLACE FUNCTION euroleague.get_team_minutes_pergame(
    p_competition TEXT, p_game_year INTEGER,
    p_start_date DATE DEFAULT NULL, p_end_date DATE DEFAULT NULL,
    p_team_ids_csv TEXT DEFAULT NULL, p_phase_csv TEXT DEFAULT NULL,
    p_opp_ids_csv TEXT DEFAULT NULL, p_home_away TEXT DEFAULT 'all',
    p_outcome TEXT DEFAULT 'all', p_opp_rank_side TEXT DEFAULT NULL,
    p_opp_rank_n INTEGER DEFAULT NULL, p_opp_rank_metric TEXT DEFAULT NULL,
    p_min_gn INTEGER DEFAULT NULL, p_max_gn INTEGER DEFAULT NULL,
    p_last_n_games INTEGER DEFAULT NULL,
    p_num_starters_off_min INTEGER DEFAULT NULL,
    p_num_starters_off_max INTEGER DEFAULT NULL,
    p_num_starters_def_min INTEGER DEFAULT NULL,
    p_num_starters_def_max INTEGER DEFAULT NULL
)
RETURNS TABLE(team_id BIGINT, minutes NUMERIC)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
SET plan_cache_mode = force_custom_plan
AS $function$
WITH normalized AS (
  SELECT coalesce(nullif(btrim(p_competition), ''), 'E') competition,
    CASE WHEN nullif(btrim(p_team_ids_csv), '') IS NULL THEN NULL::bigint[]
      ELSE string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')::bigint[] END team_ids,
    CASE WHEN nullif(btrim(p_phase_csv), '') IS NULL THEN NULL::text[]
      ELSE string_to_array(p_phase_csv, ',') END phases,
    CASE WHEN nullif(btrim(p_opp_ids_csv), '') IS NULL THEN NULL::bigint[]
      ELSE string_to_array(regexp_replace(p_opp_ids_csv, '\s+', '', 'g'), ',')::bigint[] END opp_ids,
    coalesce(nullif(btrim(p_home_away), ''), 'all') home_away,
    coalesce(nullif(btrim(p_outcome), ''), 'all') outcome,
    nullif(btrim(p_opp_rank_side), '') rank_side,
    coalesce(nullif(btrim(p_opp_rank_metric), ''), 'net') rank_metric
), schedule_ranked AS (
  SELECT fs.*, row_number() OVER (
    PARTITION BY fs.team_id ORDER BY fs.game_date DESC, fs.game_id DESC
  ) team_game_rank
  FROM euroleague.final_schedule_mv fs CROSS JOIN normalized n
  WHERE fs.competition = n.competition AND fs.game_year = p_game_year
), opponent_ranks AS (
  SELECT r.team_id, r.off_rank, r.def_rank, r.net_rank,
    count(*) OVER () team_count
  FROM euroleague.team_ppp_ratings_mv r CROSS JOIN normalized n
  WHERE r.competition = n.competition AND r.game_year = p_game_year
), games_filtered AS MATERIALIZED (
  SELECT sr.game_id, sr.team_id
  FROM schedule_ranked sr CROSS JOIN normalized n
  LEFT JOIN opponent_ranks r ON r.team_id = sr.opp_team_id
  WHERE (p_start_date IS NULL OR sr.game_date >= p_start_date)
    AND (p_end_date IS NULL OR sr.game_date <= p_end_date)
    AND (n.team_ids IS NULL OR sr.team_id = ANY(n.team_ids))
    AND (n.phases IS NULL OR sr.phase = ANY(n.phases))
    AND (n.opp_ids IS NULL OR sr.opp_team_id = ANY(n.opp_ids))
    AND (n.home_away = 'all' OR n.home_away = 'home' AND sr.is_home
      OR n.home_away = 'away' AND NOT sr.is_home)
    AND (n.outcome = 'all' OR n.outcome = 'win' AND sr.has_won
      OR n.outcome = 'loss' AND NOT sr.has_won)
    AND (n.rank_side IS NULL OR p_opp_rank_n IS NULL
      OR n.rank_side = 'top' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank
        WHEN 'def' THEN r.def_rank ELSE r.net_rank END <= p_opp_rank_n
      OR n.rank_side = 'bottom' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank
        WHEN 'def' THEN r.def_rank ELSE r.net_rank END
        > r.team_count - p_opp_rank_n)
    AND (p_min_gn IS NULL OR sr.round_number >= p_min_gn)
    AND (p_max_gn IS NULL OR sr.round_number <= p_max_gn)
    AND (p_last_n_games IS NULL OR sr.team_game_rank <= p_last_n_games)
)
SELECT l.team_id, round(sum(l.action_span_seconds) / 60.0, 3)::numeric AS minutes
FROM euroleague.lineup_totals_by_game l
JOIN games_filtered g USING (game_id, team_id)
WHERE l.competition = (SELECT competition FROM normalized)
  AND l.game_year = p_game_year
  AND l.type_lineup = 'offense'
  AND (p_num_starters_off_min IS NULL OR l.own_starters >= p_num_starters_off_min)
  AND (p_num_starters_off_max IS NULL OR l.own_starters <= p_num_starters_off_max)
  AND (p_num_starters_def_min IS NULL OR l.opp_starters >= p_num_starters_def_min)
  AND (p_num_starters_def_max IS NULL OR l.opp_starters <= p_num_starters_def_max)
GROUP BY l.team_id
$function$;

REVOKE ALL ON FUNCTION euroleague.get_team_minutes_pergame(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, integer, integer, integer, integer, integer, integer
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_team_minutes_pergame(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, integer, integer, integer, integer, integer, integer
) TO app_readonly;

COMMIT;
