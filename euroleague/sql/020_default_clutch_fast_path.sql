-- EuroLeague shadow schema -- migration 020: default-clutch fast path.
--
-- The standard app preset (pre-event margin <= 5, final 5 regulation minutes,
-- all score statuses, and overtime bypassing margin/status) is the expected
-- dominant clutch request. Compute its exact action counts and segment/window
-- intersections once per changed game, inside that game's publication
-- transaction, at five-player lineup/game grain.
--
-- Source selection after this migration:
--   no clutch      -> lineup_totals_by_game
--   default clutch -> default_clutch_lineup_totals_by_game
--   custom clutch  -> clutch_team_game_facts()
--
-- All three sources expose the same additive contract. Ratios remain app/read-
-- layer calculations after the requested games have been aggregated.

BEGIN;

SET LOCAL search_path TO euroleague, public;

CREATE TABLE euroleague.default_clutch_lineup_totals_by_game (
  game_id            BIGINT NOT NULL
                     REFERENCES euroleague.schedule(game_id) ON DELETE CASCADE,
  team_id            BIGINT NOT NULL REFERENCES euroleague.teams(team_id),
  own_lineup         TEXT[] NOT NULL CHECK (cardinality(own_lineup) = 5),
  own_starters       SMALLINT NOT NULL CHECK (own_starters BETWEEN 0 AND 5),
  opp_starters       SMALLINT NOT NULL CHECK (opp_starters BETWEEN 0 AND 5),
  type_lineup        TEXT NOT NULL CHECK (type_lineup IN ('offense', 'defense')),
  possessions        BIGINT NOT NULL DEFAULT 0,
  points             BIGINT NOT NULL DEFAULT 0,
  fg2_made           BIGINT NOT NULL DEFAULT 0,
  fg2_att            BIGINT NOT NULL DEFAULT 0,
  fg3_made           BIGINT NOT NULL DEFAULT 0,
  fg3_att            BIGINT NOT NULL DEFAULT 0,
  ts_possessions     BIGINT NOT NULL DEFAULT 0,
  fgm                BIGINT NOT NULL DEFAULT 0,
  fga                BIGINT NOT NULL DEFAULT 0,
  ft_attempts        BIGINT NOT NULL DEFAULT 0,
  orebounds          BIGINT NOT NULL DEFAULT 0,
  oreb_opportunities BIGINT NOT NULL DEFAULT 0,
  turnovers          BIGINT NOT NULL DEFAULT 0,
  steals             BIGINT NOT NULL DEFAULT 0,
  seconds            NUMERIC CHECK (seconds IS NULL OR seconds >= 0),
  derivation_version TEXT NOT NULL DEFAULT 'default-clutch-v1',
  derived_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (
    game_id, team_id, own_lineup, own_starters, opp_starters, type_lineup
  ),
  CHECK ((type_lineup = 'offense') = (seconds IS NOT NULL))
);

CREATE INDEX euroleague_default_clutch_lineup_totals_game_team_idx
  ON euroleague.default_clutch_lineup_totals_by_game
     (game_id, team_id, type_lineup);

ALTER TABLE euroleague.default_clutch_lineup_totals_by_game
  ENABLE ROW LEVEL SECURITY;
CREATE POLICY app_readonly_select_all
  ON euroleague.default_clutch_lineup_totals_by_game
  FOR SELECT TO app_readonly USING (true);

CREATE OR REPLACE FUNCTION euroleague.refresh_default_clutch_for_games(
  game_ids BIGINT[]
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $function$
DECLARE
  target_game_ids bigint[];
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    SELECT array_agg(s.game_id ORDER BY s.game_id)
      INTO target_game_ids
      FROM euroleague.schedule s;
    DELETE FROM euroleague.default_clutch_lineup_totals_by_game;
  ELSE
    SELECT array_agg(DISTINCT x ORDER BY x)
      INTO target_game_ids
      FROM unnest(game_ids) x;
    DELETE FROM euroleague.default_clutch_lineup_totals_by_game c
     WHERE c.game_id = ANY(target_game_ids);
  END IF;

  IF target_game_ids IS NULL OR array_length(target_game_ids, 1) IS NULL THEN
    RETURN 0;
  END IF;

  INSERT INTO euroleague.default_clutch_lineup_totals_by_game (
    game_id, team_id, own_lineup, own_starters, opp_starters, type_lineup,
    possessions, points, fg2_made, fg2_att, fg3_made, fg3_att,
    ts_possessions, fgm, fga, ft_attempts, orebounds,
    oreb_opportunities, turnovers, steals, seconds,
    derivation_version
  )
  SELECT
    f.game_id, f.team_id, f.own_lineup, f.own_starters, f.opp_starters,
    f.type_lineup, f.possessions, f.points, f.fg2_made, f.fg2_att,
    f.fg3_made, f.fg3_att, f.ts_possessions, f.fgm, f.fga,
    f.ft_attempts, f.orebounds, f.oreb_opportunities, f.turnovers,
    f.steals, f.seconds, 'default-clutch-v1'
  FROM euroleague.clutch_team_game_facts(
    target_game_ids, 5, 'all', 300, false
  ) f;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

-- Backfill the currently published games once. Subsequent publications call
-- the same function with only their changed game IDs.
SELECT euroleague.refresh_default_clutch_for_games(NULL::bigint[]);

-- One branch is executed, not three UNION branches left to planner pruning.
-- This makes the performance contract explicit and independently testable.
CREATE OR REPLACE FUNCTION euroleague.select_team_game_facts(
    p_game_ids BIGINT[],
    p_max_margin INTEGER,
    p_margin_status TEXT,
    p_max_time_remaining INTEGER,
    p_ot_margin_filter BOOLEAN
)
RETURNS TABLE (
    game_id BIGINT, team_id BIGINT, own_lineup TEXT[],
    own_starters SMALLINT, opp_starters SMALLINT, type_lineup TEXT,
    possessions BIGINT, points BIGINT,
    fg2_made BIGINT, fg2_att BIGINT, fg3_made BIGINT, fg3_att BIGINT,
    ts_possessions BIGINT, fgm BIGINT, fga BIGINT, ft_attempts BIGINT,
    orebounds BIGINT, oreb_opportunities BIGINT, turnovers BIGINT,
    steals BIGINT, seconds NUMERIC
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
SET plan_cache_mode = force_custom_plan
AS $function$
DECLARE
  v_margin_status text := coalesce(nullif(btrim(p_margin_status), ''), 'all');
BEGIN
  IF p_max_margin IS NULL
     AND v_margin_status = 'all'
     AND p_max_time_remaining IS NULL THEN
    RETURN QUERY
    SELECT
      l.game_id,
      l.team_id,
      l.own_lineup,
      l.own_starters,
      l.opp_starters,
      l.type_lineup,
      l.possessions::bigint,
      l.points::bigint,
      l.fg2_made::bigint,
      l.fg2_att::bigint,
      l.fg3_made::bigint,
      l.fg3_att::bigint,
      l.ts_possessions::bigint,
      l.fgm::bigint,
      l.fga::bigint,
      l.ft_attempts::bigint,
      l.orebounds::bigint,
      l.oreb_opportunities::bigint,
      l.turnovers::bigint,
      l.steals::bigint,
      l.seconds
    FROM euroleague.lineup_totals_by_game l
    WHERE l.game_id = ANY(coalesce(p_game_ids, ARRAY[]::bigint[]));

  ELSIF p_max_margin = 5
        AND v_margin_status = 'all'
        AND p_max_time_remaining = 300
        AND NOT coalesce(p_ot_margin_filter, false) THEN
    RETURN QUERY
    SELECT
      c.game_id,
      c.team_id,
      c.own_lineup,
      c.own_starters,
      c.opp_starters,
      c.type_lineup,
      c.possessions,
      c.points,
      c.fg2_made,
      c.fg2_att,
      c.fg3_made,
      c.fg3_att,
      c.ts_possessions,
      c.fgm,
      c.fga,
      c.ft_attempts,
      c.orebounds,
      c.oreb_opportunities,
      c.turnovers,
      c.steals,
      c.seconds
    FROM euroleague.default_clutch_lineup_totals_by_game c
    WHERE c.game_id = ANY(coalesce(p_game_ids, ARRAY[]::bigint[]));

  ELSE
    RETURN QUERY
    SELECT f.*
    FROM euroleague.clutch_team_game_facts(
      p_game_ids,
      p_max_margin,
      v_margin_status,
      p_max_time_remaining,
      p_ot_margin_filter
    ) f;
  END IF;
END;
$function$;

-- Replace only the shared fact source. Public reader signatures and the app's
-- parameter contract remain unchanged.
CREATE OR REPLACE FUNCTION euroleague.filtered_team_game_facts(
    p_competition TEXT,
    p_game_year INTEGER,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL,
    p_team_ids_csv TEXT DEFAULT NULL,
    p_phase_csv TEXT DEFAULT NULL,
    p_opp_ids_csv TEXT DEFAULT NULL,
    p_home_away TEXT DEFAULT 'all',
    p_outcome TEXT DEFAULT 'all',
    p_opp_rank_side TEXT DEFAULT NULL,
    p_opp_rank_n INTEGER DEFAULT NULL,
    p_opp_rank_metric TEXT DEFAULT NULL,
    p_max_margin INTEGER DEFAULT NULL,
    p_margin_status TEXT DEFAULT NULL,
    p_max_time_remaining INTEGER DEFAULT NULL,
    p_ot_margin_filter BOOLEAN DEFAULT FALSE,
    p_min_gn INTEGER DEFAULT NULL,
    p_max_gn INTEGER DEFAULT NULL,
    p_last_n_games INTEGER DEFAULT NULL,
    p_num_starters_off_min INTEGER DEFAULT NULL,
    p_num_starters_off_max INTEGER DEFAULT NULL,
    p_num_starters_def_min INTEGER DEFAULT NULL,
    p_num_starters_def_max INTEGER DEFAULT NULL
)
RETURNS TABLE (
    game_id BIGINT, team_id BIGINT, team_name TEXT, has_won BOOLEAN,
    own_lineup TEXT[], own_starters SMALLINT, opp_starters SMALLINT,
    type_lineup TEXT, possessions BIGINT, points BIGINT,
    fg2_made BIGINT, fg2_att BIGINT, fg3_made BIGINT, fg3_att BIGINT,
    ts_possessions BIGINT, fgm BIGINT, fga BIGINT, ft_attempts BIGINT,
    orebounds BIGINT, oreb_opportunities BIGINT, turnovers BIGINT,
    steals BIGINT, seconds NUMERIC
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
SET plan_cache_mode = force_custom_plan
AS $function$
  WITH normalized AS (
    SELECT
      coalesce(nullif(btrim(p_competition), ''), 'E') AS competition,
      CASE WHEN nullif(btrim(p_team_ids_csv), '') IS NULL THEN NULL::bigint[]
           ELSE string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')::bigint[] END AS team_ids,
      CASE WHEN nullif(btrim(p_phase_csv), '') IS NULL THEN NULL::text[]
           ELSE string_to_array(p_phase_csv, ',') END AS phases,
      CASE WHEN nullif(btrim(p_opp_ids_csv), '') IS NULL THEN NULL::bigint[]
           ELSE string_to_array(regexp_replace(p_opp_ids_csv, '\s+', '', 'g'), ',')::bigint[] END AS opp_ids,
      coalesce(nullif(btrim(p_home_away), ''), 'all') AS home_away,
      coalesce(nullif(btrim(p_outcome), ''), 'all') AS outcome,
      nullif(btrim(p_opp_rank_side), '') AS rank_side,
      coalesce(nullif(btrim(p_opp_rank_metric), ''), 'net') AS rank_metric
  ),
  schedule_ranked AS (
    SELECT fs.*,
           row_number() OVER (
             PARTITION BY fs.team_id ORDER BY fs.game_date DESC, fs.game_id DESC
           ) AS team_game_rank
    FROM euroleague.final_schedule_mv fs
    CROSS JOIN normalized n
    WHERE fs.competition = n.competition AND fs.game_year = p_game_year
  ),
  opponent_ranks AS (
    SELECT r.team_id, r.off_rank, r.def_rank, r.net_rank,
           count(*) OVER () AS team_count
    FROM euroleague.team_ppp_ratings_mv r
    CROSS JOIN normalized n
    WHERE r.competition = n.competition AND r.game_year = p_game_year
  ),
  games AS MATERIALIZED (
    SELECT sr.game_id, sr.team_id, sr.team_name, sr.has_won
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
  facts AS MATERIALIZED (
    SELECT f.*
    FROM euroleague.select_team_game_facts(
      ARRAY(SELECT DISTINCT g.game_id FROM games g),
      p_max_margin,
      p_margin_status,
      p_max_time_remaining,
      p_ot_margin_filter
    ) f
  )
  SELECT
    g.game_id, g.team_id, g.team_name, g.has_won,
    f.own_lineup, f.own_starters, f.opp_starters, f.type_lineup,
    f.possessions, f.points, f.fg2_made, f.fg2_att,
    f.fg3_made, f.fg3_att, f.ts_possessions, f.fgm, f.fga,
    f.ft_attempts, f.orebounds, f.oreb_opportunities,
    f.turnovers, f.steals, f.seconds
  FROM games g
  JOIN facts f ON f.game_id = g.game_id AND f.team_id = g.team_id
  WHERE (p_num_starters_off_min IS NULL OR f.own_starters >= p_num_starters_off_min)
    AND (p_num_starters_off_max IS NULL OR f.own_starters <= p_num_starters_off_max)
    AND (p_num_starters_def_min IS NULL OR f.opp_starters >= p_num_starters_def_min)
    AND (p_num_starters_def_max IS NULL OR f.opp_starters <= p_num_starters_def_max)
$function$;

REVOKE ALL ON TABLE euroleague.default_clutch_lineup_totals_by_game FROM PUBLIC;
REVOKE ALL ON TABLE euroleague.default_clutch_lineup_totals_by_game FROM app_readonly;
REVOKE ALL ON FUNCTION euroleague.refresh_default_clutch_for_games(
  bigint[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION euroleague.refresh_default_clutch_for_games(
  bigint[]) FROM app_readonly;
REVOKE ALL ON FUNCTION euroleague.select_team_game_facts(
  bigint[], integer, text, integer, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION euroleague.select_team_game_facts(
  bigint[], integer, text, integer, boolean) FROM app_readonly;

COMMIT;
