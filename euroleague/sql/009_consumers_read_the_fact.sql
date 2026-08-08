-- EuroLeague shadow schema — migration 009: consumers read the event fact.
--
-- Both four-factor refresh functions stop re-deriving the event x team
-- perspective expansion from actions_raw and read
-- euroleague.action_team_context instead. Signatures, target tables, column
-- lists and outputs are unchanged; only the source differs. Migration 008
-- built and verified the fact; this is what makes it load-bearing.

BEGIN;

CREATE OR REPLACE FUNCTION euroleague.refresh_team_four_factors_by_game_for_games(
  game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.team_four_factors_by_game;
  ELSE
    DELETE FROM euroleague.team_four_factors_by_game WHERE game_id = ANY(game_ids);
  END IF;

  INSERT INTO euroleague.team_four_factors_by_game (
    game_id, team_id, game_year, own_starters, opp_starters,
    off_pts, off_poss, off_ts_poss, off_oreb, off_oreb_opp, off_tov,
    off_fta, off_fga, off_fgm, off_fg3m,
    def_pts, def_poss, def_ts_poss, def_oreb, def_oreb_opp, def_tov,
    def_fta, def_fga, def_fgm, def_fg3m, def_steals,
    derivation_version
  )
  -- No roster fan-out: the team grain needs no player dimension, so this is a
  -- straight aggregate over the fact, pivoted on type_lineup into the existing
  -- off_*/def_* column pairs. Rows the contract leaves unsided (substitutions,
  -- timeouts, period markers) carry no measure, but they are NOT excluded from
  -- the population: the query this replaces produced a team_event_context row
  -- for every event regardless of type, so an (own_starters, opp_starters)
  -- window whose only events are unsided still has to exist as an all-zero
  -- output row, matching that row-per-event population exactly. Filtering
  -- atc.type_lineup IS NOT NULL here would silently discard those windows.
  --
  -- COALESCE(..., 0) on every measure: a (game, team, own_starters,
  -- opp_starters) group can have offense-tagged rows and zero defense-tagged
  -- rows in the same narrow window (or vice versa) -- e.g. two shots and no
  -- steal/rebound/foul from anyone while that exact starter-count pairing was
  -- on. SUM(...) FILTER(...) over zero matching rows is SQL NULL, not 0, and
  -- the target columns are NOT NULL. The row-per-event source this replaces
  -- always emitted a zero-valued row for the unmatched side (every event
  -- produced a team_event_context row regardless of type), so 0 is the
  -- output-identical value here, not a changed one.
  SELECT
    atc.game_id,
    atc.team_id,
    s.season::smallint AS game_year,
    atc.own_starters,
    atc.opp_starters,
    coalesce(sum(atc.points)             FILTER (WHERE atc.type_lineup = 'offense'), 0)::numeric,
    coalesce(sum(atc.possession_flag)    FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.ts_possessions)     FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.orebounds)          FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.oreb_opportunities) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.turnovers)          FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.ft_attempts)        FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.fga)                FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.fgm)                FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.fg3_made)           FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.points)             FILTER (WHERE atc.type_lineup = 'defense'), 0)::numeric,
    coalesce(sum(atc.possession_flag)    FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.ts_possessions)     FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.orebounds)          FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.oreb_opportunities) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.turnovers)          FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.ft_attempts)        FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.fga)                FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.fgm)                FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.fg3_made)           FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.steals)             FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    'existing-israeli-contract-v1'
  FROM euroleague.action_team_context atc
  JOIN euroleague.schedule s ON s.game_id = atc.game_id
 WHERE (game_ids IS NULL OR atc.game_id = ANY(game_ids))
 GROUP BY atc.game_id, atc.team_id, s.season, atc.own_starters, atc.opp_starters;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

COMMIT;
