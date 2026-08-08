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

BEGIN;

SET LOCAL search_path TO euroleague, public;

-- ---------------------------------------------------------------------------
-- The player grain, off the fact.
--
-- The body below is PLAYER_GRAIN_FROM_FACT from
-- euroleague/scripts/verify_action_team_context.py, ported verbatim with its
-- %(game_ids)s::bigint[] placeholders replaced by this function's game_ids
-- parameter, plus the four hardcoded-zero columns and the two lineage columns
-- the gate deliberately does not compare. That query is migration 008's
-- acceptance gate: it was diffed both ways against every stored row of
-- player_four_factors_by_game across all 84 games and reproduced them exactly.
-- Rewriting it here from the migration 007 body would have produced a second
-- version of a query whose only guarantee is that it is the verified one.
--
-- Population and measures come from different places, and that split is the
-- contract rather than a convenience. player_minutes reads matchup_segments,
-- which holds each segment's duration exactly once, so floor time needs no
-- DISTINCT and no MAX-per-segment convention. counts reads
-- action_team_context. A player can be on court for a whole segment in which
-- his team recorded no event on one side; that combination has real minutes
-- and no event, so an event-sourced grain could not produce it, and the LEFT
-- JOIN is what keeps its zero row.
--
-- WHERE atc.type_lineup IS NOT NULL inside counts is correct here and is NOT
-- the same predicate the team function above had to give up. On the team side
-- the population and the measures came from one scan of the fact, so filtering
-- removed whole output rows. Here the population comes from matchup_segments
-- via player_minutes -> complete_grid and the fact supplies measures only,
-- through a LEFT JOIN. Unsided rows (substitutions, timeouts, period markers)
-- carry no measure and cost no floor time, so excluding them cannot remove a
-- population row.
--
-- Every measure is wrapped in coalesce(..., 0): the LEFT JOIN yields NULL for
-- an observed combination that measured nothing on that side, and the target
-- columns are NOT NULL. This matches the migration 007 body, which coalesced
-- the same columns for the same reason.
--
-- The PERFORM euroleague.refresh_stint_timing_for_games(game_ids) that opened
-- the migration 007 body is gone. Migration 008 moved that call into
-- refresh_action_team_context_for_games and left this one alone rather than
-- edit a function it was not verifying. Now that this function reads the fact,
-- the timing refresh has already run as part of building what it reads.
CREATE OR REPLACE FUNCTION euroleague.refresh_player_four_factors_by_game_for_games(
  game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.player_four_factors_by_game;
  ELSE
    DELETE FROM euroleague.player_four_factors_by_game
    WHERE game_id = ANY(game_ids);
  END IF;

  -- Column list follows the ported SELECT's order, not migration 007's. The
  -- verified query's projection is reproduced unchanged and the six extra
  -- columns are appended, so no expression had to be resequenced.
  INSERT INTO euroleague.player_four_factors_by_game (
    game_id, team_id, player_id, is_on_key, type_lineup,
    game_year, num_starters, own_starters, opp_starters,
    total_points, total_poss, ts_poss_count, oreb_count,
    oreb_opportunities, tov_count, steal_count, total_ft_attempts,
    total_fga, total_fgm, total_fg3_made,
    player_ts_poss_count, player_tov_count, minutes,
    fg2_made, fg2_att, fg3_made, fg3_att,
    layup_made, layup_att, dunk_made, dunk_att, onoff_minutes,
    deflection_count, c3_made, c3_att, c3_known_att,
    load_run_id, derivation_version
  )
  WITH real_roster AS (
    SELECT fr.game_id, fr.team_id, fr.player_id
      FROM euroleague.full_rosters fr
      JOIN euroleague.players p ON p.player_id = fr.player_id
     WHERE (game_ids IS NULL OR fr.game_id = ANY(game_ids))
       AND lower(p.provider_player_id) NOT IN ('team', 'total')
       AND lower(btrim(p.display_name)) NOT IN ('team', 'total')
  ),
  -- Minutes come from matchup_segments, which holds each segment's duration
  -- exactly once. is_on_key is a plain membership test of the roster player
  -- against the segment's own lineup -- EuroLeague lineups are first-class, so
  -- no lineup derivation is needed here.
  player_minutes AS (
    SELECT ms.game_id, ms.team_id, rr.player_id,
           CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END::smallint AS is_on_key,
           ms.own_starters, ms.opp_starters,
           round(sum(ms.segment_seconds) / 60.0, 3) AS minutes
      FROM euroleague.matchup_segments ms
      JOIN real_roster rr
        ON rr.game_id = ms.game_id AND rr.team_id = ms.team_id
      LEFT JOIN euroleague.lineup_players lp
        ON lp.lineup_id = ms.own_lineup_id AND lp.player_id = rr.player_id
     WHERE game_ids IS NULL OR ms.game_id = ANY(game_ids)
     GROUP BY ms.game_id, ms.team_id, rr.player_id,
              CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END,
              ms.own_starters, ms.opp_starters
  ),
  -- The output grain: every observed (game, team, player, is_on_key,
  -- own_starters, opp_starters) combination, on both sides.
  complete_grid AS (
    SELECT pm.game_id, pm.team_id, pm.player_id, pm.is_on_key,
           pm.own_starters, pm.opp_starters, pm.minutes,
           side.type_lineup
      FROM player_minutes pm
      CROSS JOIN (VALUES ('offense'::text), ('defense'::text)) AS side(type_lineup)
  ),
  counts AS (
    SELECT atc.game_id, atc.team_id, rr.player_id,
           CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END::smallint AS is_on_key,
           atc.type_lineup, atc.own_starters, atc.opp_starters,
           sum(atc.points)::numeric            AS total_points,
           sum(atc.possession_flag)::bigint    AS total_poss,
           sum(atc.ts_possessions)::bigint     AS ts_poss_count,
           sum(atc.orebounds)::bigint          AS oreb_count,
           sum(atc.oreb_opportunities)::bigint AS oreb_opportunities,
           sum(atc.turnovers)::bigint          AS tov_count,
           sum(atc.steals)::bigint             AS steal_count,
           sum(atc.ft_attempts)::bigint        AS total_ft_attempts,
           sum(atc.fga)::bigint                AS total_fga,
           sum(atc.fgm)::bigint                AS total_fgm,
           sum(atc.fg3_made)::bigint           AS total_fg3_made,
           -- Player-attributed variants: only when this player took the action,
           -- and only on offense. Mirrors off_player_ts_possessions in 002.
           sum(CASE WHEN atc.type_lineup = 'offense'
                     AND atc.action_player_id = rr.player_id
                    THEN atc.ts_possessions ELSE 0 END)::bigint
             AS player_ts_poss_count,
           sum(CASE WHEN atc.type_lineup = 'offense'
                     AND atc.action_player_id = rr.player_id
                    THEN atc.turnovers ELSE 0 END)::bigint
             AS player_tov_count,
           sum(atc.fg2_made)::integer          AS fg2_made,
           sum(atc.fg2_att)::integer           AS fg2_att,
           sum(atc.fg3_made)::integer          AS fg3_made,
           sum(atc.fg3_att)::integer           AS fg3_att,
           sum(atc.layup_made)::integer        AS layup_made,
           sum(atc.layup_att)::integer         AS layup_att,
           sum(atc.dunk_made)::integer         AS dunk_made,
           sum(atc.dunk_att)::integer          AS dunk_att
      FROM euroleague.action_team_context atc
      JOIN real_roster rr
        ON rr.game_id = atc.game_id AND rr.team_id = atc.team_id
      LEFT JOIN euroleague.lineup_players lp
        ON lp.lineup_id = atc.own_lineup_id AND lp.player_id = rr.player_id
     WHERE (game_ids IS NULL OR atc.game_id = ANY(game_ids))
       AND atc.type_lineup IS NOT NULL
     GROUP BY atc.game_id, atc.team_id, rr.player_id,
              CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END,
              atc.type_lineup, atc.own_starters, atc.opp_starters
  )
  SELECT cg.game_id, cg.team_id, cg.player_id, cg.is_on_key, cg.type_lineup,
         s.season                              AS game_year,
         cg.own_starters                       AS num_starters,
         cg.own_starters, cg.opp_starters,
         coalesce(c.total_points, 0)::numeric  AS total_points,
         coalesce(c.total_poss, 0)::bigint     AS total_poss,
         coalesce(c.ts_poss_count, 0)::bigint  AS ts_poss_count,
         coalesce(c.oreb_count, 0)::bigint     AS oreb_count,
         coalesce(c.oreb_opportunities, 0)::bigint AS oreb_opportunities,
         coalesce(c.tov_count, 0)::bigint      AS tov_count,
         coalesce(c.steal_count, 0)::bigint    AS steal_count,
         coalesce(c.total_ft_attempts, 0)::bigint AS total_ft_attempts,
         coalesce(c.total_fga, 0)::bigint      AS total_fga,
         coalesce(c.total_fgm, 0)::bigint      AS total_fgm,
         coalesce(c.total_fg3_made, 0)::bigint AS total_fg3_made,
         coalesce(c.player_ts_poss_count, 0)::bigint AS player_ts_poss_count,
         coalesce(c.player_tov_count, 0)::bigint AS player_tov_count,
         CASE WHEN cg.type_lineup = 'offense'
              THEN coalesce(cg.minutes, 0) ELSE 0 END::numeric AS minutes,
         coalesce(c.fg2_made, 0)::integer      AS fg2_made,
         coalesce(c.fg2_att, 0)::integer       AS fg2_att,
         coalesce(c.fg3_made, 0)::integer      AS fg3_made,
         coalesce(c.fg3_att, 0)::integer       AS fg3_att,
         coalesce(c.layup_made, 0)::integer    AS layup_made,
         coalesce(c.layup_att, 0)::integer     AS layup_att,
         coalesce(c.dunk_made, 0)::integer     AS dunk_made,
         coalesce(c.dunk_att, 0)::integer      AS dunk_att,
         CASE WHEN cg.type_lineup = 'offense'
              THEN coalesce(cg.minutes, 0) ELSE 0 END::numeric AS onoff_minutes,
         -- deflection_count, c3_made, c3_att, c3_known_att. EuroLeague has no
         -- shots endpoint and no deflection event; hardcoded 0 since 002.
         0, 0, 0, 0,
         s.last_seen_load_run_id,
         'existing-israeli-contract-v1'
    FROM complete_grid cg
    JOIN euroleague.schedule s ON s.game_id = cg.game_id
    LEFT JOIN counts c
      ON c.game_id = cg.game_id
     AND c.team_id = cg.team_id
     AND c.player_id = cg.player_id
     AND c.is_on_key = cg.is_on_key
     AND c.type_lineup = cg.type_lineup
     AND c.own_starters = cg.own_starters
     AND c.opp_starters = cg.opp_starters;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

COMMIT;
