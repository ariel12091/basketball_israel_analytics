-- EuroLeague shadow schema -- migration 012: promote actions-based consumers.
--
-- This is the destructive half of the schema simplification. It is one
-- transaction: snapshot current outputs, rebuild them from canonical actions,
-- prove bidirectional parity, update team-rating materializations, and only
-- then remove the obsolete EuroLeague middle tables.

BEGIN;

SET LOCAL search_path TO euroleague, public;

CREATE TEMP TABLE baseline_player_four_factors AS
SELECT * FROM euroleague.player_four_factors_by_game;

CREATE TEMP TABLE baseline_team_four_factors AS
SELECT * FROM euroleague.team_four_factors_by_game;

CREATE TEMP TABLE baseline_team_game_ratings AS
SELECT * FROM euroleague.team_game_ratings_mv;

CREATE TEMP TABLE baseline_team_ppp_ratings AS
SELECT * FROM euroleague.team_ppp_ratings_mv;

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
  SELECT
    atc.game_id,
    atc.team_id,
    s.season::smallint,
    atc.own_starters,
    atc.opp_starters,
    coalesce(sum(atc.points) FILTER (WHERE atc.type_lineup = 'offense'), 0)::numeric,
    coalesce(sum(atc.possession_flag) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.ts_possessions) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.orebounds) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.oreb_opportunities) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.turnovers) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.ft_attempts) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.fga) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.fgm) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.fg3_made) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.points) FILTER (WHERE atc.type_lineup = 'defense'), 0)::numeric,
    coalesce(sum(atc.possession_flag) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.ts_possessions) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.orebounds) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.oreb_opportunities) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.turnovers) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.ft_attempts) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.fga) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.fgm) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.fg3_made) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.steals) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    'actions-v1'
  FROM euroleague.action_team_context_actions atc
  JOIN euroleague.schedule s ON s.game_id = atc.game_id
 WHERE game_ids IS NULL OR atc.game_id = ANY(game_ids)
 GROUP BY atc.game_id, atc.team_id, s.season,
          atc.own_starters, atc.opp_starters;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

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
    SELECT fr.game_id, fr.team_id, fr.player_id, fr.source_player_name
      FROM euroleague.full_rosters fr
      JOIN euroleague.players p ON p.player_id = fr.player_id
     WHERE (game_ids IS NULL OR fr.game_id = ANY(game_ids))
       AND lower(p.provider_player_id) NOT IN ('team', 'total')
       AND lower(btrim(p.display_name)) NOT IN ('team', 'total')
  ),
  player_minutes AS (
    SELECT
      ms.game_id,
      ms.team_id,
      rr.player_id,
      CASE WHEN rr.source_player_name = ANY(ms.own_lineup)
           THEN 1 ELSE 0 END::smallint AS is_on_key,
      ms.own_starters,
      ms.opp_starters,
      round(sum(ms.segment_seconds) / 60.0, 3) AS minutes
    FROM euroleague.matchup_segments_actions ms
    JOIN real_roster rr
      ON rr.game_id = ms.game_id AND rr.team_id = ms.team_id
   WHERE game_ids IS NULL OR ms.game_id = ANY(game_ids)
   GROUP BY ms.game_id, ms.team_id, rr.player_id,
            CASE WHEN rr.source_player_name = ANY(ms.own_lineup)
                 THEN 1 ELSE 0 END,
            ms.own_starters, ms.opp_starters
  ),
  complete_grid AS (
    SELECT pm.game_id, pm.team_id, pm.player_id, pm.is_on_key,
           pm.own_starters, pm.opp_starters, pm.minutes,
           side.type_lineup
      FROM player_minutes pm
      CROSS JOIN (
        VALUES ('offense'::text), ('defense'::text)
      ) AS side(type_lineup)
  ),
  counts AS (
    SELECT
      atc.game_id,
      atc.team_id,
      rr.player_id,
      CASE WHEN rr.source_player_name = ANY(atc.own_lineup)
           THEN 1 ELSE 0 END::smallint AS is_on_key,
      atc.type_lineup,
      atc.own_starters,
      atc.opp_starters,
      sum(atc.points)::numeric AS total_points,
      sum(atc.possession_flag)::bigint AS total_poss,
      sum(atc.ts_possessions)::bigint AS ts_poss_count,
      sum(atc.orebounds)::bigint AS oreb_count,
      sum(atc.oreb_opportunities)::bigint AS oreb_opportunities,
      sum(atc.turnovers)::bigint AS tov_count,
      sum(atc.steals)::bigint AS steal_count,
      sum(atc.ft_attempts)::bigint AS total_ft_attempts,
      sum(atc.fga)::bigint AS total_fga,
      sum(atc.fgm)::bigint AS total_fgm,
      sum(atc.fg3_made)::bigint AS total_fg3_made,
      sum(CASE WHEN atc.type_lineup = 'offense'
                AND atc.action_player_id = rr.player_id
               THEN atc.ts_possessions ELSE 0 END)::bigint
        AS player_ts_poss_count,
      sum(CASE WHEN atc.type_lineup = 'offense'
                AND atc.action_player_id = rr.player_id
               THEN atc.turnovers ELSE 0 END)::bigint
        AS player_tov_count,
      sum(atc.fg2_made)::integer AS fg2_made,
      sum(atc.fg2_att)::integer AS fg2_att,
      sum(atc.fg3_made)::integer AS fg3_made,
      sum(atc.fg3_att)::integer AS fg3_att,
      sum(atc.layup_made)::integer AS layup_made,
      sum(atc.layup_att)::integer AS layup_att,
      sum(atc.dunk_made)::integer AS dunk_made,
      sum(atc.dunk_att)::integer AS dunk_att
    FROM euroleague.action_team_context_actions atc
    JOIN real_roster rr
      ON rr.game_id = atc.game_id AND rr.team_id = atc.team_id
   WHERE (game_ids IS NULL OR atc.game_id = ANY(game_ids))
     AND atc.type_lineup IS NOT NULL
   GROUP BY atc.game_id, atc.team_id, rr.player_id,
            CASE WHEN rr.source_player_name = ANY(atc.own_lineup)
                 THEN 1 ELSE 0 END,
            atc.type_lineup, atc.own_starters, atc.opp_starters
  )
  SELECT
    cg.game_id,
    cg.team_id,
    cg.player_id,
    cg.is_on_key,
    cg.type_lineup,
    s.season AS game_year,
    cg.own_starters AS num_starters,
    cg.own_starters,
    cg.opp_starters,
    coalesce(c.total_points, 0)::numeric,
    coalesce(c.total_poss, 0)::bigint,
    coalesce(c.ts_poss_count, 0)::bigint,
    coalesce(c.oreb_count, 0)::bigint,
    coalesce(c.oreb_opportunities, 0)::bigint,
    coalesce(c.tov_count, 0)::bigint,
    coalesce(c.steal_count, 0)::bigint,
    coalesce(c.total_ft_attempts, 0)::bigint,
    coalesce(c.total_fga, 0)::bigint,
    coalesce(c.total_fgm, 0)::bigint,
    coalesce(c.total_fg3_made, 0)::bigint,
    coalesce(c.player_ts_poss_count, 0)::bigint,
    coalesce(c.player_tov_count, 0)::bigint,
    CASE WHEN cg.type_lineup = 'offense'
         THEN coalesce(cg.minutes, 0) ELSE 0 END::numeric,
    coalesce(c.fg2_made, 0)::integer,
    coalesce(c.fg2_att, 0)::integer,
    coalesce(c.fg3_made, 0)::integer,
    coalesce(c.fg3_att, 0)::integer,
    coalesce(c.layup_made, 0)::integer,
    coalesce(c.layup_att, 0)::integer,
    coalesce(c.dunk_made, 0)::integer,
    coalesce(c.dunk_att, 0)::integer,
    CASE WHEN cg.type_lineup = 'offense'
         THEN coalesce(cg.minutes, 0) ELSE 0 END::numeric,
    0, 0, 0, 0,
    s.last_seen_load_run_id,
    'actions-v1'
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

SELECT euroleague.refresh_player_four_factors_by_game_for_games(NULL::bigint[]);
SELECT euroleague.refresh_team_four_factors_by_game_for_games(NULL::bigint[]);

DO $migration$
BEGIN
  IF EXISTS (
    (SELECT to_jsonb(b) - 'derivation_version' - 'derived_at'
       FROM pg_temp.baseline_player_four_factors b
     EXCEPT
     SELECT to_jsonb(n) - 'derivation_version' - 'derived_at'
       FROM euroleague.player_four_factors_by_game n)
    UNION ALL
    (SELECT to_jsonb(n) - 'derivation_version' - 'derived_at'
       FROM euroleague.player_four_factors_by_game n
     EXCEPT
     SELECT to_jsonb(b) - 'derivation_version' - 'derived_at'
       FROM pg_temp.baseline_player_four_factors b)
  ) THEN
    RAISE EXCEPTION
      'actions-based player four-factor output differs from baseline';
  END IF;

  IF EXISTS (
    (SELECT to_jsonb(b) - 'derivation_version'
       FROM pg_temp.baseline_team_four_factors b
     EXCEPT
     SELECT to_jsonb(n) - 'derivation_version'
       FROM euroleague.team_four_factors_by_game n)
    UNION ALL
    (SELECT to_jsonb(n) - 'derivation_version'
       FROM euroleague.team_four_factors_by_game n
     EXCEPT
     SELECT to_jsonb(b) - 'derivation_version'
       FROM pg_temp.baseline_team_four_factors b)
  ) THEN
    RAISE EXCEPTION
      'actions-based team four-factor output differs from baseline';
  END IF;
END;
$migration$;

DROP MATERIALIZED VIEW euroleague.team_ppp_ratings_mv;
DROP MATERIALIZED VIEW euroleague.team_game_ratings_mv;

CREATE MATERIALIZED VIEW euroleague.team_game_ratings_mv AS
SELECT
  fs.game_id,
  s.competition,
  s.season AS game_year,
  s.round_number,
  s.phase,
  s.scheduled_at::date AS game_date,
  fs.team_id,
  fs.opp_team_id,
  fs.is_home,
  fs.has_won,
  fs.team_points AS off_pts,
  fs.opp_points AS def_pts,
  count(*) FILTER (
    WHERE a.possession_offense_team_id = fs.team_id
  ) AS off_poss,
  count(*) FILTER (
    WHERE a.possession_offense_team_id = fs.opp_team_id
  ) AS def_poss
FROM euroleague.final_schedule fs
JOIN euroleague.schedule s ON s.game_id = fs.game_id
LEFT JOIN euroleague.actions a
  ON a.game_id = fs.game_id AND a.end_possession
GROUP BY
  fs.game_id, s.competition, s.season, s.round_number, s.phase,
  s.scheduled_at, fs.team_id, fs.opp_team_id, fs.is_home, fs.has_won,
  fs.team_points, fs.opp_points
WITH NO DATA;

CREATE UNIQUE INDEX euroleague_team_game_ratings_mv_pk
  ON euroleague.team_game_ratings_mv(game_id, team_id);

CREATE INDEX euroleague_team_game_ratings_mv_window_idx
  ON euroleague.team_game_ratings_mv(competition, game_year, game_date);

CREATE MATERIALIZED VIEW euroleague.team_ppp_ratings_mv AS
WITH agg AS (
  SELECT
    g.competition, g.game_year, g.team_id,
    count(*) AS games_played,
    count(*) FILTER (WHERE g.has_won) AS wins,
    count(*) FILTER (WHERE NOT g.has_won) AS losses,
    sum(g.off_pts)::bigint AS off_pts,
    sum(g.off_poss)::bigint AS off_poss,
    sum(g.def_pts)::bigint AS def_pts,
    sum(g.def_poss)::bigint AS def_poss
  FROM euroleague.team_game_ratings_mv g
  GROUP BY g.competition, g.game_year, g.team_id
),
rated AS (
  SELECT
    a.*,
    round(100.0 * a.off_pts / NULLIF(a.off_poss, 0), 1) AS off_ppp,
    round(100.0 * a.def_pts / NULLIF(a.def_poss, 0), 1) AS def_ppp
  FROM agg a
)
SELECT
  r.competition, r.game_year, r.team_id, t.display_name AS team_name,
  r.off_ppp, r.def_ppp,
  round(r.off_ppp - r.def_ppp, 1) AS net_rtg,
  r.games_played, r.wins, r.losses,
  r.off_pts, r.def_pts, r.off_poss, r.def_poss,
  dense_rank() OVER (
    PARTITION BY r.competition, r.game_year
    ORDER BY r.off_ppp - r.def_ppp DESC
  ) AS rank_net_rtg,
  dense_rank() OVER (
    PARTITION BY r.competition, r.game_year ORDER BY r.off_ppp DESC
  ) AS rank_off_ppp,
  dense_rank() OVER (
    PARTITION BY r.competition, r.game_year ORDER BY r.def_ppp ASC
  ) AS rank_def_ppp,
  dense_rank() OVER (
    PARTITION BY r.competition, r.game_year ORDER BY r.off_ppp DESC
  ) AS off_rank,
  dense_rank() OVER (
    PARTITION BY r.competition, r.game_year ORDER BY r.def_ppp ASC
  ) AS def_rank,
  dense_rank() OVER (
    PARTITION BY r.competition, r.game_year
    ORDER BY r.off_ppp - r.def_ppp DESC
  ) AS net_rank
FROM rated r
JOIN euroleague.teams t ON t.team_id = r.team_id
WITH NO DATA;

CREATE UNIQUE INDEX euroleague_team_ppp_ratings_mv_pk
  ON euroleague.team_ppp_ratings_mv(competition, game_year, team_id);

GRANT SELECT ON
  euroleague.team_game_ratings_mv,
  euroleague.team_ppp_ratings_mv
TO app_readonly;

SELECT euroleague.refresh_app_materialized_views();

DO $migration$
BEGIN
  IF EXISTS (
    (SELECT to_jsonb(b) FROM pg_temp.baseline_team_game_ratings b
     EXCEPT
     SELECT to_jsonb(n) FROM euroleague.team_game_ratings_mv n)
    UNION ALL
    (SELECT to_jsonb(n) FROM euroleague.team_game_ratings_mv n
     EXCEPT
     SELECT to_jsonb(b) FROM pg_temp.baseline_team_game_ratings b)
  ) THEN
    RAISE EXCEPTION 'actions-based team game ratings differ from baseline';
  END IF;

  IF EXISTS (
    (SELECT to_jsonb(b) FROM pg_temp.baseline_team_ppp_ratings b
     EXCEPT
     SELECT to_jsonb(n) FROM euroleague.team_ppp_ratings_mv n)
    UNION ALL
    (SELECT to_jsonb(n) FROM euroleague.team_ppp_ratings_mv n
     EXCEPT
     SELECT to_jsonb(b) FROM pg_temp.baseline_team_ppp_ratings b)
  ) THEN
    RAISE EXCEPTION 'actions-based season team ratings differ from baseline';
  END IF;
END;
$migration$;

DROP FUNCTION euroleague.refresh_action_team_context_for_games(bigint[]);
DROP FUNCTION euroleague.refresh_stint_timing_for_games(bigint[]);

DROP TABLE euroleague.action_team_context;
DROP TABLE euroleague.matchup_segments;
DROP TABLE euroleague.pws;
DROP TABLE euroleague.stints;
DROP TABLE euroleague.action_lineups;
DROP TABLE euroleague.lineup_players;
DROP TABLE euroleague.lineups;
DROP TABLE euroleague.possessions;
DROP TABLE euroleague.actions_clean;

DO $migration$
BEGIN
  IF to_regclass('euroleague.actions_clean') IS NOT NULL
     OR to_regclass('euroleague.possessions') IS NOT NULL
     OR to_regclass('euroleague.lineups') IS NOT NULL
     OR to_regclass('euroleague.lineup_players') IS NOT NULL
     OR to_regclass('euroleague.action_lineups') IS NOT NULL
     OR to_regclass('euroleague.stints') IS NOT NULL
     OR to_regclass('euroleague.pws') IS NOT NULL
     OR to_regclass('euroleague.action_team_context') IS NOT NULL
     OR to_regclass('euroleague.matchup_segments') IS NOT NULL
  THEN
    RAISE EXCEPTION 'obsolete EuroLeague relations remain after cutover';
  END IF;
END;
$migration$;

COMMIT;
