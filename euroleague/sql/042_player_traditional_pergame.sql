-- EuroLeague shadow schema -- migration 042: typed player/game traditional
-- fact and the non-clutch app reader that consumes it.
--
-- The former non-clutch path used the generic dynamic reader.  That reader
-- cross-joined every selected roster player to every selected five-player
-- lineup row and then evaluated name = ANY(lineup) to recover exposure.  The
-- Israeli companion does this work once during publication and stores the
-- additive result at (game, team, player) grain.  This migration adopts that
-- physical shape without copying Israeli provider semantics:
--
--   * official traditional counts remain sourced from the immutable
--     full_rosters.boxscore_stats evidence;
--   * exposure uses exact seconds and mapped player IDs already stored in
--     lineup_totals_by_game; player usage remains a canonical PBP derivative
--     from player_four_factors_by_game;
--   * team usage remains the additive team_four_factors_by_game result;
--   * ratios are calculated only after the app-selected games are summed.
--
-- The table is private.  app_readonly reaches it only through the scoped
-- SECURITY DEFINER reader.  Standard clutch keeps its existing incremental
-- cache and custom clutch remains action-grained.

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE TABLE euroleague.player_traditional_by_game (
  competition         text    NOT NULL,
  game_year           integer NOT NULL,
  game_id              bigint  NOT NULL REFERENCES euroleague.schedule(game_id),
  team_id              bigint  NOT NULL REFERENCES euroleague.teams(team_id),
  player_id            bigint  NOT NULL REFERENCES euroleague.players(player_id),
  load_run_id          bigint  NOT NULL REFERENCES euroleague.load_runs(load_run_id),

  gp                   integer NOT NULL CHECK (gp IN (0, 1)),
  poss_on_floor        numeric NOT NULL DEFAULT 0 CHECK (poss_on_floor >= 0),
  minutes              numeric NOT NULL DEFAULT 0 CHECK (minutes >= 0),

  pts                  numeric NOT NULL DEFAULT 0,
  reb                  numeric NOT NULL DEFAULT 0,
  oreb                 numeric NOT NULL DEFAULT 0,
  dreb                 numeric NOT NULL DEFAULT 0,
  ast                  numeric NOT NULL DEFAULT 0,
  stl                  numeric NOT NULL DEFAULT 0,
  blk                  numeric NOT NULL DEFAULT 0,
  tov                  numeric NOT NULL DEFAULT 0,
  fg2m                 numeric NOT NULL DEFAULT 0,
  fg2a                 numeric NOT NULL DEFAULT 0,
  fg3m                 numeric NOT NULL DEFAULT 0,
  fg3a                 numeric NOT NULL DEFAULT 0,
  ftm                  numeric NOT NULL DEFAULT 0,
  fta                  numeric NOT NULL DEFAULT 0,

  player_ts_poss       numeric NOT NULL DEFAULT 0,
  player_tov           numeric NOT NULL DEFAULT 0,
  team_ts_poss         numeric NOT NULL DEFAULT 0,
  team_tov             numeric NOT NULL DEFAULT 0,
  team_poss            numeric NOT NULL DEFAULT 0,

  derivation_version   text NOT NULL DEFAULT 'player-traditional-pergame-v1',
  derived_at           timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (game_id, team_id, player_id)
);

CREATE INDEX euroleague_player_traditional_by_game_filter_idx
  ON euroleague.player_traditional_by_game
     (competition, game_year, team_id, game_id, player_id);

ALTER TABLE euroleague.player_traditional_by_game ENABLE ROW LEVEL SECURITY;
CREATE POLICY app_readonly_select_all
  ON euroleague.player_traditional_by_game
  FOR SELECT TO app_readonly USING (true);
REVOKE ALL ON TABLE euroleague.player_traditional_by_game FROM PUBLIC;
REVOKE ALL ON TABLE euroleague.player_traditional_by_game FROM app_readonly;

CREATE OR REPLACE FUNCTION euroleague.refresh_player_traditional_by_game_for_games(
  p_game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
SET search_path = pg_catalog, euroleague, public
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF p_game_ids IS NULL OR array_length(p_game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.player_traditional_by_game;
  ELSE
    DELETE FROM euroleague.player_traditional_by_game
    WHERE game_id = ANY(p_game_ids);
  END IF;

  INSERT INTO euroleague.player_traditional_by_game (
    competition, game_year, game_id, team_id, player_id, load_run_id,
    gp, poss_on_floor, minutes,
    pts, reb, oreb, dreb, ast, stl, blk, tov,
    fg2m, fg2a, fg3m, fg3a, ftm, fta,
    player_ts_poss, player_tov, team_ts_poss, team_tov, team_poss,
    derivation_version, derived_at
  )
  WITH roster AS MATERIALIZED (
    SELECT s.competition, s.season::integer AS game_year,
      fr.game_id, fr.team_id, fr.player_id, fr.load_run_id,
      coalesce((fr.boxscore_stats ->> 'Points')::numeric, 0) pts,
      coalesce((fr.boxscore_stats ->> 'TotalRebounds')::numeric, 0) reb,
      coalesce((fr.boxscore_stats ->> 'OffensiveRebounds')::numeric, 0) oreb,
      coalesce((fr.boxscore_stats ->> 'DefensiveRebounds')::numeric, 0) dreb,
      coalesce((fr.boxscore_stats ->> 'Assistances')::numeric, 0) ast,
      coalesce((fr.boxscore_stats ->> 'Steals')::numeric, 0) stl,
      coalesce((fr.boxscore_stats ->> 'BlocksFavour')::numeric, 0) blk,
      coalesce((fr.boxscore_stats ->> 'Turnovers')::numeric, 0) tov,
      coalesce((fr.boxscore_stats ->> 'FieldGoalsMade2')::numeric, 0) fg2m,
      coalesce((fr.boxscore_stats ->> 'FieldGoalsAttempted2')::numeric, 0) fg2a,
      coalesce((fr.boxscore_stats ->> 'FieldGoalsMade3')::numeric, 0) fg3m,
      coalesce((fr.boxscore_stats ->> 'FieldGoalsAttempted3')::numeric, 0) fg3a,
      coalesce((fr.boxscore_stats ->> 'FreeThrowsMade')::numeric, 0) ftm,
      coalesce((fr.boxscore_stats ->> 'FreeThrowsAttempted')::numeric, 0) fta
    FROM euroleague.full_rosters fr
    JOIN euroleague.schedule s ON s.game_id = fr.game_id
    JOIN euroleague.players p ON p.player_id = fr.player_id
    WHERE (p_game_ids IS NULL OR fr.game_id = ANY(p_game_ids))
      AND lower(p.provider_player_id) NOT IN ('team', 'total')
      AND lower(btrim(p.display_name)) NOT IN ('team', 'total')
  ), exposure AS MATERIALIZED (
    -- lineup_totals already carries the resolved five player IDs. Expanding
    -- those five IDs once during publication preserves the legacy reader's
    -- exact lineup seconds without crossing the roster against lineup names.
    SELECT l.game_id, l.team_id, u.player_id,
      coalesce(sum(l.possessions), 0)::numeric AS poss_on_floor,
      coalesce(sum(l.seconds), 0)::numeric / 60.0 AS minutes
    FROM euroleague.lineup_totals_by_game l
    CROSS JOIN LATERAL unnest(l.player_ids) AS u(player_id)
    WHERE l.type_lineup = 'offense'
      AND (p_game_ids IS NULL OR l.game_id = ANY(p_game_ids))
    GROUP BY l.game_id, l.team_id, u.player_id
  ), player_usage AS MATERIALIZED (
    SELECT pf.game_id, pf.team_id, pf.player_id,
      coalesce(sum(pf.player_ts_poss_count), 0)::numeric AS player_ts_poss,
      coalesce(sum(pf.player_tov_count), 0)::numeric AS player_tov
    FROM euroleague.player_four_factors_by_game pf
    WHERE pf.type_lineup = 'offense'
      AND (p_game_ids IS NULL OR pf.game_id = ANY(p_game_ids))
    GROUP BY pf.game_id, pf.team_id, pf.player_id
  ), team_usage AS MATERIALIZED (
    SELECT tf.game_id, tf.team_id,
      coalesce(sum(tf.off_ts_poss), 0)::numeric AS team_ts_poss,
      coalesce(sum(tf.off_tov), 0)::numeric AS team_tov,
      coalesce(sum(tf.off_poss), 0)::numeric AS team_poss
    FROM euroleague.team_four_factors_by_game tf
    WHERE p_game_ids IS NULL OR tf.game_id = ANY(p_game_ids)
    GROUP BY tf.game_id, tf.team_id
  ), typed AS (
    SELECT r.*,
      coalesce(e.poss_on_floor, 0)::numeric AS poss_on_floor,
      coalesce(e.minutes, 0)::numeric AS minutes,
      coalesce(pu.player_ts_poss, 0)::numeric AS player_ts_poss,
      coalesce(pu.player_tov, 0)::numeric AS player_tov,
      coalesce(t.team_ts_poss, 0)::numeric AS team_ts_poss,
      coalesce(t.team_tov, 0)::numeric AS team_tov,
      coalesce(t.team_poss, 0)::numeric AS team_poss
    FROM roster r
    LEFT JOIN exposure e
      ON e.game_id = r.game_id AND e.team_id = r.team_id
     AND e.player_id = r.player_id
    LEFT JOIN player_usage pu
      ON pu.game_id = r.game_id AND pu.team_id = r.team_id
     AND pu.player_id = r.player_id
    LEFT JOIN team_usage t
      ON t.game_id = r.game_id AND t.team_id = r.team_id
  )
  SELECT competition, game_year, game_id, team_id, player_id, load_run_id,
    1, poss_on_floor, minutes,
    pts, reb, oreb, dreb, ast, stl, blk, tov,
    fg2m, fg2a, fg3m, fg3a, ftm, fta,
    player_ts_poss, player_tov, team_ts_poss, team_tov, team_poss,
    'player-traditional-pergame-v1', now()
  FROM typed
  WHERE minutes > 0 OR poss_on_floor > 0
     OR pts + reb + ast + stl + blk + tov + fg2a + fg3a + fta > 0;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

REVOKE ALL ON FUNCTION
  euroleague.refresh_player_traditional_by_game_for_games(bigint[])
  FROM PUBLIC;
REVOKE ALL ON FUNCTION
  euroleague.refresh_player_traditional_by_game_for_games(bigint[])
  FROM app_readonly;

SELECT euroleague.refresh_player_traditional_by_game_for_games(NULL::bigint[]);
ANALYZE euroleague.player_traditional_by_game;

CREATE OR REPLACE FUNCTION euroleague.get_player_traditional_pergame(
  p_competition text, p_game_year integer,
  p_start_date date DEFAULT NULL, p_end_date date DEFAULT NULL,
  p_team_ids_csv text DEFAULT NULL, p_phase_csv text DEFAULT NULL,
  p_opp_ids_csv text DEFAULT NULL, p_home_away text DEFAULT 'all',
  p_outcome text DEFAULT 'all', p_opp_rank_side text DEFAULT NULL,
  p_opp_rank_n integer DEFAULT NULL, p_opp_rank_metric text DEFAULT NULL,
  p_min_gn integer DEFAULT NULL, p_max_gn integer DEFAULT NULL,
  p_last_n_games integer DEFAULT NULL
)
RETURNS TABLE (
  team_id bigint, player_id bigint, team_name text, "Player" text,
  gp integer, poss_on_floor numeric, minutes numeric,
  pts numeric, reb numeric, oreb numeric, dreb numeric,
  ast numeric, stl numeric, blk numeric, dfl numeric, tov numeric,
  fgm numeric, fga numeric, fg_pct numeric,
  "3pm" numeric, "3pa" numeric, tp_pct numeric,
  ftm numeric, fta numeric, ft_pct numeric, efg numeric,
  ts numeric, usg_pct numeric
)
LANGUAGE sql
STABLE
SECURITY DEFINER
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
  SELECT s.game_id, s.team_id
  FROM schedule_ranked s CROSS JOIN normalized n
  LEFT JOIN opponent_ranks r ON r.team_id = s.opp_team_id
  WHERE (p_start_date IS NULL OR s.game_date >= p_start_date)
    AND (p_end_date IS NULL OR s.game_date <= p_end_date)
    AND (n.team_ids IS NULL OR s.team_id = ANY(n.team_ids))
    AND (n.phases IS NULL OR s.phase = ANY(n.phases))
    AND (n.opp_ids IS NULL OR s.opp_team_id = ANY(n.opp_ids))
    AND (n.home_away = 'all' OR n.home_away = 'home' AND s.is_home
      OR n.home_away = 'away' AND NOT s.is_home)
    AND (n.outcome = 'all' OR n.outcome = 'win' AND s.has_won
      OR n.outcome = 'loss' AND NOT s.has_won)
    AND (n.rank_side IS NULL OR p_opp_rank_n IS NULL
      OR n.rank_side = 'top' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank
        WHEN 'def' THEN r.def_rank ELSE r.net_rank END <= p_opp_rank_n
      OR n.rank_side = 'bottom' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank
        WHEN 'def' THEN r.def_rank ELSE r.net_rank END
        > r.team_count - p_opp_rank_n)
    AND (p_min_gn IS NULL OR s.round_number >= p_min_gn)
    AND (p_max_gn IS NULL OR s.round_number <= p_max_gn)
    AND (p_last_n_games IS NULL OR s.team_game_rank <= p_last_n_games)
), agg AS (
  SELECT f.team_id, f.player_id, sum(f.gp)::integer gp,
    sum(f.poss_on_floor)::numeric poss_on_floor,
    sum(f.minutes)::numeric minutes,
    sum(f.pts)::numeric pts, sum(f.reb)::numeric reb,
    sum(f.oreb)::numeric oreb, sum(f.dreb)::numeric dreb,
    sum(f.ast)::numeric ast, sum(f.stl)::numeric stl,
    sum(f.blk)::numeric blk, sum(f.tov)::numeric tov,
    sum(f.fg2m)::numeric fg2m, sum(f.fg2a)::numeric fg2a,
    sum(f.fg3m)::numeric fg3m, sum(f.fg3a)::numeric fg3a,
    sum(f.ftm)::numeric ftm, sum(f.fta)::numeric fta,
    sum(f.player_ts_poss)::numeric player_ts_poss,
    sum(f.player_tov)::numeric player_tov,
    sum(f.team_ts_poss)::numeric team_ts_poss,
    sum(f.team_tov)::numeric team_tov,
    sum(f.team_poss)::numeric team_poss
  FROM euroleague.player_traditional_by_game f
  JOIN games_filtered g USING (game_id, team_id)
  WHERE f.competition = (SELECT competition FROM normalized)
    AND f.game_year = p_game_year
  GROUP BY f.team_id, f.player_id
)
SELECT a.team_id, a.player_id, t.display_name,
  euroleague.person_display_name(p.display_name),
  a.gp, a.poss_on_floor, a.minutes,
  a.pts, a.reb, a.oreb, a.dreb, a.ast, a.stl, a.blk,
  NULL::numeric, a.tov,
  a.fg2m + a.fg3m, a.fg2a + a.fg3a,
  round(100 * (a.fg2m + a.fg3m) / nullif(a.fg2a + a.fg3a, 0), 1),
  a.fg3m, a.fg3a, round(100 * a.fg3m / nullif(a.fg3a, 0), 1),
  a.ftm, a.fta, round(100 * a.ftm / nullif(a.fta, 0), 1),
  round(100 * (a.fg2m + 1.5 * a.fg3m) / nullif(a.fg2a + a.fg3a, 0), 1),
  round(100 * a.pts / nullif(2 * a.player_ts_poss, 0), 1),
  round(100 * (a.player_ts_poss + a.player_tov) * a.team_poss
    / nullif((a.team_ts_poss + a.team_tov) * a.poss_on_floor, 0), 1)
FROM agg a
JOIN euroleague.teams t ON t.team_id = a.team_id
JOIN euroleague.players p ON p.player_id = a.player_id
$function$;

REVOKE ALL ON FUNCTION euroleague.get_player_traditional_pergame(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, integer, integer
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_player_traditional_pergame(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, integer, integer
) TO app_readonly;

COMMIT;
