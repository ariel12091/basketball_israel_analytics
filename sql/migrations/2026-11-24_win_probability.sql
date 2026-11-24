-- Win probability: docs/shared_definitions.md, "Win probability" and "Win
-- probability added". Applied by scripts/apply_win_probability.R (dry run by
-- default). Afterwards re-apply sql/security/enable_readonly_rls.sql, which
-- lists win_prob_states and game_win_probability for app_readonly.
--
--   win_prob_model        the fitted constants, one row per fit, one active;
--                         written by scripts/fit_win_probability.R --write
--   win_prob_states       one row per possession of a game, both leagues: the
--                         single definition of the model's inputs, read by the
--                         fit script and by game_win_probability
--   game_win_probability  the home team's chance of winning at the start of
--                         each possession, from the active fit

CREATE TABLE IF NOT EXISTS basketball_test.win_prob_model (
  fit_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  fitted_on date NOT NULL,
  seasons text NOT NULL,
  games integer NOT NULL,
  possession_states integer NOT NULL,
  sigma double precision NOT NULL,              -- spread of the margin over 40 minutes, points
  c_seconds double precision NOT NULL,          -- time floor that stands in for a tie going to overtime
  home_advantage double precision NOT NULL,     -- points over a whole game
  rating_weight double precision NOT NULL,      -- k: points of expected margin per point of rating difference
  rating_shrink_poss double precision NOT NULL, -- K: possessions of zero added to every pre-game rating
  ball_value double precision NOT NULL,         -- b: points a possession is worth to the team that has it
  checks text,                                  -- the fit's out-of-sample results, as the fit script printed them
  active boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS win_prob_model_one_active
  ON basketball_test.win_prob_model (active) WHERE active;
COMMENT ON TABLE basketball_test.win_prob_model IS
  'Win-probability constants, one row per fit (scripts/fit_win_probability.R). Exactly one row is active; game_win_probability() reads it. docs/shared_definitions.md, "Win probability".';

-- p_league: 'il' (basketball_test) or 'euro' (euroleague: EuroLeague and
-- EuroCup). Any other value returns no rows. p_game_ids NULL = every game.
-- A possession starts after the previous final_end_poss (NULL is not an end),
-- over the offensive rows in event order; the last unfinished possession of a
-- game has no row. seconds_left is time left in regulation, or in the current
-- overtime period. The pre-game rating inputs are the team's net points and
-- possessions in its earlier games of the same season (no carry-over between
-- seasons); the per-game totals equal the event rows' own sums in both leagues
-- (checked 2026-11-24: 1,028 of 1,028 Israeli and 1,710 of 1,710 EuroLeague
-- team-games). One row per game more, is_final, carries the final score.
CREATE OR REPLACE FUNCTION basketball_test.win_prob_states(p_league text, p_game_ids integer[])
RETURNS TABLE (
  game_id integer,
  season text,
  possession_no integer,
  is_final boolean,
  period integer,
  seconds_left numeric,
  home_team_id integer,
  away_team_id integer,
  home_has_ball boolean,
  home_score numeric,
  away_score numeric,
  neutral boolean,
  home_won boolean,
  home_prior_net numeric,
  home_prior_poss numeric,
  away_prior_net numeric,
  away_prior_poss numeric
)
LANGUAGE sql
STABLE
SET plan_cache_mode = force_custom_plan
AS $function$
  WITH games AS (
    SELECT f.game_id AS gid, f.game_year::text AS season_key, f.game_date AS gdate,
           f.team_id AS home_id, f.opp_team_id AS away_id, f.has_won AS won, f.game_type = 35 AS is_neutral
      FROM basketball_test.final_schedule_mv f
     WHERE p_league = 'il' AND f.is_home AND (p_game_ids IS NULL OR f.game_id = ANY (p_game_ids))
    UNION ALL
    SELECT f.game_id, f.competition || ' ' || f.game_year, f.game_date,
           f.team_id, f.opp_team_id, f.has_won, COALESCE(s.phase = 'FF', FALSE)
      FROM euroleague.final_schedule f
      JOIN euroleague.schedule s ON s.game_id = f.game_id
     WHERE p_league = 'euro' AND f.is_home AND (p_game_ids IS NULL OR f.game_id = ANY (p_game_ids))
  ), events AS (
    SELECT d.game_id AS gid, d.id::numeric AS ord, d.team_id AS tid, d.quarter AS per,
           d.end_game_seconds_remaining::numeric AS secs,
           COALESCE(d.team_score, 0)::numeric AS pts, COALESCE(d.final_end_poss, FALSE) AS pend
      FROM basketball_test.df_pts_poss_lineups_longer_mv d
      JOIN games g ON g.gid = d.game_id
     WHERE p_league = 'il' AND d.type_lineup = 'offense'
    UNION ALL
    SELECT r.game_id, r.source_event_order::numeric, r.team_id, r.period,
           CASE WHEN r.period <= 4 THEN 2400 - r.elapsed_seconds
                ELSE 2400 + 300 * (r.period - 4) - r.elapsed_seconds END::numeric,
           COALESCE(r.points, 0)::numeric, COALESCE(r.final_end_poss, FALSE)
      FROM euroleague.ribbon_events_v r
      JOIN games g ON g.gid = r.game_id
     WHERE p_league = 'euro' AND r.type_lineup = 'offense'
  ), team_games AS (
    SELECT t.game_id AS gid, t.team_id AS tid, f.game_year::text AS season_key, f.game_date AS gdate,
           (t.pts - o.pts)::numeric AS net, t.off_poss::numeric AS poss
      FROM basketball_test.team_metrics_by_game_mv t
      JOIN basketball_test.team_metrics_by_game_mv o ON o.game_id = t.game_id AND o.team_id <> t.team_id
      JOIN basketball_test.final_schedule_mv f ON f.game_id = t.game_id AND f.team_id = t.team_id
     WHERE p_league = 'il'
    UNION ALL
    SELECT t.gid, t.tid, f.competition || ' ' || f.game_year, f.game_date, (t.pts - o.pts)::numeric, t.poss::numeric
      FROM (SELECT x.game_id AS gid, x.team_id AS tid, sum(x.off_pts) AS pts, sum(x.off_poss) AS poss
              FROM euroleague.team_four_factors_by_game x GROUP BY x.game_id, x.team_id) t
      JOIN (SELECT x.game_id AS gid, x.team_id AS tid, sum(x.off_pts) AS pts
              FROM euroleague.team_four_factors_by_game x GROUP BY x.game_id, x.team_id) o
        ON o.gid = t.gid AND o.tid <> t.tid
      JOIN euroleague.final_schedule f ON f.game_id = t.gid AND f.team_id = t.tid
     WHERE p_league = 'euro'
  ), running AS (
    SELECT e.gid, e.ord, e.tid, e.per, e.secs, e.pend,
           sum(e.pend::int) OVER w - e.pend::int AS pnum,
           sum(CASE WHEN e.tid = g.home_id THEN e.pts ELSE 0 END) OVER w
             - CASE WHEN e.tid = g.home_id THEN e.pts ELSE 0 END AS h_before,
           sum(CASE WHEN e.tid = g.away_id THEN e.pts ELSE 0 END) OVER w
             - CASE WHEN e.tid = g.away_id THEN e.pts ELSE 0 END AS a_before
      FROM events e
      JOIN games g ON g.gid = e.gid
    WINDOW w AS (PARTITION BY e.gid ORDER BY e.ord, e.tid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
  ), poss AS (
    SELECT r.gid, r.pnum,
           (array_agg(r.per ORDER BY r.ord, r.tid))[1] AS per,
           (array_agg(r.tid ORDER BY r.ord DESC, r.tid DESC))[1] AS offense_id,
           (array_agg(r.h_before ORDER BY r.ord, r.tid))[1] AS h0,
           (array_agg(r.a_before ORDER BY r.ord, r.tid))[1] AS a0,
           (array_agg(r.secs ORDER BY r.ord DESC, r.tid DESC))[1] AS end_secs
      FROM running r
     GROUP BY r.gid, r.pnum
    HAVING bool_or(r.pend)
  ), timed AS (
    SELECT p.*,
           lag(p.end_secs) OVER x AS prev_end,
           lag(p.per) OVER x AS prev_per,
           row_number() OVER x AS n
      FROM poss p
    WINDOW x AS (PARTITION BY p.gid ORDER BY p.pnum)
  ), finals AS (
    SELECT e.gid,
           max(e.per) AS per,
           sum(CASE WHEN e.tid = g.home_id THEN e.pts ELSE 0 END) AS h_final,
           sum(CASE WHEN e.tid = g.away_id THEN e.pts ELSE 0 END) AS a_final
      FROM events e
      JOIN games g ON g.gid = e.gid
     GROUP BY e.gid
  ), rows_ AS (
    SELECT t.gid, t.n::int AS n, FALSE AS final_row, t.per,
           CASE WHEN t.prev_per IS NULL OR t.prev_per <> t.per
                THEN CASE WHEN t.per <= 4 THEN 2400 - 600 * (t.per - 1) ELSE 300 END
                ELSE t.prev_end END::numeric AS secs_left,
           (t.offense_id = g.home_id) AS home_ball, t.h0, t.a0
      FROM timed t
      JOIN games g ON g.gid = t.gid
    UNION ALL
    SELECT f.gid, (SELECT count(*) FROM timed t WHERE t.gid = f.gid)::int + 1, TRUE, f.per, 0::numeric,
           NULL::boolean, f.h_final, f.a_final
      FROM finals f
  )
  SELECT g.gid, g.season_key, x.n, x.final_row, x.per, x.secs_left,
         g.home_id, g.away_id, x.home_ball, x.h0, x.a0, g.is_neutral, g.won,
         COALESCE(hp.net, 0), COALESCE(hp.poss, 0), COALESCE(ap.net, 0), COALESCE(ap.poss, 0)
    FROM rows_ x
    JOIN games g ON g.gid = x.gid
    LEFT JOIN LATERAL (SELECT sum(tg.net) AS net, sum(tg.poss) AS poss FROM team_games tg
                        WHERE tg.tid = g.home_id AND tg.season_key = g.season_key AND tg.gdate < g.gdate) hp ON TRUE
    LEFT JOIN LATERAL (SELECT sum(tg.net) AS net, sum(tg.poss) AS poss FROM team_games tg
                        WHERE tg.tid = g.away_id AND tg.season_key = g.season_key AND tg.gdate < g.gdate) ap ON TRUE
   ORDER BY g.gid, x.n
$function$;

-- P(home wins) = Phi((margin + b * ball + mu * t / 2400) / (sigma * sqrt((t + c) / 2400))),
-- mu = home advantage (0 at a neutral venue) + k * rating difference,
-- rating = 100 * prior net / (prior possessions + K). The final row is 1 or 0
-- from the result. No rows when no fit is active.
CREATE OR REPLACE FUNCTION basketball_test.game_win_probability(p_league text, p_game_id integer)
RETURNS TABLE (
  possession_no integer,
  is_final boolean,
  period integer,
  seconds_left numeric,
  home_team_id integer,
  away_team_id integer,
  home_has_ball boolean,
  home_score numeric,
  away_score numeric,
  home_win_probability double precision,
  fit_id integer
)
LANGUAGE sql
STABLE
SET plan_cache_mode = force_custom_plan
AS $function$
  SELECT s.possession_no, s.is_final, s.period, s.seconds_left, s.home_team_id, s.away_team_id,
         s.home_has_ball, s.home_score, s.away_score,
         CASE WHEN s.is_final THEN CASE WHEN s.home_won THEN 1.0 ELSE 0.0 END
              ELSE 0.5 * erfc(-z.z / sqrt(2.0)) END,
         m.fit_id
    FROM basketball_test.win_prob_states(p_league, ARRAY[p_game_id]) s
   CROSS JOIN (SELECT * FROM basketball_test.win_prob_model WHERE active) m
   CROSS JOIN LATERAL (
     SELECT ((s.home_score - s.away_score)::double precision
             + m.ball_value * CASE WHEN s.home_has_ball THEN 1 ELSE -1 END
             + (CASE WHEN s.neutral THEN 0 ELSE m.home_advantage END
                + m.rating_weight * 100 * (s.home_prior_net::double precision / (s.home_prior_poss::double precision + m.rating_shrink_poss)
                                         - s.away_prior_net::double precision / (s.away_prior_poss::double precision + m.rating_shrink_poss)))
               * s.seconds_left::double precision / 2400)
            / (m.sigma * sqrt((s.seconds_left::double precision + m.c_seconds) / 2400)) AS z
   ) z
   ORDER BY s.possession_no
$function$;
