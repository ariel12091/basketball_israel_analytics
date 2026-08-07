-- ===========================================================================
-- 005: EuroLeague team ratings.
--
-- Until now the opponent-strength filter in onoff_compute and
-- four_factors_compute derived team offensive/defensive/net rating inline,
-- inside each function, from player_game_context. That meant the definition of
-- "net rating" existed in two places and would have existed in a third as soon
-- as a Team Ratings surface was built -- three copies that could silently drift.
--
-- This migration makes one relation the source of truth:
--
--   team_game_ratings_mv   one row per (game_id, team_id): RAW COUNTS only
--   team_ppp_ratings_mv    season aggregate, derived from the above
--
-- Both compute functions now rank opponents by aggregating
-- team_game_ratings_mv over the caller's date window. That keeps the existing
-- behaviour -- opponent strength still reflects the selected window rather
-- than the whole season -- while removing the duplicate definitions. A
-- season-level table alone could not have done that without changing what the
-- filter means.
--
-- Raw-count source: official box-score points (final_schedule.team_points /
-- opp_points) and a COUNT over possessions. Deliberately NOT
-- player_game_context, which carries one row per on-court player per
-- possession -- a 5x fan-out. Ratios survive that fan-out but raw counts do
-- not, and this table stores raw counts so every consumer can sum numerators
-- and denominators and divide once.
--
-- Apply order: 001 -> 002 -> 004 -> 005. (003 is superseded; do not apply.)
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1. Per-team-game raw counts.
-- ---------------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS euroleague.team_ppp_ratings_mv;
DROP MATERIALIZED VIEW IF EXISTS euroleague.team_game_ratings_mv;

CREATE MATERIALIZED VIEW euroleague.team_game_ratings_mv AS
SELECT
  fs.game_id,
  s.competition,
  s.season                AS game_year,
  s.round_number,
  s.phase,
  s.scheduled_at::date    AS game_date,
  fs.team_id,
  fs.opp_team_id,
  fs.is_home,
  fs.has_won,
  fs.team_points          AS off_pts,
  fs.opp_points           AS def_pts,
  -- Possessions the team had the ball, and possessions its opponent did.
  -- def_poss is the opponent's off_poss by construction.
  count(*) FILTER (WHERE p.offense_team_id = fs.team_id)     AS off_poss,
  count(*) FILTER (WHERE p.offense_team_id = fs.opp_team_id) AS def_poss
FROM euroleague.final_schedule fs
JOIN euroleague.schedule s    ON s.game_id = fs.game_id
LEFT JOIN euroleague.possessions p ON p.game_id = fs.game_id
GROUP BY
  fs.game_id, s.competition, s.season, s.round_number, s.phase,
  s.scheduled_at, fs.team_id, fs.opp_team_id, fs.is_home, fs.has_won,
  fs.team_points, fs.opp_points
WITH NO DATA;

CREATE UNIQUE INDEX euroleague_team_game_ratings_mv_pk
  ON euroleague.team_game_ratings_mv (game_id, team_id);

CREATE INDEX euroleague_team_game_ratings_mv_window_idx
  ON euroleague.team_game_ratings_mv (competition, game_year, game_date);

-- ---------------------------------------------------------------------------
-- 2. Season aggregate.
--    Sums raw counts and divides once -- never averages per-game ratios.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW euroleague.team_ppp_ratings_mv AS
WITH agg AS (
  SELECT
    g.competition,
    g.game_year,
    g.team_id,
    count(*)                                    AS games,
    count(*) FILTER (WHERE g.has_won)           AS wins,
    count(*) FILTER (WHERE NOT g.has_won)       AS losses,
    sum(g.off_pts)::bigint                      AS off_pts,
    sum(g.off_poss)::bigint                     AS off_poss,
    sum(g.def_pts)::bigint                      AS def_pts,
    sum(g.def_poss)::bigint                     AS def_poss
  FROM euroleague.team_game_ratings_mv g
  GROUP BY g.competition, g.game_year, g.team_id
),
rated AS (
  SELECT
    a.*,
    round(100.0 * a.off_pts / NULLIF(a.off_poss, 0), 1) AS off_rtg,
    round(100.0 * a.def_pts / NULLIF(a.def_poss, 0), 1) AS def_rtg
  FROM agg a
)
SELECT
  r.competition, r.game_year, r.team_id, t.display_name AS team_name,
  r.games, r.wins, r.losses,
  r.off_pts, r.off_poss, r.def_pts, r.def_poss,
  r.off_rtg, r.def_rtg,
  round(r.off_rtg - r.def_rtg, 1) AS net_rtg,
  dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY r.off_rtg DESC)             AS off_rank,
  dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY r.def_rtg ASC)              AS def_rank,
  dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY r.off_rtg - r.def_rtg DESC) AS net_rank
FROM rated r
JOIN euroleague.teams t ON t.team_id = r.team_id
WITH NO DATA;

CREATE UNIQUE INDEX euroleague_team_ppp_ratings_mv_pk
  ON euroleague.team_ppp_ratings_mv (competition, game_year, team_id);

-- ---------------------------------------------------------------------------
-- 3. Refresh entry point. team_game_ratings_mv feeds team_ppp_ratings_mv, so
--    it must refresh first; both must precede the player MVs, which call the
--    compute functions that now read team_game_ratings_mv.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION euroleague.refresh_app_materialized_views()
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  REFRESH MATERIALIZED VIEW euroleague.final_schedule_mv;
  REFRESH MATERIALIZED VIEW euroleague.team_game_ratings_mv;
  REFRESH MATERIALIZED VIEW euroleague.team_ppp_ratings_mv;
  REFRESH MATERIALIZED VIEW euroleague.player_onoff_default_mv;
  REFRESH MATERIALIZED VIEW euroleague.player_advanced_stats_mv;
END;
$function$;

-- ---------------------------------------------------------------------------
-- 4. Read access for the app.
-- ---------------------------------------------------------------------------

GRANT SELECT ON
  euroleague.team_game_ratings_mv,
  euroleague.team_ppp_ratings_mv
TO app_readonly;
