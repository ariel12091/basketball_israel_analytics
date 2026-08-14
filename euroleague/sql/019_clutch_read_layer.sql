-- EuroLeague shadow schema -- migration 019: shared clutch read layer.
--
-- Mirrors the Israeli clutch contract at the application boundary:
--   * margin/status are evaluated from the row team's PRE-EVENT score;
--   * the time limit applies to regulation only;
--   * overtime bypasses margin/status unless p_ot_margin_filter is true.
--
-- EuroLeague minutes use score-state intervals intersected with canonical
-- matchup segments. This is deliberately stricter than measuring from the
-- first to last qualifying event, which would bridge non-clutch stretches.

BEGIN;

SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.clutch_margin_qualifies(
    p_own_score INTEGER,
    p_opp_score INTEGER,
    p_max_margin INTEGER,
    p_margin_status TEXT
)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $function$
  SELECT
    (p_max_margin IS NULL OR abs(p_own_score - p_opp_score) <= p_max_margin)
    AND CASE COALESCE(NULLIF(btrim(p_margin_status), ''), 'all')
      WHEN 'all'      THEN true
      WHEN 'leading'  THEN p_own_score > p_opp_score
      WHEN 'trailing' THEN p_own_score < p_opp_score
      WHEN 'tied'     THEN p_own_score = p_opp_score
      ELSE false
    END
$function$;

CREATE OR REPLACE FUNCTION euroleague.clutch_event_qualifies(
    p_period INTEGER,
    p_event_elapsed_seconds NUMERIC,
    p_pre_own_score INTEGER,
    p_pre_opp_score INTEGER,
    p_max_margin INTEGER,
    p_margin_status TEXT,
    p_max_time_remaining INTEGER,
    p_ot_margin_filter BOOLEAN
)
RETURNS BOOLEAN
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $function$
  SELECT
    (
      p_period > 4
      OR p_max_time_remaining IS NULL
      OR greatest(2400 - p_event_elapsed_seconds, 0) <= p_max_time_remaining
    )
    AND (
      (p_period > 4 AND NOT coalesce(p_ot_margin_filter, false))
      OR euroleague.clutch_margin_qualifies(
           p_pre_own_score, p_pre_opp_score, p_max_margin, p_margin_status
         )
    )
$function$;

-- Exact qualifying duration for every requested canonical lineup segment.
-- The calculation is set-based so one score-state pass serves all segments.
-- Score-state
-- intervals begin immediately after a scoring event; the scoring event itself
-- is classified by clutch_event_qualifies() from its pre-event score.
CREATE OR REPLACE FUNCTION euroleague.clutch_segment_durations(
    p_game_ids BIGINT[],
    p_max_margin INTEGER,
    p_margin_status TEXT,
    p_max_time_remaining INTEGER,
    p_ot_margin_filter BOOLEAN
)
RETURNS TABLE (game_id BIGINT, team_id BIGINT, segment_id INTEGER, seconds NUMERIC)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
AS $function$
  WITH game_bounds AS (
    SELECT ms.game_id, ms.team_id,
           max(ms.end_elapsed_seconds)::numeric AS game_end
    FROM euroleague.matchup_segments_actions ms
    WHERE ms.game_id = ANY(coalesce(p_game_ids, ARRAY[]::bigint[]))
    GROUP BY ms.game_id, ms.team_id
  ),
  score_states AS (
    SELECT
      gb.game_id,
      gb.team_id,
      0::numeric AS state_start,
      '-9223372036854775808'::bigint AS sort_order,
      0::integer AS own_score,
      0::integer AS opp_score,
      gb.game_end
    FROM game_bounds gb
    UNION ALL
    SELECT
      atc.game_id,
      atc.team_id,
      atc.event_elapsed_seconds,
      atc.source_event_order::bigint,
      atc.own_team_score,
      atc.opp_team_score,
      gb.game_end
    FROM euroleague.action_team_context_actions atc
    JOIN game_bounds gb ON gb.game_id = atc.game_id AND gb.team_id = atc.team_id
    WHERE atc.game_id = ANY(coalesce(p_game_ids, ARRAY[]::bigint[]))
      AND atc.points > 0
  ),
  intervals AS (
    SELECT
      ss.game_id,
      ss.team_id,
      ss.state_start,
      lead(ss.state_start, 1, ss.game_end) OVER (
        PARTITION BY ss.game_id, ss.team_id
        ORDER BY ss.state_start, ss.sort_order
      )::numeric AS state_end,
      ss.own_score,
      ss.opp_score
    FROM score_states ss
  )
  SELECT
    ms.game_id,
    ms.team_id,
    ms.segment_id,
    coalesce(sum(
      CASE WHEN euroleague.clutch_margin_qualifies(
                    i.own_score, i.opp_score, p_max_margin, p_margin_status
                  )
           THEN greatest(
                  least(i.state_end, ms.end_elapsed_seconds, 2400::numeric)
                  - greatest(
                      i.state_start,
                      ms.start_elapsed_seconds,
                      CASE WHEN p_max_time_remaining IS NULL THEN 0::numeric
                           ELSE greatest(2400 - p_max_time_remaining, 0)::numeric END
                    ),
                  0::numeric
                )
           ELSE 0::numeric END
      + CASE WHEN NOT coalesce(p_ot_margin_filter, false)
                       OR euroleague.clutch_margin_qualifies(
                            i.own_score, i.opp_score,
                            p_max_margin, p_margin_status
                          )
             THEN greatest(
                    least(i.state_end, ms.end_elapsed_seconds)
                    - greatest(i.state_start, ms.start_elapsed_seconds, 2400::numeric),
                    0::numeric
                  )
             ELSE 0::numeric END
    ), 0)::numeric AS seconds
  FROM euroleague.matchup_segments_actions ms
  JOIN intervals i
    ON i.game_id = ms.game_id AND i.team_id = ms.team_id
   AND i.state_end >= ms.start_elapsed_seconds
   AND i.state_start <= ms.end_elapsed_seconds
  WHERE ms.game_id = ANY(coalesce(p_game_ids, ARRAY[]::bigint[]))
  GROUP BY ms.game_id, ms.team_id, ms.segment_id
$function$;

-- One additive row per game/team/five-player lineup/starter context/side.
-- All public clutch consumers use this function, so event and duration
-- semantics cannot drift between Team Ratings and Lineup Data.
CREATE OR REPLACE FUNCTION euroleague.clutch_team_game_facts(
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
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
AS $function$
  WITH segment_rows AS (
    SELECT
      ms.game_id, ms.team_id, ms.own_lineup,
      ms.own_starters, ms.opp_starters,
      d.seconds
    FROM euroleague.matchup_segments_actions ms
    JOIN euroleague.clutch_segment_durations(
      p_game_ids, p_max_margin, p_margin_status,
      p_max_time_remaining, p_ot_margin_filter
    ) d
      ON d.game_id = ms.game_id AND d.team_id = ms.team_id
     AND d.segment_id = ms.segment_id
    WHERE ms.game_id = ANY(coalesce(p_game_ids, ARRAY[]::bigint[]))
  ),
  segment_totals AS (
    SELECT
      sr.game_id, sr.team_id, sr.own_lineup,
      sr.own_starters, sr.opp_starters,
      sum(sr.seconds)::numeric AS seconds
    FROM segment_rows sr
    GROUP BY sr.game_id, sr.team_id, sr.own_lineup,
             sr.own_starters, sr.opp_starters
  ),
  event_counts AS (
    SELECT
      atc.game_id, atc.team_id, atc.own_lineup,
      atc.own_starters, atc.opp_starters, atc.type_lineup,
      sum(atc.possession_flag)::bigint AS possessions,
      sum(atc.points)::bigint AS points,
      sum(atc.fg2_made)::bigint AS fg2_made,
      sum(atc.fg2_att)::bigint AS fg2_att,
      sum(atc.fg3_made)::bigint AS fg3_made,
      sum(atc.fg3_att)::bigint AS fg3_att,
      sum(atc.ts_possessions)::bigint AS ts_possessions,
      sum(atc.fgm)::bigint AS fgm,
      sum(atc.fga)::bigint AS fga,
      sum(atc.ft_attempts)::bigint AS ft_attempts,
      sum(atc.orebounds)::bigint AS orebounds,
      sum(atc.oreb_opportunities)::bigint AS oreb_opportunities,
      sum(atc.turnovers)::bigint AS turnovers,
      sum(atc.steals)::bigint AS steals
    FROM euroleague.action_team_context_actions atc
    WHERE atc.game_id = ANY(coalesce(p_game_ids, ARRAY[]::bigint[]))
      AND atc.type_lineup IS NOT NULL
      AND euroleague.clutch_event_qualifies(
            atc.period,
            atc.event_elapsed_seconds,
            atc.own_team_score
              - CASE WHEN atc.event_team_id = atc.team_id THEN atc.points ELSE 0 END,
            atc.opp_team_score
              - CASE WHEN atc.event_team_id = atc.opponent_team_id THEN atc.points ELSE 0 END,
            p_max_margin, p_margin_status, p_max_time_remaining,
            p_ot_margin_filter
          )
    GROUP BY atc.game_id, atc.team_id, atc.own_lineup,
             atc.own_starters, atc.opp_starters, atc.type_lineup
  )
  SELECT
    st.game_id, st.team_id, st.own_lineup,
    st.own_starters, st.opp_starters, side.type_lineup,
    coalesce(ec.possessions, 0)::bigint,
    coalesce(ec.points, 0)::bigint,
    coalesce(ec.fg2_made, 0)::bigint,
    coalesce(ec.fg2_att, 0)::bigint,
    coalesce(ec.fg3_made, 0)::bigint,
    coalesce(ec.fg3_att, 0)::bigint,
    coalesce(ec.ts_possessions, 0)::bigint,
    coalesce(ec.fgm, 0)::bigint,
    coalesce(ec.fga, 0)::bigint,
    coalesce(ec.ft_attempts, 0)::bigint,
    coalesce(ec.orebounds, 0)::bigint,
    coalesce(ec.oreb_opportunities, 0)::bigint,
    coalesce(ec.turnovers, 0)::bigint,
    coalesce(ec.steals, 0)::bigint,
    CASE WHEN side.type_lineup = 'offense' THEN st.seconds END
  FROM segment_totals st
  CROSS JOIN (VALUES ('offense'::text), ('defense'::text)) side(type_lineup)
  LEFT JOIN event_counts ec
    ON ec.game_id = st.game_id
   AND ec.team_id = st.team_id
   AND ec.own_lineup = st.own_lineup
   AND ec.own_starters = st.own_starters
   AND ec.opp_starters = st.opp_starters
   AND ec.type_lineup = side.type_lineup
  WHERE st.seconds > 0 OR ec.game_id IS NOT NULL
$function$;

-- Shared schedule/context adapter used by all three public dynamic readers.
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
    FROM euroleague.clutch_team_game_facts(
      ARRAY(SELECT DISTINCT g.game_id FROM games g),
      p_max_margin, p_margin_status, p_max_time_remaining,
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

DROP FUNCTION IF EXISTS euroleague.get_team_ratings_dynamic(
  text, int4, date, date, text, text, text, text, text, text, int4, text,
  int4, int4, int4, int4, int4, int4, int4);

CREATE OR REPLACE FUNCTION euroleague.get_team_ratings_dynamic(
    p_competition TEXT, p_game_year INTEGER,
    p_start_date DATE DEFAULT NULL, p_end_date DATE DEFAULT NULL,
    p_team_ids_csv TEXT DEFAULT NULL, p_phase_csv TEXT DEFAULT NULL,
    p_opp_ids_csv TEXT DEFAULT NULL, p_home_away TEXT DEFAULT 'all',
    p_outcome TEXT DEFAULT 'all', p_opp_rank_side TEXT DEFAULT NULL,
    p_opp_rank_n INTEGER DEFAULT NULL, p_opp_rank_metric TEXT DEFAULT NULL,
    p_max_margin INTEGER DEFAULT NULL, p_margin_status TEXT DEFAULT NULL,
    p_max_time_remaining INTEGER DEFAULT NULL,
    p_ot_margin_filter BOOLEAN DEFAULT FALSE,
    p_min_gn INTEGER DEFAULT NULL, p_max_gn INTEGER DEFAULT NULL,
    p_last_n_games INTEGER DEFAULT NULL,
    p_num_starters_off_min INTEGER DEFAULT NULL,
    p_num_starters_off_max INTEGER DEFAULT NULL,
    p_num_starters_def_min INTEGER DEFAULT NULL,
    p_num_starters_def_max INTEGER DEFAULT NULL
)
RETURNS TABLE (
    game_year INT, team_id BIGINT, team_name TEXT,
    off_ppp NUMERIC, def_ppp NUMERIC, net_rtg NUMERIC,
    games_played BIGINT, wins BIGINT, losses BIGINT,
    off_poss BIGINT, def_poss BIGINT,
    rank_net_rtg BIGINT, rank_off_ppp BIGINT, rank_def_ppp BIGINT
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
AS $function$
  WITH facts AS (
    SELECT * FROM euroleague.filtered_team_game_facts(
      p_competition, p_game_year, p_start_date, p_end_date,
      p_team_ids_csv, p_phase_csv, p_opp_ids_csv, p_home_away, p_outcome,
      p_opp_rank_side, p_opp_rank_n, p_opp_rank_metric,
      p_max_margin, p_margin_status, p_max_time_remaining, p_ot_margin_filter,
      p_min_gn, p_max_gn, p_last_n_games,
      p_num_starters_off_min, p_num_starters_off_max,
      p_num_starters_def_min, p_num_starters_def_max
    )
  ),
  agg AS (
    SELECT
      f.team_id, max(f.team_name) AS team_name,
      count(DISTINCT f.game_id) AS games_played,
      count(DISTINCT f.game_id) FILTER (WHERE f.has_won) AS wins,
      count(DISTINCT f.game_id) FILTER (WHERE NOT f.has_won) AS losses,
      sum(f.points) FILTER (WHERE f.type_lineup = 'offense') AS off_pts,
      sum(f.possessions) FILTER (WHERE f.type_lineup = 'offense') AS off_poss,
      sum(f.points) FILTER (WHERE f.type_lineup = 'defense') AS def_pts,
      sum(f.possessions) FILTER (WHERE f.type_lineup = 'defense') AS def_poss
    FROM facts f GROUP BY f.team_id
  ),
  rated AS (
    SELECT a.*,
      round(100.0 * a.off_pts / nullif(a.off_poss, 0), 1) AS off_ppp,
      round(100.0 * a.def_pts / nullif(a.def_poss, 0), 1) AS def_ppp
    FROM agg a
  )
  SELECT
    p_game_year, r.team_id, r.team_name,
    r.off_ppp, r.def_ppp, round(r.off_ppp - r.def_ppp, 1),
    r.games_played::bigint, r.wins::bigint, r.losses::bigint,
    r.off_poss::bigint, r.def_poss::bigint,
    dense_rank() OVER (ORDER BY r.off_ppp - r.def_ppp DESC),
    dense_rank() OVER (ORDER BY r.off_ppp DESC),
    dense_rank() OVER (ORDER BY r.def_ppp ASC)
  FROM rated r
  ORDER BY r.off_ppp - r.def_ppp DESC NULLS LAST
$function$;

DROP FUNCTION IF EXISTS euroleague.get_team_four_factors_dynamic(
  text, int4, date, date, text, text, text, text, text, text, int4, text,
  int4, int4, int4, int4, int4, int4, int4);

CREATE OR REPLACE FUNCTION euroleague.get_team_four_factors_dynamic(
    p_competition TEXT, p_game_year INTEGER,
    p_start_date DATE DEFAULT NULL, p_end_date DATE DEFAULT NULL,
    p_team_ids_csv TEXT DEFAULT NULL, p_phase_csv TEXT DEFAULT NULL,
    p_opp_ids_csv TEXT DEFAULT NULL, p_home_away TEXT DEFAULT 'all',
    p_outcome TEXT DEFAULT 'all', p_opp_rank_side TEXT DEFAULT NULL,
    p_opp_rank_n INTEGER DEFAULT NULL, p_opp_rank_metric TEXT DEFAULT NULL,
    p_max_margin INTEGER DEFAULT NULL, p_margin_status TEXT DEFAULT NULL,
    p_max_time_remaining INTEGER DEFAULT NULL,
    p_ot_margin_filter BOOLEAN DEFAULT FALSE,
    p_min_gn INTEGER DEFAULT NULL, p_max_gn INTEGER DEFAULT NULL,
    p_last_n_games INTEGER DEFAULT NULL,
    p_num_starters_off_min INTEGER DEFAULT NULL,
    p_num_starters_off_max INTEGER DEFAULT NULL,
    p_num_starters_def_min INTEGER DEFAULT NULL,
    p_num_starters_def_max INTEGER DEFAULT NULL
)
RETURNS TABLE (
    game_year INT, team_id BIGINT, team_name TEXT,
    off_ppp NUMERIC, def_ppp NUMERIC, net_rtg NUMERIC,
    off_efg NUMERIC, def_efg NUMERIC, off_ts NUMERIC, def_ts NUMERIC,
    off_oreb NUMERIC, def_oreb NUMERIC, off_tov NUMERIC, def_tov NUMERIC,
    off_ftr NUMERIC, def_ftr NUMERIC, off_poss BIGINT, def_poss BIGINT
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
AS $function$
  WITH facts AS (
    SELECT * FROM euroleague.filtered_team_game_facts(
      p_competition, p_game_year, p_start_date, p_end_date,
      p_team_ids_csv, p_phase_csv, p_opp_ids_csv, p_home_away, p_outcome,
      p_opp_rank_side, p_opp_rank_n, p_opp_rank_metric,
      p_max_margin, p_margin_status, p_max_time_remaining, p_ot_margin_filter,
      p_min_gn, p_max_gn, p_last_n_games,
      p_num_starters_off_min, p_num_starters_off_max,
      p_num_starters_def_min, p_num_starters_def_max
    )
  ),
  agg AS (
    SELECT
      f.team_id, max(f.team_name) AS team_name,
      sum(f.points) FILTER (WHERE f.type_lineup = 'offense') AS off_pts,
      sum(f.possessions) FILTER (WHERE f.type_lineup = 'offense') AS off_poss,
      sum(f.ts_possessions) FILTER (WHERE f.type_lineup = 'offense') AS off_ts_poss,
      sum(f.orebounds) FILTER (WHERE f.type_lineup = 'offense') AS off_oreb,
      sum(f.oreb_opportunities) FILTER (WHERE f.type_lineup = 'offense') AS off_oreb_opp,
      sum(f.turnovers) FILTER (WHERE f.type_lineup = 'offense') AS off_tov,
      sum(f.ft_attempts) FILTER (WHERE f.type_lineup = 'offense') AS off_fta,
      sum(f.fga) FILTER (WHERE f.type_lineup = 'offense') AS off_fga,
      sum(f.fgm) FILTER (WHERE f.type_lineup = 'offense') AS off_fgm,
      sum(f.fg3_made) FILTER (WHERE f.type_lineup = 'offense') AS off_fg3m,
      sum(f.points) FILTER (WHERE f.type_lineup = 'defense') AS def_pts,
      sum(f.possessions) FILTER (WHERE f.type_lineup = 'defense') AS def_poss,
      sum(f.ts_possessions) FILTER (WHERE f.type_lineup = 'defense') AS def_ts_poss,
      sum(f.orebounds) FILTER (WHERE f.type_lineup = 'defense') AS def_oreb,
      sum(f.oreb_opportunities) FILTER (WHERE f.type_lineup = 'defense') AS def_oreb_opp,
      sum(f.turnovers) FILTER (WHERE f.type_lineup = 'defense') AS def_tov,
      sum(f.ft_attempts) FILTER (WHERE f.type_lineup = 'defense') AS def_fta,
      sum(f.fga) FILTER (WHERE f.type_lineup = 'defense') AS def_fga,
      sum(f.fgm) FILTER (WHERE f.type_lineup = 'defense') AS def_fgm,
      sum(f.fg3_made) FILTER (WHERE f.type_lineup = 'defense') AS def_fg3m
    FROM facts f GROUP BY f.team_id
  )
  SELECT
    p_game_year, a.team_id, a.team_name,
    round(100.0 * a.off_pts / nullif(a.off_poss, 0), 1),
    round(100.0 * a.def_pts / nullif(a.def_poss, 0), 1),
    round(100.0 * a.off_pts / nullif(a.off_poss, 0)
        - 100.0 * a.def_pts / nullif(a.def_poss, 0), 1),
    round(100.0 * (a.off_fgm + 0.5 * a.off_fg3m) / nullif(a.off_fga, 0), 1),
    round(100.0 * (a.def_fgm + 0.5 * a.def_fg3m) / nullif(a.def_fga, 0), 1),
    round(100.0 * a.off_pts / nullif(2 * a.off_ts_poss, 0), 1),
    round(100.0 * a.def_pts / nullif(2 * a.def_ts_poss, 0), 1),
    round(100.0 * a.off_oreb / nullif(a.off_oreb_opp, 0), 1),
    round(100.0 * a.def_oreb / nullif(a.def_oreb_opp, 0), 1),
    round(100.0 * a.off_tov / nullif(a.off_poss, 0), 1),
    round(100.0 * a.def_tov / nullif(a.def_poss, 0), 1),
    round(100.0 * a.off_fta / nullif(a.off_fga, 0), 1),
    round(100.0 * a.def_fta / nullif(a.def_fga, 0), 1),
    a.off_poss::bigint, a.def_poss::bigint
  FROM agg a
  ORDER BY 100.0 * a.off_pts / nullif(a.off_poss, 0)
         - 100.0 * a.def_pts / nullif(a.def_poss, 0) DESC NULLS LAST
$function$;

DROP FUNCTION IF EXISTS euroleague.get_team_minutes_dynamic(
  text, int4, date, date, text, text, text, text, text, text, int4, text,
  int4, int4, int4, int4, int4, int4, int4);

CREATE OR REPLACE FUNCTION euroleague.get_team_minutes_dynamic(
    p_competition TEXT, p_game_year INTEGER,
    p_start_date DATE DEFAULT NULL, p_end_date DATE DEFAULT NULL,
    p_team_ids_csv TEXT DEFAULT NULL, p_phase_csv TEXT DEFAULT NULL,
    p_opp_ids_csv TEXT DEFAULT NULL, p_home_away TEXT DEFAULT 'all',
    p_outcome TEXT DEFAULT 'all', p_opp_rank_side TEXT DEFAULT NULL,
    p_opp_rank_n INTEGER DEFAULT NULL, p_opp_rank_metric TEXT DEFAULT NULL,
    p_max_margin INTEGER DEFAULT NULL, p_margin_status TEXT DEFAULT NULL,
    p_max_time_remaining INTEGER DEFAULT NULL,
    p_ot_margin_filter BOOLEAN DEFAULT FALSE,
    p_min_gn INTEGER DEFAULT NULL, p_max_gn INTEGER DEFAULT NULL,
    p_last_n_games INTEGER DEFAULT NULL,
    p_num_starters_off_min INTEGER DEFAULT NULL,
    p_num_starters_off_max INTEGER DEFAULT NULL,
    p_num_starters_def_min INTEGER DEFAULT NULL,
    p_num_starters_def_max INTEGER DEFAULT NULL
)
RETURNS TABLE (team_id BIGINT, minutes NUMERIC)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
AS $function$
  SELECT f.team_id, round(sum(f.seconds) / 60.0, 3)::numeric
  FROM euroleague.filtered_team_game_facts(
    p_competition, p_game_year, p_start_date, p_end_date,
    p_team_ids_csv, p_phase_csv, p_opp_ids_csv, p_home_away, p_outcome,
    p_opp_rank_side, p_opp_rank_n, p_opp_rank_metric,
    p_max_margin, p_margin_status, p_max_time_remaining, p_ot_margin_filter,
    p_min_gn, p_max_gn, p_last_n_games,
    p_num_starters_off_min, p_num_starters_off_max,
    p_num_starters_def_min, p_num_starters_def_max
  ) f
  WHERE f.type_lineup = 'offense'
  GROUP BY f.team_id
$function$;

DROP FUNCTION IF EXISTS euroleague.fetch_lineups_dynamic(
  text, int4, date, date, text, text, text, text, text, text, int4, text,
  int4, int4, int4, int4, int4, int4, int4, int4, text, text, int4);

CREATE OR REPLACE FUNCTION euroleague.fetch_lineups_dynamic(
    p_competition TEXT, p_game_year INTEGER,
    p_start_date DATE DEFAULT NULL, p_end_date DATE DEFAULT NULL,
    p_team_ids_csv TEXT DEFAULT NULL, p_phase_csv TEXT DEFAULT NULL,
    p_opp_ids_csv TEXT DEFAULT NULL, p_home_away TEXT DEFAULT 'all',
    p_outcome TEXT DEFAULT 'all', p_opp_rank_side TEXT DEFAULT NULL,
    p_opp_rank_n INTEGER DEFAULT NULL, p_opp_rank_metric TEXT DEFAULT NULL,
    p_max_margin INTEGER DEFAULT NULL, p_margin_status TEXT DEFAULT NULL,
    p_max_time_remaining INTEGER DEFAULT NULL,
    p_ot_margin_filter BOOLEAN DEFAULT FALSE,
    p_min_gn INTEGER DEFAULT NULL, p_max_gn INTEGER DEFAULT NULL,
    p_last_n_games INTEGER DEFAULT NULL,
    p_num_starters_off_min INTEGER DEFAULT NULL,
    p_num_starters_off_max INTEGER DEFAULT NULL,
    p_num_starters_def_min INTEGER DEFAULT NULL,
    p_num_starters_def_max INTEGER DEFAULT NULL,
    p_unit_size INTEGER DEFAULT 5,
    p_players_on_csv TEXT DEFAULT NULL,
    p_players_off_csv TEXT DEFAULT NULL,
    p_min_poss INTEGER DEFAULT 0
)
RETURNS TABLE (
    team_id BIGINT, unit_key TEXT, unit_size SMALLINT,
    player_ids BIGINT[], player_names_str TEXT,
    off_poss BIGINT, off_pts BIGINT, off_fg2_made BIGINT, off_fg2_att BIGINT,
    off_fg3_made BIGINT, off_fg3_att BIGINT, off_ts_poss BIGINT,
    off_fgm BIGINT, off_fga BIGINT, off_fta BIGINT,
    off_oreb BIGINT, off_oreb_opp BIGINT, off_tov BIGINT, off_steals BIGINT,
    def_poss BIGINT, def_pts BIGINT, def_fg2_made BIGINT, def_fg2_att BIGINT,
    def_fg3_made BIGINT, def_fg3_att BIGINT, def_ts_poss BIGINT,
    def_fgm BIGINT, def_fga BIGINT, def_fta BIGINT,
    def_oreb BIGINT, def_oreb_opp BIGINT, def_tov BIGINT, def_steals BIGINT,
    minutes NUMERIC
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
AS $function$
  WITH normalized AS (
    SELECT
      CASE WHEN nullif(btrim(p_players_on_csv), '') IS NULL THEN NULL::bigint[]
           ELSE string_to_array(regexp_replace(p_players_on_csv, '\s+', '', 'g'), ',')::bigint[] END AS players_on,
      CASE WHEN nullif(btrim(p_players_off_csv), '') IS NULL THEN NULL::bigint[]
           ELSE string_to_array(regexp_replace(p_players_off_csv, '\s+', '', 'g'), ',')::bigint[] END AS players_off
  ),
  facts AS MATERIALIZED (
    SELECT * FROM euroleague.filtered_team_game_facts(
      p_competition, p_game_year, p_start_date, p_end_date,
      p_team_ids_csv, p_phase_csv, p_opp_ids_csv, p_home_away, p_outcome,
      p_opp_rank_side, p_opp_rank_n, p_opp_rank_metric,
      p_max_margin, p_margin_status, p_max_time_remaining, p_ot_margin_filter,
      p_min_gn, p_max_gn, p_last_n_games,
      p_num_starters_off_min, p_num_starters_off_max,
      p_num_starters_def_min, p_num_starters_def_max
    )
  ),
  lineup_identity AS (
    SELECT DISTINCT
      l.game_id, l.team_id, l.own_lineup, l.lineup_key, l.player_ids
    FROM euroleague.lineup_totals_by_game l
    WHERE l.competition = coalesce(nullif(btrim(p_competition), ''), 'E')
      AND l.game_year = p_game_year
  ),
  unit_rows AS (
    SELECT
      sl.team_id, sl.unit_key, sl.unit_size, sl.player_ids,
      f.type_lineup, f.possessions, f.points,
      f.fg2_made, f.fg2_att, f.fg3_made, f.fg3_att,
      f.ts_possessions, f.fgm, f.fga, f.ft_attempts,
      f.orebounds, f.oreb_opportunities, f.turnovers, f.steals, f.seconds
    FROM facts f
    JOIN lineup_identity li
      ON li.game_id = f.game_id AND li.team_id = f.team_id
     AND li.own_lineup = f.own_lineup
    JOIN euroleague.sub_lineups sl
      ON sl.competition = coalesce(nullif(btrim(p_competition), ''), 'E')
     AND sl.game_year = p_game_year AND sl.team_id = li.team_id
     AND sl.lineup_key = li.lineup_key
    CROSS JOIN normalized n
    WHERE sl.unit_size = p_unit_size::smallint
      AND (n.players_on IS NULL OR sl.player_ids @> n.players_on)
      AND (n.players_off IS NULL OR NOT (sl.player_ids && n.players_off))
  ),
  agg AS (
    SELECT
      u.team_id, u.unit_key, u.unit_size, u.player_ids,
      sum(u.possessions) FILTER (WHERE u.type_lineup = 'offense') AS off_poss,
      sum(u.points) FILTER (WHERE u.type_lineup = 'offense') AS off_pts,
      sum(u.fg2_made) FILTER (WHERE u.type_lineup = 'offense') AS off_fg2_made,
      sum(u.fg2_att) FILTER (WHERE u.type_lineup = 'offense') AS off_fg2_att,
      sum(u.fg3_made) FILTER (WHERE u.type_lineup = 'offense') AS off_fg3_made,
      sum(u.fg3_att) FILTER (WHERE u.type_lineup = 'offense') AS off_fg3_att,
      sum(u.ts_possessions) FILTER (WHERE u.type_lineup = 'offense') AS off_ts_poss,
      sum(u.fgm) FILTER (WHERE u.type_lineup = 'offense') AS off_fgm,
      sum(u.fga) FILTER (WHERE u.type_lineup = 'offense') AS off_fga,
      sum(u.ft_attempts) FILTER (WHERE u.type_lineup = 'offense') AS off_fta,
      sum(u.orebounds) FILTER (WHERE u.type_lineup = 'offense') AS off_oreb,
      sum(u.oreb_opportunities) FILTER (WHERE u.type_lineup = 'offense') AS off_oreb_opp,
      sum(u.turnovers) FILTER (WHERE u.type_lineup = 'offense') AS off_tov,
      sum(u.steals) FILTER (WHERE u.type_lineup = 'offense') AS off_steals,
      sum(u.possessions) FILTER (WHERE u.type_lineup = 'defense') AS def_poss,
      sum(u.points) FILTER (WHERE u.type_lineup = 'defense') AS def_pts,
      sum(u.fg2_made) FILTER (WHERE u.type_lineup = 'defense') AS def_fg2_made,
      sum(u.fg2_att) FILTER (WHERE u.type_lineup = 'defense') AS def_fg2_att,
      sum(u.fg3_made) FILTER (WHERE u.type_lineup = 'defense') AS def_fg3_made,
      sum(u.fg3_att) FILTER (WHERE u.type_lineup = 'defense') AS def_fg3_att,
      sum(u.ts_possessions) FILTER (WHERE u.type_lineup = 'defense') AS def_ts_poss,
      sum(u.fgm) FILTER (WHERE u.type_lineup = 'defense') AS def_fgm,
      sum(u.fga) FILTER (WHERE u.type_lineup = 'defense') AS def_fga,
      sum(u.ft_attempts) FILTER (WHERE u.type_lineup = 'defense') AS def_fta,
      sum(u.orebounds) FILTER (WHERE u.type_lineup = 'defense') AS def_oreb,
      sum(u.oreb_opportunities) FILTER (WHERE u.type_lineup = 'defense') AS def_oreb_opp,
      sum(u.turnovers) FILTER (WHERE u.type_lineup = 'defense') AS def_tov,
      sum(u.steals) FILTER (WHERE u.type_lineup = 'defense') AS def_steals,
      sum(u.seconds) FILTER (WHERE u.type_lineup = 'offense') AS seconds
    FROM unit_rows u
    GROUP BY u.team_id, u.unit_key, u.unit_size, u.player_ids
  )
  SELECT
    a.team_id, a.unit_key, a.unit_size, a.player_ids,
    names.player_names_str,
    a.off_poss::bigint, a.off_pts::bigint,
    a.off_fg2_made::bigint, a.off_fg2_att::bigint,
    a.off_fg3_made::bigint, a.off_fg3_att::bigint,
    a.off_ts_poss::bigint, a.off_fgm::bigint, a.off_fga::bigint,
    a.off_fta::bigint, a.off_oreb::bigint, a.off_oreb_opp::bigint,
    a.off_tov::bigint, a.off_steals::bigint,
    a.def_poss::bigint, a.def_pts::bigint,
    a.def_fg2_made::bigint, a.def_fg2_att::bigint,
    a.def_fg3_made::bigint, a.def_fg3_att::bigint,
    a.def_ts_poss::bigint, a.def_fgm::bigint, a.def_fga::bigint,
    a.def_fta::bigint, a.def_oreb::bigint, a.def_oreb_opp::bigint,
    a.def_tov::bigint, a.def_steals::bigint,
    round(coalesce(a.seconds, 0) / 60.0, 1)
  FROM agg a
  CROSS JOIN LATERAL (
    SELECT string_agg(
      coalesce(euroleague.person_display_name(p.display_name), '#' || x.pid::text),
      ', ' ORDER BY x.ord
    ) AS player_names_str
    FROM unnest(a.player_ids) WITH ORDINALITY x(pid, ord)
    LEFT JOIN euroleague.players p ON p.player_id = x.pid
  ) names
  WHERE coalesce(a.off_poss, 0) + coalesce(a.def_poss, 0)
        >= coalesce(p_min_poss, 0)
$function$;

REVOKE ALL ON FUNCTION euroleague.clutch_margin_qualifies(
  integer, integer, integer, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION euroleague.clutch_event_qualifies(
  integer, numeric, integer, integer, integer, text, integer, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION euroleague.clutch_segment_durations(
  bigint[], integer, text, integer, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION euroleague.clutch_team_game_facts(
  bigint[], integer, text, integer, boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION euroleague.filtered_team_game_facts(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, text, integer, boolean, integer, integer, integer,
  integer, integer, integer, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION euroleague.get_team_ratings_dynamic(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, text, integer, boolean, integer, integer, integer,
  integer, integer, integer, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION euroleague.get_team_four_factors_dynamic(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, text, integer, boolean, integer, integer, integer,
  integer, integer, integer, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION euroleague.get_team_minutes_dynamic(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, text, integer, boolean, integer, integer, integer,
  integer, integer, integer, integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION euroleague.fetch_lineups_dynamic(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, text, integer, boolean, integer, integer, integer,
  integer, integer, integer, integer, integer, text, text, integer
) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION euroleague.get_team_ratings_dynamic(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, text, integer, boolean, integer, integer, integer,
  integer, integer, integer, integer) TO app_readonly;
GRANT EXECUTE ON FUNCTION euroleague.get_team_four_factors_dynamic(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, text, integer, boolean, integer, integer, integer,
  integer, integer, integer, integer) TO app_readonly;
GRANT EXECUTE ON FUNCTION euroleague.get_team_minutes_dynamic(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, text, integer, boolean, integer, integer, integer,
  integer, integer, integer, integer) TO app_readonly;
GRANT EXECUTE ON FUNCTION euroleague.fetch_lineups_dynamic(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, text, integer, boolean, integer, integer, integer,
  integer, integer, integer, integer, integer, text, text, integer
) TO app_readonly;

COMMIT;
