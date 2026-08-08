-- EuroLeague shadow schema -- migration 008.
-- Persisted event x team-perspective fact.
--
-- One row per (action, perspective team): two rows per action, the long form
-- the Israeli pipeline settled on. Consumers filter and sum; no consumer
-- re-implements the perspective CASE.
--
-- Nothing reads this table in 008. Migration 009 switches the four-factor
-- refreshes onto it, gated on reproducing every stored row.
--
-- Side assignment is per event type RELATIVE to the perspective team. Steals,
-- blocks, committed fouls and defensive rebounds sit on the acting team's
-- DEFENSE; every other event type -- shots, free throws, turnovers,
-- offensive rebounds, assists, fouls drawn, and every administrative code
-- (substitutions, timeouts, jump balls and similar, which carry a team_id
-- but no offense/defense role of their own) -- sits on the acting team's
-- OFFENSE by default. Only the measure-carrying types are proven by the 008
-- gate: 2FGM/2FGA/3FGM/3FGA/FTM/FTA/TO/O on offense and ST on defense. AS,
-- RV, FV, CM, D and every administrative code carry no measure column
-- today, so their side is assigned by rule and unverified until something
-- counts them. Administrative codes still need a side (never NULL) so that
-- a segment made up only of them is not invisible to a consumer that
-- groups by type_lineup -- matchup_segments already counts these events
-- toward floor time, and the fact must agree with it.

BEGIN;

SET LOCAL search_path TO euroleague, public;

-- The joint segment is an entity; its duration is an attribute of that entity,
-- not of every event inside it. Storing the duration here rather than on each
-- event row is a deliberate deviation from the Israeli backbone, which
-- denormalises it and consequently needs a fill-in ETL pass, a MAX-per-segment
-- convention repeated at four call sites, and a standing validator asserting
-- the repeated copies have not drifted (count(DISTINCT segment_seconds) = 1).
-- One row per segment makes all three unnecessary. Durations are identical;
-- only the storage grain differs.
CREATE TABLE IF NOT EXISTS euroleague.matchup_segments (
  game_id                bigint   NOT NULL,
  team_id                bigint   NOT NULL,
  segment_id             integer  NOT NULL,
  own_lineup_id          bigint   NOT NULL,
  opp_lineup_id          bigint   NOT NULL,
  own_starters           smallint,
  opp_starters           smallint,
  start_event_order          integer NOT NULL,
  end_event_order_exclusive  integer NOT NULL,
  start_elapsed_seconds  numeric,
  end_elapsed_seconds    numeric,
  segment_seconds        numeric  NOT NULL,
  load_run_id            bigint,
  derivation_version     text     NOT NULL,
  derived_at             timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (game_id, team_id, segment_id),
  FOREIGN KEY (game_id, own_lineup_id)
    REFERENCES euroleague.lineups (game_id, lineup_id),
  FOREIGN KEY (game_id, opp_lineup_id)
    REFERENCES euroleague.lineups (game_id, lineup_id),
  FOREIGN KEY (team_id) REFERENCES euroleague.teams (team_id),
  CONSTRAINT matchup_segments_seconds_nonnegative
    CHECK (segment_seconds >= 0),
  CONSTRAINT matchup_segments_segment_id_positive
    CHECK (segment_id >= 0),
  -- Half-open, matching the stints convention already in this schema. This is
  -- what lets the fact resolve its segment_id by range join instead of the
  -- two INSERTs having to share a staging table.
  CONSTRAINT matchup_segments_half_open
    CHECK (end_event_order_exclusive > start_event_order)
);

CREATE TABLE IF NOT EXISTS euroleague.action_team_context (
  game_id                bigint   NOT NULL,
  source_event_order     integer  NOT NULL,
  team_id                bigint   NOT NULL,
  opponent_team_id       bigint   NOT NULL,
  period                 smallint,

  type_lineup            text,
  own_lineup_id          bigint   NOT NULL,
  opp_lineup_id          bigint   NOT NULL,
  own_stint_id           bigint,
  opp_stint_id           bigint,
  own_starters           smallint,
  opp_starters           smallint,

  event_team_id          bigint,
  action_player_id       bigint,
  play_type              text,
  play_info              text,
  synthetic_ft_trip_id   text,
  parent_play_type       text,
  ft_reverse_order       integer,

  points                 integer  NOT NULL DEFAULT 0,
  ts_possessions         integer  NOT NULL DEFAULT 0,
  orebounds              integer  NOT NULL DEFAULT 0,
  oreb_opportunities     integer  NOT NULL DEFAULT 0,
  turnovers              integer  NOT NULL DEFAULT 0,
  steals                 integer  NOT NULL DEFAULT 0,
  ft_attempts            integer  NOT NULL DEFAULT 0,
  fga                    integer  NOT NULL DEFAULT 0,
  fgm                    integer  NOT NULL DEFAULT 0,
  fg2_made               integer  NOT NULL DEFAULT 0,
  fg2_att                integer  NOT NULL DEFAULT 0,
  fg3_made               integer  NOT NULL DEFAULT 0,
  fg3_att                integer  NOT NULL DEFAULT 0,
  layup_made             integer  NOT NULL DEFAULT 0,
  layup_att              integer  NOT NULL DEFAULT 0,
  dunk_made              integer  NOT NULL DEFAULT 0,
  dunk_att               integer  NOT NULL DEFAULT 0,

  possession_flag        smallint NOT NULL DEFAULT 0,
  final_end_poss         boolean  NOT NULL DEFAULT false,
  endpoint_reason        text,

  event_elapsed_seconds  numeric,
  segment_id             integer,

  own_team_score         integer  NOT NULL DEFAULT 0,
  opp_team_score         integer  NOT NULL DEFAULT 0,

  load_run_id            bigint,
  derivation_version     text     NOT NULL,
  derived_at             timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (game_id, source_event_order, team_id),
  FOREIGN KEY (game_id, source_event_order)
    REFERENCES euroleague.actions_raw (game_id, source_event_order)
    ON DELETE CASCADE,
  FOREIGN KEY (game_id, team_id, segment_id)
    REFERENCES euroleague.matchup_segments (game_id, team_id, segment_id),
  FOREIGN KEY (game_id, own_lineup_id)
    REFERENCES euroleague.lineups (game_id, lineup_id),
  FOREIGN KEY (game_id, opp_lineup_id)
    REFERENCES euroleague.lineups (game_id, lineup_id),
  FOREIGN KEY (team_id) REFERENCES euroleague.teams (team_id),
  FOREIGN KEY (opponent_team_id) REFERENCES euroleague.teams (team_id),
  CONSTRAINT action_team_context_side_check
    CHECK (type_lineup IS NULL OR type_lineup IN ('offense', 'defense')),
  CONSTRAINT action_team_context_distinct_teams
    CHECK (team_id <> opponent_team_id),
  CONSTRAINT action_team_context_possession_flag_check
    CHECK (possession_flag IN (0, 1))
);

CREATE INDEX IF NOT EXISTS action_team_context_agg_idx
  ON euroleague.action_team_context (game_id, team_id, type_lineup);

CREATE INDEX IF NOT EXISTS action_team_context_lineup_idx
  ON euroleague.action_team_context (own_lineup_id);

-- Per-game refresh: populates matchup_segments (parent) then
-- action_team_context (child, FK to the segment). Safely re-runnable --
-- deletes the target games' rows and re-inserts inside the same transaction.
-- Both INSERTs re-derive the clock chain (clock_parts -> raw_elapsed ->
-- event_clock -> game_ends) independently; that duplication is accepted
-- because a shared temp table would need an ON COMMIT auto-cleanup clause,
-- and apply_shadow_schema() refuses any statement containing that clause's
-- keyword followed by whitespace. The twenty measure CASE expressions in
-- event_metrics (lifted verbatim from migration 002, lines 317-351) are
-- derived once, in the fact INSERT only.
--
-- event_base carries one column beyond migration 002/007's version:
-- ar.period, needed because action_team_context.period is part of the
-- target column list below. It is not one of the twenty measure
-- expressions, so adding it does not touch the verbatim block.
CREATE OR REPLACE FUNCTION euroleague.refresh_action_team_context_for_games(
  game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  PERFORM euroleague.refresh_stint_timing_for_games(game_ids);

  -- Child first: action_team_context references matchup_segments.
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.action_team_context;
    DELETE FROM euroleague.matchup_segments;
  ELSE
    DELETE FROM euroleague.action_team_context WHERE game_id = ANY(game_ids);
    DELETE FROM euroleague.matchup_segments WHERE game_id = ANY(game_ids);
  END IF;

  -- Parent first. This INSERT needs only the clock and the two lineups per
  -- event -- NOT event_metrics -- so it shares just the cheap part of the
  -- chain with the fact below. That is deliberate: a temp table would have
  -- let both statements share one derivation, but its auto-cleanup clause
  -- carries exactly the keyword apply_shadow_schema() refuses, and the
  -- guard is worth more than the saved scan.
  INSERT INTO euroleague.matchup_segments (
    game_id, team_id, segment_id, own_lineup_id, opp_lineup_id,
    own_starters, opp_starters,
    start_event_order, end_event_order_exclusive,
    start_elapsed_seconds, end_elapsed_seconds, segment_seconds,
    load_run_id, derivation_version
  )
  WITH target_games AS (
    SELECT s.* FROM euroleague.schedule s
     WHERE game_ids IS NULL OR s.game_id = ANY(game_ids)
  ),
  clock_parts AS (
    SELECT
      ar.game_id,
      ar.source_event_order,
      ar.period,
      CASE WHEN ar.period <= 4 THEN (ar.period - 1) * 600
           ELSE 2400 + (ar.period - 5) * 300 END::numeric AS period_start,
      CASE WHEN ar.period <= 4 THEN 600 ELSE 300 END::numeric AS period_length,
      CASE
        WHEN ar.marker_time ~ '^\d{1,2}:\d{2}$' THEN
          split_part(ar.marker_time, ':', 1)::integer * 60
          + split_part(ar.marker_time, ':', 2)::integer
      END::numeric AS clock_remaining
    FROM euroleague.actions_raw ar
    JOIN target_games tg ON tg.game_id = ar.game_id
    WHERE game_ids IS NULL OR ar.game_id = ANY(game_ids)
  ),
  raw_elapsed AS (
    SELECT
      cp.*,
      CASE
        WHEN cp.clock_remaining IS NOT NULL THEN
          cp.period_start + cp.period_length
          - least(greatest(cp.clock_remaining, 0), cp.period_length)
        ELSE NULL
      END::numeric AS raw_event_elapsed_seconds
    FROM clock_parts cp
  ),
  event_clock AS (
    SELECT
      re.game_id,
      re.source_event_order,
      coalesce(
        max(re.raw_event_elapsed_seconds) OVER (
          PARTITION BY re.game_id
          ORDER BY re.source_event_order
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        re.period_start
      )::numeric AS event_elapsed_seconds
    FROM raw_elapsed re
  ),
  game_ends AS (
    SELECT
      ar.game_id,
      (2400 + greatest(max(ar.period) - 4, 0) * 300)::numeric
        AS game_end_elapsed_seconds
    FROM euroleague.actions_raw ar
    JOIN target_games tg ON tg.game_id = ar.game_id
    WHERE game_ids IS NULL OR ar.game_id = ANY(game_ids)
    GROUP BY ar.game_id
  ),
  game_bounds AS (
    SELECT ar.game_id, max(ar.source_event_order) + 1 AS end_event_order_exclusive
      FROM euroleague.actions_raw ar
      JOIN target_games tg ON tg.game_id = ar.game_id
     WHERE game_ids IS NULL OR ar.game_id = ANY(game_ids)
     GROUP BY ar.game_id
  ),
  lineup_sided AS MATERIALIZED (
    SELECT
      al.game_id, al.source_event_order,
      ec.event_elapsed_seconds, ge.game_end_elapsed_seconds,
      side.team_id, side.own_lineup_id, side.opp_lineup_id,
      own_lineup.starter_count AS own_starters,
      opp_lineup.starter_count AS opp_starters,
      tg.last_seen_load_run_id
    FROM euroleague.action_lineups al
    JOIN target_games tg ON tg.game_id = al.game_id
    JOIN event_clock ec
      ON ec.game_id = al.game_id AND ec.source_event_order = al.source_event_order
    JOIN game_ends ge ON ge.game_id = al.game_id
    CROSS JOIN LATERAL (
      VALUES
        (tg.home_team_id, al.home_lineup_id, al.away_lineup_id),
        (tg.away_team_id, al.away_lineup_id, al.home_lineup_id)
    ) AS side(team_id, own_lineup_id, opp_lineup_id)
    JOIN euroleague.lineups own_lineup ON own_lineup.lineup_id = side.own_lineup_id
    JOIN euroleague.lineups opp_lineup ON opp_lineup.lineup_id = side.opp_lineup_id
    WHERE game_ids IS NULL OR al.game_id = ANY(game_ids)
  ),
  -- lineup_lagged/lineup_marked/numbered: same three-step split as
  -- joint_lagged/joint_marked/joint_numbered in migrations 002/007. A
  -- window function's argument cannot itself contain a window function
  -- (PostgreSQL: "window function calls cannot be nested"), so lag() OVER w
  -- and the running sum() OVER (...) cannot collapse into one CTE.
  lineup_lagged AS (
    SELECT ls.*,
      lag(ls.own_lineup_id) OVER w AS previous_own_lineup_id,
      lag(ls.opp_lineup_id) OVER w AS previous_opp_lineup_id
    FROM lineup_sided ls
    WINDOW w AS (PARTITION BY ls.game_id, ls.team_id ORDER BY ls.source_event_order)
  ),
  lineup_marked AS (
    SELECT ll.*,
      CASE WHEN ll.previous_own_lineup_id IS DISTINCT FROM ll.own_lineup_id
             OR ll.previous_opp_lineup_id IS DISTINCT FROM ll.opp_lineup_id
           THEN 1 ELSE 0 END AS new_segment
    FROM lineup_lagged ll
  ),
  numbered AS (
    SELECT lm.*,
      sum(lm.new_segment) OVER (
        PARTITION BY lm.game_id, lm.team_id
        ORDER BY lm.source_event_order
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS segment_number
    FROM lineup_marked lm
  ),
  starts AS (
    SELECT game_id, team_id, segment_number,
           own_lineup_id, opp_lineup_id, own_starters, opp_starters,
           min(source_event_order)       AS start_event_order,
           min(event_elapsed_seconds)    AS start_elapsed_seconds,
           max(game_end_elapsed_seconds) AS game_end_elapsed_seconds,
           max(last_seen_load_run_id)    AS load_run_id
      FROM numbered
     GROUP BY game_id, team_id, segment_number,
              own_lineup_id, opp_lineup_id, own_starters, opp_starters
  ),
  ordered AS (
    SELECT s.*,
           lead(s.start_event_order) OVER w    AS next_start_event_order,
           lead(s.start_elapsed_seconds) OVER w AS next_start_elapsed_seconds
      FROM starts s
    WINDOW w AS (PARTITION BY s.game_id, s.team_id ORDER BY s.segment_number)
  )
  SELECT
    o.game_id, o.team_id, o.segment_number,
    o.own_lineup_id, o.opp_lineup_id, o.own_starters, o.opp_starters,
    o.start_event_order,
    coalesce(o.next_start_event_order, gb.end_event_order_exclusive),
    o.start_elapsed_seconds,
    coalesce(o.next_start_elapsed_seconds, o.game_end_elapsed_seconds),
    greatest(
      coalesce(o.next_start_elapsed_seconds, o.game_end_elapsed_seconds)
      - o.start_elapsed_seconds, 0
    )::numeric,
    o.load_run_id,
    'action-team-context-v1'
  FROM ordered o
  JOIN game_bounds gb ON gb.game_id = o.game_id;

  INSERT INTO euroleague.action_team_context (
    game_id, source_event_order, team_id, opponent_team_id, period,
    type_lineup, own_lineup_id, opp_lineup_id, own_stint_id, opp_stint_id,
    own_starters, opp_starters,
    event_team_id, action_player_id, play_type, play_info,
    synthetic_ft_trip_id, parent_play_type, ft_reverse_order,
    points, ts_possessions, orebounds, oreb_opportunities, turnovers,
    steals, ft_attempts, fga, fgm, fg2_made, fg2_att, fg3_made, fg3_att,
    layup_made, layup_att, dunk_made, dunk_att,
    possession_flag, final_end_poss, endpoint_reason,
    event_elapsed_seconds, segment_id,
    own_team_score, opp_team_score,
    load_run_id, derivation_version
  )
  WITH target_games AS (
    SELECT s.* FROM euroleague.schedule s
     WHERE game_ids IS NULL OR s.game_id = ANY(game_ids)
  ),
  clock_parts AS (
    SELECT
      ar.game_id,
      ar.source_event_order,
      ar.period,
      CASE WHEN ar.period <= 4 THEN (ar.period - 1) * 600
           ELSE 2400 + (ar.period - 5) * 300 END::numeric AS period_start,
      CASE WHEN ar.period <= 4 THEN 600 ELSE 300 END::numeric AS period_length,
      CASE
        WHEN ar.marker_time ~ '^\d{1,2}:\d{2}$' THEN
          split_part(ar.marker_time, ':', 1)::integer * 60
          + split_part(ar.marker_time, ':', 2)::integer
      END::numeric AS clock_remaining
    FROM euroleague.actions_raw ar
    JOIN target_games tg ON tg.game_id = ar.game_id
    WHERE game_ids IS NULL OR ar.game_id = ANY(game_ids)
  ),
  raw_elapsed AS (
    SELECT
      cp.*,
      CASE
        WHEN cp.clock_remaining IS NOT NULL THEN
          cp.period_start + cp.period_length
          - least(greatest(cp.clock_remaining, 0), cp.period_length)
        ELSE NULL
      END::numeric AS raw_event_elapsed_seconds
    FROM clock_parts cp
  ),
  event_clock AS (
    SELECT
      re.game_id,
      re.source_event_order,
      coalesce(
        max(re.raw_event_elapsed_seconds) OVER (
          PARTITION BY re.game_id
          ORDER BY re.source_event_order
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        re.period_start
      )::numeric AS event_elapsed_seconds
    FROM raw_elapsed re
  ),
  game_ends AS (
    SELECT
      ar.game_id,
      (2400 + greatest(max(ar.period) - 4, 0) * 300)::numeric
        AS game_end_elapsed_seconds
    FROM euroleague.actions_raw ar
    JOIN target_games tg ON tg.game_id = ar.game_id
    WHERE game_ids IS NULL OR ar.game_id = ANY(game_ids)
    GROUP BY ar.game_id
  ),
  event_base AS (
    SELECT
      ar.game_id,
      ar.source_event_order,
      ar.period,
      ar.team_id AS event_team_id,
      ar.player_id AS action_player_id,
      ar.play_type,
      ar.play_info,
      ac.synthetic_ft_trip_id,
      root.play_type AS parent_play_type,
      al.home_lineup_id,
      al.away_lineup_id,
      tg.home_team_id,
      tg.away_team_id,
      p.offense_team_id AS endpoint_offense_team_id,
      row_number() OVER (
        PARTITION BY ar.game_id, ac.synthetic_ft_trip_id
        ORDER BY ar.source_event_order DESC
      ) AS ft_reverse_order
    FROM target_games tg
    JOIN euroleague.actions_raw ar ON ar.game_id = tg.game_id
    JOIN euroleague.actions_clean ac
      ON ac.game_id = ar.game_id
     AND ac.source_event_order = ar.source_event_order
    JOIN euroleague.actions_raw root
      ON root.game_id = ac.game_id
     AND root.source_event_order = ac.synthetic_parent_order
    JOIN euroleague.action_lineups al
      ON al.game_id = ar.game_id
     AND al.source_event_order = ar.source_event_order
    LEFT JOIN euroleague.possessions p
      ON p.game_id = ar.game_id
     AND p.endpoint_source_event_order = ar.source_event_order
    WHERE game_ids IS NULL OR ar.game_id = ANY(game_ids)
  ),
  event_metrics AS (
    SELECT
      eb.*,
      CASE eb.play_type
        WHEN '2FGM' THEN 2 WHEN '3FGM' THEN 3 WHEN 'FTM' THEN 1 ELSE 0
      END::integer AS points,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA', '3FGM', '3FGA') THEN 1
           WHEN eb.play_type IN ('FTM', 'FTA')
            AND eb.synthetic_ft_trip_id IS NOT NULL
            AND eb.parent_play_type = 'CM'
            AND eb.ft_reverse_order = 1 THEN 1 ELSE 0 END::integer
        AS ts_possessions,
      CASE WHEN eb.play_type = 'O' THEN 1 ELSE 0 END::integer AS orebounds,
      CASE WHEN eb.play_type IN ('2FGA', '3FGA') THEN 1
           WHEN eb.play_type = 'FTA'
            AND eb.synthetic_ft_trip_id IS NOT NULL
            AND eb.parent_play_type = 'CM'
            AND eb.ft_reverse_order = 1 THEN 1 ELSE 0 END::integer
        AS oreb_opportunities,
      CASE WHEN eb.play_type = 'TO' THEN 1 ELSE 0 END::integer AS turnovers,
      CASE WHEN eb.play_type = 'ST' THEN 1 ELSE 0 END::integer AS steals,
      CASE WHEN eb.play_type IN ('FTM', 'FTA') THEN 1 ELSE 0 END::integer
        AS ft_attempts,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA', '3FGM', '3FGA') THEN 1 ELSE 0 END::integer AS fga,
      CASE WHEN eb.play_type IN ('2FGM', '3FGM') THEN 1 ELSE 0 END::integer AS fgm,
      CASE WHEN eb.play_type = '3FGM' THEN 1 ELSE 0 END::integer AS fg3_made,
      CASE WHEN eb.play_type = '2FGM' THEN 1 ELSE 0 END::integer AS fg2_made,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA') THEN 1 ELSE 0 END::integer AS fg2_att,
      CASE WHEN eb.play_type IN ('3FGM', '3FGA') THEN 1 ELSE 0 END::integer AS fg3_att,
      CASE WHEN eb.play_type = '2FGM' AND eb.play_info ILIKE '%lay%up%' THEN 1 ELSE 0 END::integer AS layup_made,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA') AND eb.play_info ILIKE '%lay%up%' THEN 1 ELSE 0 END::integer AS layup_att,
      CASE WHEN eb.play_type = '2FGM' AND eb.play_info ILIKE '%dunk%' THEN 1 ELSE 0 END::integer AS dunk_made,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA') AND eb.play_info ILIKE '%dunk%' THEN 1 ELSE 0 END::integer AS dunk_att
    FROM event_base eb
  ),
  cum_scores AS MATERIALIZED (
    -- Cumulative score per team through each event, for clutch filtering.
    SELECT em.game_id, em.source_event_order, em.event_team_id,
           sum(em.points) OVER (
             PARTITION BY em.game_id, em.event_team_id
             ORDER BY em.source_event_order
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           )::integer AS team_running_score
      FROM event_metrics em
  ),
  sided AS MATERIALIZED (
    SELECT
      em.*,
      ec.event_elapsed_seconds,
      side.team_id, side.opponent_team_id,
      side.own_lineup_id, side.opp_lineup_id,
      own_lineup.starter_count AS own_starters,
      opp_lineup.starter_count AS opp_starters,
      -- Side assignment, relative to the perspective team. Steals, blocks,
      -- committed fouls and defensive rebounds sit on the acting team's
      -- DEFENSE (a steal recorded for team X credits X's defense and the
      -- opponent's offense). Every other event type -- shots, free throws,
      -- turnovers, offensive rebounds, assists, fouls drawn, and every
      -- administrative code with no basketball-role meaning of its own
      -- (substitutions, timeouts, jump balls, and similar) -- sits on the
      -- acting team's OFFENSE by default. Only the play types enumerated in
      -- this file's header comment carry a proven measure column; the rest
      -- (administrative codes included) still need a side so that a segment
      -- containing only administrative events is not invisible to any
      -- consumer that groups by type_lineup -- matchup_segments already
      -- counts these events toward floor time, and the fact must agree with
      -- it rather than silently dropping the bucket.
      CASE
        WHEN em.event_team_id IS NULL THEN NULL
        WHEN em.play_type IN ('ST','FV','CM','D')
          THEN CASE WHEN em.event_team_id = side.team_id
                    THEN 'defense' ELSE 'offense' END
        ELSE CASE WHEN em.event_team_id = side.team_id
                  THEN 'offense' ELSE 'defense' END
      END AS type_lineup,
      CASE WHEN em.endpoint_offense_team_id IS NULL THEN 0 ELSE 1 END::smallint
        AS possession_flag
    FROM event_metrics em
    JOIN event_clock ec
      ON ec.game_id = em.game_id
     AND ec.source_event_order = em.source_event_order
    CROSS JOIN LATERAL (
      VALUES
        (em.home_team_id, em.away_team_id, em.home_lineup_id, em.away_lineup_id),
        (em.away_team_id, em.home_team_id, em.away_lineup_id, em.home_lineup_id)
    ) AS side(team_id, opponent_team_id, own_lineup_id, opp_lineup_id)
    JOIN euroleague.lineups own_lineup ON own_lineup.lineup_id = side.own_lineup_id
    JOIN euroleague.lineups opp_lineup ON opp_lineup.lineup_id = side.opp_lineup_id
  )
  SELECT
    sd.game_id, sd.source_event_order, sd.team_id, sd.opponent_team_id, sd.period,
    sd.type_lineup, sd.own_lineup_id, sd.opp_lineup_id,
    own_stint.stint_id, opp_stint.stint_id,
    sd.own_starters, sd.opp_starters,
    sd.event_team_id, sd.action_player_id, sd.play_type, sd.play_info,
    sd.synthetic_ft_trip_id, sd.parent_play_type, sd.ft_reverse_order,
    sd.points, sd.ts_possessions, sd.orebounds, sd.oreb_opportunities,
    sd.turnovers, sd.steals, sd.ft_attempts, sd.fga, sd.fgm,
    sd.fg2_made, sd.fg2_att, sd.fg3_made, sd.fg3_att,
    sd.layup_made, sd.layup_att, sd.dunk_made, sd.dunk_att,
    sd.possession_flag,
    coalesce(ac.final_end_possession, false),
    ac.endpoint_reason,
    sd.event_elapsed_seconds,
    ms.segment_id,
    coalesce(own_score.team_running_score, 0),
    coalesce(opp_score.team_running_score, 0),
    tg.last_seen_load_run_id,
    'action-team-context-v1'
  FROM sided sd
  JOIN target_games tg ON tg.game_id = sd.game_id
  JOIN euroleague.actions_clean ac
    ON ac.game_id = sd.game_id AND ac.source_event_order = sd.source_event_order
  -- The segments written immediately above. Half-open, so exactly one matches.
  JOIN euroleague.matchup_segments ms
    ON ms.game_id = sd.game_id
   AND ms.team_id = sd.team_id
   AND sd.source_event_order >= ms.start_event_order
   AND sd.source_event_order <  ms.end_event_order_exclusive
  LEFT JOIN euroleague.stints own_stint
    ON own_stint.game_id = sd.game_id
   AND own_stint.team_id = sd.team_id
   AND sd.source_event_order >= own_stint.start_event_order
   AND sd.source_event_order <  own_stint.end_event_order_exclusive
  LEFT JOIN euroleague.stints opp_stint
    ON opp_stint.game_id = sd.game_id
   AND opp_stint.team_id = sd.opponent_team_id
   AND sd.source_event_order >= opp_stint.start_event_order
   AND sd.source_event_order <  opp_stint.end_event_order_exclusive
  LEFT JOIN cum_scores own_score
    ON own_score.game_id = sd.game_id
   AND own_score.source_event_order = sd.source_event_order
   AND own_score.event_team_id = sd.team_id
  LEFT JOIN cum_scores opp_score
    ON opp_score.game_id = sd.game_id
   AND opp_score.source_event_order = sd.source_event_order
   AND opp_score.event_team_id = sd.opponent_team_id;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

COMMIT;
