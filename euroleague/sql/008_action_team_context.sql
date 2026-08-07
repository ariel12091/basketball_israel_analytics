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
-- DEFENSE; shots, free throws, turnovers, offensive rebounds, assists and
-- fouls drawn sit on its OFFENSE. Only the measure-carrying types are proven
-- by the 008 gate: 2FGM/2FGA/3FGM/3FGA/FTM/FTA/TO/O on offense and ST on
-- defense. AS, RV, FV, CM and D carry no measure column today, so their side
-- is assigned by rule and unverified until something counts them.

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

COMMIT;
