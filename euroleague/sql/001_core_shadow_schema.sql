-- EUROLEAGUE SHADOW SCHEMA
-- First applied for the isolated one-game trial on 2026-08-06.
--
-- EuroLeague shadow schema. The design follows the Israeli project's grains,
-- primary-key/upsert discipline, source preservation, and QA gates while
-- retaining EuroLeague-specific provider fields.
--
-- Package-first contract:
--   * euroleague-api supplies schedules, PBP normalization, box scores,
--     starters, and Lineup_A/Lineup_B.
--   * project code supplies checkpointing/provenance, deterministic
--     relationships and possessions, reconciliation, and persistence.

BEGIN;

CREATE SCHEMA IF NOT EXISTS euroleague;

CREATE TABLE IF NOT EXISTS euroleague.load_runs (
  load_run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  competition varchar(1) NOT NULL CHECK (competition IN ('E', 'U')),
  season smallint NOT NULL CHECK (season BETWEEN 2000 AND 2100),
  package_name text NOT NULL DEFAULT 'euroleague-api',
  package_version text NOT NULL,
  collector_version text NOT NULL,
  status text NOT NULL DEFAULT 'running'
    CHECK (status IN ('running', 'completed', 'partial', 'failed')),
  started_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  requested_games integer NOT NULL DEFAULT 0 CHECK (requested_games >= 0),
  successful_games integer NOT NULL DEFAULT 0 CHECK (successful_games >= 0),
  failed_games integer NOT NULL DEFAULT 0 CHECK (failed_games >= 0),
  request_parameters jsonb NOT NULL DEFAULT '{}'::jsonb,
  error_summary jsonb NOT NULL DEFAULT '[]'::jsonb,
  CHECK (completed_at IS NULL OR completed_at >= started_at),
  CHECK (successful_games + failed_games <= requested_games)
);

COMMENT ON TABLE euroleague.load_runs IS
  'One restartable extraction/normalization run with package and collector lineage.';

CREATE TABLE IF NOT EXISTS euroleague.teams (
  team_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  competition varchar(1) NOT NULL CHECK (competition IN ('E', 'U')),
  provider_team_code text NOT NULL CHECK (btrim(provider_team_code) <> ''),
  display_name text NOT NULL CHECK (btrim(display_name) <> ''),
  first_seen_season smallint CHECK (first_seen_season BETWEEN 2000 AND 2100),
  last_seen_season smallint CHECK (last_seen_season BETWEEN 2000 AND 2100),
  source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (competition, provider_team_code),
  CHECK (
    first_seen_season IS NULL
    OR last_seen_season IS NULL
    OR first_seen_season <= last_seen_season
  )
);

CREATE TABLE IF NOT EXISTS euroleague.players (
  player_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  competition varchar(1) NOT NULL CHECK (competition IN ('E', 'U')),
  provider_player_id text NOT NULL CHECK (btrim(provider_player_id) <> ''),
  display_name text NOT NULL CHECK (btrim(display_name) <> ''),
  source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (competition, provider_player_id)
);

CREATE TABLE IF NOT EXISTS euroleague.schedule (
  game_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  competition varchar(1) NOT NULL CHECK (competition IN ('E', 'U')),
  season smallint NOT NULL CHECK (season BETWEEN 2000 AND 2100),
  gamecode integer NOT NULL CHECK (gamecode > 0),
  round_number integer CHECK (round_number > 0),
  phase text,
  scheduled_at timestamptz,
  status text,
  home_team_id bigint NOT NULL REFERENCES euroleague.teams(team_id),
  away_team_id bigint NOT NULL REFERENCES euroleague.teams(team_id),
  home_points integer CHECK (home_points >= 0),
  away_points integer CHECK (away_points >= 0),
  first_seen_load_run_id bigint NOT NULL
    REFERENCES euroleague.load_runs(load_run_id),
  last_seen_load_run_id bigint NOT NULL
    REFERENCES euroleague.load_runs(load_run_id),
  source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (competition, season, gamecode),
  CHECK (home_team_id <> away_team_id)
);

CREATE INDEX IF NOT EXISTS euroleague_schedule_season_round_idx
  ON euroleague.schedule (competition, season, round_number, gamecode);

CREATE TABLE IF NOT EXISTS euroleague.source_artifacts (
  source_artifact_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  load_run_id bigint NOT NULL REFERENCES euroleague.load_runs(load_run_id),
  game_id bigint REFERENCES euroleague.schedule(game_id),
  source_key text NOT NULL CHECK (btrim(source_key) <> ''),
  artifact_type text NOT NULL
    CHECK (
      artifact_type IN (
        'schedule', 'play_by_play', 'boxscore', 'shots', 'standings',
        'game_stats', 'team_stats', 'player_stats'
      )
    ),
  package_method text,
  source_endpoint text,
  retrieved_at timestamptz NOT NULL,
  http_status integer CHECK (http_status BETWEEN 100 AND 599),
  row_count integer CHECK (row_count >= 0),
  content_sha256 varchar(64),
  storage_uri text,
  payload jsonb,
  metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
  UNIQUE (load_run_id, source_key),
  CHECK (storage_uri IS NOT NULL OR payload IS NOT NULL),
  CHECK (
    content_sha256 IS NULL
    OR content_sha256 ~ '^[0-9a-fA-F]{64}$'
  )
);

COMMENT ON TABLE euroleague.source_artifacts IS
  'Immutable manifest/payload evidence for package or direct reliability-wrapper retrievals.';

CREATE INDEX IF NOT EXISTS euroleague_source_artifacts_game_idx
  ON euroleague.source_artifacts (game_id, artifact_type, retrieved_at DESC);

CREATE TABLE IF NOT EXISTS euroleague.full_rosters (
  game_id bigint NOT NULL REFERENCES euroleague.schedule(game_id) ON DELETE CASCADE,
  team_id bigint NOT NULL REFERENCES euroleague.teams(team_id),
  player_id bigint NOT NULL REFERENCES euroleague.players(player_id),
  load_run_id bigint NOT NULL REFERENCES euroleague.load_runs(load_run_id),
  source_player_name text NOT NULL,
  jersey_number text,
  is_starter boolean NOT NULL DEFAULT false,
  is_playing boolean,
  raw_minutes text,
  minutes_seconds integer CHECK (minutes_seconds >= 0),
  roster_source text NOT NULL DEFAULT 'boxscore'
    CHECK (roster_source IN ('boxscore', 'pbp_recovered')),
  boxscore_stats jsonb NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (game_id, team_id, player_id)
);

CREATE INDEX IF NOT EXISTS euroleague_full_rosters_player_idx
  ON euroleague.full_rosters (player_id, game_id);

CREATE TABLE IF NOT EXISTS euroleague.team_boxscores (
  game_id bigint NOT NULL REFERENCES euroleague.schedule(game_id) ON DELETE CASCADE,
  team_id bigint NOT NULL REFERENCES euroleague.teams(team_id),
  load_run_id bigint NOT NULL REFERENCES euroleague.load_runs(load_run_id),
  is_home boolean NOT NULL,
  points integer NOT NULL CHECK (points >= 0),
  fg2_made integer NOT NULL CHECK (fg2_made >= 0),
  fg2_attempted integer NOT NULL CHECK (fg2_attempted >= fg2_made),
  fg3_made integer NOT NULL CHECK (fg3_made >= 0),
  fg3_attempted integer NOT NULL CHECK (fg3_attempted >= fg3_made),
  ft_made integer NOT NULL CHECK (ft_made >= 0),
  ft_attempted integer NOT NULL CHECK (ft_attempted >= ft_made),
  offensive_rebounds integer NOT NULL CHECK (offensive_rebounds >= 0),
  defensive_rebounds integer NOT NULL CHECK (defensive_rebounds >= 0),
  assists integer NOT NULL CHECK (assists >= 0),
  steals integer NOT NULL CHECK (steals >= 0),
  turnovers integer NOT NULL CHECK (turnovers >= 0),
  blocks_favour integer NOT NULL CHECK (blocks_favour >= 0),
  blocks_against integer NOT NULL CHECK (blocks_against >= 0),
  fouls_committed integer NOT NULL CHECK (fouls_committed >= 0),
  fouls_received integer NOT NULL CHECK (fouls_received >= 0),
  raw_totals jsonb NOT NULL,
  PRIMARY KEY (game_id, team_id),
  UNIQUE (game_id, is_home)
);

CREATE TABLE IF NOT EXISTS euroleague.actions_raw (
  game_id bigint NOT NULL REFERENCES euroleague.schedule(game_id) ON DELETE CASCADE,
  source_event_order integer NOT NULL CHECK (source_event_order >= 0),
  load_run_id bigint NOT NULL REFERENCES euroleague.load_runs(load_run_id),
  source_artifact_id bigint NOT NULL
    REFERENCES euroleague.source_artifacts(source_artifact_id),
  period smallint NOT NULL CHECK (period >= 1),
  provider_event_type text,
  provider_play_number text,
  team_id bigint REFERENCES euroleague.teams(team_id),
  player_id bigint REFERENCES euroleague.players(player_id),
  provider_team_code text,
  provider_player_id text,
  play_type text,
  player_name text,
  team_name text,
  jersey_number text,
  minute integer,
  marker_time text,
  points_home integer CHECK (points_home >= 0),
  points_away integer CHECK (points_away >= 0),
  comment text,
  play_info text,
  raw_event jsonb NOT NULL,
  PRIMARY KEY (game_id, source_event_order)
);

COMMENT ON COLUMN euroleague.actions_raw.source_event_order IS
  'Package TRUE_NUMBEROFPLAY, namespaced by game; provider play number is retained separately.';

CREATE INDEX IF NOT EXISTS euroleague_actions_raw_period_order_idx
  ON euroleague.actions_raw (game_id, period, source_event_order);

CREATE INDEX IF NOT EXISTS euroleague_actions_raw_team_type_idx
  ON euroleague.actions_raw (team_id, play_type, game_id);

CREATE INDEX IF NOT EXISTS euroleague_actions_raw_player_idx
  ON euroleague.actions_raw (player_id, game_id)
  WHERE player_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS euroleague.actions_clean (
  game_id bigint NOT NULL,
  source_event_order integer NOT NULL,
  synthetic_parent_order integer NOT NULL,
  synthetic_ft_trip_id text,
  final_end_possession boolean NOT NULL,
  endpoint_reason text,
  grouping_status text NOT NULL
    CHECK (grouping_status IN ('confirmed', 'provisional', 'unresolved')),
  grouping_confidence_pct numeric(5,2) NOT NULL
    CHECK (grouping_confidence_pct BETWEEN 0 AND 100),
  decision_trace jsonb NOT NULL DEFAULT '[]'::jsonb,
  parser_version text NOT NULL,
  derived_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (game_id, source_event_order),
  FOREIGN KEY (game_id, source_event_order)
    REFERENCES euroleague.actions_raw(game_id, source_event_order)
    ON DELETE CASCADE,
  FOREIGN KEY (game_id, synthetic_parent_order)
    REFERENCES euroleague.actions_raw(game_id, source_event_order)
    DEFERRABLE INITIALLY DEFERRED,
  CHECK (endpoint_reason IS NOT NULL OR NOT final_end_possession)
);

CREATE INDEX IF NOT EXISTS euroleague_actions_clean_parent_idx
  ON euroleague.actions_clean (game_id, synthetic_parent_order);

CREATE INDEX IF NOT EXISTS euroleague_actions_clean_ft_trip_idx
  ON euroleague.actions_clean (game_id, synthetic_ft_trip_id)
  WHERE synthetic_ft_trip_id IS NOT NULL;

COMMENT ON COLUMN euroleague.actions_clean.synthetic_ft_trip_id IS
  'Internal possession-counting/audit grouping; not a separately published fact table.';

CREATE INDEX IF NOT EXISTS euroleague_actions_clean_endpoint_idx
  ON euroleague.actions_clean (game_id, source_event_order)
  WHERE final_end_possession;

CREATE TABLE IF NOT EXISTS euroleague.possessions (
  game_id bigint NOT NULL REFERENCES euroleague.schedule(game_id) ON DELETE CASCADE,
  game_possession_number integer NOT NULL CHECK (game_possession_number > 0),
  offense_team_id bigint NOT NULL REFERENCES euroleague.teams(team_id),
  team_possession_number integer NOT NULL CHECK (team_possession_number > 0),
  endpoint_source_event_order integer NOT NULL,
  period smallint NOT NULL CHECK (period >= 1),
  endpoint_reason text NOT NULL,
  grouping_status text NOT NULL
    CHECK (grouping_status IN ('confirmed', 'provisional', 'unresolved')),
  grouping_confidence_pct numeric(5,2) NOT NULL
    CHECK (grouping_confidence_pct BETWEEN 0 AND 100),
  parser_version text NOT NULL,
  PRIMARY KEY (game_id, game_possession_number),
  UNIQUE (game_id, offense_team_id, team_possession_number),
  FOREIGN KEY (game_id, endpoint_source_event_order)
    REFERENCES euroleague.actions_clean(game_id, source_event_order)
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS euroleague_possessions_team_idx
  ON euroleague.possessions (offense_team_id, game_id, team_possession_number);

CREATE TABLE IF NOT EXISTS euroleague.lineups (
  lineup_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  game_id bigint NOT NULL REFERENCES euroleague.schedule(game_id) ON DELETE CASCADE,
  team_id bigint NOT NULL REFERENCES euroleague.teams(team_id),
  lineup_hash varchar(64) NOT NULL,
  player_count smallint NOT NULL CHECK (player_count BETWEEN 0 AND 20),
  starter_count smallint NOT NULL CHECK (starter_count BETWEEN 0 AND 5),
  structure_valid boolean NOT NULL,
  source_package_version text NOT NULL,
  load_run_id bigint NOT NULL REFERENCES euroleague.load_runs(load_run_id),
  UNIQUE (game_id, lineup_id),
  UNIQUE (game_id, team_id, lineup_hash),
  CHECK (lineup_hash ~ '^[0-9a-fA-F]{64}$'),
  CHECK (NOT structure_valid OR player_count = 5)
);

CREATE TABLE IF NOT EXISTS euroleague.lineup_players (
  lineup_id bigint NOT NULL REFERENCES euroleague.lineups(lineup_id) ON DELETE CASCADE,
  player_id bigint NOT NULL REFERENCES euroleague.players(player_id),
  package_slot smallint NOT NULL CHECK (package_slot BETWEEN 1 AND 20),
  is_starter boolean NOT NULL,
  PRIMARY KEY (lineup_id, package_slot)
);

COMMENT ON COLUMN euroleague.lineup_players.package_slot IS
  'Preserves package list order for lineage; it has no basketball-position meaning.';

COMMENT ON COLUMN euroleague.lineups.lineup_hash IS
  'SHA-256 of the sorted provider player IDs, including duplicates if present.';

CREATE TABLE IF NOT EXISTS euroleague.action_lineups (
  game_id bigint NOT NULL,
  source_event_order integer NOT NULL,
  home_lineup_id bigint NOT NULL,
  away_lineup_id bigint NOT NULL,
  validate_on_court_player boolean NOT NULL,
  lineup_structure_valid boolean NOT NULL,
  source_package_version text NOT NULL,
  load_run_id bigint NOT NULL REFERENCES euroleague.load_runs(load_run_id),
  PRIMARY KEY (game_id, source_event_order),
  FOREIGN KEY (game_id, source_event_order)
    REFERENCES euroleague.actions_raw(game_id, source_event_order)
    ON DELETE CASCADE,
  FOREIGN KEY (game_id, home_lineup_id)
    REFERENCES euroleague.lineups(game_id, lineup_id),
  FOREIGN KEY (game_id, away_lineup_id)
    REFERENCES euroleague.lineups(game_id, lineup_id),
  CHECK (home_lineup_id <> away_lineup_id)
);

CREATE INDEX IF NOT EXISTS euroleague_action_lineups_home_idx
  ON euroleague.action_lineups (home_lineup_id, game_id, source_event_order);

CREATE INDEX IF NOT EXISTS euroleague_action_lineups_away_idx
  ON euroleague.action_lineups (away_lineup_id, game_id, source_event_order);

CREATE INDEX IF NOT EXISTS euroleague_action_lineups_invalid_idx
  ON euroleague.action_lineups (game_id, source_event_order)
  WHERE NOT validate_on_court_player OR NOT lineup_structure_valid;

CREATE TABLE IF NOT EXISTS euroleague.stints (
  stint_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  game_id bigint NOT NULL REFERENCES euroleague.schedule(game_id) ON DELETE CASCADE,
  team_id bigint NOT NULL REFERENCES euroleague.teams(team_id),
  lineup_id bigint NOT NULL REFERENCES euroleague.lineups(lineup_id),
  stint_number integer NOT NULL CHECK (stint_number > 0),
  start_event_order integer NOT NULL,
  end_event_order_exclusive integer NOT NULL,
  start_elapsed_seconds numeric(8,3) CHECK (start_elapsed_seconds >= 0),
  end_elapsed_seconds numeric(8,3) CHECK (end_elapsed_seconds >= 0),
  duration_seconds numeric(8,3) CHECK (duration_seconds >= 0),
  invalid_actor_rows integer NOT NULL DEFAULT 0 CHECK (invalid_actor_rows >= 0),
  lineup_structure_valid boolean NOT NULL,
  qa_status text NOT NULL DEFAULT 'review'
    CHECK (qa_status IN ('clear', 'review', 'blocked')),
  publishable boolean NOT NULL DEFAULT false,
  UNIQUE (game_id, stint_id),
  UNIQUE (game_id, team_id, stint_number),
  UNIQUE (game_id, team_id, start_event_order),
  FOREIGN KEY (game_id, start_event_order)
    REFERENCES euroleague.actions_raw(game_id, source_event_order),
  CHECK (end_event_order_exclusive > start_event_order),
  CHECK (
    end_elapsed_seconds IS NULL
    OR start_elapsed_seconds IS NULL
    OR end_elapsed_seconds >= start_elapsed_seconds
  ),
  CHECK (NOT publishable OR (lineup_structure_valid AND qa_status = 'clear'))
);

COMMENT ON TABLE euroleague.stints IS
  'Contiguous package-lineup intervals; event boundaries are half-open [start, end).';

CREATE INDEX IF NOT EXISTS euroleague_stints_lineup_idx
  ON euroleague.stints (lineup_id, game_id, stint_number);

CREATE TABLE IF NOT EXISTS euroleague.pws (
  game_id bigint NOT NULL,
  game_possession_number integer NOT NULL,
  offense_lineup_id bigint NOT NULL,
  defense_lineup_id bigint NOT NULL,
  offense_stint_id bigint NOT NULL,
  defense_stint_id bigint NOT NULL,
  num_starters_offense smallint NOT NULL
    CHECK (num_starters_offense BETWEEN 0 AND 5),
  num_starters_defense smallint NOT NULL
    CHECK (num_starters_defense BETWEEN 0 AND 5),
  lineup_validation_clear boolean NOT NULL,
  PRIMARY KEY (game_id, game_possession_number),
  FOREIGN KEY (game_id, game_possession_number)
    REFERENCES euroleague.possessions(game_id, game_possession_number)
    ON DELETE CASCADE,
  FOREIGN KEY (game_id, offense_lineup_id)
    REFERENCES euroleague.lineups(game_id, lineup_id),
  FOREIGN KEY (game_id, defense_lineup_id)
    REFERENCES euroleague.lineups(game_id, lineup_id),
  FOREIGN KEY (game_id, offense_stint_id)
    REFERENCES euroleague.stints(game_id, stint_id),
  FOREIGN KEY (game_id, defense_stint_id)
    REFERENCES euroleague.stints(game_id, stint_id),
  CHECK (offense_lineup_id <> defense_lineup_id),
  CHECK (offense_stint_id <> defense_stint_id)
);

COMMENT ON TABLE euroleague.pws IS
  'Narrow possession-to-lineup/stint bridge; unlike the Israeli table it does not duplicate every action column.';

CREATE INDEX IF NOT EXISTS euroleague_pws_lineups_idx
  ON euroleague.pws (offense_lineup_id, defense_lineup_id, game_id);

CREATE TABLE IF NOT EXISTS euroleague.reconciliation_metrics (
  load_run_id bigint NOT NULL REFERENCES euroleague.load_runs(load_run_id),
  game_id bigint NOT NULL REFERENCES euroleague.schedule(game_id) ON DELETE CASCADE,
  team_id bigint NOT NULL REFERENCES euroleague.teams(team_id),
  metric text NOT NULL,
  pbp_value integer NOT NULL,
  official_value integer NOT NULL,
  difference integer GENERATED ALWAYS AS (pbp_value - official_value) STORED,
  matches boolean GENERATED ALWAYS AS (pbp_value = official_value) STORED,
  PRIMARY KEY (load_run_id, game_id, team_id, metric)
);

CREATE TABLE IF NOT EXISTS euroleague.game_qa (
  load_run_id bigint NOT NULL REFERENCES euroleague.load_runs(load_run_id),
  game_id bigint NOT NULL REFERENCES euroleague.schedule(game_id) ON DELETE CASCADE,
  total_possessions integer NOT NULL CHECK (total_possessions >= 0),
  possession_difference integer CHECK (possession_difference >= 0),
  same_team_transitions integer NOT NULL CHECK (same_team_transitions >= 0),
  provisional_ft_rows integer NOT NULL CHECK (provisional_ft_rows >= 0),
  unresolved_ft_rows integer NOT NULL CHECK (unresolved_ft_rows >= 0),
  duplicate_endpoint_incidents integer NOT NULL
    CHECK (duplicate_endpoint_incidents >= 0),
  missing_parent_targets integer NOT NULL CHECK (missing_parent_targets >= 0),
  possession_structural_status text NOT NULL
    CHECK (possession_structural_status IN ('pass', 'fail')),
  possession_review_status text NOT NULL
    CHECK (possession_review_status IN ('clear', 'review')),
  boxscore_metrics_exact boolean NOT NULL,
  score_progression_exact boolean NOT NULL,
  score_progression_reconciled boolean NOT NULL,
  lineup_structure_valid boolean NOT NULL,
  lineup_invalid_actor_rows integer NOT NULL
    CHECK (lineup_invalid_actor_rows >= 0),
  publication_status text NOT NULL DEFAULT 'blocked'
    CHECK (publication_status IN ('clear', 'review', 'blocked')),
  PRIMARY KEY (load_run_id, game_id),
  CHECK (
    publication_status <> 'clear'
    OR (
      possession_structural_status = 'pass'
      AND boxscore_metrics_exact
      AND score_progression_reconciled
      AND lineup_structure_valid
    )
  )
);

CREATE TABLE IF NOT EXISTS euroleague.qa_incidents (
  qa_incident_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  load_run_id bigint NOT NULL REFERENCES euroleague.load_runs(load_run_id),
  game_id bigint NOT NULL REFERENCES euroleague.schedule(game_id) ON DELETE CASCADE,
  source_event_order integer,
  category text NOT NULL,
  severity text NOT NULL CHECK (severity IN ('info', 'warning', 'error')),
  status text NOT NULL DEFAULT 'open'
    CHECK (status IN ('open', 'accepted', 'resolved')),
  rule_code text NOT NULL,
  summary text NOT NULL,
  details jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz,
  FOREIGN KEY (game_id, source_event_order)
    REFERENCES euroleague.actions_raw(game_id, source_event_order),
  CHECK (resolved_at IS NULL OR resolved_at >= created_at)
);

CREATE INDEX IF NOT EXISTS euroleague_qa_incidents_open_idx
  ON euroleague.qa_incidents (severity, category, game_id)
  WHERE status = 'open';

COMMIT;
