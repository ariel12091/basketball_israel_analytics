-- EuroLeague shadow schema -- migration 010: canonical columnar action fact.
--
-- One row per package PBP event. Every package field is represented by a
-- typed column; possession-parser annotations live beside the source values.
-- The immutable JSON source remains only in actions_raw.

BEGIN;

SET LOCAL search_path TO euroleague, public;

DO $migration$
BEGIN
  IF EXISTS (
    WITH expected(key) AS (
      VALUES
        ('Season'),
        ('Gamecode'),
        ('TYPE'),
        ('NUMBEROFPLAY'),
        ('CODETEAM'),
        ('PLAYER_ID'),
        ('PLAYTYPE'),
        ('PLAYER'),
        ('TEAM'),
        ('DORSAL'),
        ('MINUTE'),
        ('MARKERTIME'),
        ('POINTS_A'),
        ('POINTS_B'),
        ('COMMENT'),
        ('PLAYINFO'),
        ('PERIOD'),
        ('TRUE_NUMBEROFPLAY'),
        ('Lineup_A'),
        ('Lineup_B'),
        ('IsHomeTeam'),
        ('validate_on_court_player')
    ),
    observed AS (
      SELECT DISTINCT key
      FROM euroleague.actions_raw ar
      CROSS JOIN LATERAL jsonb_object_keys(ar.raw_event) AS keys(key)
    ),
    differences AS (
      (SELECT key FROM expected EXCEPT SELECT key FROM observed)
      UNION ALL
      (SELECT key FROM observed EXCEPT SELECT key FROM expected)
    )
    SELECT 1 FROM differences
  ) THEN
    RAISE EXCEPTION
      'actions_raw package fields differ from the canonical actions contract';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM euroleague.actions_raw
    WHERE jsonb_typeof(raw_event -> 'Lineup_A') IS DISTINCT FROM 'array'
       OR jsonb_typeof(raw_event -> 'Lineup_B') IS DISTINCT FROM 'array'
       OR jsonb_array_length(raw_event -> 'Lineup_A') <> 5
       OR jsonb_array_length(raw_event -> 'Lineup_B') <> 5
       OR jsonb_typeof(raw_event -> 'validate_on_court_player')
            IS DISTINCT FROM 'boolean'
  ) THEN
    RAISE EXCEPTION
      'actions_raw contains invalid package lineup fields';
  END IF;
END;
$migration$;

CREATE TABLE IF NOT EXISTS euroleague.actions (
  game_id bigint NOT NULL REFERENCES euroleague.schedule(game_id) ON DELETE CASCADE,
  source_event_order integer NOT NULL CHECK (source_event_order >= 0),
  load_run_id bigint NOT NULL REFERENCES euroleague.load_runs(load_run_id),
  source_artifact_id bigint NOT NULL
    REFERENCES euroleague.source_artifacts(source_artifact_id),

  -- Complete typed package event.
  season integer NOT NULL,
  gamecode integer NOT NULL CHECK (gamecode > 0),
  provider_event_type integer NOT NULL,
  provider_play_number integer NOT NULL,
  provider_team_code text NOT NULL,
  provider_player_id text,
  play_type text NOT NULL,
  player_name text,
  team_name text,
  jersey_number integer,
  minute integer NOT NULL,
  marker_time text,
  points_a integer CHECK (points_a >= 0),
  points_b integer CHECK (points_b >= 0),
  comment text,
  play_info text,
  period smallint NOT NULL CHECK (period >= 1),
  is_home_team boolean,
  lineup_a text[] NOT NULL CHECK (cardinality(lineup_a) = 5),
  lineup_b text[] NOT NULL CHECK (cardinality(lineup_b) = 5),
  validate_on_court_player boolean NOT NULL,

  -- Named roster actors resolve here; pseudo-actors remain provider-only.
  team_id bigint REFERENCES euroleague.teams(team_id),
  player_id bigint REFERENCES euroleague.players(player_id),
  source_package_version text NOT NULL,

  -- Deterministic possession annotations.
  synthetic_parent_order integer NOT NULL,
  synthetic_ft_trip_id text,
  end_possession boolean NOT NULL,
  endpoint_reason text,
  grouping_status text NOT NULL
    CHECK (grouping_status IN ('confirmed', 'provisional', 'unresolved')),
  grouping_confidence_pct numeric(5,2) NOT NULL
    CHECK (grouping_confidence_pct BETWEEN 0 AND 100),
  decision_trace text[] NOT NULL DEFAULT ARRAY[]::text[],
  parser_version text NOT NULL,
  derived_at timestamptz NOT NULL DEFAULT now(),

  -- Populated only on possession-ending events.
  game_possession_number integer,
  possession_offense_team_id bigint REFERENCES euroleague.teams(team_id),
  team_possession_number integer,

  PRIMARY KEY (game_id, source_event_order),
  FOREIGN KEY (game_id, source_event_order)
    REFERENCES euroleague.actions_raw(game_id, source_event_order)
    ON DELETE CASCADE,
  FOREIGN KEY (game_id, synthetic_parent_order)
    REFERENCES euroleague.actions(game_id, source_event_order)
    DEFERRABLE INITIALLY DEFERRED,
  UNIQUE (game_id, game_possession_number),
  UNIQUE (game_id, possession_offense_team_id, team_possession_number),
  CHECK (array_position(lineup_a, NULL::text) IS NULL),
  CHECK (array_position(lineup_b, NULL::text) IS NULL),
  CHECK (
    (end_possession AND endpoint_reason IS NOT NULL
      AND game_possession_number > 0
      AND possession_offense_team_id IS NOT NULL
      AND team_possession_number > 0)
    OR
    (NOT end_possession AND endpoint_reason IS NULL
      AND game_possession_number IS NULL
      AND possession_offense_team_id IS NULL
      AND team_possession_number IS NULL)
  )
);

COMMENT ON TABLE euroleague.actions IS
  'Canonical one-row-per-package-event fact with package lineups and possession annotations.';

COMMENT ON COLUMN euroleague.actions.lineup_a IS
  'Package Lineup_A: the five home-team players on court in package order.';

COMMENT ON COLUMN euroleague.actions.lineup_b IS
  'Package Lineup_B: the five away-team players on court in package order.';

CREATE INDEX IF NOT EXISTS euroleague_actions_endpoint_idx
  ON euroleague.actions (game_id, game_possession_number)
  WHERE end_possession;

CREATE INDEX IF NOT EXISTS euroleague_actions_team_type_idx
  ON euroleague.actions (team_id, play_type, game_id);

CREATE INDEX IF NOT EXISTS euroleague_actions_player_idx
  ON euroleague.actions (player_id, game_id)
  WHERE player_id IS NOT NULL;

INSERT INTO euroleague.actions (
  game_id,
  source_event_order,
  load_run_id,
  source_artifact_id,
  season,
  gamecode,
  provider_event_type,
  provider_play_number,
  provider_team_code,
  provider_player_id,
  play_type,
  player_name,
  team_name,
  jersey_number,
  minute,
  marker_time,
  points_a,
  points_b,
  comment,
  play_info,
  period,
  is_home_team,
  lineup_a,
  lineup_b,
  validate_on_court_player,
  team_id,
  player_id,
  source_package_version,
  synthetic_parent_order,
  synthetic_ft_trip_id,
  end_possession,
  endpoint_reason,
  grouping_status,
  grouping_confidence_pct,
  decision_trace,
  parser_version,
  derived_at,
  game_possession_number,
  possession_offense_team_id,
  team_possession_number
)
SELECT
  ar.game_id,
  ar.source_event_order,
  ar.load_run_id,
  ar.source_artifact_id,
  (ar.raw_event ->> 'Season')::numeric::integer,
  (ar.raw_event ->> 'Gamecode')::numeric::integer,
  (ar.raw_event ->> 'TYPE')::numeric::integer,
  (ar.raw_event ->> 'NUMBEROFPLAY')::numeric::integer,
  ar.raw_event ->> 'CODETEAM',
  ar.raw_event ->> 'PLAYER_ID',
  ar.raw_event ->> 'PLAYTYPE',
  ar.raw_event ->> 'PLAYER',
  ar.raw_event ->> 'TEAM',
  (ar.raw_event ->> 'DORSAL')::numeric::integer,
  (ar.raw_event ->> 'MINUTE')::numeric::integer,
  ar.raw_event ->> 'MARKERTIME',
  (ar.raw_event ->> 'POINTS_A')::numeric::integer,
  (ar.raw_event ->> 'POINTS_B')::numeric::integer,
  ar.raw_event ->> 'COMMENT',
  ar.raw_event ->> 'PLAYINFO',
  (ar.raw_event ->> 'PERIOD')::numeric::smallint,
  (ar.raw_event ->> 'IsHomeTeam')::boolean,
  ARRAY(
    SELECT value
    FROM jsonb_array_elements_text(ar.raw_event -> 'Lineup_A')
      WITH ORDINALITY AS lineup(value, package_slot)
    ORDER BY package_slot
  ),
  ARRAY(
    SELECT value
    FROM jsonb_array_elements_text(ar.raw_event -> 'Lineup_B')
      WITH ORDINALITY AS lineup(value, package_slot)
    ORDER BY package_slot
  ),
  (ar.raw_event ->> 'validate_on_court_player')::boolean,
  ar.team_id,
  ar.player_id,
  lr.package_version,
  ac.synthetic_parent_order,
  ac.synthetic_ft_trip_id,
  ac.final_end_possession,
  ac.endpoint_reason,
  ac.grouping_status,
  ac.grouping_confidence_pct,
  ARRAY(
    SELECT value
    FROM jsonb_array_elements_text(ac.decision_trace)
      WITH ORDINALITY AS trace(value, trace_order)
    ORDER BY trace_order
  ),
  ac.parser_version,
  ac.derived_at,
  p.game_possession_number,
  p.offense_team_id,
  p.team_possession_number
FROM euroleague.actions_raw ar
JOIN euroleague.actions_clean ac
  ON ac.game_id = ar.game_id
 AND ac.source_event_order = ar.source_event_order
JOIN euroleague.load_runs lr ON lr.load_run_id = ar.load_run_id
LEFT JOIN euroleague.possessions p
  ON p.game_id = ar.game_id
 AND p.endpoint_source_event_order = ar.source_event_order
ON CONFLICT (game_id, source_event_order) DO NOTHING;

DO $migration$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM euroleague.actions_raw ar
    FULL JOIN euroleague.actions a
      ON a.game_id = ar.game_id
     AND a.source_event_order = ar.source_event_order
    WHERE ar.game_id IS NULL OR a.game_id IS NULL
  ) THEN
    RAISE EXCEPTION 'canonical actions event keys differ from actions_raw';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM euroleague.actions a
    JOIN euroleague.actions_raw ar
      ON ar.game_id = a.game_id
     AND ar.source_event_order = a.source_event_order
    WHERE a.season::numeric IS DISTINCT FROM (ar.raw_event ->> 'Season')::numeric
       OR a.gamecode::numeric IS DISTINCT FROM (ar.raw_event ->> 'Gamecode')::numeric
       OR a.provider_event_type::numeric IS DISTINCT FROM (ar.raw_event ->> 'TYPE')::numeric
       OR a.provider_play_number::numeric IS DISTINCT FROM (ar.raw_event ->> 'NUMBEROFPLAY')::numeric
       OR a.provider_team_code IS DISTINCT FROM ar.raw_event ->> 'CODETEAM'
       OR a.provider_player_id IS DISTINCT FROM ar.raw_event ->> 'PLAYER_ID'
       OR a.play_type IS DISTINCT FROM ar.raw_event ->> 'PLAYTYPE'
       OR a.player_name IS DISTINCT FROM ar.raw_event ->> 'PLAYER'
       OR a.team_name IS DISTINCT FROM ar.raw_event ->> 'TEAM'
       OR a.jersey_number::numeric IS DISTINCT FROM (ar.raw_event ->> 'DORSAL')::numeric
       OR a.minute::numeric IS DISTINCT FROM (ar.raw_event ->> 'MINUTE')::numeric
       OR a.marker_time IS DISTINCT FROM ar.raw_event ->> 'MARKERTIME'
       OR a.points_a::numeric IS DISTINCT FROM (ar.raw_event ->> 'POINTS_A')::numeric
       OR a.points_b::numeric IS DISTINCT FROM (ar.raw_event ->> 'POINTS_B')::numeric
       OR a.comment IS DISTINCT FROM ar.raw_event ->> 'COMMENT'
       OR a.play_info IS DISTINCT FROM ar.raw_event ->> 'PLAYINFO'
       OR a.period::numeric IS DISTINCT FROM (ar.raw_event ->> 'PERIOD')::numeric
       OR a.source_event_order::numeric
            IS DISTINCT FROM (ar.raw_event ->> 'TRUE_NUMBEROFPLAY')::numeric
       OR to_jsonb(a.lineup_a) IS DISTINCT FROM ar.raw_event -> 'Lineup_A'
       OR to_jsonb(a.lineup_b) IS DISTINCT FROM ar.raw_event -> 'Lineup_B'
       OR a.is_home_team::text IS DISTINCT FROM ar.raw_event ->> 'IsHomeTeam'
       OR a.validate_on_court_player::text
            IS DISTINCT FROM ar.raw_event ->> 'validate_on_court_player'
  ) THEN
    RAISE EXCEPTION
      'canonical actions columns disagree with the complete package event';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM euroleague.actions a
    JOIN euroleague.actions_clean ac
      ON ac.game_id = a.game_id
     AND ac.source_event_order = a.source_event_order
    LEFT JOIN euroleague.possessions p
      ON p.game_id = a.game_id
     AND p.endpoint_source_event_order = a.source_event_order
    WHERE a.synthetic_parent_order IS DISTINCT FROM ac.synthetic_parent_order
       OR a.synthetic_ft_trip_id IS DISTINCT FROM ac.synthetic_ft_trip_id
       OR a.end_possession IS DISTINCT FROM ac.final_end_possession
       OR a.endpoint_reason IS DISTINCT FROM ac.endpoint_reason
       OR a.grouping_status IS DISTINCT FROM ac.grouping_status
       OR a.grouping_confidence_pct IS DISTINCT FROM ac.grouping_confidence_pct
       OR a.parser_version IS DISTINCT FROM ac.parser_version
       OR a.end_possession IS DISTINCT FROM (p.game_id IS NOT NULL)
       OR a.game_possession_number IS DISTINCT FROM p.game_possession_number
       OR a.possession_offense_team_id IS DISTINCT FROM p.offense_team_id
       OR a.team_possession_number IS DISTINCT FROM p.team_possession_number
  ) THEN
    RAISE EXCEPTION
      'canonical actions possession annotations disagree with derived facts';
  END IF;
END;
$migration$;

COMMIT;
