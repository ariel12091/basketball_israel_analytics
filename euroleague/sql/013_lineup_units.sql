-- EuroLeague shadow schema -- migration 013: 2-5 player lineup units.
--
-- Mimics the Israeli sub_lineups shape: a fact at five-player-lineup grain per
-- game, plus a metric-free season mapping that expands each lineup into its 26
-- sub-units. The 26x expansion multiplies distinct lineups per season, never
-- team-game-segments. Measured on the 84 loaded games: 17,144 fact rows and
-- 84,058 mapping rows.
--
-- Nothing is written back to action_team_context_actions or
-- matchup_segments_actions. They keep their provider-name arrays untouched.

BEGIN;

SET LOCAL search_path TO euroleague, public;

CREATE TABLE IF NOT EXISTS euroleague.lineup_totals_by_game (
  game_id            bigint   NOT NULL REFERENCES euroleague.schedule(game_id)
                                ON DELETE CASCADE,
  team_id            bigint   NOT NULL REFERENCES euroleague.teams(team_id),
  lineup_key         text     NOT NULL,
  type_lineup        text     NOT NULL CHECK (type_lineup IN ('offense', 'defense')),
  opp_starters       smallint NOT NULL CHECK (opp_starters BETWEEN 0 AND 5),

  competition        text     NOT NULL,
  game_year          integer  NOT NULL,
  own_starters       smallint NOT NULL CHECK (own_starters BETWEEN 0 AND 5),
  own_lineup         text[]   NOT NULL CHECK (cardinality(own_lineup) = 5),
  player_ids         bigint[] NOT NULL CHECK (cardinality(player_ids) = 5),

  possessions        integer  NOT NULL DEFAULT 0,
  points             integer  NOT NULL DEFAULT 0,
  fg2_made           integer  NOT NULL DEFAULT 0,
  fg2_att            integer  NOT NULL DEFAULT 0,
  fg3_made           integer  NOT NULL DEFAULT 0,
  fg3_att            integer  NOT NULL DEFAULT 0,
  ts_possessions     integer  NOT NULL DEFAULT 0,
  fgm                integer  NOT NULL DEFAULT 0,
  fga                integer  NOT NULL DEFAULT 0,
  ft_attempts        integer  NOT NULL DEFAULT 0,
  orebounds          integer  NOT NULL DEFAULT 0,
  oreb_opportunities integer  NOT NULL DEFAULT 0,
  turnovers          integer  NOT NULL DEFAULT 0,
  steals             integer  NOT NULL DEFAULT 0,
  seconds            numeric  CHECK (seconds IS NULL OR seconds >= 0),

  load_run_id        bigint REFERENCES euroleague.load_runs(load_run_id),
  derivation_version text NOT NULL,
  derived_at         timestamptz NOT NULL DEFAULT now(),

  PRIMARY KEY (game_id, team_id, lineup_key, type_lineup, opp_starters),
  -- Floor time lives on offense rows only, so a naive SUM across both
  -- contexts cannot double-count minutes. This makes that a schema
  -- guarantee rather than a convention a later query might forget.
  CHECK ((type_lineup = 'offense') = (seconds IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS euroleague_lineup_totals_by_game_season_idx
  ON euroleague.lineup_totals_by_game
     (competition, game_year, team_id, lineup_key, type_lineup);

-- Identity and mapping only. A unit maps to MANY lineups; that many-to-many is
-- what makes a 2-player unit answerable at all.
--
-- Grain difference from the Israeli relation of the same name: that one holds
-- sizes 2-4 and synthesizes size 5 from the full lineup hash elsewhere. This
-- one holds 2-5 uniformly, and because unit_key and lineup_key use the same
-- md5 construction, unit_key = lineup_key at size 5 automatically.
CREATE TABLE IF NOT EXISTS euroleague.sub_lineups (
  competition  text     NOT NULL,
  game_year    integer  NOT NULL,
  team_id      bigint   NOT NULL REFERENCES euroleague.teams(team_id),
  lineup_key   text     NOT NULL,
  unit_key     text     NOT NULL,
  unit_size    smallint NOT NULL CHECK (unit_size BETWEEN 2 AND 5),
  player_ids   bigint[] NOT NULL CHECK (cardinality(player_ids) = unit_size),
  created_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (competition, game_year, team_id, lineup_key, unit_key)
);

-- The join direction the read layer uses: unit_key -> its lineups.
CREATE INDEX IF NOT EXISTS euroleague_sub_lineups_unit_idx
  ON euroleague.sub_lineups
     (competition, game_year, team_id, unit_key, unit_size);

-- ---------------------------------------------------------------------------
-- refresh_lineup_totals_by_game
-- ---------------------------------------------------------------------------
--
-- Two aggregations plus one identity join. Both aggregations group on
-- own_lineup, which is already present and already sorted on every source row,
-- so name resolution runs once per distinct lineup per game (3,233 across the
-- 84 loaded games) instead of once per event (95,216).
--
-- The row set is driven from SEGMENTS, not from events, with the event counts
-- left-joined on. The Israeli original builds from its event fact and
-- therefore loses a segment's minutes when every event in it has a NULL
-- context. Here a lineup that was on court but recorded no offensive or
-- defensive event still gets its row and its seconds, with zero counts.
--
-- If two distinct own_lineup arrays in one game resolved to the same
-- lineup_key -- which can only happen when a lineup name is missing from
-- full_rosters -- this INSERT violates the primary key and the load fails
-- closed. That is deliberate. Gate G1 diagnoses the cause.

CREATE OR REPLACE FUNCTION euroleague.refresh_lineup_totals_by_game(
  game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.lineup_totals_by_game;
  ELSE
    DELETE FROM euroleague.lineup_totals_by_game WHERE game_id = ANY(game_ids);
  END IF;

  INSERT INTO euroleague.lineup_totals_by_game (
    game_id, team_id, lineup_key, type_lineup, opp_starters,
    competition, game_year, own_starters, own_lineup, player_ids,
    possessions, points, fg2_made, fg2_att, fg3_made, fg3_att,
    ts_possessions, fgm, fga, ft_attempts,
    orebounds, oreb_opportunities, turnovers, steals, seconds,
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
  seg AS (
    SELECT
      ms.game_id,
      ms.team_id,
      ms.own_lineup,
      ms.opp_starters,
      max(ms.own_starters)    AS own_starters,
      sum(ms.segment_seconds) AS seconds
    FROM euroleague.matchup_segments_actions ms
    WHERE game_ids IS NULL OR ms.game_id = ANY(game_ids)
    GROUP BY ms.game_id, ms.team_id, ms.own_lineup, ms.opp_starters
  ),
  distinct_lineups AS (
    SELECT DISTINCT s.game_id, s.team_id, s.own_lineup FROM seg s
  ),
  keyed AS (
    SELECT
      d.game_id,
      d.team_id,
      d.own_lineup,
      ids.player_ids,
      md5(array_to_string(ids.player_ids, '_')) AS lineup_key
    FROM distinct_lineups d
    CROSS JOIN LATERAL (
      SELECT ARRAY(
        SELECT rr.player_id
          FROM real_roster rr
         WHERE rr.game_id = d.game_id
           AND rr.team_id = d.team_id
           AND rr.source_player_name = ANY(d.own_lineup)
         ORDER BY rr.player_id
      ) AS player_ids
    ) ids
  ),
  counts AS (
    SELECT
      atc.game_id,
      atc.team_id,
      atc.own_lineup,
      atc.type_lineup,
      atc.opp_starters,
      sum(atc.possession_flag)::integer    AS possessions,
      sum(atc.points)::integer             AS points,
      sum(atc.fg2_made)::integer           AS fg2_made,
      sum(atc.fg2_att)::integer            AS fg2_att,
      sum(atc.fg3_made)::integer           AS fg3_made,
      sum(atc.fg3_att)::integer            AS fg3_att,
      sum(atc.ts_possessions)::integer     AS ts_possessions,
      sum(atc.fgm)::integer                AS fgm,
      sum(atc.fga)::integer                AS fga,
      sum(atc.ft_attempts)::integer        AS ft_attempts,
      sum(atc.orebounds)::integer          AS orebounds,
      sum(atc.oreb_opportunities)::integer AS oreb_opportunities,
      sum(atc.turnovers)::integer          AS turnovers,
      sum(atc.steals)::integer             AS steals
    FROM euroleague.action_team_context_actions atc
    WHERE (game_ids IS NULL OR atc.game_id = ANY(game_ids))
      AND atc.type_lineup IS NOT NULL
    GROUP BY atc.game_id, atc.team_id, atc.own_lineup,
             atc.type_lineup, atc.opp_starters
  ),
  game_run AS (
    SELECT a.game_id, max(a.load_run_id) AS load_run_id
      FROM euroleague.actions a
     WHERE game_ids IS NULL OR a.game_id = ANY(game_ids)
     GROUP BY a.game_id
  )
  SELECT
    seg.game_id,
    seg.team_id,
    k.lineup_key,
    side.type_lineup,
    seg.opp_starters,
    sch.competition,
    sch.season,
    seg.own_starters,
    seg.own_lineup,
    k.player_ids,
    coalesce(c.possessions, 0),
    coalesce(c.points, 0),
    coalesce(c.fg2_made, 0),
    coalesce(c.fg2_att, 0),
    coalesce(c.fg3_made, 0),
    coalesce(c.fg3_att, 0),
    coalesce(c.ts_possessions, 0),
    coalesce(c.fgm, 0),
    coalesce(c.fga, 0),
    coalesce(c.ft_attempts, 0),
    coalesce(c.orebounds, 0),
    coalesce(c.oreb_opportunities, 0),
    coalesce(c.turnovers, 0),
    coalesce(c.steals, 0),
    CASE WHEN side.type_lineup = 'offense' THEN seg.seconds END,
    gr.load_run_id,
    'units-v1'
  FROM seg
  JOIN keyed k
    ON k.game_id = seg.game_id
   AND k.team_id = seg.team_id
   AND k.own_lineup = seg.own_lineup
  JOIN euroleague.schedule sch ON sch.game_id = seg.game_id
  LEFT JOIN game_run gr ON gr.game_id = seg.game_id
  CROSS JOIN (VALUES ('offense'), ('defense')) AS side(type_lineup)
  LEFT JOIN counts c
    ON c.game_id = seg.game_id
   AND c.team_id = seg.team_id
   AND c.own_lineup = seg.own_lineup
   AND c.opp_starters = seg.opp_starters
   AND c.type_lineup = side.type_lineup;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

-- ---------------------------------------------------------------------------
-- refresh_sub_lineups
-- ---------------------------------------------------------------------------
--
-- A deterministic index expansion over the five sorted resolved IDs, driven by
-- a static 26-row VALUES list: 10 pairs, 10 triples, 5 quads, 1 quintet. Never
-- a cross join against a roster.
--
-- Reads lineup_totals_by_game, so this performs no name resolution at all --
-- resolution happened once, upstream. Rows are additive identity and are never
-- deleted per game: a lineup observed in an earlier game of the same season
-- stays mapped.
--
-- player_ids is already ascending and idxs is ascending, so each subset is
-- produced in sorted order and unit_key is order-independent by construction:
-- {A,B} and {B,A} cannot both exist.

CREATE OR REPLACE FUNCTION euroleague.refresh_sub_lineups(
  game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  INSERT INTO euroleague.sub_lineups (
    competition, game_year, team_id, lineup_key, unit_key, unit_size, player_ids
  )
  WITH masks(unit_size, idxs) AS (
    VALUES
      (2::smallint, ARRAY[1,2]), (2::smallint, ARRAY[1,3]),
      (2::smallint, ARRAY[1,4]), (2::smallint, ARRAY[1,5]),
      (2::smallint, ARRAY[2,3]), (2::smallint, ARRAY[2,4]),
      (2::smallint, ARRAY[2,5]), (2::smallint, ARRAY[3,4]),
      (2::smallint, ARRAY[3,5]), (2::smallint, ARRAY[4,5]),
      (3::smallint, ARRAY[1,2,3]), (3::smallint, ARRAY[1,2,4]),
      (3::smallint, ARRAY[1,2,5]), (3::smallint, ARRAY[1,3,4]),
      (3::smallint, ARRAY[1,3,5]), (3::smallint, ARRAY[1,4,5]),
      (3::smallint, ARRAY[2,3,4]), (3::smallint, ARRAY[2,3,5]),
      (3::smallint, ARRAY[2,4,5]), (3::smallint, ARRAY[3,4,5]),
      (4::smallint, ARRAY[1,2,3,4]), (4::smallint, ARRAY[1,2,3,5]),
      (4::smallint, ARRAY[1,2,4,5]), (4::smallint, ARRAY[1,3,4,5]),
      (4::smallint, ARRAY[2,3,4,5]),
      (5::smallint, ARRAY[1,2,3,4,5])
  )
  SELECT DISTINCT
    l.competition,
    l.game_year,
    l.team_id,
    l.lineup_key,
    md5(array_to_string(u.ids, '_')) AS unit_key,
    m.unit_size,
    u.ids
  FROM euroleague.lineup_totals_by_game l
  CROSS JOIN masks m
  CROSS JOIN LATERAL (
    SELECT ARRAY(
      SELECT l.player_ids[i] FROM unnest(m.idxs) AS i ORDER BY 1
    ) AS ids
  ) u
  WHERE game_ids IS NULL OR l.game_id = ANY(game_ids)
  ON CONFLICT DO NOTHING;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

GRANT SELECT ON
  euroleague.lineup_totals_by_game,
  euroleague.sub_lineups
TO app_readonly;

COMMIT;
