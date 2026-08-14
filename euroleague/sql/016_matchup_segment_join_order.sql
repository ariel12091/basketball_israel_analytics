-- EuroLeague shadow schema -- migration 016: exact matchup-side expansion.
--
-- Every canonical action already contains the exact home and away lineups.
-- Materialize that pair and expand it to two team perspectives before joining
-- starter metadata. This prevents PostgreSQL from enumerating every own/opp
-- lineup candidate within the game and filtering the cross product afterward.

BEGIN;

SET LOCAL search_path TO euroleague, public;

DO $migration$
DECLARE
  definition text;
  start_at integer;
  end_at integer;
  replacement text := $replacement$  event_lineups AS MATERIALIZED (
    SELECT
      a.game_id,
      a.source_event_order,
      ec.event_elapsed_seconds,
      ge.game_end_elapsed_seconds,
      tg.home_team_id,
      tg.away_team_id,
      ARRAY(SELECT x FROM unnest(a.lineup_a) x ORDER BY x) AS lineup_a,
      ARRAY(SELECT x FROM unnest(a.lineup_b) x ORDER BY x) AS lineup_b,
      tg.last_seen_load_run_id
    FROM euroleague.actions a
    JOIN target_games tg ON tg.game_id = a.game_id
    JOIN event_clock ec
      ON ec.game_id = a.game_id
     AND ec.source_event_order = a.source_event_order
    JOIN game_ends ge ON ge.game_id = a.game_id
  ),
  event_sides AS MATERIALIZED (
    SELECT
      el.game_id, el.source_event_order,
      el.event_elapsed_seconds, el.game_end_elapsed_seconds,
      el.home_team_id AS team_id, el.away_team_id AS opponent_team_id,
      el.lineup_a AS own_lineup, el.lineup_b AS opp_lineup,
      el.last_seen_load_run_id
    FROM event_lineups el
    UNION ALL
    SELECT
      el.game_id, el.source_event_order,
      el.event_elapsed_seconds, el.game_end_elapsed_seconds,
      el.away_team_id, el.home_team_id,
      el.lineup_b, el.lineup_a,
      el.last_seen_load_run_id
    FROM event_lineups el
  ),
  lineup_sided AS MATERIALIZED (
    SELECT
      es.game_id,
      es.source_event_order,
      es.event_elapsed_seconds,
      es.game_end_elapsed_seconds,
      es.team_id,
      es.own_lineup,
      es.opp_lineup,
      own_count.starters AS own_starters,
      opp_count.starters AS opp_starters,
      es.last_seen_load_run_id
    FROM event_sides es
    JOIN starter_counts own_count
      ON own_count.game_id = es.game_id
     AND own_count.team_id = es.team_id
     AND own_count.lineup = es.own_lineup
    JOIN starter_counts opp_count
      ON opp_count.game_id = es.game_id
     AND opp_count.team_id = es.opponent_team_id
     AND opp_count.lineup = es.opp_lineup
  ),
$replacement$;
BEGIN
  SELECT pg_get_functiondef(p.oid)
    INTO definition
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
   WHERE n.nspname = 'euroleague'
     AND p.proname = 'refresh_actions_consumer_candidates'
     AND pg_get_function_identity_arguments(p.oid) = 'game_ids bigint[]';

  IF definition IS NULL THEN
    RAISE EXCEPTION 'refresh_actions_consumer_candidates(bigint[]) is missing';
  END IF;

  IF strpos(definition, 'event_sides AS MATERIALIZED') > 0 THEN
    RETURN;
  END IF;

  start_at := strpos(definition, E'  lineup_sided AS MATERIALIZED (\n');
  end_at := strpos(definition, E'  lineup_lagged AS (\n');
  IF start_at = 0 OR end_at = 0 OR end_at <= start_at THEN
    RAISE EXCEPTION 'migration 016 could not locate the reviewed lineup block';
  END IF;
  IF strpos(substring(definition FROM start_at FOR end_at - start_at),
            'CROSS JOIN LATERAL') = 0 THEN
    RAISE EXCEPTION 'migration 016 found an unexpected lineup block';
  END IF;

  definition := left(definition, start_at - 1)
                || replacement
                || substring(definition FROM end_at);
  EXECUTE definition;
END
$migration$;

COMMIT;
