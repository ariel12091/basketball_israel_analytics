-- Harden database access for the Shiny application.
--
-- Run this as the owner of objects in the target schemas. The script:
--   * removes effective access for PUBLIC, anon, and authenticated;
--   * preserves service_role access for Supabase administration;
--   * keeps app_readonly read-only access to relations;
--   * grants app_readonly EXECUTE only on functions called by the app;
--   * enables RLS on base tables with a policy only for app_readonly.
--
-- Two relation-grant modes exist, selected per schema:
--
--   * blanket (Israeli schemas) -- app_readonly gets SELECT on every relation,
--     because the current SQL functions are SECURITY INVOKER and read
--     underlying relations as the caller. Reducing relation access further
--     requires a separately reviewed SECURITY DEFINER API.
--   * curated (euroleague) -- app_readonly gets SELECT only on the enumerated
--     read layer. The shadow schema keeps raw provider evidence
--     (actions_raw, source_artifacts), load bookkeeping (game_qa,
--     qa_incidents, reconciliation_metrics) and the large derived facts
--     closed to the app, and that boundary must survive this script.
--
-- service_role is granted only where Supabase already administers the schema.
-- euroleague is a private shadow schema that is deliberately absent from
-- Supabase's Data-API exposed schemas, so it gets no browser-facing or
-- Supabase-managed role access at all.

DO $$
DECLARE
  v_schema text;
  v_table text;
  v_role text;
  v_relation text;
  v_function_signature text;
  v_curated_relations boolean;
  v_grant_service_role boolean;
  v_function_names text[];
  has_service_role boolean;
  has_app_readonly boolean;
  target_schemas constant text[] := ARRAY[
    'basketball_test',
    'basketball',
    'euroleague'
  ];
  app_function_names constant text[] := ARRAY[
    'onoff_compute',
    'four_factors_compute',
    'four_factors_dashboard_compute',
    'fetch_lineups_csv_v2',
    'fetch_lineups_all',
    'fetch_lineups_four_factors_csv',
    'fetch_lineups_four_factors',
    'get_team_ratings_dynamic',
    'get_team_four_factors_dynamic',
    'get_team_four_factors_dashboard_dynamic',
    'get_player_traditional_dynamic',
    'get_player_traditional_from_games',
    'get_player_traditional_custom_clutch'
  ];
  -- EuroLeague app API. The refresh_* functions are publication machinery and
  -- are deliberately absent: they mutate the schema and must never be callable
  -- by the app role. effective_period and person_display_name are helpers
  -- inlined into app-facing SECURITY INVOKER functions and views, so the
  -- caller needs EXECUTE on them.
  euro_function_names constant text[] := ARRAY[
    'onoff_compute',
    'four_factors_compute',
    'four_factors_dashboard_compute',
    'fetch_lineups_dynamic',
    'fetch_lineups_direct',
    'fetch_lineups_pergame',
    'get_team_ratings_dynamic',
    'get_team_ratings_direct',
    'get_team_ratings_pergame',
    'get_team_four_factors_dynamic',
    'get_team_four_factors_direct',
    'get_team_four_factors_pergame',
    'get_team_minutes_dynamic',
    'get_team_minutes_direct',
    'get_team_minutes_pergame',
    'get_team_dashboard_dynamic',
    'get_player_traditional_dynamic',
    'get_player_traditional_pergame',
    'get_player_traditional_clutch',
    'get_player_traditional_custom_clutch',
    'get_player_traditional_standard_clutch',
    'effective_period',
    'person_display_name'
  ];
  -- EuroLeague read layer, in the order the migrations granted it
  -- (004, 005/006/012, 013, 014).
  euro_app_relations constant text[] := ARRAY[
    'schedule',
    'teams',
    'players',
    'full_rosters',
    'player_four_factors_by_game',
    'final_schedule',
    'final_schedule_mv',
    'player_game_context',
    'player_onoff_default_mv',
    'player_advanced_stats_mv',
    'player_traditional_stats_mv',
    'default_clutch_player_totals_by_game',
    'load_runs',
    'team_game_ratings_mv',
    'team_ppp_ratings_mv',
    'team_four_factors_by_game',
    'team_four_factors_mv',
    'lineup_totals_by_game',
    'sub_lineups',
    'sub_lineups_stats_mv'
  ];
BEGIN
  SELECT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'service_role'
  ) INTO has_service_role;

  SELECT EXISTS (
    SELECT 1 FROM pg_roles WHERE rolname = 'app_readonly'
  ) INTO has_app_readonly;

  IF NOT has_app_readonly THEN
    RAISE EXCEPTION 'Required role app_readonly does not exist';
  END IF;

  FOR v_schema IN
    SELECT nspname
    FROM pg_namespace
    WHERE nspname = ANY(target_schemas)
  LOOP
    v_curated_relations := v_schema = 'euroleague';
    v_grant_service_role := has_service_role AND v_schema <> 'euroleague';
    v_function_names := CASE
      WHEN v_schema = 'euroleague' THEN euro_function_names
      ELSE app_function_names
    END;

    -- PUBLIC privileges are inherited by every role. Remove them first.
    EXECUTE format('REVOKE ALL PRIVILEGES ON SCHEMA %I FROM PUBLIC', v_schema);
    EXECUTE format('REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I FROM PUBLIC', v_schema);
    EXECUTE format('REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I FROM PUBLIC', v_schema);
    EXECUTE format('REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA %I FROM PUBLIC', v_schema);
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES IN SCHEMA %I REVOKE ALL PRIVILEGES ON TABLES FROM PUBLIC',
      v_schema
    );
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES IN SCHEMA %I REVOKE ALL PRIVILEGES ON SEQUENCES FROM PUBLIC',
      v_schema
    );
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES IN SCHEMA %I REVOKE ALL PRIVILEGES ON FUNCTIONS FROM PUBLIC',
      v_schema
    );

    -- The Shiny app connects directly as app_readonly. Browser-facing
    -- Supabase roles must not access these private analytics schemas.
    FOREACH v_role IN ARRAY ARRAY['anon', 'authenticated']
    LOOP
      IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_role) THEN
        EXECUTE format('REVOKE ALL PRIVILEGES ON SCHEMA %I FROM %I', v_schema, v_role);
        EXECUTE format(
          'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I FROM %I',
          v_schema,
          v_role
        );
        EXECUTE format(
          'REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I FROM %I',
          v_schema,
          v_role
        );
        EXECUTE format(
          'REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA %I FROM %I',
          v_schema,
          v_role
        );
        EXECUTE format(
          'ALTER DEFAULT PRIVILEGES IN SCHEMA %I REVOKE ALL PRIVILEGES ON TABLES FROM %I',
          v_schema,
          v_role
        );
        EXECUTE format(
          'ALTER DEFAULT PRIVILEGES IN SCHEMA %I REVOKE ALL PRIVILEGES ON SEQUENCES FROM %I',
          v_schema,
          v_role
        );
        EXECUTE format(
          'ALTER DEFAULT PRIVILEGES IN SCHEMA %I REVOKE ALL PRIVILEGES ON FUNCTIONS FROM %I',
          v_schema,
          v_role
        );
      END IF;
    END LOOP;

    -- Preserve Supabase administrative access explicitly after removing
    -- PUBLIC privileges. The shadow schema is excluded on purpose.
    IF v_grant_service_role THEN
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO service_role', v_schema);
      EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO service_role', v_schema);
      EXECUTE format('GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA %I TO service_role', v_schema);
      EXECUTE format(
        'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO service_role',
        v_schema
      );
      EXECUTE format(
        'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT EXECUTE ON FUNCTIONS TO service_role',
        v_schema
      );
    ELSIF has_service_role THEN
      EXECUTE format('REVOKE ALL PRIVILEGES ON SCHEMA %I FROM service_role', v_schema);
      EXECUTE format(
        'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I FROM service_role',
        v_schema
      );
      EXECUTE format(
        'REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA %I FROM service_role',
        v_schema
      );
    END IF;

    -- The application role can read but cannot create or mutate objects.
    EXECUTE format('REVOKE CREATE ON SCHEMA %I FROM app_readonly', v_schema);
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO app_readonly', v_schema);
    EXECUTE format('REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I FROM app_readonly', v_schema);
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES IN SCHEMA %I REVOKE ALL PRIVILEGES ON SEQUENCES FROM app_readonly',
      v_schema
    );

    IF v_curated_relations THEN
      -- Start from no relation access so a relation dropped from the read
      -- layer loses its grant, then re-grant the enumerated list. A new
      -- relation is closed until it is added here on purpose.
      EXECUTE format(
        'REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I FROM app_readonly',
        v_schema
      );
      EXECUTE format(
        'ALTER DEFAULT PRIVILEGES IN SCHEMA %I REVOKE ALL PRIVILEGES ON TABLES FROM app_readonly',
        v_schema
      );

      FOREACH v_relation IN ARRAY euro_app_relations
      LOOP
        IF EXISTS (
          SELECT 1
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = v_schema
            AND c.relname = v_relation
            AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
        ) THEN
          EXECUTE format(
            'GRANT SELECT ON %I.%I TO app_readonly',
            v_schema,
            v_relation
          );
        ELSE
          RAISE EXCEPTION
            'Read-layer relation %.% is listed for app_readonly but does not exist',
            v_schema,
            v_relation;
        END IF;
      END LOOP;
    ELSE
      EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO app_readonly', v_schema);
      EXECUTE format(
        'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO app_readonly',
        v_schema
      );
    END IF;

    -- Remove inherited/default function access, then add only the app API.
    EXECUTE format(
      'REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA %I FROM app_readonly',
      v_schema
    );
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES IN SCHEMA %I REVOKE ALL PRIVILEGES ON FUNCTIONS FROM app_readonly',
      v_schema
    );

    FOR v_function_signature IN
      SELECT format(
        '%I.%I(%s)',
        n.nspname,
        p.proname,
        pg_get_function_identity_arguments(p.oid)
      )
      FROM pg_proc p
      JOIN pg_namespace n ON n.oid = p.pronamespace
      WHERE n.nspname = v_schema
        AND p.proname = ANY(v_function_names)
    LOOP
      EXECUTE format(
        'GRANT EXECUTE ON FUNCTION %s TO app_readonly',
        v_function_signature
      );
    END LOOP;

    -- RLS is defense in depth for base tables. Only the direct app role gets
    -- a read policy; ETL/object owners and service_role retain owner/bypass
    -- behavior according to their PostgreSQL role attributes.
    FOR v_table IN
      SELECT c.relname
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = v_schema
        AND c.relkind IN ('r', 'p')
    LOOP
      EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY', v_schema, v_table);
      EXECUTE format('DROP POLICY IF EXISTS rls_read_all ON %I.%I', v_schema, v_table);
      -- Legacy name from an earlier pass. One policy name per convention:
      -- leaving both makes the effective grant impossible to read off the
      -- catalog.
      EXECUTE format(
        'DROP POLICY IF EXISTS rls_read_all_app_readonly ON %I.%I',
        v_schema,
        v_table
      );
      EXECUTE format(
        'DROP POLICY IF EXISTS app_readonly_select_all ON %I.%I',
        v_schema,
        v_table
      );
      EXECUTE format(
        'CREATE POLICY app_readonly_select_all ON %I.%I FOR SELECT TO app_readonly USING (true)',
        v_schema,
        v_table
      );
    END LOOP;
  END LOOP;
END
$$;
