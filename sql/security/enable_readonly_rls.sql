-- Harden database access for the Shiny application.
--
-- Run this as the owner of objects in the target schemas. The script:
--   * removes effective access for PUBLIC, anon, and authenticated;
--   * preserves service_role access for Supabase administration;
--   * keeps app_readonly read-only access to relations;
--   * grants app_readonly EXECUTE only on functions called by the app;
--   * enables RLS on base tables with a policy only for app_readonly.
--
-- app_readonly still requires broad SELECT because the current SQL functions
-- are SECURITY INVOKER and read underlying relations as the caller. Reducing
-- relation access further requires a separately reviewed SECURITY DEFINER API.

DO $$
DECLARE
  v_schema text;
  v_table text;
  v_role text;
  v_function_signature text;
  has_service_role boolean;
  has_app_readonly boolean;
  app_function_names constant text[] := ARRAY[
    'onoff_compute',
    'four_factors_compute',
    'fetch_lineups_csv_v2',
    'fetch_lineups_all',
    'fetch_lineups_four_factors_csv',
    'fetch_lineups_four_factors',
    'get_team_ratings_dynamic',
    'get_team_four_factors_dynamic',
    'get_player_traditional_dynamic'
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
    WHERE nspname IN ('basketball_test', 'basketball')
  LOOP
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
    -- PUBLIC privileges.
    IF has_service_role THEN
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
    END IF;

    -- The application role can read but cannot create or mutate objects.
    EXECUTE format('REVOKE CREATE ON SCHEMA %I FROM app_readonly', v_schema);
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO app_readonly', v_schema);
    EXECUTE format('REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA %I FROM app_readonly', v_schema);
    EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO app_readonly', v_schema);
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO app_readonly',
      v_schema
    );
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES IN SCHEMA %I REVOKE ALL PRIVILEGES ON SEQUENCES FROM app_readonly',
      v_schema
    );

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
        AND p.proname = ANY(app_function_names)
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
