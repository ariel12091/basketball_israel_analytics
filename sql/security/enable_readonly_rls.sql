DO $$
DECLARE
  v_schema text;
  v_table text;
  has_anon boolean;
  has_authenticated boolean;
  has_service_role boolean;
  has_app_readonly boolean;
  read_roles text;
BEGIN
  SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon') INTO has_anon;
  SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated') INTO has_authenticated;
  SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role') INTO has_service_role;
  SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_readonly') INTO has_app_readonly;

  read_roles := NULL;
  IF has_anon THEN
    read_roles := 'anon';
  END IF;
  IF has_authenticated THEN
    read_roles := COALESCE(read_roles || ', ', '') || 'authenticated';
  END IF;
  IF has_service_role THEN
    read_roles := COALESCE(read_roles || ', ', '') || 'service_role';
  END IF;
  IF has_app_readonly THEN
    read_roles := COALESCE(read_roles || ', ', '') || 'app_readonly';
  END IF;

  FOR v_schema IN
    SELECT nspname
    FROM pg_namespace
    WHERE nspname IN ('basketball_test', 'basketball')
  LOOP
    IF has_anon THEN
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO anon', v_schema);
      EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO anon', v_schema);
      EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO anon', v_schema);
    END IF;

    IF has_authenticated THEN
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO authenticated', v_schema);
      EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO authenticated', v_schema);
      EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO authenticated', v_schema);
    END IF;

    IF has_service_role THEN
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO service_role', v_schema);
      EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO service_role', v_schema);
      EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO service_role', v_schema);
    END IF;
    IF has_app_readonly THEN
      EXECUTE format('GRANT USAGE ON SCHEMA %I TO app_readonly', v_schema);
      EXECUTE format('GRANT SELECT ON ALL TABLES IN SCHEMA %I TO app_readonly', v_schema);
      EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT SELECT ON TABLES TO app_readonly', v_schema);
    END IF;

    FOR v_table IN
      SELECT c.relname
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = v_schema
        AND c.relkind IN ('r', 'p')
    LOOP
      EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY', v_schema, v_table);
      EXECUTE format('DROP POLICY IF EXISTS rls_read_all ON %I.%I', v_schema, v_table);

      IF read_roles IS NOT NULL THEN
        EXECUTE format(
          'CREATE POLICY rls_read_all ON %I.%I FOR SELECT TO %s USING (true)',
          v_schema,
          v_table,
          read_roles
        );
      END IF;
    END LOOP;
  END LOOP;
END
$$;
