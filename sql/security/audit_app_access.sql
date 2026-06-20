-- Read-only verification for the Shiny database security boundary.
-- Expected result: zero rows.

WITH target_schemas(schema_name) AS (
  SELECT nspname
  FROM pg_namespace
  WHERE nspname IN ('basketball_test', 'basketball')
),
untrusted_roles(role_name) AS (
  VALUES ('anon'), ('authenticated')
),
existing_untrusted_roles AS (
  SELECT role_name
  FROM untrusted_roles
  JOIN pg_roles ON pg_roles.rolname = role_name
),
allowed_app_routines(routine_name) AS (
  VALUES
    ('onoff_compute'),
    ('four_factors_compute'),
    ('fetch_lineups_csv_v2'),
    ('fetch_lineups_all'),
    ('fetch_lineups_four_factors_csv'),
    ('fetch_lineups_four_factors'),
    ('get_team_ratings_dynamic'),
    ('get_team_four_factors_dynamic'),
    ('get_player_traditional_dynamic')
),
required_app_relations(relation_name) AS (
  VALUES
    ('app_meta'),
    ('df_pts_poss_lineups_longer_mv'),
    ('final_schedule_mv'),
    ('full_rosters'),
    ('lineup_four_factors_by_game'),
    ('mv_lineup_totals_by_day'),
    ('onoff_default_mv'),
    ('player_advanced_stats_mv'),
    ('player_traditional_stats_mv'),
    ('sub_lineups'),
    ('sub_lineups_stats'),
    ('team_four_factors_mv'),
    ('team_metrics_by_game_mv'),
    ('team_ppp_ratings_mv')
),
relations AS (
  SELECT n.nspname AS schema_name, c.oid, c.relname
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN target_schemas s ON s.schema_name = n.nspname
  WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
),
routines AS (
  SELECT n.nspname AS schema_name, p.oid, p.proname
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  JOIN target_schemas s ON s.schema_name = n.nspname
),
violations AS (
  SELECT
    'untrusted_schema_usage'::text AS violation,
    r.role_name AS role_name,
    s.schema_name AS object_name
  FROM existing_untrusted_roles r
  CROSS JOIN target_schemas s
  WHERE has_schema_privilege(r.role_name, s.schema_name, 'USAGE')

  UNION ALL

  SELECT
    'untrusted_relation_select',
    r.role_name,
    x.schema_name || '.' || x.relname
  FROM existing_untrusted_roles r
  CROSS JOIN relations x
  WHERE has_table_privilege(r.role_name, x.oid, 'SELECT')

  UNION ALL

  SELECT
    'untrusted_routine_execute',
    r.role_name,
    x.schema_name || '.' || x.proname
  FROM existing_untrusted_roles r
  CROSS JOIN routines x
  WHERE has_function_privilege(r.role_name, x.oid, 'EXECUTE')

  UNION ALL

  SELECT
    'app_schema_create',
    'app_readonly',
    s.schema_name
  FROM target_schemas s
  WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_readonly')
    AND has_schema_privilege('app_readonly', s.schema_name, 'CREATE')

  UNION ALL

  SELECT
    'app_schema_usage_missing',
    'app_readonly',
    s.schema_name
  FROM target_schemas s
  WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_readonly')
    AND NOT has_schema_privilege('app_readonly', s.schema_name, 'USAGE')

  UNION ALL

  SELECT
    'app_relation_write',
    'app_readonly',
    x.schema_name || '.' || x.relname
  FROM relations x
  WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_readonly')
    AND has_table_privilege('app_readonly', x.oid, 'INSERT,UPDATE,DELETE,TRUNCATE')

  UNION ALL

  SELECT
    'app_required_relation_select_missing',
    'app_readonly',
    x.schema_name || '.' || x.relname
  FROM relations x
  JOIN required_app_relations required
    ON required.relation_name = x.relname
  WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_readonly')
    AND NOT has_table_privilege('app_readonly', x.oid, 'SELECT')

  UNION ALL

  SELECT
    'app_unexpected_routine_execute',
    'app_readonly',
    x.schema_name || '.' || x.proname
  FROM routines x
  WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_readonly')
    AND has_function_privilege('app_readonly', x.oid, 'EXECUTE')
    AND x.proname NOT IN (SELECT routine_name FROM allowed_app_routines)

  UNION ALL

  SELECT
    'app_required_routine_execute_missing',
    'app_readonly',
    x.schema_name || '.' || x.proname
  FROM routines x
  JOIN allowed_app_routines allowed
    ON allowed.routine_name = x.proname
  WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_readonly')
    AND NOT has_function_privilege('app_readonly', x.oid, 'EXECUTE')
)
SELECT violation, role_name, object_name
FROM violations
ORDER BY violation, role_name, object_name;
