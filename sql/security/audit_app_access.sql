-- Read-only verification for the Shiny database security boundary.
-- Expected result: zero rows.
--
-- Covers the Israeli analytics schemas and the EuroLeague shadow schema. The
-- two differ in one dimension only: Israeli schemas grant app_readonly SELECT
-- on every relation (the SQL functions are SECURITY INVOKER), while
-- euroleague grants a curated read layer and must keep raw provider evidence
-- and load bookkeeping closed. `curated_relations` selects that mode, and
-- `allow_service_role` records that euroleague is deliberately outside
-- Supabase's managed surface.

WITH target_schemas(schema_name, curated_relations, allow_service_role) AS (
  SELECT
    nspname,
    nspname = 'euroleague',
    nspname <> 'euroleague'
  FROM pg_namespace
  WHERE nspname IN ('basketball_test', 'basketball', 'euroleague')
),
untrusted_roles(role_name) AS (
  VALUES ('anon'), ('authenticated')
),
existing_untrusted_roles AS (
  SELECT role_name
  FROM untrusted_roles
  JOIN pg_roles ON pg_roles.rolname = role_name
),
israeli_app_routines(routine_name) AS (
  VALUES
    ('onoff_compute'),
    ('four_factors_compute'),
    ('four_factors_dashboard_compute'),
    ('fetch_lineups_csv_v2'),
    ('fetch_lineups_all'),
    ('fetch_lineups_four_factors_csv'),
    ('fetch_lineups_four_factors'),
    ('get_team_ratings_dynamic'),
    ('get_team_four_factors_dynamic'),
    ('get_player_traditional_dynamic'),
    ('get_player_traditional_from_games'),
    ('get_player_traditional_custom_clutch')
),
-- The EuroLeague refresh_* publication functions are excluded on purpose:
-- they mutate the schema and must not be executable by the app role.
euro_app_routines(routine_name) AS (
  VALUES
    ('onoff_compute'),
    ('four_factors_compute'),
    ('four_factors_dashboard_compute'),
    ('fetch_lineups_dynamic'),
    ('fetch_lineups_direct'),
    ('fetch_lineups_pergame'),
    ('get_team_ratings_dynamic'),
    ('get_team_ratings_direct'),
    ('get_team_ratings_pergame'),
    ('get_team_four_factors_dynamic'),
    ('get_team_four_factors_direct'),
    ('get_team_four_factors_pergame'),
    ('get_team_minutes_dynamic'),
    ('get_team_minutes_direct'),
    ('get_team_minutes_pergame'),
    ('get_player_traditional_dynamic'),
    ('get_player_traditional_pergame'),
    ('get_player_traditional_clutch'),
    ('get_player_traditional_custom_clutch'),
    ('get_player_traditional_standard_clutch'),
    ('effective_period'),
    ('person_display_name')
),
allowed_app_routines(schema_name, routine_name) AS (
  SELECT s.schema_name, r.routine_name
  FROM target_schemas s
  JOIN israeli_app_routines r ON NOT s.curated_relations
  UNION ALL
  SELECT s.schema_name, r.routine_name
  FROM target_schemas s
  JOIN euro_app_routines r ON s.curated_relations
),
israeli_app_relations(relation_name) AS (
  VALUES
    ('app_meta'),
    ('df_pts_poss_lineups_longer_mv'),
    ('final_schedule_mv'),
    ('full_rosters'),
    ('lineup_four_factors_by_game'),
    ('mv_lineup_totals_by_day'),
    ('onoff_default_mv'),
    ('player_advanced_stats_mv'),
    ('player_traditional_by_game'),
    ('player_stats_actions_by_game'),
    ('player_traditional_stats_mv'),
    ('default_clutch_player_totals_by_game'),
    ('sub_lineups'),
    ('sub_lineups_stats'),
    ('team_four_factors_mv'),
    ('team_metrics_by_game_mv'),
    ('team_ppp_ratings_mv')
),
euro_app_relations(relation_name) AS (
  VALUES
    ('schedule'),
    ('teams'),
    ('players'),
    ('full_rosters'),
    ('player_four_factors_by_game'),
    ('final_schedule'),
    ('final_schedule_mv'),
    ('player_game_context'),
    ('player_onoff_default_mv'),
    ('player_advanced_stats_mv'),
    ('player_traditional_stats_mv'),
    ('default_clutch_player_totals_by_game'),
    ('load_runs'),
    ('team_game_ratings_mv'),
    ('team_ppp_ratings_mv'),
    ('team_four_factors_by_game'),
    ('team_four_factors_mv'),
    ('lineup_totals_by_game'),
    ('sub_lineups'),
    ('sub_lineups_stats_mv')
),
required_app_relations(schema_name, relation_name) AS (
  SELECT s.schema_name, r.relation_name
  FROM target_schemas s
  JOIN israeli_app_relations r ON NOT s.curated_relations
  UNION ALL
  SELECT s.schema_name, r.relation_name
  FROM target_schemas s
  JOIN euro_app_relations r ON s.curated_relations
),
relations AS (
  SELECT
    n.nspname AS schema_name,
    c.oid,
    c.relname,
    c.relkind,
    c.relrowsecurity,
    s.curated_relations
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN target_schemas s ON s.schema_name = n.nspname
  WHERE c.relkind IN ('r', 'p', 'v', 'm', 'f')
),
base_tables AS (
  SELECT *
  FROM relations
  WHERE relkind IN ('r', 'p')
),
routines AS (
  SELECT n.nspname AS schema_name, p.oid, p.proname
  FROM pg_proc p
  JOIN pg_namespace n ON n.oid = p.pronamespace
  JOIN target_schemas s ON s.schema_name = n.nspname
),
app_read_policies AS (
  SELECT pol.polrelid
  FROM pg_policy pol
  WHERE pol.polname = 'app_readonly_select_all'
    AND pol.polcmd IN ('r', '*')
    AND pol.polpermissive
    AND EXISTS (
      SELECT 1
      FROM unnest(pol.polroles) AS role_oid
      WHERE pg_get_userbyid(role_oid) = 'app_readonly'
    )
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

  -- The shadow schema is not part of Supabase's managed surface. Any
  -- service_role access to it is unintended.
  SELECT
    'service_role_unexpected_access',
    'service_role',
    s.schema_name
  FROM target_schemas s
  WHERE NOT s.allow_service_role
    AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'service_role')
    AND has_schema_privilege('service_role', s.schema_name, 'USAGE')

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
    ON required.schema_name = x.schema_name
   AND required.relation_name = x.relname
  WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_readonly')
    AND NOT has_table_privilege('app_readonly', x.oid, 'SELECT')

  UNION ALL

  -- Curated schemas only: anything readable beyond the declared read layer is
  -- a widening of the app's data surface.
  SELECT
    'app_unexpected_relation_select',
    'app_readonly',
    x.schema_name || '.' || x.relname
  FROM relations x
  WHERE x.curated_relations
    AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_readonly')
    AND has_table_privilege('app_readonly', x.oid, 'SELECT')
    AND NOT EXISTS (
      SELECT 1
      FROM required_app_relations required
      WHERE required.schema_name = x.schema_name
        AND required.relation_name = x.relname
    )

  UNION ALL

  SELECT
    'app_unexpected_routine_execute',
    'app_readonly',
    x.schema_name || '.' || x.proname
  FROM routines x
  WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_readonly')
    AND has_function_privilege('app_readonly', x.oid, 'EXECUTE')
    AND NOT EXISTS (
      SELECT 1
      FROM allowed_app_routines allowed
      WHERE allowed.schema_name = x.schema_name
        AND allowed.routine_name = x.proname
    )

  UNION ALL

  SELECT
    'app_required_routine_execute_missing',
    'app_readonly',
    x.schema_name || '.' || x.proname
  FROM routines x
  JOIN allowed_app_routines allowed
    ON allowed.schema_name = x.schema_name
   AND allowed.routine_name = x.proname
  WHERE EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_readonly')
    AND NOT has_function_privilege('app_readonly', x.oid, 'EXECUTE')

  UNION ALL

  -- RLS is defense in depth for base tables. A table created after the last
  -- hardening pass silently misses it, so check the catalog rather than
  -- trusting the apply script to have seen every table.
  SELECT
    'rls_disabled',
    'app_readonly',
    x.schema_name || '.' || x.relname
  FROM base_tables x
  WHERE NOT x.relrowsecurity

  UNION ALL

  -- RLS enabled without the read policy locks the app out instead of
  -- protecting it; that is a failure, not a stricter setting.
  SELECT
    'rls_read_policy_missing',
    'app_readonly',
    x.schema_name || '.' || x.relname
  FROM base_tables x
  WHERE x.relrowsecurity
    AND NOT EXISTS (
      SELECT 1 FROM app_read_policies p WHERE p.polrelid = x.oid
    )

  UNION ALL

  -- One policy name per convention. A second policy is permissive and widens
  -- the effective grant beyond what this file declares.
  SELECT
    'rls_unexpected_policy',
    coalesce(
      (
        SELECT string_agg(pg_get_userbyid(role_oid), ',' ORDER BY role_oid)
        FROM unnest(pol.polroles) AS role_oid
      ),
      'PUBLIC'
    ),
    x.schema_name || '.' || x.relname || ' (' || pol.polname || ')'
  FROM pg_policy pol
  JOIN base_tables x ON x.oid = pol.polrelid
  WHERE pol.polname <> 'app_readonly_select_all'
)
SELECT violation, role_name, object_name
FROM violations
ORDER BY violation, role_name, object_name;
