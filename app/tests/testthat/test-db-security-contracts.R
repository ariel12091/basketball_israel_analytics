# Slice out one declaration block so an allowlist assertion cannot pass on a
# name that appears somewhere else in the file.
sql_block_between <- function(sql, start_marker, end_marker) {
  start <- regexpr(start_marker, sql, fixed = TRUE)
  expect_gt(start, 0L)
  rest <- substring(sql, start + attr(start, "match.length"))
  end <- regexpr(end_marker, rest, fixed = TRUE)
  expect_gt(end, 0L)
  substring(rest, 1L, end - 1L)
}

test_that("database security script does not grant browser-facing roles access", {
  sql <- read_repo_txt("..", "sql", "security", "enable_readonly_rls.sql")

  expect_match(sql, "REVOKE ALL PRIVILEGES ON SCHEMA %I FROM PUBLIC", fixed = TRUE)
  expect_match(sql, "ARRAY['anon', 'authenticated']", fixed = TRUE)
  expect_match(sql, "REVOKE ALL PRIVILEGES ON ALL TABLES", fixed = TRUE)
  expect_match(sql, "REVOKE ALL PRIVILEGES ON ALL FUNCTIONS", fixed = TRUE)

  expect_false(grepl("GRANT\\s+.+\\s+TO\\s+anon", sql, ignore.case = TRUE))
  expect_false(grepl("GRANT\\s+.+\\s+TO\\s+authenticated", sql, ignore.case = TRUE))
  expect_false(grepl("FOR SELECT TO anon", sql, fixed = TRUE))
  expect_false(grepl("FOR SELECT TO authenticated", sql, fixed = TRUE))
})

test_that("app role is read-only and function execution is allowlisted", {
  sql <- read_repo_txt("..", "sql", "security", "enable_readonly_rls.sql")

  expect_match(sql, "REVOKE CREATE ON SCHEMA %I FROM app_readonly", fixed = TRUE)
  expect_match(sql, "GRANT SELECT ON ALL TABLES IN SCHEMA %I TO app_readonly", fixed = TRUE)
  expect_match(sql, "REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA %I FROM app_readonly", fixed = TRUE)
  expect_match(sql, "p.proname = ANY(v_function_names)", fixed = TRUE)
  expect_match(sql, "FOR SELECT TO app_readonly USING (true)", fixed = TRUE)

  expected_functions <- c(
    "onoff_compute",
    "four_factors_compute",
    "fetch_lineups_csv_v2",
    "fetch_lineups_all",
    "fetch_lineups_four_factors_csv",
    "fetch_lineups_four_factors",
    "get_team_ratings_dynamic",
    "get_team_four_factors_dynamic",
    "get_player_traditional_dynamic"
  )
  for (function_name in expected_functions) {
    expect_match(sql, sprintf("'%s'", function_name), fixed = TRUE)
  }
})

test_that("hardening covers the EuroLeague shadow schema on the same terms", {
  sql <- read_repo_txt("..", "sql", "security", "enable_readonly_rls.sql")

  expect_match(sql, "'euroleague'", fixed = TRUE)

  # The EuroLeague app API, and only it.
  euro_functions <- c(
    "onoff_compute",
    "four_factors_compute",
    "fetch_lineups_dynamic",
    "get_team_ratings_dynamic",
    "get_team_ratings_direct",
    "get_team_four_factors_dynamic",
    "get_team_four_factors_direct",
    "get_team_minutes_dynamic",
    "get_player_traditional_dynamic",
    "effective_period",
    "person_display_name"
  )
  euro_block <- sql_block_between(
    sql,
    "euro_function_names constant text[] := ARRAY[",
    "];"
  )
  for (function_name in euro_functions) {
    expect_match(euro_block, sprintf("'%s'", function_name), fixed = TRUE)
  }

  audit_sql <- read_repo_txt("..", "sql", "security", "audit_app_access.sql")
  for (function_name in euro_functions) {
    expect_match(audit_sql, sprintf("('%s')", function_name), fixed = TRUE)
  }

  # Publication machinery mutates the schema and must never be app-callable.
  expect_false(grepl("refresh_", euro_block, fixed = TRUE))

  # The shadow schema uses a curated relation list, so a new relation is
  # closed to the app until it is added on purpose.
  expect_match(sql, "v_curated_relations := v_schema = 'euroleague'", fixed = TRUE)
  expect_match(
    sql,
    "ALTER DEFAULT PRIVILEGES IN SCHEMA %I REVOKE ALL PRIVILEGES ON TABLES FROM app_readonly",
    fixed = TRUE
  )
  expect_match(sql, "GRANT SELECT ON %I.%I TO app_readonly", fixed = TRUE)

  # It is also outside Supabase's managed surface.
  expect_match(sql, "v_grant_service_role := has_service_role AND v_schema <> 'euroleague'", fixed = TRUE)
})

test_that("hardening enforces a single RLS policy name", {
  sql <- read_repo_txt("..", "sql", "security", "enable_readonly_rls.sql")

  expect_match(sql, "ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY", fixed = TRUE)
  for (legacy_policy in c("rls_read_all", "rls_read_all_app_readonly", "app_readonly_select_all")) {
    expect_match(
      sql,
      sprintf("DROP POLICY IF EXISTS %s ON", legacy_policy),
      fixed = TRUE
    )
  }
  expect_match(sql, "CREATE POLICY app_readonly_select_all ON", fixed = TRUE)
})

test_that("read-only database privilege audit covers public exposure and app writes", {
  sql <- read_repo_txt("..", "sql", "security", "audit_app_access.sql")

  expect_match(sql, "untrusted_schema_usage", fixed = TRUE)
  expect_match(sql, "untrusted_relation_select", fixed = TRUE)
  expect_match(sql, "untrusted_routine_execute", fixed = TRUE)
  expect_match(sql, "app_schema_create", fixed = TRUE)
  expect_match(sql, "app_schema_usage_missing", fixed = TRUE)
  expect_match(sql, "app_relation_write", fixed = TRUE)
  expect_match(sql, "app_required_relation_select_missing", fixed = TRUE)
  expect_match(sql, "app_unexpected_routine_execute", fixed = TRUE)
  expect_match(sql, "app_required_routine_execute_missing", fixed = TRUE)
})

test_that("audit verifies RLS state from the catalog, not from the apply script", {
  sql <- read_repo_txt("..", "sql", "security", "audit_app_access.sql")

  expect_match(sql, "rls_disabled", fixed = TRUE)
  expect_match(sql, "rls_read_policy_missing", fixed = TRUE)
  expect_match(sql, "rls_unexpected_policy", fixed = TRUE)
  expect_match(sql, "NOT x.relrowsecurity", fixed = TRUE)
})

test_that("audit scopes allowlists per schema and covers the shadow schema", {
  sql <- read_repo_txt("..", "sql", "security", "audit_app_access.sql")

  expect_match(sql, "'euroleague'", fixed = TRUE)
  expect_match(sql, "app_unexpected_relation_select", fixed = TRUE)
  expect_match(sql, "service_role_unexpected_access", fixed = TRUE)
  expect_match(sql, "allowed_app_routines(schema_name, routine_name)", fixed = TRUE)
  expect_match(sql, "required_app_relations(schema_name, relation_name)", fixed = TRUE)

  euro_routine_block <- sql_block_between(
    sql,
    "euro_app_routines(routine_name) AS (",
    "\n),"
  )
  expect_match(euro_routine_block, "'fetch_lineups_dynamic'", fixed = TRUE)
  expect_false(grepl("refresh_", euro_routine_block, fixed = TRUE))
})

test_that("database security apply script is dry-run by default", {
  sql <- read_repo_txt("..", "scripts", "apply_db_security.R")

  expect_match(sql, 'Sys.getenv("CONFIRM_DB_SECURITY_APPLY", "0")', fixed = TRUE)
  expect_match(sql, "dbRollback(con)", fixed = TRUE)
  expect_match(sql, "dbCommit(con)", fixed = TRUE)
  expect_match(sql, "if (confirm_apply)", fixed = TRUE)
})
