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
  expect_match(sql, "p.proname = ANY(app_function_names)", fixed = TRUE)
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

test_that("database security apply script is dry-run by default", {
  sql <- read_repo_txt("..", "scripts", "apply_db_security.R")

  expect_match(sql, 'Sys.getenv("CONFIRM_DB_SECURITY_APPLY", "0")', fixed = TRUE)
  expect_match(sql, "dbRollback(con)", fixed = TRUE)
  expect_match(sql, "dbCommit(con)", fixed = TRUE)
  expect_match(sql, "if (confirm_apply)", fixed = TRUE)
})
