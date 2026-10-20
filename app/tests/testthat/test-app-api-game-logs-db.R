library(testthat)
# The applied app_api Game Logs readers, called as the web app's login
# (app_readonly, execution amendment 2026-10-20). Same parity proof as the
# pre-apply scratch-schema run in test-app-api-sql-catalog-db.R
# (helper-app-api-parity.R), now against the production objects and grants.
# Skips until scripts/apply_publication.R and scripts/apply_web_foundation.R
# are applied.

creds <- db_role_require("readonly")
api_con <- db_role_connect(creds, bigint = "numeric")
withr::defer(DBI::dbDisconnect(api_con))
skip_if_not(db_web_foundation_applied(api_con),
            "web foundation not applied yet (app_api / basketball_test.publication)")

test_that("the readers match Shiny's own season pulls, as the web login", {
  expect_identical(DBI::dbGetQuery(api_con, "SELECT current_user AS u")$u, "app_readonly")
  # The completed seasons the goldens use (plan, Global Constraints).
  expect_app_api_game_logs_parity(api_con, "app_api", il_season = 2026L, euro_season = 2025L)
})

test_that("browser-facing roles cannot reach the readers", {
  for (role in c("anon", "authenticated")) {
    privs <- DBI::dbGetQuery(api_con,
      "SELECT has_schema_privilege($1, 'app_api', 'USAGE') AS usage,
              bool_or(has_function_privilege($1, p.oid, 'EXECUTE')) AS execute
         FROM pg_proc p WHERE p.pronamespace = 'app_api'::regnamespace",
      params = list(role))
    expect_false(privs$usage, info = role)
    expect_false(privs$execute, info = role)
  }
})
