library(testthat)
# Current-role identity proof: the web app uses the existing app_readonly login.
# No new roles, passwords, memberships, or role defaults are created or changed.
skip_if_not(Sys.getenv("RUN_DB_TESTS", "0") == "1")
skip_if_not_installed("DBI")
skip_if_not_installed("RPostgres")
for (key in c("PG_HOST", "PG_DB", "PG_USER", "PG_PASS")) {
  if (!nzchar(Sys.getenv(key))) skip(paste("Missing environment variable", key))
}
if (!grepl("^app_readonly([.]|$)", Sys.getenv("PG_USER"))) {
  stop("Identity proof must run as the existing app_readonly login, never the DDL owner")
}
con <- DBI::dbConnect(RPostgres::Postgres(), host=Sys.getenv("PG_HOST"),
  port=5432L, dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode="require", connect_timeout=15)
withr::defer(DBI::dbDisconnect(con))
test_that("the existing login reads the app API and the schedule", {
  expect_identical(DBI::dbGetQuery(con, "SELECT current_user AS u")$u, "app_readonly")
  expect_equal(nrow(DBI::dbGetQuery(con,
    "SELECT game_id FROM basketball_test.final_schedule_mv LIMIT 1")), 1L)
  expect_true(DBI::dbGetQuery(con,
    "SELECT has_schema_privilege('app_api','USAGE') AS ok")$ok)
})
test_that("the existing login has no write, CREATE, or unapproved routine access", {
  expect_false(DBI::dbGetQuery(con,
    "SELECT has_table_privilege('basketball_test.app_meta','INSERT') AS ok")$ok)
  expect_false(DBI::dbGetQuery(con,
    "SELECT has_schema_privilege('basketball_test','CREATE') AS ok")$ok)
  expect_false(DBI::dbGetQuery(con,
    "SELECT has_schema_privilege('app_api','CREATE') AS ok")$ok)
  expect_false(DBI::dbGetQuery(con,
    "SELECT has_function_privilege('basketball_test.compute_player_traditional_by_game(integer[])','EXECUTE') AS ok")$ok)
})
test_that("privileged roles are unreachable", {
  rows <- DBI::dbGetQuery(con,
    "SELECT pg_has_role(current_user,oid,'MEMBER') AS reachable FROM pg_roles WHERE rolname IN ('anon','authenticated','service_role','postgres')")
  expect_equal(nrow(rows),4L)
  expect_false(any(rows$reachable))
})
