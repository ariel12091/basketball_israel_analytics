library(testthat)

# Identity proof for the web app's login role (spec gap G3): with app_web's
# real credential, permitted reads succeed, writes and functions outside the
# allowlist fail, and no other role is reachable.
#
# Skips until scripts/apply_web_foundation.R has been applied (the role must
# exist) and whenever APP_WEB_PASSWORD is unset. Always port 5432: the
# transaction pooler on 6543 is not used for live tests (CLAUDE.md, pooler
# pitfall). Every statement that could write runs inside a transaction that
# is rolled back, so a wrong grant cannot leave a trace.

skip_if_not(Sys.getenv("RUN_DB_TESTS", "0") == "1")
skip_if_not_installed("DBI")
skip_if_not_installed("RPostgres")

pg_env <- c("PG_HOST", "PG_DB", "PG_USER", "PG_PASS")
missing_env <- pg_env[!nzchar(Sys.getenv(pg_env))]
if (length(missing_env)) {
  skip(paste("Missing DB env:", paste(missing_env, collapse = ", ")))
}
if (!nzchar(Sys.getenv("APP_WEB_PASSWORD"))) {
  skip("APP_WEB_PASSWORD is not set")
}

web_test_port <- 5432L

connect_as <- function(user, password) {
  DBI::dbConnect(
    RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"),
    port = web_test_port,
    dbname = Sys.getenv("PG_DB"),
    user = user,
    password = password,
    sslmode = Sys.getenv("PG_SSLMODE", unset = "require"),
    connect_timeout = 15L
  )
}

# Does the role exist yet? Asked as the ordinary app role, so a missing role
# skips cleanly while a wrong password for an existing role fails loudly.
probe <- connect_as(Sys.getenv("PG_USER"), Sys.getenv("PG_PASS"))
role_exists <- nrow(DBI::dbGetQuery(
  probe,
  "SELECT 1 FROM pg_roles WHERE rolname = 'app_web'"
)) == 1L
DBI::dbDisconnect(probe)
if (!role_exists) {
  skip("Role app_web does not exist yet (scripts/apply_web_foundation.R not applied)")
}

# Supabase's pooler names a login <role>.<project-ref>; keep the suffix the
# app role uses. APP_WEB_USER overrides.
web_user <- Sys.getenv(
  "APP_WEB_USER",
  unset = sub("^[^.]+", "app_web", Sys.getenv("PG_USER"))
)
con <- connect_as(web_user, Sys.getenv("APP_WEB_PASSWORD"))
on.exit(DBI::dbDisconnect(con), add = TRUE)

# Runs `sql` inside a transaction that is always rolled back and returns the
# error message, or NA when the statement succeeded.
error_in_rollback <- function(sql) {
  DBI::dbBegin(con)
  on.exit(try(DBI::dbRollback(con), silent = TRUE), add = TRUE)
  tryCatch({
    DBI::dbExecute(con, sql)
    NA_character_
  }, error = function(e) conditionMessage(e))
}

test_that("the connection is app_web with its role-default timeouts", {
  expect_identical(DBI::dbGetQuery(con, "SELECT current_user AS u")$u, "app_web")
  expect_identical(DBI::dbGetQuery(con, "SHOW statement_timeout")[[1]], "3s")
  expect_identical(
    DBI::dbGetQuery(con, "SHOW idle_in_transaction_session_timeout")[[1]],
    "5s"
  )
})

test_that("app_web reads what app_readonly reads", {
  rows <- DBI::dbGetQuery(
    con,
    "SELECT game_id FROM basketball_test.final_schedule_mv LIMIT 1"
  )
  expect_equal(nrow(rows), 1L)
  expect_true(DBI::dbGetQuery(
    con,
    "SELECT has_schema_privilege('app_api', 'USAGE') AS ok"
  )$ok)
})

test_that("app_web cannot write or create objects", {
  expect_match(
    error_in_rollback("INSERT INTO basketball_test.app_meta DEFAULT VALUES"),
    "permission denied"
  )
  expect_match(
    error_in_rollback("CREATE TABLE app_api.app_web_probe (i int)"),
    "permission denied"
  )
  expect_match(
    error_in_rollback("CREATE TABLE basketball_test.app_web_probe (i int)"),
    "permission denied"
  )
})

test_that("functions outside the allowlist are not executable", {
  # A read-only compute function that exists in basketball_test and is not on
  # the app allowlist (sql/security/enable_readonly_rls.sql).
  sig <- "basketball_test.compute_player_traditional_by_game(integer[])"
  exists_row <- DBI::dbGetQuery(
    con,
    "SELECT to_regprocedure($1) IS NOT NULL AS ok",
    params = list(sig)
  )
  expect_true(exists_row$ok)
  expect_false(DBI::dbGetQuery(
    con,
    "SELECT has_function_privilege(to_regprocedure($1), 'EXECUTE') AS ok",
    params = list(sig)
  )$ok)
  expect_match(
    error_in_rollback(
      "SELECT * FROM basketball_test.compute_player_traditional_by_game(ARRAY[]::int4[])"
    ),
    "permission denied"
  )
})

test_that("no other role is reachable with app_web's credential", {
  for (target in c("anon", "authenticated", "service_role", "postgres")) {
    expect_match(
      error_in_rollback(sprintf("SET ROLE %s", target)),
      "permission denied",
      info = target
    )
  }
})
