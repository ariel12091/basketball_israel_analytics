library(testthat)
# Identity proof (gap G3) for the web app's database login. Under the execution
# amendment (2026-10-20) the web app uses the EXISTING app_readonly login: no
# new role, password, membership or role default. This file proves, with that
# credential, that permitted reads succeed, writes and functions outside the
# allowlist fail, the publication writers are denied, and no other role is
# reachable.
#
# Refusals are asserted by SQLSTATE 42501 (insufficient_privilege) through
# db_probe_sqlstate() (helper-db-roles.R): each probe runs inside a DO block
# that always raises afterwards, in a transaction that is rolled back, so even a
# privilege that should not exist could not change data. Write and DDL probes
# use an ordinary transaction (a READ ONLY one would refuse them with 25006
# before any privilege check); function-call probes use READ ONLY.
#
# Skips (does not fail) until the web foundation is applied
# (scripts/apply_publication.R, then scripts/apply_web_foundation.R).

creds <- db_role_require("readonly")
con <- db_role_connect(creds)
withr::defer(DBI::dbDisconnect(con))
skip_if_not(db_web_foundation_applied(con),
            "web foundation not applied yet (app_api / basketball_test.publication)")

expect_denied <- function(sql, read_only = FALSE) {
  expect_identical(db_probe_sqlstate(con, sql, read_only = read_only), "42501", info = sql)
}

test_that("the connection is the existing app_readonly login", {
  expect_identical(DBI::dbGetQuery(con, "SELECT current_user AS u")$u, "app_readonly")
  expect_identical(DBI::dbGetQuery(con, "SELECT session_user AS u")$u, "app_readonly")
})

test_that("permitted reads succeed", {
  expect_identical(db_probe_sqlstate(con,
    "SELECT game_id FROM basketball_test.final_schedule_mv LIMIT 1", read_only = TRUE), "ok")
  expect_equal(nrow(DBI::dbGetQuery(con,
    "SELECT league FROM basketball_test.publication")), 3L)
  v <- DBI::dbGetQuery(con, "SELECT * FROM app_api.version()")
  expect_setequal(v$league, c("il", "euroleague", "eurocup"))
  payload <- DBI::dbGetQuery(con,
    "SELECT app_api.game_logs_il($1)::text AS p", params = list(1900L))$p[[1]]
  expect_true(jsonlite::fromJSON(payload)$state %in%
                c("published", "publishing", "failed", "stale"))
})

test_that("writes and DDL are denied", {
  expect_denied("INSERT INTO basketball_test.app_meta (key, value) VALUES ('x', 'x')")
  expect_denied("UPDATE basketball_test.publication SET version = version")
  expect_denied("DELETE FROM basketball_test.schedule WHERE false")
  expect_denied("CREATE TABLE basketball_test.web_identity_probe (n int)")
  expect_denied("CREATE FUNCTION app_api.web_identity_probe() RETURNS int LANGUAGE sql AS 'SELECT 1'")
  for (rel in c("basketball_test.app_meta", "basketball_test.publication",
                "basketball_test.schedule")) {
    privs <- DBI::dbGetQuery(con,
      "SELECT has_table_privilege($1, 'INSERT') AS ins, has_table_privilege($1, 'UPDATE') AS upd,
              has_table_privilege($1, 'DELETE') AS del, has_table_privilege($1, 'TRUNCATE') AS trunc",
      params = list(rel))
    expect_false(any(unlist(privs)), info = rel)
  }
  for (schema in c("basketball_test", "euroleague", "app_api")) {
    expect_false(DBI::dbGetQuery(con, "SELECT has_schema_privilege($1, 'CREATE') AS ok",
                                 params = list(schema))$ok, info = schema)
  }
})

test_that("functions outside the allowlist and the publication writers are denied", {
  expect_denied("SELECT basketball_test.compute_player_traditional_by_game(ARRAY[0]::int[])",
                read_only = TRUE)
  run <- "'00000000-0000-0000-0000-000000000000'::uuid"
  expect_denied(paste0("SELECT basketball_test.begin_publication('il', ", run, ")"),
                read_only = TRUE)
  expect_denied(paste0("SELECT basketball_test.fence_publication('il', ", run, ")"),
                read_only = TRUE)
  expect_denied(paste0("SELECT basketball_test.finish_publication('il', ", run, ", true)"),
                read_only = TRUE)
  expect_denied(paste0("SELECT basketball_test.publication_owner_live('il', 0, now())"),
                read_only = TRUE)
})

test_that("no other role is reachable", {
  roles <- c("anon", "authenticated", "service_role", "postgres")
  rows <- DBI::dbGetQuery(con,
    "SELECT rolname, pg_has_role(current_user, oid, 'MEMBER') AS reachable
       FROM pg_roles WHERE rolname = ANY($1::text[])",
    params = list(paste0("{", paste(roles, collapse = ","), "}")))
  expect_setequal(rows$rolname, roles)
  expect_false(any(rows$reachable))
  for (role in roles) expect_denied(sprintf("SET LOCAL ROLE %s", role))
})
