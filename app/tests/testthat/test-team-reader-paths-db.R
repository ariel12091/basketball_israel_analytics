library(testthat)

# docs/shared_definitions.md, R10: a fast path is an optimisation, not a second
# definition. Each pair below is one answer computed two ways; every shared
# column of every row must be equal.

skip_if_not(Sys.getenv("RUN_DB_TESTS", "0") == "1")
skip_if_not_installed("DBI")
skip_if_not_installed("RPostgres")

pg_env <- c("PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASS")
missing_env <- pg_env[!nzchar(Sys.getenv(pg_env))]
if (length(missing_env)) {
  skip(paste("Missing DB env:", paste(missing_env, collapse = ", ")))
}

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", unset = "require")
)
on.exit(DBI::dbDisconnect(con), add = TRUE)

# Fixed SQL, no parameters, sent in immediate mode: the transaction pooler
# can cross prepared statements between clients (CLAUDE.md, port 6543), and
# these files must not fail because the app happens to be in use.
dbq <- function(sql) DBI::dbGetQuery(con, sql, immediate = TRUE)

same_table <- function(a, b, key = "team_id") {
  if (nrow(a) != nrow(b)) return(sprintf("rows %d vs %d", nrow(a), nrow(b)))
  if (!nrow(a)) return("same")
  a <- a[order(as.integer(a[[key]])), ]; b <- b[order(as.integer(b[[key]])), ]
  cols <- setdiff(intersect(names(a), names(b)), c("team_name", "game_year"))
  bad <- Filter(function(cc) !isTRUE(all.equal(suppressWarnings(as.numeric(a[[cc]])), suppressWarnings(as.numeric(b[[cc]])))), cols)
  if (length(bad)) paste("differ:", paste(bad, collapse = ", ")) else "same"
}

test_that("the Traditional reader's per-game path equals its action-row path, both sides", {
  presets <- c("", ", p_home_away := 'home'", ", p_last_n_games := 5", ", p_end_date := '2026-03-20'",
               ", p_opp_rank_side := 'top', p_opp_rank_n := 4", ", p_min_gn := 5, p_max_gn := 12")
  for (side in c("offense", "defense")) for (args in presets) {
    call <- function(extra) dbq(sprintf(
      "SELECT * FROM basketball_test.get_team_traditional_dynamic(2026%s, p_side := '%s'%s)", args, side, extra))
    fast <- call("")
    expect_gt(nrow(fast), 0, label = paste(side, args, "rows"))
    expect_identical(same_table(fast, call(", p_use_game_totals := FALSE")), "same", label = paste(side, args))
  }
})

test_that("the team views behind Tab 3's default state equal their dynamic readers", {
  for (gy in c(2026, 2027)) for (pair in list(c("team_ppp_ratings_mv", "get_team_ratings_dynamic"),
                                              c("team_four_factors_mv", "get_team_four_factors_dynamic"))) {
    view <- dbq(sprintf("SELECT * FROM basketball_test.%s WHERE game_year::int = %d", pair[1], gy))
    reader <- dbq(sprintf("SELECT * FROM basketball_test.%s(%d)", pair[2], gy))
    expect_gt(nrow(view), 0, label = paste(pair[1], gy, "rows"))
    expect_identical(same_table(view, reader), "same", label = paste(pair[1], gy))
  }
})
