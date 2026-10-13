library(testthat)

# docs/shared_definitions.md, R10: a fast path is an optimisation, not a second
# definition. The player tabs read a season view when no filter is set and a
# stored reader otherwise; for an unfiltered season the two must return the
# same numbers.

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

keyed <- function(d) { d$.key <- paste(as.integer(d$team_id), as.integer(d$player_id)); d }
# Largest difference per shared numeric column over the players both hold.
# A value present on one side only counts as Inf.
gaps <- function(a, b, skip = character(0)) {
  a <- keyed(a); b <- keyed(b)
  both <- intersect(a$.key, b$.key)
  x <- a[match(both, a$.key), ]; y <- b[match(both, b$.key), ]
  cols <- setdiff(intersect(names(a), names(b)), c(".key", "team_id", "player_id", "game_year", "Year", skip))
  cols <- Filter(function(cc) is.numeric(x[[cc]]) && is.numeric(y[[cc]]), cols)
  out <- vapply(cols, function(cc) {
    u <- x[[cc]]; v <- y[[cc]]
    if (any(is.na(u) != is.na(v))) return(Inf)
    if (all(is.na(u))) 0 else max(abs(u - v), na.rm = TRUE)
  }, numeric(1))
  list(gap = out, matched = length(both), only_a = length(setdiff(a$.key, b$.key)), only_b = length(setdiff(b$.key, a$.key)))
}
expect_no_gap <- function(g, label, tol = 1e-9) {
  bad <- names(g$gap)[g$gap > tol]
  expect_identical(bad, character(0), label = paste(label, "columns that differ"))
}
SEASONS <- list(`2026` = c("2025-09-01", "2026-07-01"), `2027` = c("2026-09-01", "2027-07-01"))

test_that("Player Stats: the season view equals the per-game reader", {
  for (gy in names(SEASONS)) {
    g <- gaps(dbq(sprintf("SELECT * FROM basketball_test.player_traditional_stats_mv WHERE game_year = %s", gy)),
              dbq(sprintf("SELECT * FROM basketball_test.get_player_traditional_from_games(%s)", gy)))
    expect_gt(g$matched, 100, label = paste(gy, "players"))
    expect_equal(c(g$only_a, g$only_b), c(0, 0), label = paste(gy, "players on one side only"))
    expect_gte(length(g$gap), 24, label = paste(gy, "shared columns"))
    expect_no_gap(g, gy)
  }
})

test_that("On/Off Four Factors: the season view equals both stored readers", {
  for (gy in names(SEASONS)) {
    view <- dbq(sprintf("SELECT * FROM basketball_test.player_advanced_stats_mv WHERE game_year = %s", gy))
    for (fn in c("four_factors_compute", "four_factors_dashboard_compute")) {
      g <- gaps(view, dbq(sprintf("SELECT * FROM basketball_test.%s(%s)", fn, gy)))
      expect_equal(c(g$only_a, g$only_b), c(0, 0), label = paste(gy, fn, "players on one side only"))
      expect_gte(length(g$gap), 37, label = paste(gy, fn, "shared columns"))
      expect_no_gap(g, paste(gy, fn))
    }
  }
})

# The On/Off Summary pair is equal except in two documented ways
# (docs/shared_definitions.md, deviation register): the view keeps players with
# no ON or no OFF bucket, which the reader drops; a side with possessions and no
# points is NULL in the view and 0 in the reader; and minutes are rounded
# differently (the view rounds the season's second total to one decimal, the
# reader sums per-game values already cut to three). Percentiles are ranks within each source's own player set, so
# they are compared on the players both rank, with room for that difference.
# A side with no rating has no percentile: until 2026-10-13 the view ranked
# pr_on_net over every row, NULLs last, so real values topped out near 0.84.
test_that("On/Off Summary: the season view equals the reader for every player with a real sample", {
  for (gy in names(SEASONS)) {
    d <- SEASONS[[gy]]
    view <- dbq(sprintf('SELECT * FROM basketball_test.onoff_default_mv WHERE "Year" = %s', gy))
    reader <- dbq(sprintf("SELECT * FROM basketball_test.onoff_compute('%s'::date, '%s'::date, NULL::text, 0, 0, -1e9, '%s')", d[1], d[2], gy))
    all_rows <- gaps(view, reader)
    expect_equal(all_rows$only_b, 0, label = paste(gy, "players only in the reader"))
    sampled <- function(x) x[x[["ON Poss"]] >= 20 & x[["OFF Poss"]] >= 20, ]
    g <- gaps(sampled(view), sampled(reader))
    expect_gt(g$matched, 100, label = paste(gy, "sampled players"))
    is_pr <- grepl("^pr_", names(g$gap))
    exact <- g$gap[!is_pr & names(g$gap) != "minutes"]
    expect_identical(names(exact)[exact > 1e-9], character(0), label = paste(gy, "value columns that differ"))
    expect_lte(g$gap[["minutes"]], 0.1, label = paste(gy, "minutes gap"))
    expect_lte(max(g$gap[is_pr]), 0.03, label = paste(gy, "largest percentile gap"))
    for (side in c("On", "Off")) {
      pr <- view[[paste0("pr_", tolower(side), "_net")]]; val <- view[[paste(side, "Net RTG")]]
      expect_identical(is.na(pr), is.na(val), label = paste(gy, side, "Net RTG percentile present exactly where the rating is"))
      # Just under 1 only when players tie for the best rating.
      expect_gt(max(pr, na.rm = TRUE), 0.98, label = paste(gy, "best", side, "Net RTG percentile"))
    }
  }
})
