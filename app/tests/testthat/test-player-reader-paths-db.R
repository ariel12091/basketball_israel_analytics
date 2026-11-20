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

con <- db_test_connect(
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
# Current season nightly, every season weekly (helper-db-connect.R). The
# dates bracket the whole season, as the On/Off reader is called by Shiny.
SEASONS <- stats::setNames(lapply(db_test_seasons(), function(gy)
  c(sprintf("%d-09-01", gy - 1L), sprintf("%d-07-01", gy))), db_test_seasons())

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

# Compare's Starters vs Bench presets read player box scores from the
# action-row reader. Starters 0-5 on both sides must be the unfiltered season,
# and the two halves of a split must add back up to it: events by the credited
# lineup, minutes by the physical one (minutes to rounding, since each half
# rounds its own second total).
test_that("Player Stats: the starters filter of the action-row reader is a partition of the season", {
  ctx <- "NULL,NULL,NULL,NULL,NULL,'all','all','all',NULL,'net',NULL,'all',NULL,FALSE,NULL,NULL,NULL"
  rdr <- function(gy, st) dbq(sprintf("SELECT * FROM basketball_test.get_player_traditional_custom_clutch(%s,%s,%s)", gy, ctx, st))
  for (gy in names(SEASONS)) {
    season <- dbq(sprintf("SELECT * FROM basketball_test.get_player_traditional_from_games(%s)", gy))
    g <- gaps(season, rdr(gy, "0,5,0,5"))
    expect_equal(c(g$only_a, g$only_b), c(0, 0), label = paste(gy, "players on one side only"))
    expect_gte(length(g$gap), 24, label = paste(gy, "shared columns"))
    expect_no_gap(g, paste(gy, "starters 0-5"))
    for (split in list(c("3,5,NULL,NULL", "0,2,NULL,NULL"), c("NULL,NULL,3,5", "NULL,NULL,0,2"))) {
      hi <- rdr(gy, split[1]); lo <- rdr(gy, split[2])
      for (cc in c("pts", "reb", "ast", "stl", "blk", "tov", "fgm", "fga", "3pm", "3pa", "ftm", "fta")) {
        expect_identical(sum(hi[[cc]]) + sum(lo[[cc]]), sum(season[[cc]]), label = paste(gy, split[1], cc))
      }
      expect_lte(abs(sum(hi$minutes) + sum(lo$minutes) - sum(season$minutes)), 0.05 * nrow(season),
                 label = paste(gy, split[1], "minutes"))
    }
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

# EuroLeague and EuroCup: the web's On/Off and Compare read this pair as
# their Four Factors fast path (web/src/server/onoffRepo.ts
# seasonFourFactors). The rating columns it joins come from
# player_onoff_default_mv; for a player with a side of at most 3 possessions
# the reader leaves them NULL where the view has a value (D19).
FF_JOINED <- c("Net RTG Diff" = "Net RTG Diff", "Off ON Diff" = "Off ON Diff",
               "Def ON Diff" = "Def ON Diff", minutes = "minutes",
               off_on_ppp = "Off ON PPP", off_off_ppp = "Off OFF PPP",
               def_on_ppp = "Def ON PPP", def_off_ppp = "Def OFF PPP")
euro_ff_reader <- function(comp, gy, teams = "NULL")
  dbq(sprintf("SELECT * FROM euroleague.four_factors_dashboard_compute('%s', %d, '%d-09-01'::date, '%d-07-01'::date, %s)",
              comp, gy, gy, gy + 1L, teams))

test_that("EuroLeague On/Off Four Factors: the season views equal the stored reader", {
  for (comp in c("E", "U")) for (gy in db_test_euro_seasons()) {
    lab <- paste(comp, gy)
    where <- sprintf("competition = '%s' AND game_year = %d", comp, gy)
    view <- dbq(paste("SELECT * FROM euroleague.player_advanced_stats_mv WHERE", where))
    reader <- euro_ff_reader(comp, gy)
    expect_gt(nrow(view), 0, label = paste(lab, "view rows"))
    g <- gaps(view, reader, skip = "competition")
    expect_equal(c(g$only_a, g$only_b), c(0, 0), label = paste(lab, "players on one side only"))
    expect_gte(length(g$gap), 37, label = paste(lab, "shared columns"))
    expect_no_gap(g, lab)

    mv <- keyed(dbq(paste("SELECT * FROM euroleague.player_onoff_default_mv WHERE", where)))
    r <- keyed(reader); m <- mv[match(r$.key, mv$.key), ]
    poss <- function(x) ifelse(is.na(x), 0, x)
    edge <- pmin(poss(r$off_on_poss), poss(r$def_on_poss), poss(r$off_off_poss), poss(r$def_off_poss)) <= 3
    for (k in names(FF_JOINED)) {
      u <- r[[k]]; v <- m[[FF_JOINED[[k]]]]
      expect_false(any(!is.na(u) & is.na(v)), label = paste(lab, k, "in the reader, missing from the view"))
      expect_true(all(edge[is.na(u) & !is.na(v)]), label = paste(lab, k, "missing from the reader only at the edge of the sample"))
      both <- !is.na(u) & !is.na(v)
      expect_lte(max(c(0, abs(u[both] - v[both]))), 1e-9, label = paste(lab, k, "largest gap"))
    }
  }
})

# Compare's player Four Factors side reads the season views for the whole
# league and keeps the side's team on the page; the reader filters the team
# in SQL. A player's numbers must not depend on which of the two did it.
test_that("Player Four Factors: the reader's team filter changes no value", {
  two_teams <- function(d) head(sort(unique(as.integer(d$team_id))), 2)
  for (gy in names(SEASONS)) {
    all <- dbq(sprintf("SELECT * FROM basketball_test.four_factors_dashboard_compute(%s)", gy))
    for (tm in two_teams(all)) {
      one <- dbq(sprintf("SELECT * FROM basketball_test.four_factors_dashboard_compute(%s, NULL, NULL, '%d')", gy, tm))
      g <- gaps(all[all$team_id == tm, ], one)
      expect_gt(g$matched, 0, label = paste(gy, tm, "players"))
      expect_equal(c(g$only_a, g$only_b), c(0, 0), label = paste(gy, tm, "players on one side only"))
      expect_no_gap(g, paste(gy, tm))
    }
  }
  for (comp in c("E", "U")) for (gy in db_test_euro_seasons()) {
    all <- euro_ff_reader(comp, gy)
    for (tm in two_teams(all)) {
      one <- euro_ff_reader(comp, gy, sprintf("'%d'", tm))
      g <- gaps(all[all$team_id == tm, ], one, skip = "competition")
      expect_gt(g$matched, 0, label = paste(comp, gy, tm, "players"))
      expect_equal(c(g$only_a, g$only_b), c(0, 0), label = paste(comp, gy, tm, "players on one side only"))
      expect_no_gap(g, paste(comp, gy, tm))
    }
  }
})
