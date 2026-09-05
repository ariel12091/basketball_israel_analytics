# The readers are the only league-aware code in the feature. Database calls
# are separately env-gated; the canonical-frame checks remain pure.

test_that("ribbon_normalise_lanes labels the clicked team own and the other opp", {
  raw <- data.frame(
    team_id = c(7L, 7L, 10L), player_id = c(1L, 2L, 3L),
    player_label = c("A", "B", "C"), start_elapsed = c(0, 0, 0),
    end_elapsed = c(100, 100, 100), stringsAsFactors = FALSE
  )
  out <- ribbon_normalise_lanes(raw, own_team_id = 7L)
  expect_identical(out$side, c("own", "own", "opp"))
  expect_identical(out$player_key, c("1", "2", "3"))
})

test_that("ribbon_normalise_lanes returns only canonical columns", {
  raw <- data.frame(team_id = 7L, player_id = 1L, player_label = "A",
                    start_elapsed = 0, end_elapsed = 10, extra_junk = "drop me")
  out <- ribbon_normalise_lanes(raw, own_team_id = 7L)
  expect_identical(sort(names(out)),
                   sort(c("side", "player_key", "player_label",
                          "start_elapsed", "end_elapsed")))
})

test_that("ribbon_normalise_lanes keys on player_id, never on the label", {
  raw <- data.frame(team_id = c(7L, 7L), player_id = c(101L, 202L),
                    player_label = c("NEW NEW", "NEW NEW"),
                    start_elapsed = c(0, 0), end_elapsed = c(100, 100))
  out <- ribbon_normalise_lanes(raw, own_team_id = 7L)
  expect_identical(out$player_key, c("101", "202"))
  expect_identical(length(unique(out$player_key)), 2L)
})

test_that("ribbon_health_message speaks only when segments were excluded", {
  expect_null(ribbon_health_message(0))
  expect_null(ribbon_health_message(NULL))
  expect_match(ribbon_health_message(3), "3")
  expect_match(ribbon_health_message(3), "not drawn")
})

test_that("ribbon_sign_margin flips the sign when the clicked team is away", {
  m <- data.frame(elapsed = c(0, 60), points_a = c(0, 10), points_b = c(0, 4))
  expect_identical(ribbon_sign_margin(m, 5L, 5L)$margin, c(0, 6))
  expect_identical(ribbon_sign_margin(m, 9L, 5L)$margin, c(0, -6))
})

test_that("the euro reader never pairs lineup names with ids positionally", {
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  sql <- regmatches(src, regexpr('RIBBON_SQL_EURO <- "(.|\n)*?"\n', src, perl = TRUE))
  expect_true(nzchar(sql))
  expect_false(grepl("unnest\\s*\\([^)]*own_lineup[^)]*,", sql))
  expect_match(sql, "unnest\\(s\\.player_ids\\)")
})

test_that("the Israeli lane label comes from the per-game roster", {
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  sql <- regmatches(src, regexpr('RIBBON_SQL_ISRAEL <- "(.|\n)*?"\n', src, perl = TRUE))
  expect_true(nzchar(sql))
  expect_match(sql, "full_rosters r")
  expect_match(sql, "r\\.game_id\\s*=\\s*\\$1")
})

test_that("the Israeli ribbon SQL carries no type_lineup predicate", {
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  sql <- regmatches(src, regexpr('RIBBON_SQL_ISRAEL <- "(.|\n)*?"\n', src, perl = TRUE))
  expect_true(nzchar(sql))
  expect_false(grepl("type_lineup\\s*=", sql))
  expect_false(grepl("type_lineup\\s+IS\\s+NOT\\s+NULL", sql, ignore.case = TRUE))
  expect_false(grepl("GROUP BY[^)]*type_lineup", sql, ignore.case = TRUE))
})

test_that("euro segment lineups resolve to one player-id set", {
  skip_on_cran()
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")

  con <- DBI::dbConnect(RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
    connect_timeout = 15L, bigint = "numeric")
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  unresolved <- DBI::dbGetQuery(con, "
    WITH seg AS (
      SELECT DISTINCT game_id, team_id, own_lineup
      FROM euroleague.matchup_segments_actions WHERE segment_seconds > 0)
    SELECT COUNT(*) AS n FROM seg s
    LEFT JOIN (SELECT DISTINCT game_id, team_id, own_lineup, player_ids
               FROM euroleague.lineup_totals_by_game) l
      ON l.game_id=s.game_id AND l.team_id=s.team_id AND l.own_lineup=s.own_lineup
    WHERE l.player_ids IS NULL")
  expect_identical(as.numeric(unresolved$n[1]), 0)
})

test_that("the ribbon segment count matches type-lineup-absent grouping", {
  skip_on_cran()
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")

  con <- DBI::dbConnect(RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
    connect_timeout = 15L, bigint = "numeric")
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  counts <- DBI::dbGetQuery(con, "
    SELECT
      (SELECT COUNT(*) FROM (
         SELECT team_id, segment_id, lineup_hash
         FROM basketball_test.df_pts_poss_lineups_longer_mv WHERE game_id=115
         GROUP BY team_id, segment_id, lineup_hash HAVING MAX(segment_seconds)>0) a)
         AS all_perspectives,
      (SELECT COUNT(*) FROM (
         SELECT team_id, segment_id, lineup_hash
         FROM basketball_test.df_pts_poss_lineups_longer_mv
         WHERE game_id=115 AND type_lineup='offense'
         GROUP BY team_id, segment_id, lineup_hash HAVING MAX(segment_seconds)>0) b)
         AS offense_only")
  expect_gt(counts$all_perspectives[1], counts$offense_only[1])
})
