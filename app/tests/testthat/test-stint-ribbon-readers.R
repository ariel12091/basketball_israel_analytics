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

# This guard deliberately reads euroleague.ribbon_segments_v, NOT the base
# table euroleague.matchup_segments_actions. app_readonly (the role the
# deployed app actually runs as) is denied on the base table on purpose -- the
# EuroLeague schema keeps raw/derived-fact tables closed and exposes only a
# curated read layer, of which this view is the entire point (see
# sql/euroleague/ribbon_views.sql). A guard that only runs under an elevated
# ETL role isn't guarding what the application sees, and it fails outright
# (not skip) for anyone who runs the suite with app/.Renviron credentials. Do
# not point this back at the base table.
test_that("euro segment lineups resolve to one player-id set", {
  skip_if_not(nzchar(Sys.getenv("RUN_DB_TESTS")), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")

  con <- DBI::dbConnect(RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
    connect_timeout = 15L, bigint = "numeric")
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # Three checks in one round trip against the view:
  #   - empty_ids: a segment whose player_ids came back NULL or empty means
  #     the join in ribbon_segments_v missed -- a lane set with no players.
  #   - fanout: (game_id, team_id, segment_id) appearing more than once means
  #     the own_lineup join produced two different player-id sets for the
  #     same segment -- a player would get duplicate lanes.
  #   - total_rows: a bare "zero violations" on empty_ids/fanout would also
  #     pass vacuously if the view were empty or the filter wrong, so this
  #     asserts a healthy lower bound too -- it is not sufficient on its own,
  #     but it is a necessary companion to the other two.
  counts <- DBI::dbGetQuery(con, "
    SELECT
      (SELECT COUNT(*) FROM euroleague.ribbon_segments_v
         WHERE player_ids IS NULL OR cardinality(player_ids) = 0) AS empty_ids,
      (SELECT COUNT(*) FROM (
         SELECT game_id, team_id, segment_id
         FROM euroleague.ribbon_segments_v
         GROUP BY game_id, team_id, segment_id
         HAVING COUNT(*) > 1) dup) AS fanout,
      (SELECT COUNT(*) FROM euroleague.ribbon_segments_v) AS total_rows")

  expect_identical(as.numeric(counts$empty_ids[1]), 0)
  expect_identical(as.numeric(counts$fanout[1]), 0)
  expect_gt(counts$total_rows[1], 1000)
})

test_that("the ribbon segment count matches type-lineup-absent grouping", {
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
