# Gameflow stint ratings (docs/shared_definitions.md, "Gameflow stint rating")
# are summed in the web app from the readers' `events` rows. Those rows must
# count the On/Off way (credited lineup), so for every player the ON sums of
# his rows equal player_four_factors_by_game, and each team's possession ends
# equal team_metrics_by_game_mv. Run with RUN_DB_TESTS=1 on port 5432.

events_db_con <- function() {
  DBI::dbConnect(RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
    connect_timeout = 15L, bigint = "numeric")
}

# Evaluate just the reader's assignment out of global.R (the suite does not
# source it): the real constant, never a copy.
events_reader_sql <- function(league) {
  const <- if (identical(league, "israel")) "RIBBON_SQL_ISRAEL" else "RIBBON_SQL_EURO"
  src <- paste(readLines(testthat::test_path("..", "..", "R", "global.R"),
                         warn = FALSE), collapse = "\n")
  assign_txt <- regmatches(src, regexpr(paste0(const, ' <- "(.|\n)*?"\n'),
                                        src, perl = TRUE))
  stopifnot(nzchar(assign_txt))
  eval(parse(text = assign_txt))
}

# Trusted integer fixtures, interpolated client-side (pooler-safe).
events_for <- function(con, league, game_id, team_id) {
  sql <- events_reader_sql(league)
  sql <- gsub("$1", "?game_id", sql, fixed = TRUE)
  sql <- gsub("$2", "?team_id", sql, fixed = TRUE)
  row <- DBI::dbGetQuery(con, DBI::sqlInterpolate(con, sql,
                                                  game_id = game_id,
                                                  team_id = team_id))
  expect_true("events" %in% names(row), info = "reader returns an events column")
  if (!"events" %in% names(row) || is.na(row$events[1])) return(NULL)
  jsonlite::fromJSON(row$events[1], simplifyVector = FALSE)
}

# Sum one perspective's rows for one player: list(poss, pts).
events_sums <- function(ev, team_id, player_id, off) {
  keep <- Filter(function(e) e$team_id == team_id && isTRUE(e$off) == off &&
                   player_id %in% unlist(e$players), ev)
  c(poss = sum(vapply(keep, function(e) as.numeric(isTRUE(e$poss_end)), 0)),
    pts = sum(vapply(keep, function(e) as.numeric(e$pts), 0)))
}

EVENTS_LEAGUES <- list(
  israel = list(
    schema = "basketball_test",
    games = function(con) c(673L, 390L),
    team_poss = "SELECT team_id, off_poss_raw AS poss
                 FROM basketball_test.team_metrics_by_game_mv
                 WHERE game_id = %d"),
  euroleague = list(
    schema = "euroleague",
    # The latest published game of each competition.
    games = function(con) DBI::dbGetQuery(con, "
      SELECT DISTINCT ON (competition) game_id
      FROM euroleague.final_schedule_mv
      ORDER BY competition, game_date DESC, game_id DESC")$game_id,
    team_poss = NULL)
)

check_events_league <- function(con, league) {
  cfg <- EVENTS_LEAGUES[[league]]
  for (gid in as.integer(cfg$games(con))) {
    ff <- DBI::dbGetQuery(con, sprintf("
      SELECT team_id, player_id, type_lineup,
             SUM(total_poss) AS poss, SUM(total_points) AS pts
      FROM %s.player_four_factors_by_game
      WHERE game_id = %d AND is_on_key = 1
        AND type_lineup IN ('offense', 'defense')
      GROUP BY team_id, player_id, type_lineup", cfg$schema, gid))
    expect_gt(nrow(ff), 0)
    teams <- unique(as.integer(ff$team_id))
    ev <- events_for(con, league, gid, teams[1])
    expect_false(is.null(ev), info = sprintf("%s game %d has events", league, gid))
    if (is.null(ev)) next
    for (k in seq_len(nrow(ff))) {
      got <- events_sums(ev, ff$team_id[k], ff$player_id[k],
                         ff$type_lineup[k] == "offense")
      lbl <- sprintf("%s game %d team %s player %s %s", league, gid,
                     ff$team_id[k], ff$player_id[k], ff$type_lineup[k])
      expect_equal(unname(got["poss"]), as.numeric(ff$poss[k]), info = paste(lbl, "poss"))
      expect_equal(unname(got["pts"]), as.numeric(ff$pts[k]), info = paste(lbl, "pts"))
    }
    if (!is.null(cfg$team_poss)) {
      tp <- DBI::dbGetQuery(con, sprintf(cfg$team_poss, gid))
      for (k in seq_len(nrow(tp))) {
        mine <- Filter(function(e) e$team_id == tp$team_id[k] && isTRUE(e$off), ev)
        n <- sum(vapply(mine, function(e) as.numeric(isTRUE(e$poss_end)), 0))
        expect_equal(n, as.numeric(tp$poss[k]),
                     info = sprintf("game %d team %s team possessions", gid, tp$team_id[k]))
      }
    }
  }
}

test_that("Israeli Gameflow events match On/Off per player and team possessions", {
  skip_if_not(identical(Sys.getenv("RUN_DB_TESTS"), "1"), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")
  con <- events_db_con()
  on.exit(DBI::dbDisconnect(con))
  check_events_league(con, "israel")
})
