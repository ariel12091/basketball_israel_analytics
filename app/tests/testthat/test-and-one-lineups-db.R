library(testthat)

# A made and-one whose missed free throw the same team rebounds is one possession,
# not two (D30). The backfill removes the made shot's possession end. For a lineup
# that held that shot, the possession goes and the points stay: the points belong to
# the credited lineup, the possession to the end that follows. See
# docs/superpowers/plans/2026-11-03-and-one-own-rebound-possession.md.
#
# Expected values were read from df_pts_poss_lineups_longer_mv BEFORE the backfill
# (possessions: offense rows with final_end_poss, grouped on credit_lineup_hash;
# points: SUM(team_score) on the same rows; defensive possessions: defense rows with
# final_end_poss on the opponent's credited lineup). Each target below is a made
# shot whose credited lineup differs from the credited lineup of the next end.

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
  sslmode = Sys.getenv("PG_SSLMODE", unset = "require"),
  bigint = "numeric"
)
on.exit(DBI::dbDisconnect(con), add = TRUE)

# Game 158 (2026) is the one target whose shot lineup reaches zero possessions.
# One 2025, one 2026 and one 2027 target besides.
AND_ONE_TARGETS <- data.frame(
  game_year = c(2026, 2025, 2026, 2027),
  game_id = c(158, 62473, 61, 401),
  team_id = c(15, 28, 4, 33),
  shot_id = c(1580385, 624730749, 610132, 4010443),
  next_end_id = c(1580393, 624730757, 610146, 4010452),
  shot_lineup = c("ac788b5ac41465392e90de8ab94b0ff4", "5933b9b7c1f1c6dcee48d7de40697271",
                  "7949e3ada7da5146c9cb9151715cc79f", "0cdb46bb536c43600f386ff280026ba8"),
  next_lineup = c("95f44e338ff6a432b790f656af40fff5", "0e81c7b1c3d0de80a2822cd143e54959",
                  "071dbc54775bf255a015bd41a19d49bf", NA_character_),
  opp_team_id = c(8, 13, 8, 11),
  opp_lineup = c("2f0e02fa98196cd0e3a3fd9a23149225", "444d19ce9f33306f56d684cc883f7318",
                 "3262cebf30b717f2a817a6f19ff965e0", "5b5667df2047bdcede8713244cd1a992"),
  shot_off_poss = c(1, 9, 6, 3),
  shot_off_pts = c(2, 18, 10, 5),
  next_off_poss = c(4, 3, 1, NA),
  next_off_pts = c(4, 2, 0, NA),
  opp_def_poss = c(4, 5, 4, 6),
  stringsAsFactors = FALSE
)

lineup_counts <- function(game_id, team_id, lineup, type) {
  r <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) FILTER (WHERE final_end_poss) AS poss,
           COALESCE(SUM(team_score), 0) AS pts
    FROM basketball_test.df_pts_poss_lineups_longer_mv
    WHERE game_id = $1 AND team_id = $2 AND credit_lineup_hash = $3 AND type_lineup = $4",
    params = list(as.integer(game_id), as.integer(team_id), lineup, type))
  c(poss = as.numeric(r$poss), pts = as.numeric(r$pts))
}

test_that("the made and-one shot is no longer a possession end", {
  for (i in seq_len(nrow(AND_ONE_TARGETS))) {
    t <- AND_ONE_TARGETS[i, ]
    row <- DBI::dbGetQuery(con, "
      SELECT final_end_poss FROM basketball_test.df_pts_poss_lineups_longer_mv
      WHERE game_id = $1 AND id = $2 AND type_lineup = 'offense'",
      params = list(as.integer(t$game_id), as.numeric(t$shot_id)))
    expect_equal(nrow(row), 1L, info = paste("shot", t$shot_id))
    expect_false(isTRUE(row$final_end_poss[1]), info = paste("shot", t$shot_id, "still an end"))
    nxt <- DBI::dbGetQuery(con, "
      SELECT final_end_poss FROM basketball_test.df_pts_poss_lineups_longer_mv
      WHERE game_id = $1 AND id = $2 AND type_lineup = 'offense'",
      params = list(as.integer(t$game_id), as.numeric(t$next_end_id)))
    expect_true(isTRUE(nxt$final_end_poss[1]), info = paste("next end", t$next_end_id, "must stay an end"))
  }
})

test_that("the shot's lineup loses one possession and keeps its points", {
  for (i in seq_len(nrow(AND_ONE_TARGETS))) {
    t <- AND_ONE_TARGETS[i, ]
    got <- lineup_counts(t$game_id, t$team_id, t$shot_lineup, "offense")
    expect_equal(unname(got["poss"]), t$shot_off_poss - 1, info = paste("shot", t$shot_id, "possessions"))
    expect_equal(unname(got["pts"]), t$shot_off_pts, info = paste("shot", t$shot_id, "points"))
  }
})

test_that("the next end's lineup is unchanged", {
  has_next <- !is.na(AND_ONE_TARGETS$next_lineup)
  for (i in which(has_next)) {
    t <- AND_ONE_TARGETS[i, ]
    got <- lineup_counts(t$game_id, t$team_id, t$next_lineup, "offense")
    expect_equal(unname(got["poss"]), t$next_off_poss, info = paste("next end", t$next_end_id, "possessions"))
    expect_equal(unname(got["pts"]), t$next_off_pts, info = paste("next end", t$next_end_id, "points"))
  }
})

test_that("the opponent's matching lineup has one fewer defensive possession", {
  for (i in seq_len(nrow(AND_ONE_TARGETS))) {
    t <- AND_ONE_TARGETS[i, ]
    got <- lineup_counts(t$game_id, t$opp_team_id, t$opp_lineup, "defense")
    expect_equal(unname(got["poss"]), t$opp_def_poss - 1, info = paste("shot", t$shot_id, "defensive possessions"))
  }
})

# Game 158, team 15, lineup ac788b5a...: its only offensive possession in 2026 was
# the made and-one. After the backfill the season total is 0 possessions, so the
# reader keeps the row and its 2 points and shows no offensive rating (the zero
# case: NULLIF on a zero denominator), rather than 2 / 0 or a stale 200.
test_that("a lineup whose only possession was removed keeps its season row and points", {
  r <- DBI::dbGetQuery(con, "
    SELECT off_poss, off_pts, off_ppp
    FROM basketball_test.fetch_lineups_csv_v2(5, NULL::text, NULL::text, NULL::text, FALSE,
                                              NULL::date, NULL::date, 0, 2026)
    WHERE team_id = 15 AND sub_lineup_hash = 'ac788b5ac41465392e90de8ab94b0ff4'")
  expect_equal(nrow(r), 1L)
  expect_equal(as.numeric(r$off_poss), 0)
  expect_equal(as.numeric(r$off_pts), 2)
  expect_true(is.na(r$off_ppp))
})
