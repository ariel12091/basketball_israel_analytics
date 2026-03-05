library(testthat)

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

game_ids <- c(159L, 160L, 161L, 162L, 163L)

test_that("data parity: off oreb opportunities in team_metrics match direct lineup FF", {
  q <- "
    WITH direct AS (
      SELECT
        team_id,
        game_id,
        COALESCE(SUM(oreb_opportunities) FILTER (WHERE type_lineup = 'offense'), 0)::numeric AS off_oreb_opp_direct
      FROM basketball_test.lineup_four_factors_by_game
      WHERE game_id = ANY($1::int4[])
      GROUP BY team_id, game_id
    ),
    mv AS (
      SELECT
        team_id,
        game_id,
        COALESCE(off_oreb_opp_raw, 0)::numeric AS off_oreb_opp_mv
      FROM basketball_test.team_metrics_by_game_mv
      WHERE game_id = ANY($1::int4[])
    )
    SELECT
      COALESCE(mv.game_id, direct.game_id) AS game_id,
      COALESCE(mv.team_id, direct.team_id) AS team_id,
      direct.off_oreb_opp_direct,
      mv.off_oreb_opp_mv
    FROM mv
    FULL OUTER JOIN direct
      ON direct.game_id = mv.game_id
     AND direct.team_id = mv.team_id
    WHERE COALESCE(direct.off_oreb_opp_direct, -1) <> COALESCE(mv.off_oreb_opp_mv, -1)
    ORDER BY 1,2
  "
  mismatches <- DBI::dbGetQuery(con, q, params = list(game_ids))
  expect_equal(nrow(mismatches), 0, info = paste(capture.output(print(mismatches)), collapse = "\n"))
})

test_that("data parity: off oreb count in team_metrics matches direct lineup FF", {
  q <- "
    WITH direct AS (
      SELECT
        team_id,
        game_id,
        COALESCE(SUM(oreb_count) FILTER (WHERE type_lineup = 'offense'), 0)::numeric AS off_oreb_cnt_direct
      FROM basketball_test.lineup_four_factors_by_game
      WHERE game_id = ANY($1::int4[])
      GROUP BY team_id, game_id
    ),
    mv AS (
      SELECT
        team_id,
        game_id,
        COALESCE(off_oreb_count_raw, 0)::numeric AS off_oreb_cnt_mv
      FROM basketball_test.team_metrics_by_game_mv
      WHERE game_id = ANY($1::int4[])
    )
    SELECT
      COALESCE(mv.game_id, direct.game_id) AS game_id,
      COALESCE(mv.team_id, direct.team_id) AS team_id,
      direct.off_oreb_cnt_direct,
      mv.off_oreb_cnt_mv
    FROM mv
    FULL OUTER JOIN direct
      ON direct.game_id = mv.game_id
     AND direct.team_id = mv.team_id
    WHERE COALESCE(direct.off_oreb_cnt_direct, -1) <> COALESCE(mv.off_oreb_cnt_mv, -1)
    ORDER BY 1,2
  "
  mismatches <- DBI::dbGetQuery(con, q, params = list(game_ids))
  expect_equal(nrow(mismatches), 0, info = paste(capture.output(print(mismatches)), collapse = "\n"))
})
