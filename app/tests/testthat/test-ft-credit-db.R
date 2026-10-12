library(testthat)

# Free throws are credited to the lineup that drew the foul. The base table
# records that in credit_* columns next to the physical lineup columns; every
# aggregation of events groups on the credit columns, floor time on the
# physical ones. See docs/superpowers/plans/2026-10-12-free-throw-foul-lineup-attribution.md.

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

test_that("every base-table row carries credit columns", {
  bad <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM basketball_test.df_pts_poss_lineups_longer_mv
    WHERE credit_lineup_hash IS NULL OR credit_segment_id IS NULL")
  expect_equal(as.numeric(bad$n), 0)
})

test_that("all free throws of one trip are credited to one segment", {
  split <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM (
      SELECT 1 FROM basketball_test.df_pts_poss_lineups_longer_mv
      WHERE type = 'freeThrow'
      GROUP BY game_id, team_id, type_lineup, parent_action_id
      HAVING COUNT(DISTINCT (credit_lineup_hash, credit_segment_id)) > 1) x")
  expect_equal(as.numeric(split$n), 0)
})

test_that("team Four Factors counts each free-throw trip once", {
  bad <- DBI::dbGetQuery(con, "
    WITH expected AS (
      SELECT a.game_id, a.team_id,
             COUNT(*) FILTER (WHERE a.type = 'shot')
             + COUNT(DISTINCT a.parent_action_id) FILTER (
                 WHERE a.type = 'freeThrow' AND p.id IS NOT NULL) AS ts_poss
      FROM basketball_test.df_pts_poss_lineups_longer_mv a
      LEFT JOIN basketball_test.df_pts_poss_lineups_longer_mv p
        ON p.game_id = a.game_id AND p.id = a.parent_action_id
       AND p.type = 'foul' AND p.parameters_type = 'personal'
       AND p.team_id = a.team_id AND p.type_lineup = a.type_lineup
      WHERE a.type_lineup = 'offense'
      GROUP BY a.game_id, a.team_id
    )
    SELECT COUNT(*) AS n
    FROM basketball_test.team_metrics_by_game_mv tm
    JOIN expected e ON e.game_id = tm.game_id AND e.team_id = tm.team_id
    WHERE tm.off_ts_poss_raw <> e.ts_poss")
  expect_equal(as.numeric(bad$n), 0)
})

test_that("a player's ON and OFF Four Factors rows add up to the team's", {
  # Compared through the best-covered player of each team-game: lineups_lookup
  # also carries partial rows for players who were not on that game's roster,
  # and their ON+OFF sums never reach the team's.
  bad <- DBI::dbGetQuery(con, "
    WITH split AS (
      SELECT game_id, team_id, player_id, type_lineup,
             SUM(ts_poss_count) AS ts_poss, SUM(total_points) AS pts, SUM(total_poss) AS poss
      FROM basketball_test.player_four_factors_by_game
      GROUP BY game_id, team_id, player_id, type_lineup
    ),
    best AS (
      SELECT game_id, team_id, type_lineup,
             MAX(ts_poss) AS ts_poss, MAX(pts) AS pts, MAX(poss) AS poss
      FROM split GROUP BY game_id, team_id, type_lineup
    ),
    team AS (
      SELECT game_id, team_id, type_lineup,
             SUM(ts_poss_count) AS ts_poss, SUM(total_points) AS pts, SUM(total_poss) AS poss
      FROM basketball_test.lineup_four_factors_by_game
      GROUP BY game_id, team_id, type_lineup
    )
    SELECT COUNT(*) AS n FROM best b JOIN team t USING (game_id, team_id, type_lineup)
    WHERE b.ts_poss <> t.ts_poss OR b.pts <> t.pts OR b.poss <> t.poss")
  expect_equal(as.numeric(bad$n), 0)
})

test_that("lineup day totals add up to the team's points and possessions", {
  bad <- DBI::dbGetQuery(con, "
    WITH lineups AS (
      SELECT game_id, team_id, type_lineup,
             SUM(total_pts) AS pts, SUM(total_poss) AS poss
      FROM basketball_test.mv_lineup_totals_by_day
      GROUP BY game_id, team_id, type_lineup
    ),
    base AS (
      SELECT game_id, team_id, type_lineup,
             SUM(COALESCE(team_score, 0)) AS pts,
             COUNT(*) FILTER (WHERE final_end_poss) AS poss
      FROM basketball_test.df_pts_poss_lineups_longer_mv
      GROUP BY game_id, team_id, type_lineup
    )
    SELECT COUNT(*) AS n FROM lineups l JOIN base b USING (game_id, team_id, type_lineup)
    WHERE l.pts <> b.pts OR l.poss <> b.poss")
  expect_equal(as.numeric(bad$n), 0)
})
