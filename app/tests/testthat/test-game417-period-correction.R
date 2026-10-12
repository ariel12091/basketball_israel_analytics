game417_correction_env <- function() {
  env <- new.env(parent = globalenv())
  env$fill <- tidyr::fill
  wanted <- c("KNOWN_PERIOD_LABEL_CORRECTIONS", "apply_known_period_label_corrections",
              "compute_lineups_lookup", "compute_stints", "add_terminal_stint_join_end")
  assignments <- Filter(function(e) is.call(e) && identical(e[[1]], as.name("<-")) &&
    as.character(e[[2]]) %in% wanted,
    parse(repo_file("..", "etl", "etl_onoff.R"), keep.source = FALSE))
  expect_length(assignments, length(wanted))
  invisible(lapply(assignments, eval, envir = env))
  sys.source(repo_file("..", "etl", "period_opening_anchors.R"), envir = env)
  sys.source(repo_file("..", "etl", "lineup_coverage.R"), envir = env)
  env
}

game417_actions <- function(env, corrected = TRUE) {
  fixture <- jsonlite::fromJSON(repo_file("tests", "fixtures", "game417-period-labels.json"))
  actions <- fixture$actions
  if (corrected) actions <- env$apply_known_period_label_corrections(actions, 417L)
  parts <- strsplit(actions$quarter_time, ":", fixed = TRUE)
  actions$end_quarter_seconds_remaining <- vapply(parts, function(x)
    as.numeric(x[[1]]) * 60 + as.numeric(x[[2]]), numeric(1))
  actions$end_game_seconds_remaining <- actions$end_quarter_seconds_remaining +
    (4 - actions$quarter) * 600
  list(actions = actions, roster = fixture$roster)
}

test_that("game 417 changes only its ten mislabeled Q3 IN declarations", {
  env <- game417_correction_env()
  fixture <- jsonlite::fromJSON(repo_file("tests", "fixtures", "game417-period-labels.json"))
  original <- fixture$actions
  corrected <- env$apply_known_period_label_corrections(original, 417L)
  changed <- original$quarter != corrected$quarter
  expect_identical(original$id[changed], 4170541L:4170550L)
  expect_true(all(original$type[changed] == "substitution"))
  expect_true(all(original$quarter_time[changed] == "10:00"))
  expect_true(all(corrected$quarter[changed] == 3L))
  expect_identical(corrected[!changed, ], original[!changed, ])
  expect_identical(corrected[, setdiff(names(corrected), "quarter")],
                   original[, setdiff(names(original), "quarter")])
  expect_identical(env$apply_known_period_label_corrections(corrected, 417L), corrected)
  expect_identical(env$apply_known_period_label_corrections(original, 418L), original)
  timed <- game417_actions(env)$actions
  expect_equal(timed$end_game_seconds_remaining[changed], rep(1200, 10L))
  expect_equal(timed$team_score, original$team_score)
})

test_that("quarter metadata follows the correction without changing repaired labels", {
  env <- game417_correction_env()
  rows <- data.frame(id = c(4170540L, 4170541L, 4170550L, 4170551L, 4170542L),
    quarter = c(2L, 2L, 2L, 2L, 3L),
    parameters_current_quarter = c(2L, 2L, NA, 2L, 3L),
    parameters_quarter = c(2L, NA, 2L, 2L, 3L))
  corrected <- env$apply_known_period_label_corrections(rows, 417L)
  expect_identical(corrected$quarter, c(2L, 3L, 3L, 2L, 3L))
  expect_identical(corrected$parameters_current_quarter, c(2L, 3L, NA, 2L, 3L))
  expect_identical(corrected$parameters_quarter, c(2L, NA, 3L, 2L, 3L))
})

# Execute the real production SQL over CTE fixture inputs, never application
# tables. Only the table-binding assignments are injected; window, deduplication,
# anchor and stint logic are the unchanged production function bodies.
game417_sql_replay <- function(env, pg, corrected) {
  x <- game417_actions(env, corrected)
  env$sched_subset <- data.frame(game_id = 417L)
  env$fixture_actions <- dbplyr::tbl_lazy(x$actions, con = pg,
                                        name = "fixture_actions")
  env$fixture_roster <- dbplyr::tbl_lazy(x$roster, con = pg,
                                       name = "fixture_roster")
  fn <- env$compute_lineups_lookup
  expect_identical(body(fn)[[2]][[2]], as.name("actions"))
  expect_identical(body(fn)[[4]][[2]], as.name("full_rosters"))
  body(fn)[[2]] <- quote(actions <- fixture_actions)
  body(fn)[[4]] <- quote(full_rosters <- fixture_roster)
  sql <- as.character(dbplyr::sql_render(fn(NULL)))
  actions_cte <- paste0(
    'fixture_actions AS (SELECT * FROM jsonb_to_recordset($1::jsonb) AS a(',
    'game_id integer, id integer, quarter integer, quarter_time text, type text, ',
    'team_id integer, player_id integer, parameters_player_in integer, ',
    'parameters_player_out integer, user_time text, team_score numeric, ',
    'end_quarter_seconds_remaining numeric, end_game_seconds_remaining numeric))')
  roster_cte <- paste0(
    'fixture_roster AS (SELECT * FROM jsonb_to_recordset($2::jsonb) AS r(',
    'game_id integer, game_year integer, team_id integer, player_id integer, starter boolean))')
  lineups <- DBI::dbGetQuery(pg, paste('WITH', actions_cte, ',', roster_cte, sql),
    params = list(jsonlite::toJSON(x$actions, dataframe = "rows", na = "null"),
                  jsonlite::toJSON(x$roster, dataframe = "rows", na = "null")))
  lineups <- env$apply_period_anchor_gates(lineups, x$actions,
    dplyr::distinct(x$roster, game_id, team_id))
  env$fixture_states <- dbplyr::tbl_lazy(lineups, con = pg,
                                       name = "fixture_states")
  fn <- env$compute_stints
  expect_identical(body(fn)[[2]][[2]], as.name("poss"))
  expect_identical(body(fn)[[3]][[2]], as.name("ll"))
  body(fn)[[2]] <- quote(poss <- fixture_actions)
  body(fn)[[3]] <- quote(ll <- fixture_states)
  sql <- as.character(dbplyr::sql_render(fn(NULL)))
  states_cte <- paste0(
    'fixture_states AS (SELECT * FROM jsonb_to_recordset($2::jsonb) AS l(',
    'id integer, game_id integer, team_id integer, quarter integer, quarter_time text, ',
    'end_game_seconds_remaining numeric, lineup_id text, lineup_hash text))')
  stints <- DBI::dbGetQuery(pg, paste('WITH', actions_cte, ',', states_cte, sql),
    params = list(jsonlite::toJSON(x$actions, dataframe = "rows", na = "null"),
                  jsonlite::toJSON(lineups, dataframe = "rows", na = "null"))) |>
    dplyr::transmute(team_id = team_id_offense, game_id, q_bucket, final_start_id,
      final_end_id, segment_id, team_id_defense, lineup_hash_offense, lineup_hash_defense)
  pws <- dplyr::left_join(dplyr::mutate(x$actions, q_bucket = quarter),
    env$add_terminal_stint_join_end(stints),
    dplyr::join_by(team_id, game_id, q_bucket,
                  between(id, final_start_id, .join_end_id, bounds = "[)")))
  list(actions = x$actions, pws = pws, lineups = lineups)
}

test_that("the real SQL restores Q2/Q3 openings and passes all gameplay coverage", {
  skip_if(Sys.getenv("RUN_DB_TESTS", "0") != "1", "RUN_DB_TESTS not enabled")
  readRenviron(repo_file(".Renviron"))
  pg <- DBI::dbConnect(RPostgres::Postgres(), host = Sys.getenv("PG_HOST"),
    port = as.integer(Sys.getenv("PG_PORT", "6543")), dbname = Sys.getenv("PG_DB"),
    user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
    sslmode = Sys.getenv("PG_SSLMODE", "require"), bigint = "numeric")
  on.exit(DBI::dbDisconnect(pg), add = TRUE)
  DBI::dbExecute(pg, "SET default_transaction_read_only = on")
  DBI::dbExecute(pg, "SET statement_timeout = '30s'")
  env <- game417_correction_env()
  before <- game417_sql_replay(env, pg, FALSE)
  expect_equal(nrow(before$lineups), 932L)
  expect_error(env$assert_gameplay_lineup_coverage(
    before$actions, before$pws, before$lineups), "Gameplay lineup coverage failed")
  after <- game417_sql_replay(env, pg, TRUE)
  openings <- after$lineups |>
    dplyr::filter(quarter %in% 2:3, quarter_time == "10:00") |>
    dplyr::distinct(team_id, quarter, lineup_id, n_on)
  expect_equal(nrow(openings), 4L)
  expect_true(all(openings$n_on == 5))
  expect_invisible(env$assert_gameplay_lineup_coverage(
    after$actions, after$pws, after$lineups))
  scoring <- after$pws |> dplyr::group_by(team_id) |>
    dplyr::summarise(points = sum(team_score, na.rm = TRUE), .groups = "drop") |>
    dplyr::filter(team_id %in% c(8L, 12L)) |> dplyr::arrange(team_id)
  expect_equal(scoring$points, c(85, 86))
})
