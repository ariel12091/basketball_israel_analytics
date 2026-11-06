# The golden harness replays recorded database answers so the real app server
# runs offline and deterministically. These tests pin its contract: a recorded
# query replays exactly, anything unrecorded fails loudly, and the app's own
# db_get_query() reaches the replay connection through the injected pool.

source(repo_file("tests", "goldens", "harness.R"), local = TRUE)

golden_tmp_dir <- function() {
  dir <- tempfile("golden-fixtures-")
  dir.create(dir)
  dir
}

write_fixture <- function(dir, statement, params, value) {
  saveRDS(value, file.path(dir, paste0(golden_query_key(statement, params), ".rds")),
          compress = "xz")
}

recorded_sql <- "SELECT 1 AS x WHERE $1 = 1"

test_that("replay returns a recorded frame", {
  dir <- golden_tmp_dir()
  write_fixture(dir, recorded_sql, list(1L), data.frame(x = 1L))
  con <- golden_replay_conn(dir)
  expect_s4_class(con, "GoldenReplayConnection")
  expect_true(methods::is(con, "DBIConnection"))
  # Extra inner whitespace must not change the key.
  got <- DBI::dbGetQuery(con, "SELECT 1 AS x  WHERE $1 = 1", params = list(1L))
  expect_equal(got, data.frame(x = 1L))
  expect_true(DBI::dbIsValid(con))
  expect_identical(DBI::dbExecute(con, "SET statement_timeout = 1"), 0L)
})

test_that("replay miss is an error naming the query", {
  dir <- golden_tmp_dir()
  write_fixture(dir, recorded_sql, list(1L), data.frame(x = 1L))
  con <- golden_replay_conn(dir, strict = TRUE)
  expect_error(
    DBI::dbGetQuery(con, recorded_sql, params = list(2L)),
    "golden replay miss: SELECT 1 AS x WHERE \\$1 = 1 key=[0-9a-f]{40}"
  )
  # No params is a different key from list(1L), so also a miss.
  expect_error(DBI::dbGetQuery(con, recorded_sql), "golden replay miss")
})

test_that("query key normalises whitespace and stringifies params", {
  expect_identical(
    golden_query_key("SELECT  1\n  FROM t ", list(1L)),
    golden_query_key("SELECT 1 FROM t", list("1"))
  )
  expect_false(identical(
    golden_query_key("SELECT 1", list(1L)),
    golden_query_key("SELECT 1", list(2L))
  ))
})

test_that("record forwards to the real connection and writes the fixture", {
  src <- golden_tmp_dir()
  write_fixture(src, recorded_sql, list(1L), data.frame(x = 1L))
  out <- golden_tmp_dir()
  rec <- golden_record_conn(golden_replay_conn(src), out)
  expect_s4_class(rec, "GoldenRecordConnection")
  expect_equal(DBI::dbGetQuery(rec, recorded_sql, params = list(1L)), data.frame(x = 1L))
  key <- golden_query_key(recorded_sql, list(1L))
  expect_true(file.exists(file.path(out, paste0(key, ".rds"))))
  # What was recorded replays.
  expect_equal(
    DBI::dbGetQuery(golden_replay_conn(out), recorded_sql, params = list(1L)),
    data.frame(x = 1L)
  )
})

test_that("db_get_query goes through the replay connection", {
  dir <- golden_tmp_dir()
  write_fixture(dir, recorded_sql, list(1L), data.frame(x = 1L))
  env <- golden_app_env(golden_replay_conn(dir))
  expect_s4_class(env$pg_pool, "GoldenReplayConnection")
  expect_equal(
    env$db_get_query(env$pg_pool, recorded_sql, params = list(1L)),
    data.frame(x = 1L)
  )
  # The server is app.R's own, captured from its shinyApp() call.
  expect_true(is.function(env$server))
  expect_identical(names(formals(env$server)), c("input", "output", "session"))
  # Side effects of loading are undone.
  expect_null(getOption("ibpl.pg_pool"))
})

test_that("a replay miss inside the server fails the run", {
  # The app catches query errors in observers and logs them, so a miss would
  # otherwise only blank a table. golden_run_server() fails on any miss.
  con <- golden_replay_conn(golden_tmp_dir(), strict = TRUE)
  env <- new.env()
  env$pg_pool <- con
  env$server <- function(input, output, session) {
    observe(try(DBI::dbGetQuery(con, "SELECT 2"), silent = TRUE))
  }
  expect_error(
    golden_run_server(env, list(), function(session, output) "ran"),
    "golden replay: 1 unrecorded quer.*SELECT 2"
  )
  expect_identical(golden_replay_misses(con), character(0))
})

test_that("app code that checks out of the pool directly gets the connection", {
  con <- golden_replay_conn(golden_tmp_dir())
  expect_identical(pool::poolCheckout(con), con)
  expect_error(pool::poolReturn(con), NA)
})

test_that("golden_app_env runs are independent", {
  dir <- golden_tmp_dir()
  env1 <- golden_app_env(golden_replay_conn(dir))
  env1$cached_ref_query("k", function() "first")
  env1$GL_DATA_CACHE$set("k", "first")
  env2 <- golden_app_env(golden_replay_conn(dir))
  expect_identical(env2$cached_ref_query("k", function() "second"), "second")
  expect_true(cachem::is.key_missing(env2$GL_DATA_CACHE$get("k")))
})

toy_dt_server <- function(input, output, session) {
  output$tbl <- DT::renderDT({
    df <- data.frame(a = 1:3 * input$mult, b = letters[1:3], c = c(9, 8, 7))
    # Bare datatable(): the attached package:DT copy, as much of the app calls it.
    datatable(
      df, rownames = FALSE, extensions = "Buttons",
      options = list(
        dom = "Bt",
        buttons = csv_export_button("toy_slug"),
        order = list(list(0, "desc")),
        columnDefs = list(list(targets = 2, visible = FALSE),
                          list(targets = "_all", className = "dt-center"))
      )
    )
  })
  output$other <- DT::renderDT(DT::datatable(
    data.frame(z = 1), options = list(buttons = csv_export_button("other_slug"))
  ))
}

test_that("golden_capture_dt captures the matching datatable", {
  env <- new.env()
  env$server <- toy_dt_server
  got <- golden_run_server(env, list(mult = 2L), golden_capture_dt("toy_slug"))
  expect_equal(got$data, data.frame(a = c(2L, 4L, 6L), b = letters[1:3], c = c(9, 8, 7)))
  expect_equal(got$order, list(list(0, "desc")))
  expect_identical(got$hidden, "c")
  # The trace is removed after the run, from both copies.
  expect_false(inherits(DT::datatable, "functionWithTrace"))
  expect_false(inherits(get("datatable", "package:DT"), "functionWithTrace"))
  # A namespace-qualified DT::datatable() call is captured too.
  other <- golden_run_server(env, list(mult = 1L), golden_capture_dt("other_slug"))
  expect_equal(other$data, data.frame(z = 1))
  expect_identical(other$hidden, character(0))
})

test_that("hidden columns count the rownames column when it is shown", {
  df <- data.frame(a = 1, b = 2, c = 3)
  defs <- list(list(targets = c(1, 2), visible = FALSE),
               list(targets = 0, className = "dt-center"))
  expect_identical(golden_dt_hidden_cols(df, defs, rownames = FALSE), c("b", "c"))
  expect_identical(golden_dt_hidden_cols(df, defs, rownames = NULL), c("a", "b"))
  expect_identical(
    golden_dt_hidden_cols(df, list(list(targets = "b", visible = FALSE)), FALSE), "b"
  )
})

test_that("golden_capture_dt errors when no datatable matched", {
  env <- new.env()
  env$server <- toy_dt_server
  expect_error(
    golden_run_server(env, list(mult = 1L), golden_capture_dt("missing_slug")),
    "no datatable rendered for missing_slug"
  )
  expect_false(inherits(DT::datatable, "functionWithTrace"))
})

test_that("a table without a CSV slug can be identified by its column", {
  env <- new.env()
  env$server <- function(input, output, session) {
    output$wrong <- DT::renderDT(DT::datatable(data.frame(other = 9)))
    output$right <- DT::renderDT(DT::datatable(data.frame(`Net Diff` = 3, check.names = FALSE)))
  }
  got <- golden_run_server(env, list(), golden_capture_dt(has_col = "Net Diff"))
  expect_identical(names(got$data), "Net Diff")
  expect_equal(got$data[[1]], 3)
  expect_false(inherits(DT::datatable, "functionWithTrace"))
})

test_that("On/Off strict readers fail closed without widening Game Logs", {
  strict <- GOLDEN_STRICT_READERS_BY_TAB$onoff
  expect_true(golden_query_is_strict(strict, NULL, "run_onoff_compute_14"))
  expect_true(golden_query_is_strict(strict, NULL, "run_four_factors_compute"))
  expect_true(golden_query_is_strict(strict, NULL, "cached_season_df"))
  expect_true(golden_query_is_strict(strict, NULL, "fetch_player_traditional_filtered"))
  expect_false(golden_query_is_strict(strict, NULL, "home_dashboard_combined"))
  expect_identical(GOLDEN_STRICT_READERS_BY_TAB$game_logs, GOLDEN_STRICT_READERS)
})

# A connection whose every query fails, standing in for a live database error.
methods::setClass("GoldenFailingConnection", contains = "DBIConnection")
methods::setMethod("dbGetQuery", c("GoldenFailingConnection", "character"),
  function(conn, statement, params = NULL, ...) stop("relation does not exist"))

test_that("a recording query error is logged and fails the run", {
  # The app catches query errors and blanks the table, so without this a
  # golden could be captured from an errored state.
  rec <- golden_record_conn(methods::new("GoldenFailingConnection"), golden_tmp_dir())
  expect_error(DBI::dbGetQuery(rec, "SELECT 3"), "relation does not exist")
  expect_length(golden_record_errors(rec), 1L)
  expect_identical(golden_record_errors(rec), character(0))

  env <- new.env()
  env$pg_pool <- rec
  env$server <- function(input, output, session) {
    observe(try(DBI::dbGetQuery(rec, "SELECT 4"), silent = TRUE))
  }
  expect_error(
    golden_run_server(env, list(), function(session, output) "ran"),
    "golden record: 1 failed quer.*SELECT 4.*relation does not exist"
  )
  # Reported first even when the run then fails for a knock-on reason.
  expect_error(
    golden_run_server(env, list(), function(session, output) stop("no table")),
    "golden record: 1 failed quer.*SELECT 4"
  )
  # No fixture is written for a failed query.
  expect_length(list.files(rec@dir, pattern = "[.]rds$"), 0L)
})

test_that("recording writes an index of key, label, statement and params", {
  src <- golden_tmp_dir()
  write_fixture(src, recorded_sql, list(1L), data.frame(x = 1L))
  write_fixture(src, "SELECT 5", NULL, data.frame(y = 5L))
  out <- golden_tmp_dir()
  rec <- golden_record_conn(golden_replay_conn(src), out)
  DBI::dbGetQuery(rec, recorded_sql, params = list(1L))
  DBI::dbGetQuery(rec, recorded_sql, params = list(1L))
  DBI::dbGetQuery(rec, "SELECT 5")
  idx <- utils::read.csv(file.path(out, "index.csv"), colClasses = "character")
  expect_identical(names(idx), c("key", "label", "statement", "params"))
  expect_identical(nrow(idx), 2L)
  expect_setequal(idx$key, c(golden_query_key(recorded_sql, list(1L)),
                             golden_query_key("SELECT 5", NULL)))
  expect_identical(idx$params[idx$statement == recorded_sql], "1")
  expect_true(all(file.exists(file.path(out, paste0(idx$key, ".rds")))))
})

test_that("inputs given as steps are applied in order, flushing between", {
  env <- new.env()
  env$server <- function(input, output, session) {
    seen <- reactiveVal(character(0))
    observeEvent(input$a, seen(c(seen(), paste0("a", input$a))))
    observeEvent(input$b, seen(c(seen(), paste0("b", input$b, "/a", input$a))))
    session$userData$seen <- seen
  }
  got <- golden_run_server(env, list(list(a = 1), list(b = 2)),
                           function(session, output) isolate(session$userData$seen()))
  expect_identical(got, c("a1", "b2/a1"))
  expect_identical(golden_input_steps(list(a = 1)), list(list(a = 1)))
  expect_identical(golden_input_steps(list()), list())
})

test_that("golden_capture_dt returns NULL for no table only when allowed", {
  env <- new.env()
  env$server <- toy_dt_server
  expect_null(golden_run_server(env, list(mult = 1L),
                                golden_capture_dt("missing_slug", allow_none = TRUE)))
})

test_that("the app's random defaults repeat across loads and leave the caller's RNG alone", {
  # global.R draws the default Home team at random; unseeded, each load ran a
  # different team's Home queries and replay missed them.
  set.seed(1)
  before <- .Random.seed
  dir <- golden_tmp_dir()
  ids <- vapply(1:3, function(i) golden_app_env(golden_replay_conn(dir))$DEFAULT_HOME_TEAM_ID,
                character(1))
  expect_length(unique(ids), 1L)
  expect_identical(.Random.seed, before)
})

# ---------------- Strict replay is scoped to Game Logs ----------------
# Ruling R9: only the queries Game Logs depends on must be recorded; any other
# start-up query that misses answers with zero rows and a note.

test_that("Home's default team is pinned, whatever the catalogue draws", {
  env <- golden_app_env(golden_replay_conn(golden_tmp_dir()))
  expect_identical(env$DEFAULT_HOME_TEAM_ID, GOLDEN_HOME_TEAM_ID)
})

test_that("strictness follows the issuing function and, for lookups, the season", {
  strict <- GOLDEN_STRICT_READERS
  expect_true(golden_query_is_strict(strict, list(2026L), stack = c("f", "gl_query_schedule")))
  expect_true(golden_query_is_strict(strict, list(2031L), stack = "gl_query_lineup_ff"))
  expect_true(golden_query_is_strict(strict, NULL, stack = "etl_last_success_db"))
  expect_true(golden_query_is_strict(strict, list(2026L), stack = "fetch_gn_values"))
  expect_true(golden_query_is_strict(strict, list("E", 2025L), stack = "euro_fetch_teams"))
  # The same lookup for another season (the default season at start-up,
  # which moves with DEFAULT_GAME_YEAR) is not a Game Logs query.
  expect_false(golden_query_is_strict(strict, list(2027L), stack = "fetch_gn_values"))
  expect_false(golden_query_is_strict(strict, list("E", 2026L), stack = "euro_fetch_teams"))
  expect_false(golden_query_is_strict(strict, list(2026L), stack = c("hub_fetch", "db_get_query")))
  expect_true(golden_query_is_strict(TRUE, NULL, stack = character(0)))
  # Called as env$f() or pkg::f(), the function is still found.
  f <- function() golden_stack_functions()
  holder <- new.env()
  holder$gl_query_schedule <- function() f()
  expect_true("gl_query_schedule" %in% holder$gl_query_schedule())
  expect_error(golden_replay_conn(golden_tmp_dir(), strict = FALSE), "strict must be")
})

test_that("a miss outside the strict set answers zero rows and is noted once", {
  con <- golden_replay_conn(golden_tmp_dir())
  expect_message(got <- DBI::dbGetQuery(con, "SELECT 6"), "golden replay note: .*SELECT 6")
  expect_identical(got, data.frame())
  expect_message(DBI::dbGetQuery(con, "SELECT 6"), NA)
  expect_identical(golden_replay_notes(con), "SELECT 6")
  expect_identical(golden_replay_misses(con), character(0))
  # Inside the server it is not a failure either.
  env <- new.env()
  env$pg_pool <- con
  env$server <- function(input, output, session) observe(DBI::dbGetQuery(con, "SELECT 7"))
  expect_identical(
    suppressMessages(golden_run_server(env, list(), function(session, output) "ran")), "ran"
  )
})

test_that("a strict miss fails and names the re-record command", {
  con <- golden_replay_conn(golden_tmp_dir())
  gl_query_schedule <- function(gy) DBI::dbGetQuery(con, "SELECT 8 WHERE $1 = 1", params = list(gy))
  expect_error(
    gl_query_schedule(2026L),
    "golden replay miss: SELECT 8 .*scripts/capture_goldens.R --tab game_logs --record"
  )
})

# The committed Game Logs fixtures, copied so a test can delete some.
gl_fixture_copy <- function() {
  from <- repo_file("tests", "goldens", "fixtures", "game_logs")
  to <- golden_tmp_dir()
  file.copy(list.files(from, full.names = TRUE), to)
  to
}

gl_replay_preset <- function(dir, name) {
  source(repo_file("tests", "goldens", "presets", "game_logs.R"), local = TRUE)
  preset <- GAME_LOGS_PRESETS[[name]]
  env <- golden_app_env(golden_replay_conn(dir))
  got <- suppressMessages(golden_run_server(
    env, preset$inputs, golden_capture_dt(preset$slug, allow_none = isTRUE(preset$empty))
  ))
  list(got = got, golden = readRDS(repo_file("tests", "goldens", "out", "game_logs",
                                             paste0(name, ".rds"))),
       notes = golden_replay_notes(env$pg_pool))
}

test_that("goldens do not depend on start-up queries outside Game Logs", {
  dir <- gl_fixture_copy()
  idx <- utils::read.csv(file.path(dir, "index.csv"), colClasses = "character")
  # Home, Tab 3, on/off, EuroLeague team ratings and lineups: start-up only.
  other <- grepl(paste(
    "home_dashboard_combined", "get_team_ratings_dynamic", "get_team_minutes_",
    "onoff_default_mv", "team_game_ratings_mv", "get_team_ratings_pergame",
    "unit_key", "DISTINCT ON \\(s\\.team_id\\)", "^WITH params AS",
    "MAX\\(game_date\\)::date", "net_rtg",
    sep = "|"), idx$statement)
  # The shared lookups for the default season (2027 / EuroLeague 2026), which
  # move with DEFAULT_GAME_YEAR.
  other <- other |
    (grepl("^SELECT DISTINCT gn FROM|full_rosters", idx$statement) & idx$params == "2027") |
    (grepl("euroleague\\.final_schedule_mv", idx$statement) & idx$params == "E | 2026")
  expect_gte(sum(other), 18L)
  expect_true(all(file.remove(file.path(dir, paste0(idx$key[other], ".rds")))))
  source(repo_file("tests", "goldens", "presets", "game_logs.R"), local = TRUE)
  for (name in names(GAME_LOGS_PRESETS)) {
    r <- gl_replay_preset(dir, name)
    expect_identical(r$got, r$golden, info = name)
    expect_gt(length(r$notes), 0L)
  }
})

test_that("a missing Game Logs fixture still fails the golden", {
  dir <- gl_fixture_copy()
  idx <- utils::read.csv(file.path(dir, "index.csv"), colClasses = "character")
  sched <- idx$key[idx$statement == "SELECT * FROM basketball_test.final_schedule_mv WHERE game_year = $1" &
                     idx$params == "2026"]
  expect_length(sched, 1L)
  file.remove(file.path(dir, paste0(sched, ".rds")))
  expect_error(
    gl_replay_preset(dir, "il_summary_default"),
    "golden replay: .*golden replay miss: .*final_schedule_mv.*--tab game_logs --record"
  )
})
