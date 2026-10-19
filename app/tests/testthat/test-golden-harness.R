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
  con <- golden_replay_conn(dir)
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
  con <- golden_replay_conn(golden_tmp_dir())
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
