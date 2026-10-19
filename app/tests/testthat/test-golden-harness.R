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
