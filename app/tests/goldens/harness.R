# Golden harness: record/replay database connections and a loader for the
# real Shiny app.
#
# A golden is a recording of what today's app renders for one set of inputs.
# Recording runs the app against the live database through
# golden_record_conn(), which saves every query answer as a fixture; replay
# runs the same app through golden_replay_conn(), which answers from those
# fixtures only, so the app server runs offline and deterministically.
#
# Strict replay covers the active tab's dependencies
# (GOLDEN_STRICT_READERS_BY_TAB): an unrecorded dependency is an error.
# Every golden replays the whole session start-up too -- Home, Tab 3, the
# EuroLeague section -- and those queries change with DEFAULT_GAME_YEAR, the
# team catalogue or any unrelated SQL edit. A miss outside the strict set is
# answered with a zero-row frame and printed as a note, so such a change
# needs no re-record. If a Game Logs dependency were wrongly left out, its
# empty answer would change a golden, so the suite still fails closed.
#
# Sourced by golden tests and the capture script, deliberately not a
# helper-*.R file, so the mock-based test suite never loads the real app.

library(DBI)
library(pool)

# Repo root: the directory holding app/app.R, found by walking up from the
# working directory (tests run from app/tests/testthat, scripts from the root).
golden_repo_root <- function(start = getwd()) {
  dir <- normalizePath(start, winslash = "/", mustWork = FALSE)
  repeat {
    if (file.exists(file.path(dir, "app", "app.R"))) return(dir)
    parent <- dirname(dir)
    if (identical(parent, dir)) stop("golden harness: cannot find app/app.R above ", start)
    dir <- parent
  }
}

golden_app_dir <- function() file.path(golden_repo_root(), "app")

# Every source of the app and every server run uses this seed, so anything the
# app draws at random is the same in recording and in replay.
GOLDEN_SEED <- 20261019L

# Home's default team, pinned: global.R draws it from the season's team
# catalogue, so a catalogue sync would otherwise change which team Home asks
# about. mod_team_hub.R reads DEFAULT_HOME_TEAM_ID when the server runs, so
# setting it after sourcing is enough. "5" is the team the seeded draw gave
# when the fixtures were recorded.
GOLDEN_HOME_TEAM_ID <- "5"

GOLDEN_RERECORD_HINT <- "re-record with: Rscript scripts/capture_goldens.R --tab game_logs --record"

# The queries Game Logs depends on, named by the app function that issues
# them (found on the call stack, so an edit to their SQL is a strict miss,
# never a silent one). NULL: every call is strict. A character vector: only
# calls whose params read as one of these strings (golden_params_text()) are
# strict -- the shared reference lookups also run at start-up for the default
# season, and only the seasons the Game Logs presets and API tests use
# (Israeli 2026, EuroLeague "E" 2025) belong to Game Logs.
GOLDEN_STRICT_READERS <- list(
  # Season pulls (svc_game_logs.R, global_euro.R) and the scoreless-game list.
  gl_query_schedule = NULL,
  gl_query_lineup_totals = NULL,
  gl_query_lineup_ff = NULL,
  euro_fetch_game_log_rows = NULL,
  fetch_scoreless_games = NULL,
  # Data versions: cache keys of every season pull.
  etl_last_success_db = NULL,
  euro_data_version = NULL,
  # Reference lookups the Game Logs tabs and the API reference endpoints call.
  fetch_teams_min = "2026",
  fetch_teams_distinct = "2026",
  fetch_gn_values = "2026",
  fetch_players_basic = "2026",
  euro_fetch_seasons = "E",
  euro_fetch_teams = "E | 2025",
  euro_fetch_players_basic = "E | 2025",
  euro_fetch_phases = "E | 2025",
  euro_fetch_round_values = "E | 2025"
)

# Each tab owns its strict dependencies; unrelated startup queries remain notes.
GOLDEN_STRICT_READERS_BY_TAB <- list(
  player_stats = c(list(
    cached_season_df = NULL, fetch_player_traditional_season = NULL,
    fetch_player_traditional_filtered = NULL, load_ts_identity_lookup = NULL,
    server_tab5_traditional = NULL
  ), GOLDEN_STRICT_READERS[c('etl_last_success_db','euro_data_version',
    'fetch_teams_distinct','fetch_gn_values','euro_fetch_seasons','euro_fetch_teams',
    'euro_fetch_players_basic','euro_fetch_phases','euro_fetch_round_values')]),
  team_ratings = c(list(
    run_team_ratings_dynamic = NULL, run_team_ff_dynamic = NULL,
    run_team_ff_dashboard_dynamic = NULL, run_team_traditional_dynamic = NULL,
    fetch_team_game_minutes = NULL, tr_effective_anchor = NULL, tr_prev_match_end = NULL,
    run_team_ratings = NULL, run_team_ff = NULL, run_team_dashboard = NULL,
    run_team_minutes = NULL, et_prev_end = NULL, tr_data = NULL, tr_ff_data = NULL,
    cached_season_df = NULL
  ), GOLDEN_STRICT_READERS[c('etl_last_success_db','euro_data_version',
    'fetch_teams_distinct','fetch_gn_values','euro_fetch_seasons','euro_fetch_teams',
    'euro_fetch_phases','euro_fetch_round_values')]),
  lineups = c(list(
    run_fetch_lineups_20 = NULL,
    run_fetch_lineups_ff_20 = NULL,
    cached_season_df = NULL,
    fetch_player_traditional_season = NULL,
    server_tab10_euro_lineups = NULL
  ), GOLDEN_STRICT_READERS[c(
    "etl_last_success_db", "euro_data_version", "fetch_teams_min",
    "fetch_teams_distinct", "fetch_gn_values", "fetch_players_basic",
    "euro_fetch_seasons", "euro_fetch_teams", "euro_fetch_players_basic",
    "euro_fetch_phases", "euro_fetch_round_values"
  )]),
  game_logs = GOLDEN_STRICT_READERS,
  onoff = c(list(
    cached_season_df = NULL,
    run_onoff_compute_14 = NULL,
    run_four_factors_compute = NULL,
    fetch_player_traditional_season = NULL,
    fetch_player_traditional_filtered = NULL
  ), GOLDEN_STRICT_READERS[c(
    "etl_last_success_db", "euro_data_version", "fetch_teams_min",
    "fetch_teams_distinct", "fetch_gn_values", "fetch_players_basic",
    "euro_fetch_seasons", "euro_fetch_teams", "euro_fetch_players_basic",
    "euro_fetch_phases", "euro_fetch_round_values"
  )])
)

# Names of the functions on the current call stack (`f(...)`, `env$f(...)`
# and `pkg::f(...)` all give "f").
golden_stack_functions <- function() {
  vapply(sys.calls(), function(cl) {
    f <- cl[[1]]
    if (is.symbol(f)) return(as.character(f))
    if (is.call(f) && length(f) == 3L && is.symbol(f[[3]]) &&
        as.character(f[[1]]) %in% c("$", "::", ":::")) {
      return(as.character(f[[3]]))
    }
    ""
  }, character(1))
}

# Whether a replay miss of this query must fail. `strict` is TRUE (every
# query) or a GOLDEN_STRICT_READERS-shaped list.
golden_query_is_strict <- function(strict, params, stack = golden_stack_functions()) {
  if (isTRUE(strict)) return(TRUE)
  hits <- intersect(stack, names(strict))
  if (!length(hits)) return(FALSE)
  ptxt <- golden_params_text(params)
  any(vapply(hits, function(h) is.null(strict[[h]]) || ptxt %in% strict[[h]], logical(1)))
}

# Task-level feature coverage markers (`covers(...)`) live in the API tree.
# Sourced here when present so app-side golden tests can call covers().
golden_coverage_file <- file.path(golden_repo_root(), "api", "coverage.R")
if (file.exists(golden_coverage_file)) source(golden_coverage_file, local = TRUE)
rm(golden_coverage_file)

# The app's own query label, lifted from global.R rather than retyped, so a
# replay miss names a query the same way the app's slow-query log does.
golden_query_label <- local({
  exprs <- parse(file.path(golden_app_dir(), "R", "global.R"), keep.source = FALSE)
  env <- new.env(parent = globalenv())
  for (e in exprs) {
    if (is.call(e) && identical(as.character(e[[1]]), "<-") &&
        identical(as.character(e[[2]]), "db_query_label")) {
      eval(e, envir = env)
    }
  }
  if (!exists("db_query_label", envir = env, inherits = FALSE)) {
    stop("golden harness: db_query_label not found in global.R")
  }
  env$db_query_label
})

golden_query_key <- function(statement, params) {
  digest::digest(
    list(
      gsub("[[:space:]]+", " ", trimws(statement)),
      unname(lapply(params, as.character))
    ),
    algo = "sha1"
  )
}

golden_fixture_path <- function(dir, key) file.path(dir, paste0(key, ".rds"))

# ---------------- Connection classes ----------------
# Each implements what the app needs from a connection: dbGetQuery (with or
# without params, as db_get_query() calls it), dbExecute, dbIsValid, and
# pool::poolCheckout/poolReturn, which mod_team_hub.R calls on pg_pool directly.

methods::setClass("GoldenReplayConnection", contains = "DBIConnection",
                  slots = c(dir = "character", misses = "environment", strict = "ANY"))
methods::setClass("GoldenRecordConnection", contains = "DBIConnection",
                  slots = c(dir = "character", real = "ANY", log = "environment"))

# `strict`: GOLDEN_STRICT_READERS (the default: only Game Logs queries must be
# recorded) or TRUE (every query must be).
golden_replay_conn <- function(fixture_dir, strict = GOLDEN_STRICT_READERS) {
  if (!dir.exists(fixture_dir)) stop("golden replay: no fixture directory ", fixture_dir)
  if (!isTRUE(strict) && !(is.list(strict) && length(strict) && !is.null(names(strict)))) {
    stop("golden replay: strict must be TRUE or a named list like GOLDEN_STRICT_READERS")
  }
  misses <- new.env(parent = emptyenv())
  misses$labels <- character(0)
  misses$notes <- character(0)
  methods::new("GoldenReplayConnection",
               dir = normalizePath(fixture_dir, winslash = "/"), misses = misses,
               strict = strict)
}

# Strict misses since the last call, cleared on read. The app catches query
# errors in observers and only logs them, so golden_run_server() checks this
# log too.
golden_replay_misses <- function(conn) {
  out <- conn@misses$labels
  conn@misses$labels <- character(0)
  out
}

# Misses outside the strict set, answered with zero rows; each distinct one is
# printed once per connection when it first happens. Not cleared on read.
golden_replay_notes <- function(conn) conn@misses$notes

golden_record_conn <- function(real_con, fixture_dir) {
  dir.create(fixture_dir, recursive = TRUE, showWarnings = FALSE)
  log <- new.env(parent = emptyenv())
  log$errors <- character(0)
  log$index <- data.frame(key = character(0), label = character(0),
                          statement = character(0), params = character(0),
                          stringsAsFactors = FALSE)
  methods::new("GoldenRecordConnection",
               dir = normalizePath(fixture_dir, winslash = "/"), real = real_con, log = log)
}

# Query errors since the last call, cleared on read. A recording must never
# capture a golden from a state the app reached by swallowing an error.
golden_record_errors <- function(conn) {
  out <- conn@log$errors
  conn@log$errors <- character(0)
  out
}

# One readable line per params list for the fixture index.
golden_params_text <- function(params) {
  if (is.null(params)) return("")
  paste(vapply(params, function(p) paste(as.character(p), collapse = ","), character(1)),
        collapse = " | ")
}

# index.csv beside the fixtures says which query each <key>.rds answers.
golden_record_index <- function(conn, key, statement, params) {
  idx <- conn@log$index
  if (key %in% idx$key) return(invisible(NULL))
  conn@log$index <- rbind(idx, data.frame(
    key = key, label = golden_query_label(statement),
    statement = gsub("[[:space:]]+", " ", trimws(statement)),
    params = golden_params_text(params), stringsAsFactors = FALSE
  ))
  utils::write.csv(conn@log$index[order(conn@log$index$key), , drop = FALSE],
                   file.path(conn@dir, "index.csv"), row.names = FALSE)
  invisible(NULL)
}

methods::setMethod("dbGetQuery", c("GoldenReplayConnection", "character"),
  function(conn, statement, params = NULL, ...) {
    key <- golden_query_key(statement, params)
    path <- golden_fixture_path(conn@dir, key)
    if (!file.exists(path)) {
      label <- golden_query_label(statement)
      if (!golden_query_is_strict(conn@strict, params)) {
        if (!label %in% conn@misses$notes) {
          conn@misses$notes <- c(conn@misses$notes, label)
          message("golden replay note: unrecorded query outside the strict set, ",
                  "answered with zero rows: ", label)
        }
        return(data.frame())
      }
      hint <- if (identical(conn@strict, GOLDEN_STRICT_READERS_BY_TAB$onoff))
        sub("--tab game_logs", "--tab onoff", GOLDEN_RERECORD_HINT, fixed = TRUE) else GOLDEN_RERECORD_HINT
      msg <- sprintf("golden replay miss: %s key=%s (%s)", label, key, hint)
      conn@misses$labels <- c(conn@misses$labels, msg)
      stop(msg, call. = FALSE)
    }
    readRDS(path)
  })

methods::setMethod("dbGetQuery", c("GoldenRecordConnection", "character"),
  function(conn, statement, params = NULL, ...) {
    # Team Ratings recording may reuse already captured, byte-identical
    # auxiliary startup queries (On/Off, lineups, player rosters). The team
    # readers, measured minutes and rank-arrow schedule queries always run live.
    key <- golden_query_key(statement, params)
    path <- golden_fixture_path(conn@dir, key)
    team_query <- grepl('(basketball_test|euroleague)[.](get_team_|team_|final_schedule_mv)', statement)
    if (isTRUE(getOption('ibpl.golden.reuse_team_auxiliary')) && !team_query && file.exists(path)) {
      cached <- tryCatch(suppressWarnings(readRDS(path)), error = function(e) NULL)
      if (!is.null(cached)) return(cached)
    }
    out <- tryCatch(
      if (is.null(params)) {
        DBI::dbGetQuery(conn@real, statement)
      } else {
        DBI::dbGetQuery(conn@real, statement, params = params)
      },
      error = function(e) {
        conn@log$errors <- c(conn@log$errors, sprintf(
          "%s: %s", golden_query_label(statement), conditionMessage(e)))
        stop(e)
      }
    )
    key <- golden_query_key(statement, params)
    saveRDS(out, golden_fixture_path(conn@dir, key), compress = "xz")
    golden_record_index(conn, key, statement, params)
    out
  })

methods::setMethod("dbExecute", c("GoldenReplayConnection", "character"),
  function(conn, statement, ...) 0L)

# Recording is read-only: session SETs are forwarded, nothing is stored.
methods::setMethod("dbExecute", c("GoldenRecordConnection", "character"),
  function(conn, statement, ...) DBI::dbExecute(conn@real, statement, ...))

methods::setMethod("dbIsValid", "GoldenReplayConnection", function(dbObj, ...) TRUE)
methods::setMethod("dbIsValid", "GoldenRecordConnection",
  function(dbObj, ...) DBI::dbIsValid(dbObj@real))

for (golden_cls in c("GoldenReplayConnection", "GoldenRecordConnection")) {
  methods::setMethod("poolCheckout", golden_cls, function(pool) pool)
  methods::setMethod("poolReturn", golden_cls, function(object) invisible(NULL))
}
rm(golden_cls)

# ---------------- The real app ----------------

# Sources app/app.R itself into a fresh environment, so the files load in
# exactly app.R's order and `server` is app.R's own function, not a copy.
# shinyApp() is stubbed in that environment to capture its server argument
# instead of building an app object. The injected connection replaces the pool
# (global.R then skips dbPool, onStop and the prewarm); the UI caches are off so
# nothing is built or rendered at source time.
golden_app_env <- function(conn) {
  app_dir <- golden_app_dir()
  old_opt <- options(ibpl.pg_pool = conn)
  old_env <- Sys.getenv(c("IBPL_CACHE_UI", "IBPL_CACHE_UI_HTML"), unset = NA)
  Sys.setenv(IBPL_CACHE_UI = "false", IBPL_CACHE_UI_HTML = "false")
  on.exit({
    options(old_opt)
    for (nm in names(old_env)) {
      if (is.na(old_env[[nm]])) Sys.unsetenv(nm) else do.call(Sys.setenv, as.list(old_env[nm]))
    }
  }, add = TRUE)

  env <- new.env(parent = globalenv())
  captured <- new.env(parent = emptyenv())
  env$shinyApp <- function(ui, server, ...) {
    captured$server <- server
    invisible(NULL)
  }
  # Seeded: global.R draws the default Home team at random when sourced, and
  # that team decides which Home queries every session runs.
  withr::with_seed(GOLDEN_SEED, source(file.path(app_dir, "app.R"), local = env, chdir = TRUE))
  rm("shinyApp", envir = env)
  if (!is.function(captured$server)) stop("golden harness: app.R did not call shinyApp(ui, server)")
  env$server <- captured$server
  env$DEFAULT_HOME_TEAM_ID <- GOLDEN_HOME_TEAM_ID

  # Service-layer files app.R does not source yet.
  app_src <- paste(readLines(file.path(app_dir, "app.R"), warn = FALSE), collapse = "\n")
  svc_files <- sort(list.files(file.path(app_dir, "R"), pattern = "^svc_.*\\.R$"))
  for (f in svc_files) {
    if (!grepl(paste0("R/", f), app_src, fixed = TRUE)) {
      source(file.path(app_dir, "R", f), local = env, chdir = TRUE)
    }
  }

  # Each call sources afresh, so these start empty already; reset explicitly
  # so independence does not hang on that.
  if (exists("GL_DATA_CACHE", envir = env, inherits = FALSE)) env$GL_DATA_CACHE$reset()
  if (exists("RANKED_CACHE", envir = env, inherits = FALSE)) {
    rm(list = ls(env$RANKED_CACHE, all.names = TRUE), envir = env$RANKED_CACHE)
  }
  rm(list = ls(env$.ref_cache_env, all.names = TRUE), envir = env$.ref_cache_env)

  env$.golden_app_dir <- app_dir
  env
}

# Inputs are one named list (one setInputs() call) or an unnamed list of named
# lists, applied as successive steps.
golden_input_steps <- function(inputs) {
  if (!length(inputs)) return(list())
  if (!is.null(names(inputs))) return(list(inputs))
  inputs
}

# Runs env$server under shiny::testServer, applies each input step in one
# setInputs() call, flushes after each, and returns capture(session, output). A
# capture function may carry "golden_setup" / "golden_teardown" attributes,
# called before the server starts and after it finishes (even on error).
# When env$pg_pool replays, any query it could not answer fails the run, even
# one the app caught and logged; when it records, so does any query error.
golden_run_server <- function(env, inputs, capture) {
  replay <- if (methods::is(env$pg_pool, "GoldenReplayConnection")) env$pg_pool
  record <- if (methods::is(env$pg_pool, "GoldenRecordConnection")) env$pg_pool
  if (!is.null(replay)) golden_replay_misses(replay)
  if (!is.null(record)) golden_record_errors(record)
  steps <- golden_input_steps(inputs)
  setup <- attr(capture, "golden_setup")
  teardown <- attr(capture, "golden_teardown")
  if (is.function(teardown)) on.exit(teardown(), add = TRUE)
  if (is.function(setup)) setup()

  if (!is.null(env$.golden_app_dir)) {
    old_wd <- setwd(env$.golden_app_dir)
    on.exit(setwd(old_wd), add = TRUE)
  }

  result <- new.env(parent = emptyenv())
  withr::local_seed(GOLDEN_SEED)
  # A failed query usually surfaces later as some other error (an empty table,
  # a capture miss), so the connection's log is reported first either way.
  run_error <- tryCatch({
    shiny::testServer(env$server, {
      for (step in !!steps) {
        do.call(session$setInputs, step)
        session$flushReact()
      }
      session$flushReact()
      assign("value", (!!capture)(session, output), envir = !!result)
    })
    NULL
  }, error = function(e) e)
  golden_stop_on_log("golden record", "failed",
                     if (!is.null(record)) golden_record_errors(record))
  golden_stop_on_log("golden replay", "unrecorded",
                     if (!is.null(replay)) golden_replay_misses(replay))
  if (!is.null(run_error)) stop(run_error)
  result$value
}

golden_stop_on_log <- function(what, adjective, entries) {
  entries <- unique(entries)
  if (!length(entries)) return(invisible(NULL))
  stop(sprintf("%s: %d %s quer%s:\n%s", what, length(entries), adjective,
               if (length(entries) == 1L) "y" else "ies",
               paste(entries, collapse = "\n")), call. = FALSE)
}

# Capture for a DT table: traces DT::datatable for the run and returns the last
# table whose CSV button filename starts with `slug_prefix` (the app stamps the
# slug with a time, see csv_export_button()), as its data, its initial order
# and the names of the columns columnDefs hide. With allow_none, a run that
# rendered no such table (the app's renderer returned NULL) captures NULL.
golden_capture_dt <- function(slug_prefix = NULL, allow_none = FALSE, has_col = NULL,
                              render_pattern = NULL) {
  sink <- new.env(parent = emptyenv())
  sink$items <- list()
  push <- function(entry) sink$items[[length(sink$items) + 1L]] <- entry
  traced <- list()
  untrace_dt <- function() {
    for (where in traced) suppressMessages(untrace("datatable", where = where))
    traced <<- list()
  }

  capture <- function(session, output) {
    on.exit(untrace_dt(), add = TRUE)
    session$flushReact()
    matches <- Filter(function(entry) {
      if (!is.null(render_pattern) && !grepl(render_pattern,
          paste(unlist(entry$options$columnDefs), collapse = " "), fixed = TRUE)) return(FALSE)
      if (!is.null(has_col) && !has_col %in% names(entry$data)) return(FALSE)
      if (is.null(slug_prefix)) return(TRUE)
      fn <- entry$options$buttons[[1]]$filename
      is.character(fn) && length(fn) == 1L && startsWith(fn, slug_prefix)
    }, sink$items)
    if (!length(matches)) {
      if (isTRUE(allow_none)) return(NULL)
      stop("no datatable rendered for ", if (is.null(has_col)) slug_prefix else has_col, call. = FALSE)
    }
    hit <- matches[[length(matches)]]
    data <- as.data.frame(hit$data)
    list(
      data = data,
      order = hit$options$order,
      hidden = golden_dt_hidden_cols(data, hit$options$columnDefs, hit$rownames)
    )
  }
  # The app calls both DT::datatable() (the namespace copy) and a bare
  # datatable() (the attached package:DT copy); trace() edits one copy per
  # call, so trace each.
  attr(capture, "golden_setup") <- function() {
    wheres <- list(asNamespace("DT"))
    if ("package:DT" %in% search()) wheres <- c(wheres, list(as.environment("package:DT")))
    for (where in wheres) {
      suppressMessages(trace(
        "datatable",
        tracer = bquote(.(push)(list(
          data = data, options = options,
          # datatable() gives rownames no default; missing means shown.
          rownames = if (missing(rownames)) NULL else rownames
        ))),
        where = where,
        print = FALSE
      ))
      traced[[length(traced) + 1L]] <<- where
    }
  }
  attr(capture, "golden_teardown") <- untrace_dt
  capture
}

# Column names hidden by `visible = FALSE` columnDefs. Numeric targets are
# DataTables' 0-based indices, which count the rownames column when shown.
golden_dt_hidden_cols <- function(data, column_defs, rownames) {
  offset <- if (isFALSE(rownames)) 0L else 1L
  cols <- names(data)
  hidden <- unlist(lapply(column_defs, function(def) {
    if (!isFALSE(def$visible)) return(NULL)
    t <- def$targets
    if (is.character(t)) return(if ("_all" %in% t) cols else intersect(t, cols))
    idx <- as.integer(t)
    idx <- ifelse(idx < 0L, length(cols) + offset + idx, idx) + 1L - offset
    cols[idx[idx >= 1L & idx <= length(cols)]]
  }))
  unique(as.character(hidden))
}
