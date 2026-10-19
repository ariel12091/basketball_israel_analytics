# bootstrap.R - load the app's shared R layer into an environment for the API.
#
# The API runs the very functions Shiny runs: the canonical lookups and the
# data version from global.R / global_euro.R, and the service layer
# (app/R/svc_*.R). Nothing is copied. global.R takes its connection from the
# ibpl.pg_pool option when that is set at source time, so no second pool, no
# onStop() and no prewarm happen here; no UI is built.

# Repo root: the directory holding app/app.R and api/, found by walking up.
api_repo_root <- function(start = getwd()) {
  dir <- normalizePath(start, winslash = "/", mustWork = FALSE)
  repeat {
    if (file.exists(file.path(dir, "app", "app.R")) && dir.exists(file.path(dir, "api"))) return(dir)
    parent <- dirname(dir)
    if (identical(parent, dir)) stop("api_bootstrap: cannot find app/app.R above ", start, call. = FALSE)
    dir <- parent
  }
}

# The app's files in app.R's order (helpers, catalogues, global, logger),
# then global_euro.R, then every service file; the API's own files last.
API_APP_FILES <- c("helpers.R", "catalogues_generated.R", "global.R", "logger.R", "global_euro.R")
API_OWN_FILES <- c("respond.R", "params.R", "handlers_reference.R")

# A pool for the API's own process. Port 5432 unless PG_PORT says otherwise:
# the transaction pooler on 6543 crosses concurrent prepared statements
# (CLAUDE.md, port-6543 pitfall), and the API serves concurrent requests.
api_create_pool <- function() {
  port_txt <- trimws(Sys.getenv("PG_PORT", "5432"))
  if (!grepl("^[0-9]{1,5}$", port_txt) || as.integer(port_txt) < 1L || as.integer(port_txt) > 65535L) {
    stop("api_create_pool: PG_PORT must be a port number (1-65535), got '", port_txt, "'.", call. = FALSE)
  }
  port <- as.integer(port_txt)
  if (identical(port, 6543L)) {
    warning("api_create_pool: PG_PORT=6543 is the transaction pooler, which crosses ",
            "concurrent prepared statements; use 5432.", call. = FALSE)
  }
  timeout_ms <- suppressWarnings(as.integer(Sys.getenv("PG_STATEMENT_TIMEOUT_MS", "20000")))
  if (!is.finite(timeout_ms) || timeout_ms <= 0) timeout_ms <- 20000L
  pool::dbPool(
    drv = RPostgres::Postgres(),
    bigint = "numeric",
    host = Sys.getenv("PG_HOST"),
    port = port,
    dbname = Sys.getenv("PG_DB"),
    user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"),
    sslmode = Sys.getenv("PG_SSLMODE", "require"),
    # As in global.R: a startup `options` string is dropped by the pooler, so
    # the timeout is an ordinary statement once per connection.
    onCreate = function(con) {
      DBI::dbExecute(con, sprintf("SET statement_timeout = %d", timeout_ms))
    },
    minSize = 0,
    maxSize = as.integer(Sys.getenv("POOL_MAX", "3")),
    idleTimeout = 15000
  )
}

# Sources the shared layer and the API's handlers into `envir` and returns it.
# `conn` is any DBI connection or pool (tests pass golden_replay_conn()); NULL
# builds api_create_pool(). The runner uses globalenv(); tests pass a fresh
# environment so they never overwrite testthat's own bindings.
api_bootstrap <- function(conn = NULL, envir = globalenv(), root = api_repo_root()) {
  if (is.null(conn)) conn <- api_create_pool()
  app_dir <- file.path(root, "app")
  svc_files <- sort(list.files(file.path(app_dir, "R"), pattern = "^svc_.*[.]R$"))

  old_opt <- options(ibpl.pg_pool = conn)
  # The app's files expect app/ as the working directory, as under runApp().
  old_wd <- setwd(app_dir)
  on.exit({
    setwd(old_wd)
    options(old_opt)
  }, add = TRUE)

  for (f in c(API_APP_FILES, svc_files)) {
    source(file.path(app_dir, "R", f), local = envir)
  }
  for (f in API_OWN_FILES) {
    source(file.path(root, "api", "R", f), local = envir)
  }
  envir$API_ROOT <- root
  invisible(envir)
}
