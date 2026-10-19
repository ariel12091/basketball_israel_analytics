# run.R - start the API.
#
#   Rscript api/run.R
#
# Environment: PG_HOST, PG_DB, PG_USER, PG_PASS, PG_PORT (default 5432),
# PG_SSLMODE, POOL_MAX, PG_STATEMENT_TIMEOUT_MS; API_HOST (default
# 127.0.0.1 -- the API is not exposed publicly until access control is
# decided), API_PORT (default 3010). IBPL_API_REPLAY_DIR=<fixture dir> serves
# recorded query answers instead of a database (smoke runs, never production).

api_script_dir <- local({
  file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
  if (length(file_arg)) {
    dirname(normalizePath(sub("^--file=", "", file_arg[[1]]), winslash = "/"))
  } else {
    normalizePath("api", winslash = "/", mustWork = FALSE)
  }
})

source(file.path(api_script_dir, "R", "bootstrap.R"))
api_root <- api_repo_root(api_script_dir)

conn <- NULL
replay_dir <- Sys.getenv("IBPL_API_REPLAY_DIR", "")
if (nzchar(replay_dir)) {
  source(file.path(api_root, "app", "tests", "goldens", "harness.R"))
  conn <- golden_replay_conn(replay_dir)
  message("[api] serving recorded answers from ", replay_dir)
}
api_bootstrap(conn, envir = globalenv(), root = api_root)

api_router <- plumber::pr(file.path(api_script_dir, "plumber.R"),
                          filters = list(api_query = api_query_filter))
if (inherits(pg_pool, "Pool")) {
  api_router <- plumber::pr_hook(api_router, "exit", function() pool::poolClose(pg_pool))
}
plumber::pr_run(
  api_router,
  host = Sys.getenv("API_HOST", "127.0.0.1"),
  port = as.integer(Sys.getenv("API_PORT", "3010")),
  docs = FALSE
)
