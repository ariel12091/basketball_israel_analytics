# Capture goldens: run the real Shiny server for every preset of one tab and
# save what it renders.
#
#   Rscript scripts/capture_goldens.R --tab game_logs            # replay
#   Rscript scripts/capture_goldens.R --tab game_logs --record   # live DB
#
# Run from the repo root. Presets live in app/tests/goldens/presets/<tab>.R as
# <TAB>_PRESETS. Without --record the recorded fixtures answer every query and
# only app/tests/goldens/out/<tab>/ is rewritten. With --record the fixture
# directory is emptied and refilled from the live database: credentials from
# app/.Renviron (the read-only role), port forced to 5432 whatever that file
# says -- the 6543 transaction pooler crosses prepared statements between
# clients. Reads only. Make sure nothing else queries as app_readonly while
# recording.

args <- commandArgs(trailingOnly = TRUE)
arg_value <- function(flag) {
  i <- match(flag, args)
  if (is.na(i) || i == length(args)) stop("missing value for ", flag, call. = FALSE)
  args[[i + 1L]]
}
tab <- arg_value("--tab")
record <- "--record" %in% args
if (!grepl("^[a-z_]+$", tab)) stop("bad --tab: ", tab, call. = FALSE)
if (!file.exists(file.path("app", "app.R"))) stop("run from the repo root", call. = FALSE)

root <- normalizePath(".", winslash = "/")
goldens_dir <- file.path(root, "app", "tests", "goldens")
source(file.path(goldens_dir, "harness.R"))
source(file.path(goldens_dir, "presets", paste0(tab, ".R")))
presets <- get(paste0(toupper(tab), "_PRESETS"))

fixture_dir <- file.path(goldens_dir, "fixtures", tab)
out_dir <- file.path(goldens_dir, "out", tab)
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

record_con <- NULL
if (record) {
  readRenviron(file.path(root, "app", ".Renviron"))
  real <- DBI::dbConnect(
    RPostgres::Postgres(),
    bigint = "numeric",  # as the app's dbPool()
    host = Sys.getenv("PG_HOST"), port = 5432L, dbname = Sys.getenv("PG_DB"),
    user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
    sslmode = Sys.getenv("PG_SSLMODE", "require"), connect_timeout = 15L
  )
  DBI::dbExecute(real, "SET statement_timeout = 120000")
  unlink(fixture_dir, recursive = TRUE)
  record_con <- golden_record_conn(real, fixture_dir)
}

conn_for_run <- function() if (record) record_con else golden_replay_conn(fixture_dir)

for (name in names(presets)) {
  p <- presets[[name]]
  env <- golden_app_env(conn_for_run())
  got <- golden_run_server(env, p$inputs,
                           golden_capture_dt(p$slug, allow_none = isTRUE(p$empty)))
  if (is.null(got) && !isTRUE(p$empty)) stop(name, ": no table rendered", call. = FALSE)
  if (!is.null(got) && !nrow(got$data)) stop(name, ": table has no rows", call. = FALSE)
  saveRDS(got, file.path(out_dir, paste0(name, ".rds")), compress = "xz")
  cat(sprintf("%-28s %s rows\n", name, if (is.null(got)) "0 (NULL)" else nrow(got$data)))
}

if (record) {
  DBI::dbDisconnect(real)
  cat(sprintf("fixtures: %d queries in %s\n",
              length(list.files(fixture_dir, pattern = "[.]rds$")), fixture_dir))
}
