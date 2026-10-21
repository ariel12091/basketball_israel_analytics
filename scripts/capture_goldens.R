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
resume <- "--resume" %in% args
if (resume && !record) stop("--resume requires --record", call. = FALSE)
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
  identity <- DBI::dbGetQuery(real, "SELECT current_user AS role")$role[[1]]
  if (!identical(identity, "app_readonly")) stop("golden recording requires app_readonly")
  DBI::dbExecute(real, "SET default_transaction_read_only = on")
  DBI::dbExecute(real, "SET statement_timeout = 120000")
  if (!startsWith(normalizePath(fixture_dir, winslash = "/", mustWork = FALSE),
                  paste0(goldens_dir, "/fixtures/"))) stop("fixture directory outside goldens")
  if (!resume) unlink(fixture_dir, recursive = TRUE)
  record_con <- golden_record_conn(real, fixture_dir)
  index_path <- file.path(fixture_dir, "index.csv")
  if (resume && file.exists(index_path))
    record_con@log$index <- utils::read.csv(index_path, colClasses = "character")
}

strict <- GOLDEN_STRICT_READERS_BY_TAB[[tab]]
if (is.null(strict)) stop("no strict reader set for tab: ", tab)
conn_for_run <- function() if (record) record_con else golden_replay_conn(fixture_dir, strict)

for (name in names(presets)) {
  if (resume && file.exists(file.path(out_dir, paste0(name, ".rds")))) next
  p <- presets[[name]]
  env <- golden_app_env(conn_for_run())
  got <- golden_run_server(env, p$inputs,
                           golden_capture_dt(p$slug, allow_none = isTRUE(p$empty), has_col = p$has_col,
                                             render_pattern = p$render_pattern))
  if (is.null(got) && !isTRUE(p$empty)) stop(name, ": no table rendered", call. = FALSE)
  if (!is.null(got) && !nrow(got$data) && !isTRUE(p$allow_empty)) stop(name, ": table has no rows", call. = FALSE)
  saveRDS(got, file.path(out_dir, paste0(name, ".rds")), compress = "xz")
  cat(sprintf("%-28s %s rows\n", name, if (is.null(got)) "0 (NULL)" else nrow(got$data)))
}

if (record) {
  DBI::dbDisconnect(real)
  cat(sprintf("fixtures: %d queries in %s\n",
              length(list.files(fixture_dir, pattern = "[.]rds$")), fixture_dir))
}
