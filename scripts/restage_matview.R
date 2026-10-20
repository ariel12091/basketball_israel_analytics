# Replace a materialized view by a rebuilt copy of the SAME contents, for a
# definition change that must not change a single row (a planner or
# performance fix). Stages <name>__ftc_next from the definition file, requires
# it to equal the live view row for row, swaps it in by a short rename and
# drops the replaced copy. Any <name>__ftc_old rollback copy is left alone.
#
#   Rscript scripts/restage_matview.R <name> <definition.sql> [--apply]
#
# Without --apply it stages, compares, reports and drops the staged copy.
# Re-run scripts/apply_db_security.R afterwards: a new view has no grants
# beyond the app's SELECT, which this script gives before the swap.
args <- commandArgs(trailingOnly = TRUE)
apply <- "--apply" %in% args
pos <- args[!startsWith(args, "--")]
if (length(pos) != 2L) stop("Usage: Rscript scripts/restage_matview.R <name> <definition.sql> [--apply]")
name <- pos[[1]]; path <- pos[[2]]
stopifnot(grepl("^[a-z0-9_]+$", name), file.exists(path))

readRenviron("etl/.Renviron")
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
source(file.path("scripts", "apply_ft_credit_migration_helpers.R"))
SCHEMA <- "basketball_test"
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L, dbname = Sys.getenv("PG_DB"),
                 user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
                 sslmode = Sys.getenv("PG_SSLMODE", "require"), connect_timeout = 15L, bigint = "numeric")
on.exit(dbDisconnect(con), add = TRUE)
q <- function(sql) dbGetQuery(con, sql, immediate = TRUE)
x <- function(sql) invisible(dbExecute(con, sql, immediate = TRUE))
say <- function(...) message(sprintf(...))
x("SET client_min_messages = warning")
x("SET statement_timeout = '10min'")
x(sprintf("SET search_path TO %s, public", SCHEMA))

live <- sprintf("%s.%s", SCHEMA, name)
nxt <- paste0(live, STAGE_SUFFIX)
retired <- paste0(name, "__restage_old")
idx <- q(sprintf("SELECT indexname FROM pg_indexes WHERE schemaname = '%s' AND tablename = '%s'", SCHEMA, name))$indexname
sql <- stage_matview_sql(paste(sub("\r$", "", readLines(path, warn = FALSE)), collapse = "\n"), SCHEMA, name, idx)

x(sprintf("DROP MATERIALIZED VIEW IF EXISTS %s", nxt))
t0 <- Sys.time()
x(sql)
say("staged %s in %.1fs (%d indexes)", nxt, as.numeric(Sys.time() - t0, units = "secs"), length(idx))
cmp <- q(sprintf("SELECT (SELECT count(*) FROM %1$s) AS live_rows, (SELECT count(*) FROM %2$s) AS staged_rows,
  (SELECT count(*) FROM (SELECT * FROM %2$s EXCEPT ALL SELECT * FROM %1$s) z) AS only_staged,
  (SELECT count(*) FROM (SELECT * FROM %1$s EXCEPT ALL SELECT * FROM %2$s) z) AS only_live", live, nxt))
say("rows: %s live, %s staged; only staged %s, only live %s", cmp$live_rows, cmp$staged_rows, cmp$only_staged, cmp$only_live)
identical_rows <- cmp$only_staged == 0 && cmp$only_live == 0 && cmp$live_rows == cmp$staged_rows
if (!identical_rows || !apply) {
  x(sprintf("DROP MATERIALIZED VIEW %s", nxt))
  if (!identical_rows) stop("staged view differs from the live one; nothing swapped")
  say("DRY RUN: staged copy dropped, nothing changed")
  quit(save = "no")
}
source("etl/publication.R")
with_publication("il", function(pub) {
x(sprintf("GRANT SELECT ON %s TO app_readonly", nxt))
stmts <- c(
  sprintf("ALTER MATERIALIZED VIEW %s RENAME TO %s", live, retired),
  sprintf("ALTER MATERIALIZED VIEW %s RENAME TO %s", nxt, name),
  unlist(lapply(idx, function(i) c(
    sprintf("ALTER INDEX %s.%s RENAME TO %s__restage_old", SCHEMA, i, i),
    sprintf("ALTER INDEX %s.%s%s RENAME TO %s", SCHEMA, i, STAGE_SUFFIX, i)))),
  sprintf("DROP MATERIALIZED VIEW %s.%s", SCHEMA, retired))
swapped <- FALSE
for (attempt in 1:12) {
  swapped <- tryCatch({
    dbBegin(con)
    publication_fence(pub, con)
    x("SET LOCAL lock_timeout = '5s'")
    for (s in stmts) x(s)
    dbCommit(con); TRUE
  }, error = function(e) {
    try(dbRollback(con), silent = TRUE)
    say("swap attempt %d/12 deferred: %s", attempt, substr(conditionMessage(e), 1, 160))
    FALSE
  })
  if (swapped) break
  Sys.sleep(5)
}
if (!swapped) stop("could not swap ", name, "; the staged copy ", nxt, " is still on the server")
say("swapped: %s now runs the definition in %s; the replaced copy is dropped", live, path)
})
