# Deploys the web app's read API: sql/app_api/*.sql in file order, then the
# security hardening and audit (scripts/apply_db_security.R), all in one
# transaction (plan Tasks 1 and 5). Dry run by default: executes everything,
# smoke-calls every reader, then rolls back. CONFIRM_WEB_FOUNDATION_APPLY=1
# commits.
#
# The web app uses the EXISTING app_readonly login (execution amendment,
# 2026-10-20): this script creates no role and changes no password, role
# default, membership or connection limit. The only grants are the app_api
# allowlist entries in sql/security/enable_readonly_rls.sql (review R9).
#
# DEPLOY ORDER -- this script runs SECOND:
#   1. Rscript scripts/apply_publication.R        (publication markers)
#   2. Rscript scripts/apply_web_foundation.R     (this file)
# The app_api readers call basketball_test.publication_status(), so an APPLY
# (CONFIRM_WEB_FOUNDATION_APPLY=1) refuses to run until step 1 has been
# committed. A DRY RUN before step 1 stages the publication SQL inside its own
# rolled-back transaction, so the full stack can be validated first.
#
# Run from the repository root as the object owner (etl/.Renviron), port 5432.

# The publication objects the app_api readers depend on. Returns the missing
# ones (character(0) when step 1 is in place).
missing_publication_objects <- function(con, query = DBI::dbGetQuery) {
  required <- c(
    relation = "basketball_test.publication",
    `function` = "basketball_test.publication_status(text)",
    `function` = "basketball_test.publication_lock_key(text)"
  )
  present <- vapply(seq_along(required), function(i) {
    sql <- if (names(required)[[i]] == "relation") {
      "SELECT to_regclass($1) IS NOT NULL AS ok"
    } else {
      "SELECT to_regprocedure($1) IS NOT NULL AS ok"
    }
    isTRUE(query(con, sql, params = list(required[[i]]))$ok[[1]])
  }, logical(1))
  unname(required[!present])
}

assert_publication_applied <- function(con, query = DBI::dbGetQuery) {
  missing <- missing_publication_objects(con, query)
  if (length(missing)) {
    stop("Publication markers are not applied (missing: ", paste(missing, collapse = ", "),
         "). Apply them first: Rscript scripts/apply_publication.R ",
         "(dry run, then CONFIRM_PUBLICATION_APPLY=1), then re-run this script.",
         call. = FALSE)
  }
  invisible(TRUE)
}

# Calls every app_api reader once inside the open transaction (CLAUDE.md:
# call a function once after deploying it; plpgsql bodies are only checked
# when they run) and reports row counts, controls and payload sizes.
smoke_call_readers <- function(con) {
  versions <- DBI::dbGetQuery(con, "SELECT * FROM app_api.version()")
  print(versions)
  if (!setequal(versions$league, c("il", "euroleague", "eurocup"))) {
    stop("app_api.version() did not return the three leagues")
  }
  il_season <- DBI::dbGetQuery(con,
    "SELECT max(game_year) AS y FROM basketball_test.final_schedule_mv")$y[[1]]
  euro_season <- DBI::dbGetQuery(con,
    "SELECT max(game_year) AS y FROM euroleague.final_schedule_mv")$y[[1]]
  calls <- list(
    list("game_logs_il", "SELECT app_api.game_logs_il($1)::text AS p", list(as.integer(il_season))),
    list("game_logs_il (empty)", "SELECT app_api.game_logs_il($1)::text AS p", list(1900L)),
    list("game_logs_euro E", "SELECT app_api.game_logs_euro($1, $2)::text AS p",
         list("E", as.integer(euro_season))),
    list("game_logs_euro U", "SELECT app_api.game_logs_euro($1, $2)::text AS p",
         list("U", as.integer(euro_season)))
  )
  for (call in calls) {
    started <- Sys.time()
    text <- DBI::dbGetQuery(con, call[[2]], params = call[[3]])$p[[1]]
    elapsed <- as.numeric(difftime(Sys.time(), started, units = "secs"))
    p <- jsonlite::fromJSON(text, simplifyVector = FALSE)
    rows <- length(p$rows)
    if (!identical(as.numeric(p$row_count), as.numeric(rows))) {
      stop(call[[1]], ": row_count ", p$row_count, " != rows ", rows)
    }
    if (is.null(p$state) || is.null(p$version)) stop(call[[1]], ": no (state, version)")
    message(sprintf("%-22s season=%s state=%s version=%s rows=%d ctl_poss=%s bytes=%d %.2fs",
                    call[[1]], format(call[[3]][[length(call[[3]])]]), p$state, p$version,
                    rows, format(p$ctl_poss), nchar(text, type = "bytes"), elapsed))
  }
  scoreless <- DBI::dbGetQuery(con, "SELECT count(*) AS n FROM app_api.scoreless_games_il($1)",
                               params = list(as.integer(il_season)))$n[[1]]
  message("scoreless_games_il     season=", il_season, " rows=", scoreless)
  DBI::dbExecute(con, "SAVEPOINT unknown_competition")
  refused <- tryCatch({
    DBI::dbGetQuery(con, "SELECT app_api.game_logs_euro('X', 2025)")
    FALSE
  }, error = function(e) grepl("unknown competition", conditionMessage(e)))
  DBI::dbExecute(con, "ROLLBACK TO SAVEPOINT unknown_competition")
  if (!refused) stop("game_logs_euro accepted an unknown competition")
  message("game_logs_euro X       refused (unknown competition)")
  invisible(TRUE)
}

main <- function() {
  confirm <- identical(Sys.getenv("CONFIRM_WEB_FOUNDATION_APPLY", "0"), "1")
  if (file.exists("etl/.Renviron")) readRenviron("etl/.Renviron")
  for (key in c("PG_HOST", "PG_DB", "PG_USER", "PG_PASS")) {
    if (!nzchar(Sys.getenv(key))) stop("Missing environment variable: ", key)
  }
  paths <- sort(list.files(file.path("sql", "app_api"), "\\.sql$", full.names = TRUE))
  if (!length(paths)) stop("Run from repository root: sql/app_api is missing")
  suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
  source(file.path("scripts", "apply_db_security.R"), local = TRUE)
  con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"),
                   port = as.integer(Sys.getenv("PG_DDL_PORT", "5432")),
                   dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
                   password = Sys.getenv("PG_PASS"),
                   sslmode = Sys.getenv("PG_SSLMODE", "require"),
                   connect_timeout = 15, bigint = "numeric")
  on.exit(dbDisconnect(con), add = TRUE)
  message("Database role: ", dbGetQuery(con, "SELECT current_user AS u")$u)
  message("Mode: ", if (confirm) "APPLY (transaction will commit)"
                    else "DRY RUN (transaction will roll back)")
  # An apply refuses outright without committed publication markers. A dry run
  # instead stages them inside its own rolled-back transaction, so the whole
  # stack can be validated before either script is applied.
  stage_publication <- !confirm && length(missing_publication_objects(con)) > 0L
  if (!stage_publication) assert_publication_applied(con)

  dbBegin(con)
  open <- TRUE
  on.exit(if (open) try(dbRollback(con), silent = TRUE), add = TRUE)
  dbExecute(con, "SET LOCAL lock_timeout = '5s'")
  dbExecute(con, "SET LOCAL statement_timeout = '60s'")
  if (stage_publication) {
    message("Publication markers are not applied: DRY RUN stages ",
            "sql/publication/001_publication.sql in this rolled-back transaction only. ",
            "An apply refuses until scripts/apply_publication.R is committed.")
    pub_sql <- paste(readLines(file.path("sql", "publication", "001_publication.sql"),
                               warn = FALSE), collapse = "\n")
    dbExecute(con, gsub("__SCHEMA__", "basketball_test", pub_sql, fixed = TRUE),
              immediate = TRUE)
    assert_publication_applied(con)
  }
  for (path in paths) {
    message("Deploying: ", path)
    dbExecute(con, paste(readLines(path, warn = FALSE), collapse = "\n"), immediate = TRUE)
  }
  # Smoke calls run as the owner BEFORE the hardening: from the hardening on,
  # this transaction holds ACCESS EXCLUSIVE locks on every base table it
  # touches (RLS re-enable, policies), which blocks Shiny's reads until commit
  # or rollback. After it: the audit, a catalogue-only grant check, the end.
  smoke_call_readers(con)
  hardening_started <- Sys.time()
  message("Running security hardening + audit in the same transaction")
  run_db_security_hardening(con)
  message("Security audit: 0 violations")
  grants <- dbGetQuery(con,
    "SELECT p.proname, has_function_privilege('app_readonly', p.oid, 'EXECUTE') AS app_readonly,
            has_function_privilege('anon', p.oid, 'EXECUTE') AS anon
       FROM pg_proc p WHERE p.pronamespace = 'app_api'::regnamespace ORDER BY p.proname")
  print(grants)
  if (!all(grants$app_readonly) || any(grants$anon)) {
    stop("app_api grants are not exactly the allowlist")
  }
  if (confirm) dbCommit(con) else dbRollback(con)
  open <- FALSE
  message(sprintf("Hardening transaction held its locks for %.2fs (hardening start -> %s)",
                  as.numeric(difftime(Sys.time(), hardening_started, units = "secs")),
                  if (confirm) "commit" else "rollback"))
  message(if (confirm) "Web API committed; existing roles unchanged."
          else "DRY RUN: web API validated and rolled back; existing roles unchanged.")
}

if (sys.nframe() == 0L) main()
