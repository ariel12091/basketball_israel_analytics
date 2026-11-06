# Deploys the publication markers (sql/publication/001_publication.sql, plan
# Task 2). Dry run by default: executes the exact migration in a transaction,
# checks it, then rolls back. CONFIRM_PUBLICATION_APPLY=1 commits. No role,
# password or role-default changes.
#
# DEPLOY ORDER -- this script runs FIRST:
#   1. Rscript scripts/apply_publication.R        (this file)
#   2. Rscript scripts/apply_web_foundation.R     (app_api readers; refuses to
#      run until step 1 is committed, because its readers call
#      basketball_test.publication_status())
# The ETL / EuroLeague loader changes that call begin/fence/finish_publication
# must not reach main before step 1 is committed (takeover inventory, defect 2).
#
# Target schema: PUBLICATION_SCHEMA, default basketball_test; only an isolated
# pubtest_* schema is accepted otherwise (review R3). For basketball_test the
# security hardening and audit run in the same transaction, so the new table
# gets its RLS policy and publication_status / publication_lock_key their
# allowlisted EXECUTE grant, and nothing commits unless the audit is clean.
#
# Run from the repository root as the object owner (etl/.Renviron), port 5432.
main <- function() {
  confirm <- identical(Sys.getenv("CONFIRM_PUBLICATION_APPLY", "0"), "1")
  if (file.exists("etl/.Renviron")) readRenviron("etl/.Renviron")
  for (key in c("PG_HOST", "PG_DB", "PG_USER", "PG_PASS")) {
    if (!nzchar(Sys.getenv(key))) stop("Missing environment variable: ", key)
  }
  sql_path <- file.path("sql", "publication", "001_publication.sql")
  if (!file.exists(sql_path)) stop("Run from the repository root: ", sql_path, " is missing")
  schema <- Sys.getenv("PUBLICATION_SCHEMA", "basketball_test")
  live <- identical(schema, "basketball_test")
  if (!live && !grepl("^pubtest_[a-z0-9_]+$", schema)) {
    stop("PUBLICATION_SCHEMA must be basketball_test or an isolated pubtest_ schema")
  }
  suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
  source(file.path("etl", "publication.R"))
  source(file.path("scripts", "apply_db_security.R"), local = TRUE)

  con <- publication_connect(Sys.getenv("PG_DDL_PORT", "5432"))
  on.exit(dbDisconnect(con), add = TRUE)
  qs <- as.character(dbQuoteIdentifier(con, schema))
  message("Database role: ", dbGetQuery(con, "SELECT current_user AS u")$u)
  message("Target schema: ", schema)
  message("Mode: ", if (confirm) "APPLY (transaction will commit)"
                    else "DRY RUN (transaction will roll back)")

  sql <- paste(readLines(sql_path, warn = FALSE), collapse = "\n")
  sql <- gsub("__SCHEMA__", qs, sql, fixed = TRUE)

  dbBegin(con)
  open <- TRUE
  on.exit(if (open) try(dbRollback(con), silent = TRUE), add = TRUE)
  dbExecute(con, "SET LOCAL lock_timeout = '5s'")
  dbExecute(con, "SET LOCAL statement_timeout = '60s'")
  already <- dbGetQuery(con, "SELECT to_regclass($1) IS NOT NULL AS ok",
                        params = list(paste0(schema, ".publication")))$ok[[1]]
  message("publication table before: ", if (already) "present (idempotent re-run)" else "absent")
  dbExecute(con, paste("CREATE SCHEMA IF NOT EXISTS", qs))
  dbExecute(con, sql, immediate = TRUE)
  # Check the deployed objects by calling them (plpgsql bodies are only
  # validated when they run) BEFORE the hardening: from the hardening on, this
  # transaction holds ACCESS EXCLUSIVE locks on every base table in the schema
  # (RLS re-enable, policies), which blocks Shiny's reads until commit or
  # rollback, so nothing slow may run after it. The writers are exercised by
  # test-publication-db.R against a pubtest_* schema.
  status <- dbGetQuery(con, paste0(
    "SELECT p.league, p.state AS stored_state, s.state, s.version,
            ", qs, ".publication_lock_key(p.league)::text AS lock_key
       FROM ", qs, ".publication p
      CROSS JOIN LATERAL ", qs, ".publication_status(p.league) s
      ORDER BY p.league"))
  print(status)
  if (!setequal(status$league, c("il", "euroleague", "eurocup"))) {
    stop("publication rows missing after deploy")
  }

  # From here: hardening + audit, catalogue-only grant check, then end the
  # transaction. The lock hold is timed and reported.
  hardening_started <- Sys.time()
  if (live) {
    message("Running security hardening + audit in the same transaction")
    run_db_security_hardening(con)
    message("Security audit: 0 violations")
  }
  grants <- dbGetQuery(con, paste0(
    "SELECT p.proname,
            has_function_privilege('app_readonly', p.oid, 'EXECUTE') AS app_readonly_execute
       FROM pg_proc p WHERE p.pronamespace = $1::regnamespace
        AND p.proname IN ('publication_lock_key', 'publication_owner_live', 'publication_status',
                          'begin_publication', 'fence_publication', 'finish_publication')
      ORDER BY p.proname"), params = list(schema))
  print(grants)
  closed <- grants$app_readonly_execute[grants$proname %in%
    c("begin_publication", "fence_publication", "finish_publication", "publication_owner_live")]
  if (any(closed)) stop("app_readonly can execute a publication writer or internal helper")

  if (confirm) dbCommit(con) else dbRollback(con)
  open <- FALSE
  message(sprintf("Hardening transaction held its locks for %.2fs (hardening start -> %s)",
                  as.numeric(difftime(Sys.time(), hardening_started, units = "secs")),
                  if (confirm) "commit" else "rollback"))
  message(if (confirm) paste("Publication migration committed to", schema)
          else paste("DRY RUN: publication migration validated and rolled back for", schema))
}

if (sys.nframe() == 0L) main()
