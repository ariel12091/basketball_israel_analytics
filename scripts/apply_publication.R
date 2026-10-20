# Dry run executes the exact migration then rolls back. No role changes.
if (file.exists("etl/.Renviron")) readRenviron("etl/.Renviron")
source("etl/publication.R")
schema <- Sys.getenv("PUBLICATION_SCHEMA", "basketball_test")
if (!identical(schema, "basketball_test") && !grepl("^pubtest_[a-z0-9_]+$", schema)) {
  stop("PUBLICATION_SCHEMA must be basketball_test or an isolated pubtest_ schema")
}
con <- publication_connect(Sys.getenv("PG_DDL_PORT", "5432"))
tryCatch({
  sql <- paste(readLines("sql/publication/001_publication.sql", warn = FALSE), collapse = "\n")
  sql <- gsub("__SCHEMA__", as.character(DBI::dbQuoteIdentifier(con, schema)), sql, fixed = TRUE)
  DBI::dbBegin(con)
  DBI::dbExecute(con, paste0("CREATE SCHEMA IF NOT EXISTS ", DBI::dbQuoteIdentifier(con, schema)))
  DBI::dbExecute(con, sql, immediate = TRUE)
  if (identical(Sys.getenv("CONFIRM_PUBLICATION_APPLY", "0"), "1")) {
    DBI::dbCommit(con)
    message("Publication migration applied to ", schema)
  } else {
    DBI::dbRollback(con)
    message("DRY RUN: publication migration validated and rolled back for ", schema)
  }
}, error = function(e) {
  try(DBI::dbRollback(con), silent = TRUE)
  stop(e)
}, finally = DBI::dbDisconnect(con))
