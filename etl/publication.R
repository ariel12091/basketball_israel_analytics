# Dedicated session connection: advisory locks must never use transaction pooling.
publication_connect <- function(port = Sys.getenv("PUBLICATION_LOCK_PORT", "5432")) {
  if (as.integer(port) == 6543L) stop("Publication lock requires a session connection (5432/direct)")
  DBI::dbConnect(RPostgres::Postgres(), host = Sys.getenv("PG_HOST"),
    port = as.integer(port), dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
    connect_timeout = 15L)
}

publication_sql <- function(con, schema, fn, args) {
  paste0("SELECT ", DBI::dbQuoteIdentifier(con, schema), ".", fn, "(", args, ")")
}

publication_open <- function(league = "il", port = Sys.getenv("PUBLICATION_LOCK_PORT", "5432"),
                             schema = "basketball_test", con = NULL) {
  if (is.null(con)) con <- publication_connect(port)
  success <- FALSE
  on.exit(if (!success) try(DBI::dbDisconnect(con), silent = TRUE))
  run_id <- DBI::dbGetQuery(con, "SELECT gen_random_uuid()::text AS id")$id[[1]]
  DBI::dbGetQuery(con, paste0("SELECT pg_advisory_lock(",
    DBI::dbQuoteIdentifier(con, schema), ".publication_lock_key($1))"), params = list(league))
  DBI::dbGetQuery(con, publication_sql(con, schema, "begin_publication", "$1, $2::uuid"),
    params = list(league, run_id))
  success <- TRUE
  pub <- new.env(parent = emptyenv())
  pub$con <- con; pub$run_id <- run_id; pub$league <- league; pub$schema <- schema
  pub$closed <- FALSE
  pub
}

publication_fence <- function(pub, con) {
  if (is.null(pub) || isTRUE(pub$closed)) stop("An open publication is required")
  DBI::dbGetQuery(con, publication_sql(con, pub$schema, "fence_publication", "$1, $2::uuid"),
    params = list(pub$league, pub$run_id))
  invisible(TRUE)
}

publication_fenced_tx <- function(pub, con, expr) {
  DBI::dbBegin(con)
  committed <- FALSE
  on.exit(if (!committed) try(DBI::dbRollback(con), silent = TRUE))
  publication_fence(pub, con)
  result <- eval(substitute(expr), envir = parent.frame())
  DBI::dbCommit(con)
  committed <- TRUE
  result
}

publication_close <- function(pub, ok) {
  if (isTRUE(pub$closed)) return(invisible(NULL))
  on.exit({ pub$closed <- TRUE; try(DBI::dbDisconnect(pub$con), silent = TRUE) })
  DBI::dbGetQuery(pub$con,
    publication_sql(pub$con, pub$schema, "finish_publication", "$1, $2::uuid, $3"),
    params = list(pub$league, pub$run_id, isTRUE(ok)))
}

with_publication <- function(league = "il", fn, schema = "basketball_test", con = NULL) {
  pub <- publication_open(league, schema = schema, con = con)
  on.exit(if (!pub$closed) publication_close(pub, FALSE), add = TRUE)
  result <- fn(pub)
  publication_close(pub, TRUE)
  result
}

# Shared by etl_full's no-new-games forced refresh and its isolated DB test.
publication_refresh_sub_lineups <- function(pub, con, schema = pub$schema) {
  publication_fenced_tx(pub, con, {
    DBI::dbExecute(con, "SET LOCAL statement_timeout = 0")
    DBI::dbGetQuery(con, paste0("SELECT ", DBI::dbQuoteIdentifier(con, schema),
                              ".refresh_sub_lineups_stats()"))
  })
}
