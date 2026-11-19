# Apply sql/migrations/2026-11-19_ask_feedback.sql: the Ask chat's
# "Wrong answer?" store (app_feedback.ask_feedback, written only through
# app_feedback.submit_ask_feedback()).
#
#   Rscript scripts/apply_ask_feedback.R                     # dry run, rolls back
#   CONFIRM_ASK_FEEDBACK=1 Rscript scripts/apply_ask_feedback.R
#
# Gates before COMMIT (as the owner, etl/.Renviron):
# 1. app_readonly has USAGE on the schema and EXECUTE on the function, and no
#    privilege on the table; anon, authenticated and PUBLIC have none at all.
# 2. The function caps the texts (a 600-character question is stored as 500,
#    a blank note as NULL); that row is rolled back with a savepoint.
# After COMMIT, as the app's own login (app/.Renviron), in a transaction that
# is rolled back:
# 3. The function stores a row and returns its id; reading the table or
#    inserting into it directly is refused.

confirm_apply <- identical(Sys.getenv("CONFIRM_ASK_FEEDBACK", "0"), "1")
readRenviron(Sys.getenv("ETL_RENVIRON", "etl/.Renviron"))
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L,
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = "require", bigint = "numeric")
q <- function(sql) dbGetQuery(con, sql)
x <- function(sql) invisible(dbExecute(con, sql, immediate = TRUE))
ddl <- paste(readLines("sql/migrations/2026-11-19_ask_feedback.sql", encoding = "UTF-8"), collapse = "\n")
fn <- "app_feedback.submit_ask_feedback(text, text, text, jsonb, text, text)"
long_question <- strrep("q", 600)

cat("mode:", if (confirm_apply) "APPLY (commits)" else "DRY RUN (rolls back)", "\n")
x("BEGIN")
ok <- FALSE
tryCatch({
  x("SET LOCAL lock_timeout = '5s'")
  x(ddl)
  for (r in c("anon", "authenticated"))
    if (nrow(q(sprintf("SELECT 1 FROM pg_roles WHERE rolname = '%s'", r))))
      x(sprintf(paste("REVOKE ALL ON SCHEMA app_feedback FROM %1$s;",
                      "REVOKE ALL ON app_feedback.ask_feedback FROM %1$s;",
                      "REVOKE ALL ON FUNCTION %2$s FROM %1$s;"), r, fn))

  # Gate 1: privileges.
  acl <- q(sprintf(paste(
    "SELECT r.rolname,",
    "has_schema_privilege(r.oid, 'app_feedback', 'USAGE') AS schema_usage,",
    "has_table_privilege(r.oid, 'app_feedback.ask_feedback', 'SELECT,INSERT,UPDATE,DELETE') AS table_any,",
    "has_function_privilege(r.oid, '%s', 'EXECUTE') AS fn_execute",
    "FROM pg_roles r WHERE r.rolname IN ('app_readonly', 'anon', 'authenticated')"), fn))
  print(acl, row.names = FALSE)
  app <- acl[acl$rolname == "app_readonly", ]
  if (!nrow(app) || !app$schema_usage || app$table_any || !app$fn_execute)
    stop("app_readonly must have schema USAGE and EXECUTE only")
  others <- acl[acl$rolname != "app_readonly", ]
  if (any(unlist(others[c("schema_usage", "table_any", "fn_execute")])))
    stop("anon/authenticated must have no access")
  if (q(sprintf("SELECT has_function_privilege('public', '%s', 'EXECUTE') AS e", fn))$e)
    stop("PUBLIC can execute the function")

  # Gate 2: the caps, on a row rolled back at once.
  x("SAVEPOINT gate")
  id <- dbGetQuery(con, "SELECT app_feedback.submit_ask_feedback('il', $1, 'answer', NULL, '  ', 'gate') AS id",
                   params = list(long_question))$id
  if (is.na(id)) stop("the function stored nothing")
  row <- dbGetQuery(con, "SELECT char_length(question) AS n, note FROM app_feedback.ask_feedback WHERE feedback_id = $1",
                    params = list(id))
  if (row$n != 500 || !is.na(row$note)) stop("caps not applied: ", row$n, " / ", row$note)
  x("ROLLBACK TO SAVEPOINT gate")
  cat("caps: question cut to 500, blank note stored as NULL (row rolled back)\n")
  if (q("SELECT count(*) AS n FROM app_feedback.ask_feedback")$n != 0) stop("gate row left behind")
  ok <- TRUE
}, error = function(e) cat("FAILED:", conditionMessage(e), "\n"))

if (ok && confirm_apply) { x("COMMIT"); cat("COMMITTED\n") } else { x("ROLLBACK"); cat("ROLLED BACK\n") }
dbDisconnect(con)
if (!ok) quit(status = 1)

# Gate 3, after the commit, as the app's own login; everything rolled back.
if (confirm_apply) {
  # The owner's session is closed: the app's file may now replace PG_*.
  readRenviron(Sys.getenv("APP_RENVIRON", "app/.Renviron"))
  app <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L,
                   dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
                   password = Sys.getenv("PG_PASS"), sslmode = "require")
  dbExecute(app, "BEGIN")
  denied <- function(sql) {
    dbExecute(app, "SAVEPOINT probe")
    out <- tryCatch({ dbGetQuery(app, sql); FALSE },
                    error = function(e) grepl("permission denied", conditionMessage(e)))
    dbExecute(app, "ROLLBACK TO SAVEPOINT probe")
    out
  }
  who <- dbGetQuery(app, "SELECT current_user AS u")$u
  id <- dbGetQuery(app, "SELECT app_feedback.submit_ask_feedback('il', 'gate', 'a', NULL, NULL, 'gate') AS id")$id
  read <- denied("SELECT count(*) FROM app_feedback.ask_feedback")
  ins <- denied("INSERT INTO app_feedback.ask_feedback (league, question) VALUES ('il', 'x')")
  dbExecute(app, "ROLLBACK")
  dbDisconnect(app)
  cat(sprintf("as %s: the function stored row %s (rolled back); direct read refused: %s; direct insert refused: %s\n",
              who, id, read, ins))
  if (is.na(id) || !read || !ins) { cat("POST-COMMIT GATE FAILED: review the grants\n"); quit(status = 1) }
}
