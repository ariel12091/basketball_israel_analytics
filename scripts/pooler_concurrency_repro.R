# One client of the pooler concurrency reproduction (read-only).
#
# RPostgres sends every dbGetQuery() as prepare then execute. Through the
# Supabase transaction pooler (port 6543) a second client's query can land
# between the two steps, so the execute runs against the wrong statement. One
# client alone never fails; run several at once. See
# docs/pooler_prepared_statement_error_2026-09-05.md.
#
# Usage, from the repository root, several tags in parallel:
#   for t in b c d e; do Rscript scripts/pooler_concurrency_repro.R $t 6543 same 250 & done; wait
#
#   <tag>   one letter, unique per client
#   <port>  6543 (transaction mode) or 5432 (session mode)
#   <mode>  same      - every client runs the same statement with its own
#                       parameter (two visitors on one tab)
#           different - every client runs its own statement (two tabs)
#   <n>     queries to run
#
# Measured 2026-10-12, four clients: different/6543 80-95% bad, same/6543
# 983 of 1000 ok with no crossed values, same/5432 1000 of 1000 ok.

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 4L) stop("usage: <tag> <port> <same|different> <n>")
tag <- args[[1]]
port <- as.integer(args[[2]])
mode <- match.arg(args[[3]], c("same", "different"))
n <- as.integer(args[[4]])

readRenviron("app/.Renviron")
con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("PG_HOST"), port = port,
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
  connect_timeout = 15L, bigint = "numeric"
)

me <- match(tag, letters)
answer_col <- if (identical(mode, "same")) "answer" else paste0("answer_", tag)
sql <- sprintf("SELECT $1::int4 AS %s", answer_col)

outcomes <- character(n)
for (i in seq_len(n)) {
  want <- me * 100000L + i
  res <- tryCatch(DBI::dbGetQuery(con, sql, params = list(want)), error = function(e) e)
  outcomes[i] <- if (inherits(res, "error")) {
    paste("ERROR:", substr(gsub("\\s+", " ", conditionMessage(res)), 1, 70))
  } else if (!identical(names(res), answer_col)) {
    paste("ANOTHER CLIENT'S COLUMNS:", paste(names(res), collapse = ","))
  } else if (!identical(as.integer(res[[1]]), want)) {
    "ANOTHER CLIENT'S VALUE"
  } else {
    "ok"
  }
}
try(DBI::dbDisconnect(con), silent = TRUE)

tab <- sort(table(outcomes), decreasing = TRUE)
cat(sprintf("[%s port %d %s] %s\n", tag, port, mode,
            paste(sprintf("%s x%d", names(tab), as.integer(tab)), collapse = " | ")))
