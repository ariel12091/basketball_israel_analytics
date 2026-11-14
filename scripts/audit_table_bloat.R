# Fails (exit 1) when a large table's file is much bigger than its rows need.
#
# Why: Postgres writes a new copy of every updated row, and VACUUM marks the
# old copies free but never shrinks the file. A migration that updated every
# row of euroleague.action_team_context_actions twice left it at 2.0 GB for
# about 0.7 GB of rows; every full scan read the empty pages, and a nightly
# test spent 1.2 GiB of Supabase disk I/O per call (2026-11-14). Nothing
# reported it. Fix a flagged table with VACUUM (FULL, ANALYZE) at a quiet
# hour (it locks the table) -- see CLAUDE.md, PostgreSQL pitfalls.
#
# The estimate: rows x (row header + average column widths from pg_stats +
# line pointer), packed into 8 kB pages. It ignores alignment padding, so a
# healthy table reads about 1.0-1.4x; the threshold is 2x. pg_stats is only
# visible for tables the role can read, so run this as the ETL role (owner).
# Read-only. Heap only: index bloat is not estimated.

if (!nzchar(Sys.getenv("PG_HOST")) && file.exists("etl/.Renviron")) {
  readRenviron("etl/.Renviron")
}
required_env <- c("PG_HOST", "PG_DB", "PG_USER", "PG_PASS")
missing_env <- required_env[!nzchar(Sys.getenv(required_env))]
if (length(missing_env)) {
  stop("Missing required database environment variables: ",
       paste(missing_env, collapse = ", "))
}

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

MAX_RATIO <- 2.0       # file size / estimated size of the live rows
MIN_SIZE_MB <- 100     # smaller tables cost too little to matter
SCHEMAS <- c("basketball_test", "euroleague")

con <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = suppressWarnings(as.integer(Sys.getenv("PG_PORT", "5432"))),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"),
  connect_timeout = 10,
  bigint = "numeric"
)
on.exit(dbDisconnect(con), add = TRUE)
dbExecute(con, "SET default_transaction_read_only = on")
dbExecute(con, "SET statement_timeout = '60s'")

tables <- dbGetQuery(con, "
  WITH widths AS (
    SELECT schemaname, tablename,
           SUM((1 - null_frac) * avg_width) AS data_bytes,
           COUNT(*) AS n_cols
    FROM pg_stats
    WHERE schemaname = ANY($1::text[])
    GROUP BY schemaname, tablename
  )
  SELECT n.nspname AS schema, c.relname AS table_name, c.relkind AS kind,
         c.reltuples AS est_rows,
         c.relpages::numeric * 8192 AS heap_bytes,
         -- 23-byte header + null bitmap, rounded to 8; 4-byte line pointer
         c.reltuples * (24 + CEIL(w.n_cols / 8.0) + w.data_bytes + 4)
           / (8192 - 24) * 8192 AS needed_bytes,
         s.n_dead_tup AS dead_rows,
         GREATEST(s.last_vacuum, s.last_autovacuum) AS last_vacuum
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN widths w ON w.schemaname = n.nspname AND w.tablename = c.relname
  LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
  WHERE n.nspname = ANY($1::text[])
    AND c.relkind IN ('r', 'm')
    AND c.reltuples > 0",
  params = list(paste0("{", paste(SCHEMAS, collapse = ","), "}")))

if (!nrow(tables)) {
  stop("No table statistics visible: run as the ETL role, which owns the tables.")
}

tables$heap_mb <- round(tables$heap_bytes / 1048576)
tables$needed_mb <- round(tables$needed_bytes / 1048576)
tables$ratio <- round(tables$heap_bytes / tables$needed_bytes, 2)
tables <- tables[order(-tables$heap_bytes), ]
flagged <- tables[tables$heap_mb >= MIN_SIZE_MB & tables$ratio > MAX_RATIO, ]

cols <- c("schema", "table_name", "heap_mb", "needed_mb", "ratio",
          "dead_rows", "last_vacuum")
cat(sprintf("Largest tables (flag: >= %d MB and over %.1fx):\n",
            MIN_SIZE_MB, MAX_RATIO))
print(utils::head(tables[, cols], 15), row.names = FALSE)

if (!nrow(flagged)) {
  message("\nTable bloat audit passed.")
  quit(save = "no", status = 0L)
}

cat(sprintf("\n%d bloated table(s), about %d MB of empty pages read by every full scan:\n",
            nrow(flagged), sum(flagged$heap_mb - flagged$needed_mb)))
print(flagged[, cols], row.names = FALSE)
cat("\nFix at a quiet hour, as the ETL role, one table at a time:\n",
    "  SET lock_timeout = '10s'; SET statement_timeout = '30min';\n",
    sprintf("  VACUUM (FULL, ANALYZE) %s.%s;\n", flagged$schema, flagged$table_name),
    sep = "")
quit(save = "no", status = 1L)
