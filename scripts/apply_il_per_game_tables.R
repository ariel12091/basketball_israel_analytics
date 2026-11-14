# Turn three Israeli materialized views into tables refreshed per game or
# season (2026-11-14):
#   lineup_four_factors_by_game  -> refresh_lineup_four_factors_by_game_for_games()
#   mv_lineup_totals_by_day      -> refresh_mv_lineup_totals_by_day_for_games()
#   player_traditional_stats_mv  -> refresh_player_traditional_stats_for_games() (per season)
# Each was rebuilt in full every night with games. Names and columns stay, so
# readers are unchanged. team_four_factors_mv reads lineup_four_factors_by_game,
# so it is dropped and re-created from its own live definition around the swap.
#
# Dry run by default (rolled back). CONFIRM_IL_PER_GAME_TABLES=1 commits. Then
# re-run scripts/apply_db_security.R with CONFIRM_DB_SECURITY_APPLY=1.
#
# Gates:
# 1. Before any exclusive lock: each compute function over every game returns
#    exactly the live view's rows (EXCEPT ALL both ways). The slow part.
# 2. In the swap transaction: each table holds the view's rows;
#    team_four_factors_mv re-created with the same rows; the per-game / season
#    refresh of the latest games leaves every table unchanged (timed); grants,
#    row-level security and policy in place.

suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
readRenviron("etl/.Renviron")
apply <- identical(Sys.getenv("CONFIRM_IL_PER_GAME_TABLES"), "1")

TABLES <- list(
  lineup_four_factors_by_game = list(compute = "compute_lineup_four_factors_by_game",
    refresh = "refresh_lineup_four_factors_by_game_for_games",
    file = "sql/materialized_views/lineup_four_factors_by_game.sql"),
  mv_lineup_totals_by_day = list(compute = "compute_mv_lineup_totals_by_day",
    refresh = "refresh_mv_lineup_totals_by_day_for_games",
    file = "sql/materialized_views/sub_lineups_by_day.sql"),
  player_traditional_stats_mv = list(compute = "compute_player_traditional_stats",
    refresh = "refresh_player_traditional_stats_for_games",
    file = "sql/materialized_views/player_traditional_stats_mv.sql")
)
DEPENDENT <- "team_four_factors_mv"
READERS <- c("app_readonly", "service_role")

sql_file <- function(path) paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
fn_files <- unlist(lapply(TABLES, function(t) c(
  file.path("sql/functions", paste0(t$compute, ".sql")),
  file.path("sql/functions", paste0(t$refresh, ".sql")))))
# The CREATE INDEX statements of each table file (the file's own source of truth).
index_sql <- function(path) {
  parts <- trimws(strsplit(sql_file(path), ";", fixed = TRUE)[[1]])
  parts <- gsub("^(--[^\n]*\n)+", "", parts)
  grep("^CREATE (UNIQUE )?INDEX", trimws(parts), value = TRUE)
}

con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L,
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
  sslmode = "require", bigint = "numeric")
on.exit({ suppressWarnings(try(dbRollback(con), silent = TRUE)); dbDisconnect(con) }, add = TRUE)
q1 <- function(sql, ...) dbGetQuery(con, sql, ...)[[1]][1]
differs <- function(a, b) c(
  q1(sprintf("SELECT count(*) FROM (SELECT * FROM %s EXCEPT ALL SELECT * FROM %s) x", a, b)),
  q1(sprintf("SELECT count(*) FROM (SELECT * FROM %s EXCEPT ALL SELECT * FROM %s) x", b, a)))
timed <- function(expr) { t0 <- proc.time()[["elapsed"]]; v <- force(expr); list(v = v, s = proc.time()[["elapsed"]] - t0) }
fail <- function(...) stop(sprintf(...), call. = FALSE)

message("mode=", if (apply) "EXECUTE" else "DRY RUN (CONFIRM_IL_PER_GAME_TABLES=1 to commit)")
for (nm in c(names(TABLES), DEPENDENT)) {
  kind <- q1("SELECT relkind FROM pg_class WHERE oid = $1::regclass", params = list(paste0("basketball_test.", nm)))
  if (!identical(kind, "m")) fail("%s is relkind %s, expected a materialized view", nm, kind)
}

# ---- Gate 1: no exclusive lock; rolled back -----------------------------------
dbBegin(con)
dbExecute(con, "SET LOCAL statement_timeout = '600s'")
dbExecute(con, "SET LOCAL work_mem = '256MB'")
for (f in fn_files) dbExecute(con, sql_file(f))
for (nm in names(TABLES)) {
  r <- timed(dbExecute(con, sprintf("CREATE TEMP TABLE proof_%s ON COMMIT DROP AS SELECT * FROM basketball_test.%s(NULL::int4[])",
    nm, TABLES[[nm]]$compute)))
  gap <- differs(paste0("proof_", nm), paste0("basketball_test.", nm))
  message(sprintf("gate 1 %-28s compute(NULL) %.1fs; differs from view (compute-view, view-compute) = %s",
    nm, r$s, paste(gap, collapse = ", ")))
  if (any(gap != 0)) fail("%s: the compute function does not reproduce the view", nm)
}
dbRollback(con)

# ---- Swap ----------------------------------------------------------------------
dbBegin(con)
dbExecute(con, "SET LOCAL lock_timeout = '5s'")
dbExecute(con, "SET LOCAL statement_timeout = '600s'")
dbExecute(con, "SET LOCAL work_mem = '256MB'")
for (f in fn_files) dbExecute(con, sql_file(f))

dep_def <- sub(";[[:space:]]*$", "", q1("SELECT pg_get_viewdef('basketball_test.team_four_factors_mv'::regclass, true)"))
dep_idx <- dbGetQuery(con, "SELECT indexdef FROM pg_indexes WHERE schemaname = 'basketball_test' AND tablename = $1", params = list(DEPENDENT))$indexdef
dbExecute(con, sprintf("CREATE TEMP TABLE snap_%s ON COMMIT DROP AS SELECT * FROM basketball_test.%s", DEPENDENT, DEPENDENT))
dbExecute(con, sprintf("DROP MATERIALIZED VIEW basketball_test.%s", DEPENDENT))

grant_read <- function(nm, table = TRUE) {
  for (role in READERS) dbExecute(con, sprintf("GRANT SELECT ON basketball_test.%s TO %s", nm, role))
  if (table) {
    dbExecute(con, sprintf("ALTER TABLE basketball_test.%s ENABLE ROW LEVEL SECURITY", nm))
    dbExecute(con, sprintf("CREATE POLICY app_readonly_select_all ON basketball_test.%s FOR SELECT TO app_readonly USING (true)", nm))
  }
}

for (nm in names(TABLES)) {
  dbExecute(con, sprintf("CREATE TEMP TABLE snap_%s ON COMMIT DROP AS SELECT * FROM basketball_test.%s", nm, nm))
  dbExecute(con, sprintf("CREATE TABLE basketball_test.%s__new AS SELECT * FROM basketball_test.%s", nm, nm))
  dbExecute(con, sprintf("DROP MATERIALIZED VIEW basketball_test.%s", nm))
  dbExecute(con, sprintf("ALTER TABLE basketball_test.%s__new RENAME TO %s", nm, nm))
  for (ix in index_sql(TABLES[[nm]]$file)) dbExecute(con, ix)
  grant_read(nm)
  dbExecute(con, sprintf("ANALYZE basketball_test.%s", nm))
  kind <- q1("SELECT relkind FROM pg_class WHERE oid = $1::regclass", params = list(paste0("basketball_test.", nm)))
  gap <- differs(paste0("basketball_test.", nm), paste0("snap_", nm))
  message(sprintf("swap   %-28s relkind=%s differs=%s", nm, kind, paste(gap, collapse = ", ")))
  if (kind != "r" || any(gap != 0)) fail("%s: swap gate failed", nm)
}

dbExecute(con, sprintf("CREATE MATERIALIZED VIEW basketball_test.%s AS %s", DEPENDENT, dep_def))
for (ix in dep_idx) dbExecute(con, ix)
grant_read(DEPENDENT, table = FALSE)
gap <- differs(paste0("basketball_test.", DEPENDENT), paste0("snap_", DEPENDENT))
message(sprintf("swap   %-28s re-created; differs=%s", DEPENDENT, paste(gap, collapse = ", ")))
if (any(gap != 0)) fail("%s changed when re-created", DEPENDENT)

# The latest processed games: what a night's ETL refreshes.
latest <- dbGetQuery(con, "SELECT game_id FROM basketball_test.etl_processed_games ORDER BY processed_at DESC, game_id DESC LIMIT 2")$game_id
ids <- sprintf("ARRAY[%s]::int4[]", paste(as.integer(latest), collapse = ","))
for (nm in names(TABLES)) {
  r <- timed(q1(sprintf("SELECT basketball_test.%s(%s)", TABLES[[nm]]$refresh, ids)))
  gap <- differs(paste0("basketball_test.", nm), paste0("snap_", nm))
  message(sprintf("refresh %-27s games %s: %s rows in %.1fs; differs=%s",
    nm, paste(latest, collapse = ","), format(r$v), r$s, paste(gap, collapse = ", ")))
  if (any(gap != 0)) fail("%s: the refresh changed rows", nm)
}
empty <- q1(sprintf("SELECT basketball_test.%s(ARRAY[]::int4[])", TABLES$lineup_four_factors_by_game$refresh))
if (empty != 0 || any(differs("basketball_test.lineup_four_factors_by_game", "snap_lineup_four_factors_by_game") != 0))
  fail("an empty game list must refresh nothing")

for (nm in c(names(TABLES), DEPENDENT)) {
  ok <- vapply(READERS, function(role) isTRUE(q1("SELECT has_table_privilege($1, $2, 'SELECT')",
    params = list(role, paste0("basketball_test.", nm)))), logical(1))
  message(sprintf("access %-28s %s", nm, paste(sprintf("%s=%s", READERS, ok), collapse = " ")))
  if (!all(ok)) fail("%s: read grant missing", nm)
}

if (apply) { dbCommit(con); message("Committed. Now: CONFIRM_DB_SECURITY_APPLY=1 Rscript scripts/apply_db_security.R") } else {
  dbRollback(con); message("DRY RUN passed and rolled back.")
}
