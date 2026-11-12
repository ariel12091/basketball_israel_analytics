# Phase B: drop the moved columns from basket_player_season and the superseded
# basket_player_roster (sql/migrations/2026-11-12_basket_registration_phase_b.sql).
#
# Before: backs both tables up to Parquet (read back and compared), and checks
# that every season row's copy of the moved columns equals its registration's
# names and profile facts -- the identity link excepted, which the matcher
# stopped writing to basket_player_season in Phase A.
# After: the display-names view returns the same rows, byte for byte.
#
# Dry run by default: one transaction, every gate, then ROLLBACK (the backup
# is written either way). CONFIRM_BASKET_PHASE_B=1 commits.

confirm_apply <- identical(Sys.getenv("CONFIRM_BASKET_PHASE_B", "0"), "1")
readRenviron("etl/.Renviron")
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L,
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = "require", bigint = "numeric")
on.exit(dbDisconnect(con))
q <- function(sql) dbGetQuery(con, sql)
one <- function(sql) as.numeric(q(sql)[[1]][1])
gate <- function(ok, what) {
  cat(if (ok) "ok  " else "FAIL", what, "\n")
  if (!ok) stop(what)
}
season <- "basketball_test.basket_player_season"
reg <- "basketball_test.basket_player_registration"
roster <- "basketball_test.basket_player_roster"
view <- "basketball_test.player_display_names_v"

# Backup first, outside the transaction.
backup_dir <- file.path("exports", paste0("basket_backup_", format(Sys.Date())))
dir.create(backup_dir, recursive = TRUE, showWarnings = FALSE)
for (t in c(season, roster)) {
  df <- q(sprintf("SELECT * FROM %s ORDER BY 1, 2", t))
  path <- file.path(backup_dir, paste0(sub(".*\\.", "", t), ".parquet"))
  arrow::write_parquet(df, path)
  back <- as.data.frame(arrow::read_parquet(path))
  gate(nrow(back) == nrow(df) && identical(names(back), names(df)),
       sprintf("backup %s: %d rows, %d columns read back", path, nrow(df), ncol(df)))
}

moved <- c("player_name", "player_name_en", "position_name", "position_en", "height_m",
  "nationality_name", "nationality_code", "profile_source_url", "profile_source_content_md5",
  "profile_fetched_at", "profile_cache_hit", "basket_team_name", "date_of_birth")
view_rows <- function() q(sprintf(
  "SELECT md5(string_agg(md5(v::text), '' ORDER BY v.game_year, v.team_id, v.player_id)) AS h, count(*) AS n FROM %s v", view))

cat("mode:", if (confirm_apply) "APPLY (commits)" else "DRY RUN (rolls back)", "\n")
dbExecute(con, "BEGIN")
ok <- FALSE
tryCatch({
  dbExecute(con, "SET LOCAL lock_timeout = '5s'")
  dbExecute(con, "SET LOCAL statement_timeout = '120s'")
  missing <- one(sprintf("SELECT count(*) FROM %s s LEFT JOIN %s r USING (game_year, basket_player_id)
    WHERE r.basket_player_id IS NULL", season, reg))
  gate(missing == 0, "every season row has a registration")
  diffs <- vapply(moved, function(col) one(sprintf(
    "SELECT count(*) FROM %s s JOIN %s r USING (game_year, basket_player_id) WHERE s.%s IS DISTINCT FROM r.%s",
    season, reg, col, col)), numeric(1))
  if (any(diffs > 0)) print(diffs[diffs > 0])
  gate(all(diffs == 0), sprintf("season copies equal the registration on %d columns", length(moved)))

  before <- view_rows()
  dbExecute(con, paste(readLines("sql/migrations/2026-11-12_basket_registration_phase_b.sql",
    warn = FALSE, encoding = "UTF-8"), collapse = "\n"), immediate = TRUE)
  after <- view_rows()
  gate(identical(before$h, after$h) && before$n == after$n,
       sprintf("display-names view unchanged (%s rows)", format(before$n)))
  left <- q(sprintf("SELECT attname FROM pg_attribute WHERE attrelid = '%s'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum", season))$attname
  cat("basket_player_season now:", paste(left, collapse = ", "), "\n")
  gate(!any(c(moved, "identity_id") %in% left), "moved columns gone")
  gate(is.na(q(sprintf("SELECT to_regclass('%s')::text AS r", roster))$r), "basket_player_roster dropped")
  gate(isTRUE(q(sprintf("SELECT has_table_privilege('app_readonly', '%s', 'SELECT') AS g", season))$g),
       "app_readonly keeps SELECT on basket_player_season")
  ok <- TRUE
}, error = function(e) cat("FAILED:", conditionMessage(e), "\n"))

if (ok && confirm_apply) { dbExecute(con, "COMMIT"); cat("COMMITTED\n") } else { dbExecute(con, "ROLLBACK"); cat("ROLLED BACK\n") }
if (!ok) quit(status = 1)
