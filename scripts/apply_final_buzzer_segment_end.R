# Apply the final-buzzer segment end: deploy the function, recompute every
# game's segment clock fields, then refresh everything downstream in the ETL's
# Phase 4 order. Each step commits on its own. Writes to production.
setwd("C:/Users/ariel/documents/on_off_israel_pbp")
suppressPackageStartupMessages({library(DBI); library(RPostgres)})
readRenviron("etl/.Renviron")
S <- "C:/Users/ariel/AppData/Local/Temp/claude/C--Users-ariel-documents-on-off-israel-pbp/01e97782-7366-4d11-bca0-80d328f436bc/scratchpad"
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L, dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"), sslmode = "require",
  bigint = "numeric", connect_timeout = 15L)
dbExecute(con, "SET statement_timeout = '20min'")
dbExecute(con, "SET search_path TO basketball_test, public")
step <- function(label, sql) {
  t0 <- Sys.time(); dbBegin(con)
  r <- tryCatch({ x <- dbGetQuery(con, sql); dbCommit(con); x },
                error = function(e) { dbRollback(con); stop(label, ": ", conditionMessage(e)) })
  message(sprintf("%-60s %6.1fs %s", label, as.numeric(difftime(Sys.time(), t0, units = "secs")),
                  if (nrow(r) && ncol(r)) format(r[[1]][1]) else ""))
}
snap <- function() list(
  tm = dbGetQuery(con, "SELECT game_id, team_id, off_minutes FROM team_metrics_by_game_mv"),
  pt = dbGetQuery(con, "SELECT game_year, sum(minutes) AS m FROM player_traditional_stats_mv GROUP BY 1"))
before <- snap(); saveRDS(before, file.path(S, "buzz_before.rds"))

ids <- dbGetQuery(con, "SELECT DISTINCT game_id FROM df_pts_poss_lineups_longer_mv ORDER BY 1")$game_id
message(length(ids), " games")
fn <- paste(readLines("sql/functions/refresh_segment_clock_fields_for_games.sql"), collapse = "\n")
dbExecute(con, fn); message("deployed refresh_segment_clock_fields_for_games")
step("segment clock fields, all games", "SELECT refresh_segment_clock_fields_for_games(NULL) AS n")
for (mv in c("mv_lineup_totals_by_day", "team_ppp_ratings_mv", "lineup_four_factors_by_game")) {
  t0 <- Sys.time(); dbExecute(con, sprintf("REFRESH MATERIALIZED VIEW %s", mv))
  message(sprintf("REFRESH %s %.1fs", mv, as.numeric(difftime(Sys.time(), t0, units = "secs"))))
}
batches <- split(ids, ceiling(seq_along(ids) / 120))
season_level <- c("refresh_onoff_default_for_games", "refresh_player_advanced_stats_for_games")
for (f in c("refresh_player_four_factors_by_game_for_games", "refresh_team_metrics_by_game_for_games",
            "refresh_player_traditional_by_game_for_games", "refresh_default_clutch_player_totals_for_games",
            "refresh_player_stats_actions_for_games", season_level)) {
  bs <- if (f %in% season_level) list(ids) else batches
  for (b in bs) step(sprintf("%s [%d..%d]", f, min(b), max(b)),
                     sprintf("SELECT %s(ARRAY[%s]::int4[]) AS n", f, paste(b, collapse = ",")))
}
for (mv in c("player_traditional_stats_mv", "team_metrics_rolling_mv", "team_four_factors_mv")) {
  t0 <- Sys.time(); dbExecute(con, sprintf("REFRESH MATERIALIZED VIEW %s", mv))
  message(sprintf("REFRESH %s %.1fs", mv, as.numeric(difftime(Sys.time(), t0, units = "secs"))))
}
step("refresh_team_ratings_preset_cache_for_games",
     sprintf("SELECT refresh_team_ratings_preset_cache_for_games(ARRAY[%s]::int4[]) AS n", paste(ids, collapse = ",")))
step("refresh_sub_lineups_stats (full)", "SELECT refresh_sub_lineups_stats() AS n")
dbExecute(con,
  "INSERT INTO app_meta(key, value, updated_at) VALUES ('etl_full_last_success', $1, now())
   ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
  params = list(format(Sys.time(), "%Y-%m-%d %H:%M:%S")))
message("app cache marker advanced")

after <- snap(); saveRDS(after, file.path(S, "buzz_after.rds"))
m <- merge(before$tm, after$tm, by = c("game_id", "team_id"), suffixes = c(".b", ".a"))
message(sprintf("team_metrics rows %d; changed %d; mean %+.3f min; range %.2f..%.2f",
  nrow(m), sum(m$off_minutes.a != m$off_minutes.b), mean(m$off_minutes.a - m$off_minutes.b),
  min(m$off_minutes.a - m$off_minutes.b), max(m$off_minutes.a - m$off_minutes.b)))
print(merge(before$pt, after$pt, by = "game_year", suffixes = c(".before", ".after")))
