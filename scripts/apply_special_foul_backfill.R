# Applies the special-foul possession rule to the stored rows of 2024-25 and 2025-26, whose
# play-by-play is not re-fetched: flips final_end_poss on the rows that
# sql/migrations/2026-11-02_special_foul_possession_targets.sql selects, then refreshes what
# reads them in the ETL's Phase 4 order, and carries the same flip into the cold Parquet archive
# so a restore cannot bring the old ends back. (2026-27 goes through etl_full(game_ids = ...).)
#
#   Rscript scripts/apply_special_foul_backfill.R --seasons 2025,2026                 dry run
#   CONFIRM_SPECIAL_FOUL_BACKFILL=1 Rscript scripts/apply_special_foul_backfill.R --seasons 2025,2026
#
# Each step commits on its own, so no long transaction holds a view lock; the cache marker
# advances last, so the apps switch to the new numbers once everything is in place.

args <- commandArgs(trailingOnly = TRUE)
i <- match("--seasons", args)
stopifnot(!is.na(i), i < length(args))
seasons <- as.integer(strsplit(args[[i + 1L]], ",")[[1]])
stopifnot(length(seasons) > 0L, !anyNA(seasons), all(seasons %in% c(2025L, 2026L)))
apply <- identical(Sys.getenv("CONFIRM_SPECIAL_FOUL_BACKFILL"), "1")

readRenviron("etl/.Renviron")
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L, dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"), bigint = "numeric")
invisible(dbExecute(con, "SET statement_timeout = '15min'"))
say <- function(...) cat(sprintf(...), "\n", sep = "")
x <- function(sql) { t0 <- proc.time()[["elapsed"]]; dbExecute(con, sql); say("  %-62s %6.1fs", substr(sql, 1, 62), proc.time()[["elapsed"]] - t0) }

SQL <- paste(readLines("sql/migrations/2026-11-02_special_foul_possession_targets.sql"), collapse = "\n")
targets <- dbGetQuery(con, SQL, params = list(sprintf("{%s}", paste(seasons, collapse = ","))))
games <- sort(unique(as.integer(targets$game_id)))
say("%s: %d possession ends to remove in %d games (seasons %s)", if (apply) "APPLY" else "DRY RUN",
    nrow(targets), length(games), paste(seasons, collapse = ", "))
print(table(targets$type))
if (!apply) { dbDisconnect(con); say("Nothing changed. Set CONFIRM_SPECIAL_FOUL_BACKFILL=1 to apply."); quit(status = 0L) }

arr <- function(v) sprintf("{%s}", paste(as.integer(v), collapse = ","))
batches <- split(games, ceiling(seq_along(games) / 25L))
each_batch <- function(fn) {
  t0 <- proc.time()[["elapsed"]]
  for (b in batches) dbGetQuery(con, sprintf("SELECT basketball_test.%s('%s'::int4[])", fn, arr(b)))
  say("  %-62s %6.1fs", fn, proc.time()[["elapsed"]] - t0)
}

say("Step 1: base rows")
n <- dbExecute(con, "
  UPDATE basketball_test.df_pts_poss_lineups_longer_mv d SET final_end_poss = FALSE
  FROM unnest($1::int4[], $2::int4[]) AS t(game_id, id)
  WHERE d.game_id = t.game_id AND d.id = t.id AND d.final_end_poss",
  params = list(arr(targets$game_id), arr(targets$id)))
say("  %d rows flipped (a row per perspective)", n)

say("Step 2: views and per-game facts, in Phase 4 order")
invisible(dbExecute(con, "SET search_path TO basketball_test, public"))
for (mv in c("mv_lineup_totals_by_day", "team_ppp_ratings_mv", "lineup_four_factors_by_game"))
  x(sprintf("REFRESH MATERIALIZED VIEW %s", mv))
for (fn in c("refresh_player_four_factors_by_game_for_games", "refresh_team_metrics_by_game_for_games",
             "refresh_player_traditional_by_game_for_games", "refresh_default_clutch_player_totals_for_games",
             "refresh_player_stats_actions_for_games", "refresh_onoff_default_for_games",
             "refresh_player_advanced_stats_for_games"))
  each_batch(fn)
for (mv in c("player_traditional_stats_mv", "team_metrics_rolling_mv", "team_four_factors_mv"))
  x(sprintf("REFRESH MATERIALIZED VIEW %s", mv))
each_batch("refresh_team_ratings_preset_cache_for_games")
each_batch("refresh_sub_lineups_stats_for_games")

say("Step 3: cold archive")
backup <- "exports/cold/backup-2026-11-02-special-foul"
dir.create(backup, showWarnings = FALSE)
for (t in c("possessions", "pws")) {
  f <- sprintf("exports/cold/%s.parquet", t)
  file.copy(f, file.path(backup, basename(f)), overwrite = FALSE)
  d <- as.data.frame(arrow::read_parquet(f))
  hit <- paste(d$game_id, d$id) %in% paste(targets$game_id, targets$id) & d$final_end_poss %in% TRUE
  d$final_end_poss[hit] <- FALSE
  arrow::write_parquet(d, f)
  say("  %s: %d rows flipped, %d rows kept", t, sum(hit), nrow(arrow::read_parquet(f, col_select = "id")))
}

say("Step 4: cache marker")
stamp <- dbGetQuery(con, "SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS s")$s
dbExecute(con, "UPDATE app_meta SET value = $1 WHERE key = 'etl_full_last_success'", params = list(stamp))
say("  advanced to %s", stamp)
dbDisconnect(con)
say("Done. Run scripts/verify_special_foul_possessions.R --season <year> --compare <baseline> for each season.")
