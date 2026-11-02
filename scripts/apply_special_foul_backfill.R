# Applies the special-foul possession rule to stored rows, without re-fetching play-by-play: flips
# final_end_poss on the rows that sql/migrations/2026-11-02_special_foul_possession_targets.sql
# selects, then refreshes what reads them in the ETL's Phase 4 order, and carries the same flip
# into the cold Parquet archive so a restore cannot bring the old ends back.
# Used for 2026-27 too: etl_full(game_ids = <42 games>) failed the 2-minute Phase 4 limit and
# today's lineup gate rejected games 401 and 649 on re-fetch. Its cold rows did pin this SQL: the
# real rule removed the same 90 ends in the 40 games it re-ran, and added none.
#
#   Rscript scripts/apply_special_foul_backfill.R --seasons 2025,2026,2027                 dry run
#   CONFIRM_SPECIAL_FOUL_BACKFILL=1 Rscript scripts/apply_special_foul_backfill.R --seasons 2025,2026,2027
#   ... --resume-at sub-lineups    skip steps 1-2 up to the sub-lineup refresh after a failure there
#
# Each step commits on its own, so no long transaction holds a view lock; the cache marker
# advances last, so the apps switch to the new numbers once everything is in place.

args <- commandArgs(trailingOnly = TRUE)
i <- match("--seasons", args)
stopifnot(!is.na(i), i < length(args))
seasons <- as.integer(strsplit(args[[i + 1L]], ",")[[1]])
stopifnot(length(seasons) > 0L, !anyNA(seasons), all(seasons %in% c(2025L, 2026L, 2027L)))
apply <- identical(Sys.getenv("CONFIRM_SPECIAL_FOUL_BACKFILL"), "1")

readRenviron("etl/.Renviron")
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L, dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"), bigint = "numeric")
invisible(dbExecute(con, "SET statement_timeout = '15min'"))
say <- function(...) cat(sprintf(...), "\n", sep = "")
x <- function(sql) { t0 <- proc.time()[["elapsed"]]; dbExecute(con, sql); say("  %-62s %6.1fs", substr(sql, 1, 62), proc.time()[["elapsed"]] - t0) }

# Step 1 removes the rows the query selects, so a rerun could not find them again: the targets are
# saved before the first write and a rerun after a failure resumes from that file.
backup <- "exports/cold/backup-2026-11-02-special-foul"
targets_file <- file.path(backup, "targets.csv")
SQL <- paste(readLines("sql/migrations/2026-11-02_special_foul_possession_targets.sql"), collapse = "\n")
resuming <- file.exists(targets_file)
targets <- if (resuming) utils::read.csv(targets_file) else
  dbGetQuery(con, SQL, params = list(sprintf("{%s}", paste(seasons, collapse = ","))))
if (resuming) say("Resuming from %s", targets_file)
games <- sort(unique(as.integer(targets$game_id)))
say("%s: %d possession ends to remove in %d games (seasons %s)", if (apply) "APPLY" else "DRY RUN",
    nrow(targets), length(games), paste(seasons, collapse = ", "))
print(table(targets$type))
if (!apply) { dbDisconnect(con); say("Nothing changed. Set CONFIRM_SPECIAL_FOUL_BACKFILL=1 to apply."); quit(status = 0L) }

if (!resuming) {
  dir.create(backup, recursive = TRUE, showWarnings = FALSE)
  utils::write.csv(targets, targets_file, row.names = FALSE)
}
resume_sub <- identical(args[match("--resume-at", args) + 1L], "sub-lineups")
arr <- function(v) sprintf("{%s}", paste(as.integer(v), collapse = ","))
batches <- split(games, ceiling(seq_along(games) / 25L))
each_batch <- function(fn) {
  t0 <- proc.time()[["elapsed"]]
  for (b in batches) dbGetQuery(con, sprintf("SELECT basketball_test.%s('%s'::int4[])", fn, arr(b)))
  say("  %-62s %6.1fs", fn, proc.time()[["elapsed"]] - t0)
}

if (!resume_sub) {
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
}

# The full refresh, not the per-game one: refresh_sub_lineups_stats_for_games() recomputes the whole
# season of every sub-lineup a game touches, about 480 MB of temp files for a single 2025 game, and
# batches of 10-25 games filled the database server's disk on 2026-11-02. The full one takes 21 s.
say("Step 2b: sub-lineup stats")
x("SELECT basketball_test.refresh_sub_lineups_stats()")

say("Step 3: cold archive")
for (t in c("possessions", "pws")) {
  f <- sprintf("exports/cold/%s.parquet", t)
  file.copy(f, file.path(backup, basename(f)), overwrite = FALSE)
  d <- as.data.frame(arrow::read_parquet(f, mmap = FALSE))
  hit <- paste(d$game_id, d$id) %in% paste(targets$game_id, targets$id) & d$final_end_poss %in% TRUE
  d$final_end_poss[hit] <- FALSE
  arrow::write_parquet(d, f)
  say("  %s: %d rows flipped, %d rows kept", t, sum(hit), nrow(arrow::read_parquet(f, col_select = "id", mmap = FALSE)))
}

say("Step 4: cache marker")
stamp <- dbGetQuery(con, "SELECT to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS s")$s
dbExecute(con, "UPDATE basketball_test.app_meta SET value = $1 WHERE key = 'etl_full_last_success'", params = list(stamp))
say("  advanced to %s", stamp)
dbDisconnect(con)
say("Done. Run scripts/verify_special_foul_possessions.R --season <year> --compare <baseline> for each season.")
