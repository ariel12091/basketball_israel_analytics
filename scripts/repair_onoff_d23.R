# D23: rebuild the current season's per-game OFF rows from the current lookup.
# Defaults to a rolled-back trial. --apply commits bounded game batches.
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
readRenviron("etl/.Renviron")
args <- commandArgs(trailingOnly = TRUE)
apply <- "--apply" %in% args
year_arg <- grep("^--year=", args, value = TRUE)
year <- if (length(year_arg)) as.integer(sub("--year=", "", year_arg)) else 2027L
stopifnot(length(year) == 1L, !is.na(year), year >= 2025L)

main <- function() {
  con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L,
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
    connect_timeout = 15L, bigint = "numeric")
  on.exit({
    suppressWarnings(try(dbRollback(con), silent = TRUE))
    dbDisconnect(con) # Also releases the session advisory lock.
  }, add = TRUE)
  dbExecute(con, "SET search_path TO basketball_test, public")
  dbExecute(con, "SET statement_timeout = '120s'")
  dbExecute(con, "SET lock_timeout = '5s'")

  # Production has no publication-lock API. Serialize this repair and take a
  # table write lock in each transaction; ordinary app SELECTs remain allowed.
  locked <- dbGetQuery(con, "SELECT pg_try_advisory_lock(hashtext('basketball_test.d23')) AS ok")$ok
  if (!isTRUE(locked)) stop("Another D23 repair is active")
  ids <- dbGetQuery(con, "SELECT DISTINCT game_id FROM final_schedule_mv WHERE game_year = $1 ORDER BY game_id",
    params = list(year))$game_id
  stopifnot(length(ids) > 0L)
  message(sprintf("D23 %s: season %d, %d games", if (apply) "APPLY" else "TRIAL", year, length(ids)))

  # Reuse the nightly pin's numeric comparison and its documented tolerances.
  pin <- parse("app/tests/testthat/test-player-reader-paths-db.R")
  helpers <- new.env(parent = globalenv())
  for (expr in pin) {
    if (is.call(expr) && identical(expr[[1]], as.name("<-")) &&
        as.character(expr[[2]]) %in% c("keyed", "gaps")) eval(expr, helpers)
  }
  parity <- function() {
    view <- dbGetQuery(con, sprintf('SELECT * FROM onoff_default_mv WHERE "Year" = %d', year))
    reader <- dbGetQuery(con, sprintf("SELECT * FROM onoff_compute('%d-09-01'::date, '%d-07-01'::date, NULL::text, 0, 0, -1e9, '%d')", year - 1L, year, year))
    sampled <- function(d) d[d[["ON Poss"]] >= 20 & d[["OFF Poss"]] >= 20, ]
    g <- helpers$gaps(sampled(view), sampled(reader))
    tol <- ifelse(grepl("^pr_", names(g$gap)), .03, ifelse(names(g$gap) == "minutes", .1, 1e-9))
    bad <- names(g$gap)[g$gap > tol]
    message("Summary differing columns: ", paste(bad, collapse = ", "))
    adv <- dbGetQuery(con, sprintf("SELECT * FROM player_advanced_stats_mv WHERE game_year = %d", year))
    for (fn in c("four_factors_compute", "four_factors_dashboard_compute")) {
      ff <- helpers$gaps(adv, dbGetQuery(con, sprintf("SELECT * FROM %s(%d)", fn, year)))
      diff <- names(ff$gap)[ff$gap > 1e-9]
      message(fn, " differing columns: ", paste(diff, collapse = ", "))
      bad <- c(bad, diff)
      if (ff$only_a || ff$only_b) bad <- c(bad, "player set")
    }
    length(bad) == 0L && g$only_b == 0L
  }
  parity()
  dbBegin(con)
  dbExecute(con, "LOCK TABLE player_four_factors_by_game IN SHARE ROW EXCLUSIVE MODE")
  dbExecute(con, sprintf("CREATE TEMP TABLE d23_before ON COMMIT PRESERVE ROWS AS SELECT * FROM player_four_factors_by_game WHERE game_year = %d", year))
  started <- proc.time()[["elapsed"]]
  batches <- split(ids, ceiling(seq_along(ids) / 5L))
  for (batch in batches) {
    dbExecute(con, "LOCK TABLE player_four_factors_by_game IN SHARE ROW EXCLUSIVE MODE")
    touched <- dbGetQuery(con, sprintf("SELECT refresh_player_four_factors_by_game_for_games(ARRAY[%s]::int4[]) AS n", paste(as.integer(batch), collapse = ",")))$n
    message(sprintf("games %s: %s rows (%.1fs elapsed)", paste(batch, collapse = ","), touched, proc.time()[["elapsed"]] - started))
    if (apply) { dbCommit(con); dbBegin(con) }
  }
  delta <- dbGetQuery(con, sprintf("SELECT (SELECT count(*) FROM d23_before) AS before_rows,
    (SELECT count(*) FROM player_four_factors_by_game WHERE game_year = %d) AS after_rows,
    (SELECT count(*) FROM (SELECT * FROM player_four_factors_by_game WHERE game_year = %d EXCEPT SELECT * FROM d23_before) x) AS added_or_changed,
    (SELECT count(*) FROM (SELECT * FROM d23_before EXCEPT SELECT * FROM player_four_factors_by_game WHERE game_year = %d) x) AS removed_or_changed", year, year, year))
  print(delta)
  changed_on <- dbGetQuery(con, sprintf("SELECT count(*) AS n FROM (
    (SELECT * FROM player_four_factors_by_game WHERE game_year = %d AND is_on_key = 1 EXCEPT SELECT * FROM d23_before WHERE is_on_key = 1)
    UNION ALL
    (SELECT * FROM d23_before WHERE is_on_key = 1 EXCEPT SELECT * FROM player_four_factors_by_game WHERE game_year = %d AND is_on_key = 1)) x", year, year))$n
  message("Changed ON rows: ", changed_on)
  if (changed_on != 0) stop("Unexpected ON changes; inspect before applying")
  if (!parity()) stop("D23 parity checks still fail")
  if (apply) dbExecute(con, "UPDATE app_meta SET value = to_char(clock_timestamp(), 'YYYY-MM-DD HH24:MI:SS'), updated_at = now() WHERE key = 'etl_full_last_success'")
  if (apply) dbCommit(con) else dbRollback(con)
  message(if (apply) "Repair committed; parity passed." else "Trial rolled back; parity passed; no persistent rows changed.")
}
main()
