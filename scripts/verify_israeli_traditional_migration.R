suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
script_path <- if (length(file_arg)) sub("^--file=", "", file_arg[[1]]) else "scripts/x"
repo_root <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/", mustWork = TRUE)
readRenviron(file.path(repo_root, "app", ".Renviron"))

con <- dbConnect(
  Postgres(), host = Sys.getenv("PG_HOST"), port = 6543L,
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
  bigint = "numeric", connect_timeout = 15L
)
on.exit(dbDisconnect(con), add = TRUE)
dbExecute(con, "SET statement_timeout = '20000ms'")

timed <- function(label, sql, params = NULL) {
  started <- proc.time()[["elapsed"]]
  value <- dbGetQuery(con, sql, params = params)
  cat(sprintf("%-24s %.2fs rows=%d\n", label, proc.time()[["elapsed"]] - started, nrow(value)))
  value
}

profile <- timed("fact profile", "
SELECT count(*) AS rows, count(DISTINCT game_id) AS games
FROM basketball_test.player_traditional_by_game")
full <- timed("reader full season",
              "SELECT * FROM basketball_test.get_player_traditional_from_games($1)", list(2026L))
half <- timed("reader second half",
              "SELECT * FROM basketball_test.get_player_traditional_from_games($1,$2,$3)",
              list(2026L, as.Date("2026-02-05"), as.Date("2026-06-30")))

mv <- dbGetQuery(con, "
SELECT player_id, team_id, gp, poss_on_floor, minutes, pts, reb, oreb, dreb,
       ast, stl, blk, dfl, tov, fgm, fga, \"3pm\", \"3pa\", ftm, fta,
       fg_pct, tp_pct, ft_pct, efg, ts, usg_pct
FROM basketball_test.player_traditional_stats_mv WHERE game_year = 2026")
cols <- intersect(names(full), names(mv))
full_cmp <- full[order(full$team_id, full$player_id), cols, drop = FALSE]
mv_cmp <- mv[order(mv$team_id, mv$player_id), cols, drop = FALSE]
same_keys <- identical(
  paste(full_cmp$team_id, full_cmp$player_id, sep = ":"),
  paste(mv_cmp$team_id, mv_cmp$player_id, sep = ":")
)
different_cells <- if (same_keys) sum(mapply(function(x, y) {
  !(is.na(x) & is.na(y)) & (is.na(x) | is.na(y) | x != y)
}, full_cmp, mv_cmp)) else NA_integer_
if (isTRUE(same_keys) && different_cells > 0) {
  for (column in cols) {
    differs <- !(is.na(full_cmp[[column]]) & is.na(mv_cmp[[column]])) &
      (is.na(full_cmp[[column]]) | is.na(mv_cmp[[column]]) |
         full_cmp[[column]] != mv_cmp[[column]])
    if (any(differs)) {
      print(data.frame(
        team_id = full_cmp$team_id[differs],
        player_id = full_cmp$player_id[differs],
        column = column,
        reader = full_cmp[[column]][differs],
        season_mv = mv_cmp[[column]][differs]
      ), row.names = FALSE)
    }
  }
}

internal_denied <- tryCatch({
  dbGetQuery(con, "SELECT count(*) FROM basketball_test.compute_player_traditional_by_game(ARRAY[64942]::int4[])")
  FALSE
}, error = function(e) grepl("permission denied", conditionMessage(e), ignore.case = TRUE))

cat(sprintf("profile_rows=%s games=%s full_rows=%d half_rows=%d parity_keys=%s parity_cells=%s internal_compute_denied=%s\n",
            profile$rows[[1]], profile$games[[1]], nrow(full), nrow(half),
            same_keys, different_cells, internal_denied))
stopifnot(profile$rows[[1]] >= 8522, profile$games[[1]] == 439,
          nrow(full) == 313, nrow(half) == 266,
          same_keys, different_cells == 0, internal_denied)
