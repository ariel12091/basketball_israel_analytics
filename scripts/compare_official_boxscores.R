# =============================================================================
# scripts/compare_official_boxscores.R
#
# Read-only check of the app's SQL against the official box score.
# For every processed game of one season it downloads the box score and
# compares, per player, the box line returned by
# get_player_traditional_from_games() for that one game (the reader behind
# Player Stats). Plus-minus is reported but not judged: the provider's own
# plus-minus does not sum to five times the margin in many games, so it is no
# reference for the lineup joins.
#
# Box score source: basket.co.il first, Segev second. A box is used only when
# its team points equal the official final score; otherwise the other source
# is tried, and a game with no reconciling box is reported, not compared.
# Nothing is written to the database. Box scores are not stored.
#
# Usage:
#   Rscript scripts/compare_official_boxscores.R [game_year] [out_dir]
#   game_year defaults to 2027 (season 2026-27).
#
# Exit status 1 when a difference is not covered by KNOWN_EXCEPTIONS.
# =============================================================================

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
  library(jsonlite)
})

args <- commandArgs(trailingOnly = TRUE)
game_year <- if (length(args) >= 1) as.integer(args[[1]]) else 2027L
out_dir <- if (length(args) >= 2) args[[2]] else file.path("etl", "logs", "boxscore_compare")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

root <- normalizePath(file.path(dirname(sub("^--file=", "", grep("^--file=", commandArgs(FALSE), value = TRUE)[1])), ".."),
                      winslash = "/", mustWork = FALSE)
renv_file <- file.path(root, "app", ".Renviron")
if (file.exists(renv_file)) readRenviron(renv_file)

# Known, explained differences (R9: declared with a reason, never tolerated
# silently). scope: "counts" = box-score count stats, "minutes", "player" =
# a player present on one side only. Remove a row the day its cause is fixed.
# Count differences in games whose play-by-play misses the official score are
# covered by the feed-gap rule below, not listed here.
KNOWN_EXCEPTIONS <- data.frame(
  game_id = c(406L, 399L, 398L, 402L, 414L, 417L, 406L, 399L,
              210L, 178L, 381L, 211L, 62452L),
  scope   = c("minutes", "minutes", "minutes", "minutes", "minutes", "minutes", "player", "player",
              "player", "minutes", "minutes", "minutes", "minutes"),
  reason  = c(
    "Q4 timing reconstructed from wall clock (feed froze the Q4 clock)",
    "Misclocked substitution flurry at period starts (KNOWN_CLOCK_STAMP_CORRECTIONS); provider box minutes sum to 301",
    "Misclocked substitution flurry at period starts (KNOWN_CLOCK_STAMP_CORRECTIONS)",
    "Provider clock defects; Segev box incomplete (66-87 vs official 82-99)",
    "Primary feed reused action ids; loaded from the basket.co.il archive",
    "Period openings restored by the game-417 correction",
    "Player 1380: 0.8 min in the reconstructed Q4; not listed in the box score",
    "Player 2312: 6 s on the floor; Player Stats omits him (open finding)",
    "Old id 1143 left in rosters and lineups for this game (data-quality check C; rerun etl_full(game_ids = 210))",
    "28% of event rows have no five-man lineup (data-quality checks Q, R)",
    "Provider JSON defects (docs/game_381_json_quality_report_2026-06-10.md)",
    "Overtime starts without a lineup (data-quality check Y)",
    "Q4 feed sends five players out without declaring the incoming five (data-quality checks Q, X)"
  ),
  stringsAsFactors = FALSE
)

# Port 5432 (session pooler): on 6543 a concurrent app_readonly client, such
# as the live app, can swap prepared statements mid-run (CLAUDE.md, port 6543).
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("BOXCMP_PG_PORT", "5432")),
                 dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
                 sslmode = Sys.getenv("PG_SSLMODE", "require"), bigint = "numeric", connect_timeout = 15L)
on.exit(dbDisconnect(con), add = TRUE)
dbExecute(con, "SET default_transaction_read_only = on")

BOX_SOURCES <- list(
  basket = list(url = "https://basket.co.il/livestats/data/game-%d.json", path = function(x) x$boxscore),
  segev  = list(url = "https://stats.segevstats.com/realtimestat_heb/get_team_score.php?game_id=%d",
                path = function(x) x$result$boxscore)
)

read_json_url <- function(u) {
  txt <- tryCatch({
    con_u <- url(u, encoding = "UTF-8")
    on.exit(close(con_u))
    paste(readLines(con_u, warn = FALSE), collapse = "\n")
  }, error = function(e) NA_character_, warning = function(w) NA_character_)
  if (is.na(txt)) return(NULL)
  tryCatch(fromJSON(sub("^﻿", "", txt), simplifyVector = TRUE), error = function(e) NULL)
}

box_players <- function(box, game_id) {
  gi <- box$gameInfo
  sides <- list(list(team_id = as.integer(gi$homeTeamId), p = box$homeTeam$players),
                list(team_id = as.integer(gi$awayTeamId), p = box$awayTeam$players))
  do.call(rbind, lapply(sides, function(s) {
    p <- s$p
    n <- function(col) if (is.null(p[[col]])) rep(NA_real_, nrow(p)) else suppressWarnings(as.numeric(p[[col]]))
    mins <- vapply(strsplit(as.character(p$minutes), ":"), function(v) {
      v <- suppressWarnings(as.numeric(v)); if (length(v) == 2) v[1] + v[2] / 60 else NA_real_
    }, numeric(1))
    data.frame(
      game_id = game_id, team_id = s$team_id, player_id = as.integer(p$playerId),
      minutes = mins, pts = n("points"),
      fgm = n("fg_2m") + n("fg_3m"), fga = n("fg_2m") + n("fg_2mis") + n("fg_3m") + n("fg_3mis"),
      `3pm` = n("fg_3m"), `3pa` = n("fg_3m") + n("fg_3mis"),
      ftm = n("ft_m"), fta = n("ft_m") + n("ft_mis"),
      oreb = n("reb_o"), dreb = n("reb_d"), reb = n("reb_o") + n("reb_d"),
      ast = n("ast"), stl = n("stl"), blk = n("blk"), tov = n("to"),
      plus_minus = n("plusMinus"),
      check.names = FALSE
    )
  }))
}

# First source whose team points equal the official final score.
fetch_box <- function(game_id, official) {
  tried <- character()
  for (src in names(BOX_SOURCES)) {
    s <- BOX_SOURCES[[src]]
    x <- read_json_url(sprintf(s$url, game_id))
    Sys.sleep(0.3)
    box <- if (is.null(x)) NULL else s$path(x)
    if (is.null(box) || is.null(box$homeTeam$players)) { tried <- c(tried, paste(src, "unavailable")); next }
    b <- tryCatch(box_players(box, game_id), error = function(e) NULL)
    if (is.null(b)) { tried <- c(tried, paste(src, "unreadable")); next }
    pts <- tapply(b$pts, b$team_id, sum, na.rm = TRUE)
    want <- setNames(official$team_score, official$team_id)
    if (length(pts) == 2 && all(names(want) %in% names(pts)) && all(pts[names(want)] == want)) {
      return(list(players = b, source = src, note = paste(tried, collapse = "; ")))
    }
    tried <- c(tried, sprintf("%s %s does not reconcile with official %s", src,
                              paste(pts[names(want)], collapse = "-"), paste(want, collapse = "-")))
  }
  list(players = NULL, source = NA_character_, note = paste(tried, collapse = "; "))
}

games <- dbGetQuery(con, "
  SELECT DISTINCT s.game_id, s.game_date, s.team_id, s.team_score
  FROM basketball_test.final_schedule_mv s
  JOIN basketball_test.etl_processed_games p USING (game_id)
  WHERE s.game_year = $1
  ORDER BY s.game_date, s.game_id, s.team_id", params = list(game_year))
message(sprintf("Season %d: %d games, %d team-games", game_year,
                length(unique(games$game_id)), nrow(games)))

# The reader behind Player Stats, limited to one team on one date. A team plays
# at most once on a date, so this is that team's box for that game.
READER_SQL <- '
  SELECT player_id, team_id, minutes, pts, reb, oreb, dreb, ast, stl, blk, tov,
         fgm, fga, "3pm", "3pa", ftm, fta
  FROM basketball_test.get_player_traditional_from_games($1, $2::date, $2::date, $3)'

STATS <- c("minutes", "pts", "fgm", "fga", "3pm", "3pa", "ftm", "fta",
           "oreb", "dreb", "reb", "ast", "stl", "blk", "tov")

# The provider re-mints and recycles player ids; the identity dictionary maps a
# source id to the canonical id the app uses. Map the box the same way: game
# scope first, then season scope (player_identity_aliases_v is the ETL's feed).
aliases <- dbGetQuery(con, "
  SELECT game_id, team_id, alias_player_id, canonical_player_id
  FROM basketball_test.player_identity_aliases_v WHERE game_year = $1", params = list(game_year))
canonical_ids <- function(box) {
  key_game <- paste(aliases$game_id, aliases$team_id, aliases$alias_player_id)
  key_season <- paste(aliases$team_id, aliases$alias_player_id)
  season_rows <- is.na(aliases$game_id)
  g <- match(paste(box$game_id, box$team_id, box$player_id), key_game)
  s <- match(paste(box$team_id, box$player_id), key_season[season_rows])
  out <- box$player_id
  out[!is.na(s)] <- aliases$canonical_player_id[season_rows][s[!is.na(s)]]
  out[!is.na(g)] <- aliases$canonical_player_id[g[!is.na(g)]]
  out
}

# Games whose play-by-play points do not reach the official final score are
# missing feed events (the data-quality score reconciliation lists them). A
# count difference in such a game is explained by that gap.
feed_gap_games <- dbGetQuery(con, "
  WITH pbp AS (
    SELECT game_id, team_id, SUM(COALESCE(team_score, 0)) AS pts
    FROM basketball_test.df_pts_poss_lineups_longer_mv
    WHERE type_lineup = 'offense' GROUP BY 1, 2)
  SELECT DISTINCT s.game_id
  FROM basketball_test.final_schedule_mv s
  JOIN pbp p USING (game_id, team_id)
  WHERE s.game_year = $1 AND p.pts <> s.team_score", params = list(game_year))$game_id

rows <- list()
game_log <- list()
for (g in unique(games$game_id)) {
  gd <- games[games$game_id == g, ]
  fb <- fetch_box(g, gd)
  game_log[[length(game_log) + 1]] <- data.frame(game_id = g, source = fb$source, note = fb$note)
  if (is.null(fb$players)) { message(sprintf("  game %d: no reconciling box (%s)", g, fb$note)); next }
  box <- fb$players
  app <- do.call(rbind, lapply(seq_len(nrow(gd)), function(i) {
    dbGetQuery(con, READER_SQL, params = list(game_year, as.character(gd$game_date[i]), as.character(gd$team_id[i])))
  }))
  # A box row with no minutes and no events did not play.
  played <- !(box$minutes %in% 0 & rowSums(box[, c("pts", "fga", "fta", "reb", "ast", "stl", "blk", "tov")], na.rm = TRUE) == 0)
  box <- box[played, ]
  box$player_id <- canonical_ids(box)
  m <- merge(box, app, by = c("player_id", "team_id"), all = TRUE, suffixes = c(".box", ".app"))
  m$match <- ifelse(is.na(m$minutes.box), "app_only", ifelse(is.na(m$minutes.app), "box_only", "both"))
  for (s in STATS) {
    rows[[length(rows) + 1]] <- data.frame(
      game_id = g, source = fb$source, team_id = m$team_id, player_id = m$player_id, match = m$match,
      stat = s, box = m[[paste0(s, ".box")]], app = m[[paste0(s, ".app")]])
  }
  message(sprintf("  game %d [%s]: %d box players, %d reader rows", g, fb$source, nrow(box), nrow(app)))
}
res <- do.call(rbind, rows)
res$diff <- res$app - res$box
# Minutes: the reader rounds to 0.1 and the box to the second, hence 0.06.
res$ok <- res$match == "both" & !is.na(res$diff) &
  abs(res$diff) <= ifelse(res$stat == "minutes", 0.06, 0) + 1e-9
res$scope <- ifelse(res$match != "both", "player", ifelse(res$stat == "minutes", "minutes", "counts"))

exc_key <- paste(KNOWN_EXCEPTIONS$game_id, KNOWN_EXCEPTIONS$scope)
res$feed_gap <- !res$ok & res$scope == "counts" & res$game_id %in% feed_gap_games
res$known <- !res$ok & (res$feed_gap | paste(res$game_id, res$scope) %in% exc_key)
games_df <- do.call(rbind, game_log)

out_csv <- file.path(out_dir, sprintf("boxscore_compare_%d.csv", game_year))
write.csv(res, out_csv, row.names = FALSE)
write.csv(games_df, file.path(out_dir, sprintf("boxscore_sources_%d.csv", game_year)), row.names = FALSE)

both <- res[res$match == "both", ]
summary_df <- do.call(rbind, lapply(STATS, function(s) {
  d <- both[both$stat == s, ]
  data.frame(stat = s, player_games = nrow(d), exact = sum(d$ok), known = sum(d$known),
             unexplained = sum(!d$ok & !d$known),
             max_abs_diff = if (nrow(d)) max(abs(d$diff), na.rm = TRUE) else NA)
}))

cat(sprintf("\nSeason %d: %d games. Box source: %s. No reconciling box: %d game(s)%s\n", game_year,
            nrow(games_df), paste(names(table(games_df$source)), table(games_df$source), sep = " ", collapse = ", "),
            sum(is.na(games_df$source)),
            if (any(is.na(games_df$source))) paste0(" (", paste(games_df$game_id[is.na(games_df$source)], collapse = ", "), ")") else ""))
ids <- unique(res[, c("game_id", "team_id", "player_id", "match")])
cat(sprintf("Player-games: %d in both, %d only in the box score, %d only in the app\n",
            sum(ids$match == "both"), sum(ids$match == "box_only"), sum(ids$match == "app_only")))
cat("(minutes: within 0.06 min = exact; every other stat exact)\n\n")
print(summary_df, row.names = FALSE)

bad <- res[!res$ok & !res$known, ]
if (nrow(bad)) {
  cat("\nUnexplained differences by game:\n")
  by_game <- split(bad, bad$game_id)
  for (g in names(by_game)) {
    d <- by_game[[g]]
    cat(sprintf("  game %s [%s]: %s\n", g, d$source[1],
                paste(head(sprintf("%s:%s box=%s app=%s", d$player_id, d$stat, d$box, d$app), 8), collapse = "; ")))
  }
}
cat(sprintf("\nDetail: %s\n", normalizePath(out_csv, winslash = "/")))
if (nrow(bad) || any(is.na(games_df$source))) quit(status = 1)
