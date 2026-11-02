# Possession-end checks for the special-foul rule
# (docs/superpowers/plans/2026-11-02-special-foul-possession-ends.md). Read-only.
#
#   Rscript scripts/verify_special_foul_possessions.R --season 2027
#       report: same-team repeats, off - def histogram, mirror check, affected games
#   Rscript scripts/verify_special_foul_possessions.R --season 2027 --baseline <file.csv>
#       also write the per team-game table
#   Rscript scripts/verify_special_foul_possessions.R --season 2027 --compare <file.csv>
#       also diff the per team-game table against an earlier baseline; exits 1 on a gate failure
#
# A "same-team repeat" is a possession end followed by the same team's next end in the
# same period: the opponent never had the ball in between. Queries are those of
# docs/verification/special_foul_2026-11-02/ (poss_alt.R, poss_ft.R, poss_diff.R).

args <- commandArgs(trailingOnly = TRUE)
arg_value <- function(flag) {
  i <- match(flag, args)
  if (is.na(i) || i == length(args)) NULL else args[[i + 1L]]
}
season <- as.integer(arg_value("--season"))
stopifnot(length(season) == 1L, !is.na(season))
baseline <- arg_value("--baseline")
compare <- arg_value("--compare")

readRenviron("app/.Renviron")
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L, dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"), bigint = "numeric")
q <- function(sql) dbGetQuery(con, sql, params = list(season))
SPECIAL <- "ARRAY['technical%', 'unsportsmanlike%', 'disqualif%']"

ends <- q("
  WITH g AS (SELECT DISTINCT game_id FROM basketball_test.final_schedule_mv WHERE game_year = $1)
  SELECT p.game_id, p.quarter, p.id, p.team_id, p.type,
         lag(p.team_id) OVER w AS prev_team, lag(p.id) OVER w AS prev_id, row_number() OVER w AS k
  FROM basketball_test.df_pts_poss_lineups_longer_mv p JOIN g USING (game_id)
  WHERE p.final_end_poss AND p.type_lineup = 'offense'
  WINDOW w AS (PARTITION BY p.game_id, p.quarter ORDER BY p.event_elapsed_seconds, p.id)")
ends <- ends[order(ends$game_id, ends$quarter, ends$k), ]
special_ft <- q(sprintf("
  WITH g AS (SELECT DISTINCT game_id FROM basketball_test.final_schedule_mv WHERE game_year = $1)
  SELECT DISTINCT ft.game_id, ft.id, f.parameters_type AS foul_kind
  FROM basketball_test.df_pts_poss_lineups_longer_mv ft JOIN g USING (game_id)
  JOIN basketball_test.df_pts_poss_lineups_longer_mv f ON f.game_id = ft.game_id AND f.id = ft.parent_action_id
  WHERE ft.type = 'freeThrow' AND ft.final_end_poss AND ft.type_lineup = 'offense'
    AND f.type = 'foul' AND lower(f.parameters_type) LIKE ANY (%s)", SPECIAL))
# A special-foul free throw that is the last end of its period is a legitimate end.
last_in_q <- ends[!duplicated(ends[c("game_id", "quarter")], fromLast = TRUE), c("game_id", "id")]
special_ft$period_end <- paste(special_ft$game_id, special_ft$id) %in% paste(last_in_q$game_id, last_in_q$id)

ends$rep <- !is.na(ends$prev_team) & ends$prev_team == ends$team_id
ends$after_special <- ends$rep & paste(ends$game_id, ends$prev_id) %in% paste(special_ft$game_id, special_ft$id)
n_games <- length(unique(ends$game_id))
cat(sprintf("season %d: %d games, %d possession ends\n", season, n_games, nrow(ends)))
cat(sprintf("  same-team repeats: %d (%.2f per game); after a special-foul free throw: %d; other: %d (%.2f per game)\n",
  sum(ends$rep), sum(ends$rep) / n_games, sum(ends$after_special),
  sum(ends$rep & !ends$after_special), sum(ends$rep & !ends$after_special) / n_games))
cat(sprintf("  special-foul free throws ending a possession: %d (%d of them the last end of a period)\n",
  nrow(special_ft), sum(special_ft$period_end)))

tg <- q("
  SELECT m.game_id, m.team_id, m.off_poss_raw, m.def_poss_raw, m.off_points_raw, m.def_points_raw,
         m.off_minutes, o.def_poss_raw AS opp_def_poss_raw
  FROM basketball_test.team_metrics_by_game_mv m
  LEFT JOIN basketball_test.team_metrics_by_game_mv o ON o.game_id = m.game_id AND o.team_id = m.opp_team_id
  WHERE m.game_year = $1")
tg <- tg[order(tg$game_id, tg$team_id), ]
d <- tg$off_poss_raw - tg$def_poss_raw
cat("  per team-game off - def:\n")
print(table(cut(d, c(-Inf, -4.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 4.5, Inf),
  c("<=-5", "-4..-3", "-2", "-1", "0", "+1", "+2", "+3..+4", ">=+5"))))
mirror_bad <- sum(tg$off_poss_raw != tg$opp_def_poss_raw, na.rm = TRUE)
cat(sprintf("  mirror (off = opponent's def): %d team-games differ, %d without an opponent row\n",
  mirror_bad, sum(is.na(tg$opp_def_poss_raw))))
affected <- sort(unique(special_ft$game_id[!special_ft$period_end]))
cat(sprintf("  affected games (special-foul free throw ending a possession mid-period): %d\n  %s\n",
  length(affected), paste(affected, collapse = ",")))

# Special-foul trips each team drew, mid-period: the expected drop in its off_poss_raw.
drawn <- merge(special_ft[!special_ft$period_end, c("game_id", "id")],
               ends[c("game_id", "id", "team_id")], by = c("game_id", "id"))
drawn$trips <- rep(1L, nrow(drawn))
drawn <- if (nrow(drawn)) aggregate(trips ~ game_id + team_id, drawn, sum) else
  data.frame(game_id = integer(0), team_id = integer(0), trips = integer(0))
tg <- merge(tg, drawn, by = c("game_id", "team_id"), all.x = TRUE)
tg$trips[is.na(tg$trips)] <- 0

if (!is.null(baseline)) {
  utils::write.csv(tg, baseline, row.names = FALSE)
  cat("baseline written:", baseline, "\n")
}
failures <- character(0)
if (!is.null(compare)) {
  b <- utils::read.csv(compare)
  m <- merge(b, tg, by = c("game_id", "team_id"), suffixes = c(".before", ".after"))
  changed <- function(col) m[[paste0(col, ".before")]] != m[[paste0(col, ".after")]]
  for (col in c("off_points_raw", "def_points_raw", "off_minutes")) {
    if (any(changed(col))) failures <- c(failures, sprintf("%s changed in %d team-games", col, sum(changed(col))))
  }
  # Each team-game loses exactly the special-foul trips it drew before the change.
  drop <- m$off_poss_raw.before - m$off_poss_raw.after
  bad <- drop != m$trips.before
  if (any(bad)) failures <- c(failures, sprintf("off_poss_raw drop differs from the trips drawn in %d team-games: %s",
    sum(bad), paste(unique(m$game_id[bad]), collapse = ",")))
  if (nrow(m) != nrow(b)) failures <- c(failures, sprintf("%d baseline team-games missing now", nrow(b) - nrow(m)))
  if (mirror_bad > 0) failures <- c(failures, "mirror check fails")
  if (length(affected)) failures <- c(failures, sprintf("%d games still have a mid-period special-foul end", length(affected)))
  cat(sprintf("compare against %s: %d team-games, %d possessions removed in total\n", compare, nrow(m), sum(drop)))
  if (length(failures)) cat("GATE FAILURES:\n", paste0("  - ", failures, "\n"), sep = "") else cat("all gates pass\n")
}
dbDisconnect(con)
if (length(failures)) quit(status = 1L)
