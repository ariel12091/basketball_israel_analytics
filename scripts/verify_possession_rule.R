# Possession-end checks for a possession-end rule (--rule special-foul: D29, plan
# docs/superpowers/plans/2026-11-02-special-foul-possession-ends.md; --rule and-one: D30, plan
# docs/superpowers/plans/2026-11-03-and-one-own-rebound-possession.md; --rule phantom-ends: D31, plan
# docs/superpowers/plans/2026-11-03-phantom-possession-ends.md). Read-only.
#
#   Rscript scripts/verify_possession_rule.R --rule and-one|phantom-ends --season 2027
#       report: same-team repeats, off - def histogram, mirror check, affected games
#   Rscript scripts/verify_possession_rule.R --rule and-one --season 2027 --baseline <file.csv>
#       also write the per team-game table
#   Rscript scripts/verify_possession_rule.R --rule and-one --season 2027 --compare <file.csv>
#       also diff the per team-game table against an earlier baseline; exits 1 on a gate failure
#
# A "same-team repeat" is a possession end followed by the same team's next end in the
# same period (by id): the opponent never had the ball in between. "Target ends" are the rows
# the rule's targets SQL selects (the table RULES below): ends the rule removes, still present
# before the backfill and gone after it.

# One table per rule: its targets SQL, as in scripts/apply_possession_end_backfill.R.
RULES <- list(
  `special-foul` = list(sql = "sql/migrations/2026-11-02_special_foul_possession_targets.sql"),
  `and-one`      = list(sql = "sql/migrations/2026-11-03_and_one_possession_targets.sql"),
  `phantom-ends` = list(sql = "sql/migrations/2026-11-03_phantom_possession_end_targets.sql")
)

args <- commandArgs(trailingOnly = TRUE)
arg_value <- function(flag) {
  i <- match(flag, args)
  if (is.na(i) || i == length(args)) NULL else args[[i + 1L]]
}
rule_name <- arg_value("--rule")
stopifnot("--rule special-foul|and-one|phantom-ends is required" = !is.null(rule_name), rule_name %in% names(RULES))
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

ends <- q("
  WITH g AS (SELECT DISTINCT game_id FROM basketball_test.final_schedule_mv WHERE game_year = $1)
  SELECT p.game_id, p.quarter, p.id, p.team_id, p.type,
         lag(p.team_id) OVER w AS prev_team, lag(p.id) OVER w AS prev_id
  FROM basketball_test.df_pts_poss_lineups_longer_mv p JOIN g USING (game_id)
  WHERE p.final_end_poss AND p.type_lineup = 'offense'
  WINDOW w AS (PARTITION BY p.game_id, p.quarter ORDER BY p.id)")
targets_sql <- paste(readLines(RULES[[rule_name]]$sql), collapse = "\n")
target_rows <- dbGetQuery(con, targets_sql, params = list(sprintf("{%d}", season)))

key <- function(g, i) paste(g, i)
ends$rep <- !is.na(ends$prev_team) & ends$prev_team == ends$team_id
ends$after_target <- ends$rep & key(ends$game_id, ends$prev_id) %in% key(target_rows$game_id, target_rows$id)
n_games <- length(unique(ends$game_id))
cat(sprintf("rule %s, season %d: %d games, %d possession ends\n", rule_name, season, n_games, nrow(ends)))
cat(sprintf("  same-team repeats: %d (%.2f per game); after a target end: %d; other: %d (%.2f per game)\n",
  sum(ends$rep), sum(ends$rep) / n_games, sum(ends$after_target),
  sum(ends$rep & !ends$after_target), sum(ends$rep & !ends$after_target) / n_games))
cat(sprintf("  target ends: %d\n", nrow(target_rows)))

tg <- q("
  SELECT m.game_id, m.team_id, m.off_poss_raw, m.def_poss_raw, m.off_points_raw, m.def_points_raw,
         m.off_minutes, o.def_poss_raw AS opp_def_poss_raw
  FROM basketball_test.team_metrics_by_game_mv m
  LEFT JOIN basketball_test.team_metrics_by_game_mv o ON o.game_id = m.game_id AND o.team_id = m.opp_team_id
  WHERE m.game_year = $1 AND m.off_poss_raw IS NOT NULL")
tg <- tg[order(tg$game_id, tg$team_id), ]
d <- tg$off_poss_raw - tg$def_poss_raw
cat("  per team-game off - def:\n")
print(table(cut(d, c(-Inf, -4.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 4.5, Inf),
  c("<=-5", "-4..-3", "-2", "-1", "0", "+1", "+2", "+3..+4", ">=+5"))))
mirror_bad <- sum(tg$off_poss_raw != tg$opp_def_poss_raw, na.rm = TRUE)
cat(sprintf("  mirror (off = opponent's def): %d team-games differ, %d without an opponent row\n",
  mirror_bad, sum(is.na(tg$opp_def_poss_raw))))
affected <- sort(unique(target_rows$game_id))
cat(sprintf("  affected games: %d\n  %s\n", length(affected), paste(affected, collapse = ",")))

# Ends each team-game holds on target rows: the expected drop in its off_poss_raw.
drawn <- merge(target_rows[c("game_id", "id")], ends[c("game_id", "id", "team_id")], by = c("game_id", "id"))
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
    if (any(changed(col))) failures <- c(failures, sprintf("%s changed in %d team-games: %s", col, sum(changed(col)),
      paste(unique(m$game_id[changed(col)]), collapse = ",")))
  }
  # Each team-game loses exactly the target ends it held before the change.
  drop <- m$off_poss_raw.before - m$off_poss_raw.after
  bad <- drop != m$trips.before
  if (any(bad)) failures <- c(failures, sprintf("off_poss_raw drop differs from the target ends in %d team-games: %s",
    sum(bad), paste(unique(m$game_id[bad]), collapse = ",")))
  if (nrow(m) != nrow(b)) failures <- c(failures, sprintf("%d baseline team-games missing now", nrow(b) - nrow(m)))
  # A season may start with a few (2025 has 2); the change must not add any.
  mirror_before <- sum(b$off_poss_raw != b$opp_def_poss_raw, na.rm = TRUE)
  if (mirror_bad > mirror_before) failures <- c(failures, sprintf("mirror check: %d team-games differ, %d before", mirror_bad, mirror_before))
  if (nrow(target_rows)) failures <- c(failures, sprintf("%d target ends remain in %d games", nrow(target_rows), length(affected)))
  cat(sprintf("compare against %s: %d team-games, %d possessions removed in total\n", compare, nrow(m), sum(drop)))
  if (length(failures)) cat("GATE FAILURES:\n", paste0("  - ", failures, "\n"), sep = "") else cat("all gates pass\n")
}
dbDisconnect(con)
if (length(failures)) quit(status = 1L)
