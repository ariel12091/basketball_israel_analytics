readRenviron("app/.Renviron")
suppressMessages({library(DBI); library(RPostgres)})
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L, dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"), bigint = "numeric")
# One row per possession end (offense perspective, so team_id is the team that had the ball),
# in game order, with the previous end in the same period.
ends <- dbGetQuery(con, "
  WITH g AS (SELECT DISTINCT game_id, game_year FROM basketball_test.team_metrics_by_game_mv WHERE game_year IN (2025, 2026, 2027)),
  e AS (
    SELECT g.game_year, p.game_id, p.quarter, p.id, p.team_id, p.type, p.parameters_type, p.parameters_made,
           p.event_elapsed_seconds,
           lag(p.team_id) OVER w AS prev_team, lag(p.type) OVER w AS prev_type,
           lag(p.parameters_type) OVER w AS prev_ptype, lag(p.parameters_made) OVER w AS prev_made,
           row_number() OVER w AS k, count(*) OVER (PARTITION BY p.game_id, p.quarter) AS n_q
    FROM basketball_test.df_pts_poss_lineups_longer_mv p JOIN g USING (game_id)
    WHERE p.final_end_poss AND p.type_lineup = 'offense'
    WINDOW w AS (PARTITION BY p.game_id, p.quarter ORDER BY p.event_elapsed_seconds, p.id))
  SELECT * FROM e")
saveRDS(ends, file.path(Sys.getenv("S"), "poss_ends.rds"))
ends$repeat_end <- !is.na(ends$prev_team) & ends$prev_team == ends$team_id
for (gy in sort(unique(ends$game_year))) {
  x <- ends[ends$game_year == gy, ]
  cat(sprintf("\n== %d: %d possession ends in %d games; same team twice in a row: %d (%.2f%%, %.1f per game)\n", gy, nrow(x),
    length(unique(x$game_id)), sum(x$repeat_end), 100 * mean(x$repeat_end), sum(x$repeat_end) / length(unique(x$game_id))))
  r <- x[x$repeat_end, ]
  lab <- function(t, pt, m) paste0(t, ifelse(is.na(pt), "", paste0(":", pt)), ifelse(is.na(m), "", paste0(":", m)))
  pair <- paste(lab(r$prev_type, r$prev_ptype, r$prev_made), "->", lab(r$type, r$parameters_type, r$parameters_made))
  cat("  most common (earlier end -> repeated end):\n"); print(head(sort(table(pair), decreasing = TRUE), 15))
}
# Does broken alternation explain the per-game gaps? Per team-game: possessions, repeats by the team and by its opponent.
tg <- aggregate(cbind(poss = 1, reps = repeat_end) ~ game_year + game_id + team_id, ends, sum)
gm <- merge(tg, tg, by = c("game_year", "game_id"), suffixes = c("", "_opp"))
gm <- gm[gm$team_id != gm$team_id_opp, ]
gm$d <- gm$poss - gm$poss_opp
gm$d_wo_reps <- gm$d - (gm$reps - gm$reps_opp)
cat("\n== per team-game off - def, raw vs after removing same-team repeats (all three seasons)\n")
br <- c(-Inf, -4.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 4.5, Inf)
lb <- c("<=-5", "-4..-3", "-2", "-1", "0", "+1", "+2", "+3..+4", ">=+5")
print(rbind(raw = table(cut(gm$d, br, lb)), without_repeats = table(cut(gm$d_wo_reps, br, lb))))
cat(sprintf("  games with any repeat: %d of %d\n", sum(tapply(gm$reps, gm$game_id, sum) > 0), length(unique(gm$game_id))))
dbDisconnect(con)
