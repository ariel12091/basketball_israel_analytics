readRenviron("etl/.Renviron")
suppressMessages({library(DBI); library(RPostgres)})
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L, dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"), bigint = "numeric")
# Same alternation check as the Israeli one: possession ends in game order within a period,
# flagged when the same offense ends two in a row.
e <- dbGetQuery(con, "
  SELECT s.competition, a.season, a.game_id, a.period, a.game_possession_number, a.possession_offense_team_id AS team,
         a.endpoint_reason, a.play_type,
         lag(a.possession_offense_team_id) OVER w AS prev_team, lag(a.endpoint_reason) OVER w AS prev_reason,
         lag(a.play_type) OVER w AS prev_play
  FROM euroleague.actions a JOIN euroleague.schedule s ON s.game_id = a.game_id
  WHERE a.end_possession AND a.season IN (2024, 2025, 2026)
  WINDOW w AS (PARTITION BY a.game_id, a.period ORDER BY a.game_possession_number)")
e$rep <- !is.na(e$prev_team) & e$prev_team == e$team
for (k in split(e, list(e$competition, e$season), drop = TRUE)) {
  ng <- length(unique(k$game_id))
  cat(sprintf("\n== %s %d: %d possession ends, %d games; same team twice in a row: %d (%.2f%%, %.2f per game)\n",
    k$competition[1], k$season[1], nrow(k), ng, sum(k$rep), 100 * mean(k$rep), sum(k$rep) / ng))
  if (any(k$rep)) print(head(sort(table(paste(k$prev_reason[k$rep], k$prev_play[k$rep], "->", k$endpoint_reason[k$rep])), decreasing = TRUE), 6))
  # Per team-game off - def.
  k$poss <- 1; tg <- aggregate(poss ~ game_id + team, k, sum)
  gm <- merge(tg, tg, by = "game_id", suffixes = c("", "_opp")); gm <- gm[gm$team != gm$team_opp, ]
  d <- gm$poss - gm$poss_opp
  print(table(cut(d, c(-Inf, -4.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 4.5, Inf),
    c("<=-5", "-4..-3", "-2", "-1", "0", "+1", "+2", "+3..+4", ">=+5"))))
}
# Free-throw endpoints coming off special fouls (CMU unsportsmanlike, CMT/CMTI technical, B/C bench/coach).
print(dbGetQuery(con, "
  SELECT p.play_type AS parent_foul, count(*) AS ft_endpoints
  FROM euroleague.actions a JOIN euroleague.actions p ON p.game_id = a.game_id AND p.source_event_order = a.synthetic_parent_order
  WHERE a.end_possession AND a.play_type = 'FTM' AND a.season IN (2024, 2025, 2026)
  GROUP BY 1 ORDER BY 2 DESC"))
dbDisconnect(con)
