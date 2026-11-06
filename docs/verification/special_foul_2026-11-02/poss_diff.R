readRenviron("app/.Renviron")
suppressMessages({library(DBI); library(RPostgres)})
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L, dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"), bigint = "numeric")
g <- dbGetQuery(con, "
  SELECT m.game_year, m.game_id, m.team_id, m.team_name, m.opp_team_id, m.off_poss_raw AS off, m.def_poss_raw AS def,
         o.off_poss_raw AS opp_off, o.def_poss_raw AS opp_def, m.off_minutes
  FROM basketball_test.team_metrics_by_game_mv m
  LEFT JOIN basketball_test.team_metrics_by_game_mv o ON o.game_id = m.game_id AND o.team_id = m.opp_team_id
  WHERE m.game_year IN (2025, 2026, 2027)")
g$d <- g$off - g$def
for (gy in sort(unique(g$game_year))) {
  x <- g[g$game_year == gy, ]
  cat(sprintf("\n== %d: %d team-games\n", gy, nrow(x)))
  cat(sprintf("  mirror: off == opponent's def in %d, differs in %d, no opponent row %d\n",
    sum(x$off == x$opp_def, na.rm = TRUE), sum(x$off != x$opp_def, na.rm = TRUE), sum(is.na(x$opp_def))))
  cat("  per-game off - def:\n"); print(table(cut(x$d, c(-Inf, -4.5, -2.5, -1.5, -0.5, 0.5, 1.5, 2.5, 4.5, Inf),
    c("<=-5", "-4..-3", "-2", "-1", "0", "+1", "+2", "+3..+4", ">=+5"))))
  s <- aggregate(cbind(off, def, d, games = 1) ~ team_id + team_name, x, sum)
  s$per_game <- round(s$d / s$games, 2)
  print(s[order(-abs(s$d)), c("team_name", "games", "off", "def", "d", "per_game")], row.names = FALSE)
  big <- x[abs(x$d) >= 5, c("game_id", "team_name", "off", "def", "d", "off_minutes")]
  if (nrow(big)) { cat("  games with |off - def| >= 5:\n"); print(big[order(-abs(big$d)), ], row.names = FALSE) }
}
saveRDS(g, file.path(Sys.getenv("S"), "poss_diff.rds"))
dbDisconnect(con)
