readRenviron("app/.Renviron")
suppressMessages({library(DBI); library(RPostgres)})
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L, dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"), bigint = "numeric")
ends <- readRDS(file.path(Sys.getenv("S"), "poss_ends.rds"))
ends <- ends[order(ends$game_id, ends$quarter, ends$k), ]
key <- paste(ends$game_id, ends$quarter)
ends$prev_id <- ave(ends$id, key, FUN = function(v) c(NA, head(v, -1)))
ends$repeat_end <- !is.na(ends$prev_team) & ends$prev_team == ends$team_id
# Every free throw that ends a possession, with the foul it hangs off; flag the ones followed by a same-team repeat.
ft_end <- ends[ends$type == "freeThrow", c("game_year", "game_id", "id", "team_id")]
rep_prev <- unique(ends$prev_id[ends$repeat_end])
ft_end$then_repeat <- ft_end$id %in% rep_prev
ids <- unique(ft_end$id)
foul <- dbGetQuery(con, "
  SELECT DISTINCT ON (ft.id) ft.id, ft.parameters_type AS ft_kind, f.type AS parent_type, f.parameters_type AS foul_kind
  FROM basketball_test.df_pts_poss_lineups_longer_mv ft
  LEFT JOIN basketball_test.df_pts_poss_lineups_longer_mv f ON f.game_id = ft.game_id AND f.id = ft.parent_action_id
  WHERE ft.id = ANY($1::int4[]) AND ft.type = 'freeThrow'", list(paste0("{", paste(ids, collapse = ","), "}")))
ft_end <- merge(ft_end, foul, by = "id", all.x = TRUE)
ft_end$ft_kind[is.na(ft_end$ft_kind)] <- "(none)"
ft_end$foul_kind[is.na(ft_end$foul_kind)] <-paste0("(parent ", ft_end$parent_type[is.na(ft_end$foul_kind)], ")")
for (gy in sort(unique(ft_end$game_year))) {
  x <- ft_end[ft_end$game_year == gy, ]
  t <- aggregate(cbind(ends = 1, then_same_team_again = then_repeat) ~ foul_kind, x, sum)
  t$share_repeat <- sprintf("%.0f%%", 100 * t$then_same_team_again / t$ends)
  cat(sprintf("\n== %d: free throws that end a possession, by the foul they come from\n", gy))
  print(t[order(-t$then_same_team_again), ], row.names = FALSE)
  k <- aggregate(cbind(ends = 1, then_same_team_again = then_repeat) ~ ft_kind, x, sum)
  cat("  by free-throw label:\n"); print(k[order(-k$then_same_team_again), ], row.names = FALSE)
}
saveRDS(ends, file.path(Sys.getenv("S"), "poss_ends2.rds"))
dbDisconnect(con)
