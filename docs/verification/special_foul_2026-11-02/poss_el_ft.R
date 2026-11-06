readRenviron("etl/.Renviron")
suppressMessages({library(DBI); library(RPostgres)})
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L, dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"), bigint = "numeric")
# FT endpoints, the foul they are grouped under, and whether the same offense ends the next possession.
x <- dbGetQuery(con, "
  WITH e AS (
    SELECT a.game_id, a.season, a.source_event_order, a.synthetic_parent_order, a.play_type, a.possession_offense_team_id AS team,
           lead(a.possession_offense_team_id) OVER w AS next_team, a.period, lead(a.period) OVER w AS next_period
    FROM euroleague.actions a WHERE a.end_possession AND a.season IN (2025, 2026)
    WINDOW w AS (PARTITION BY a.game_id ORDER BY a.game_possession_number))
  SELECT s.competition, e.season, p.play_type AS parent, coalesce(p.comment, '') AS parent_comment,
         count(*) AS ft_endpoints,
         count(*) FILTER (WHERE e.next_team = e.team AND e.next_period = e.period) AS then_same_team
  FROM e JOIN euroleague.schedule s ON s.game_id = e.game_id
  LEFT JOIN euroleague.actions p ON p.game_id = e.game_id AND p.source_event_order = e.synthetic_parent_order
  WHERE e.play_type = 'FTM'
  GROUP BY 1, 2, 3, 4 HAVING count(*) FILTER (WHERE e.next_team = e.team AND e.next_period = e.period) > 0 OR count(*) > 200
  ORDER BY 1, 2, 6 DESC")
print(x, row.names = FALSE)
# Foul codes the provider used per season, to see whether new codes appeared in 2026-27.
print(dbGetQuery(con, "
  SELECT play_type, count(*) FILTER (WHERE season = 2025) AS s2025, count(*) FILTER (WHERE season = 2026) AS s2026
  FROM euroleague.actions WHERE season IN (2025, 2026) AND (play_type LIKE 'C%' OR play_type IN ('B', 'OF', 'RV'))
  GROUP BY 1 ORDER BY 1"), row.names = FALSE)
dbDisconnect(con)
