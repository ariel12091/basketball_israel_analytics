readRenviron("app/.Renviron")
suppressMessages({library(DBI); library(RPostgres)})
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")

out <- file.path(Sys.getenv("SCRATCH"), "layup_net_rim_out.txt")
sink(out)
options(width = 240)

# Grain: one row per (team, season). Shots: one row per shot per perspective
# (type_lineup = offense -> shooting team, defense -> defending team).
# Names keyed (team_id, game_year) per committed MV convention.
res <- dbGetQuery(con, "
  WITH shots AS (
    SELECT d.game_id, d.id, d.team_id, d.type_lineup, d.parameters_type,
           d.parameters_points, d.parameters_made, s.game_year,
           (d.parameters_type IN ('lay-up','dunk','allyhoop')) AS is_rim
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN basketball_test.schedule s ON s.game_id = d.game_id
    WHERE d.type = 'shot'
  ),
  ast AS (
    SELECT DISTINCT game_id, parent_action_id
    FROM basketball_test.df_pts_poss_lineups_longer_mv
    WHERE type = 'assist'
  ),
  sides AS (
    SELECT sh.game_year, sh.team_id, sh.type_lineup,
           COUNT(*) AS fga,
           COUNT(*) FILTER (WHERE sh.is_rim) AS rim_att,
           COUNT(*) FILTER (WHERE sh.is_rim AND sh.parameters_made = 'made') AS rim_made,
           SUM(CASE WHEN sh.is_rim AND sh.parameters_made = 'made'
                    THEN sh.parameters_points ELSE 0 END) AS rim_pts,
           COUNT(*) FILTER (WHERE sh.is_rim AND sh.parameters_made = 'made'
             AND EXISTS (SELECT 1 FROM ast a
                         WHERE a.game_id = sh.game_id AND a.parent_action_id = sh.id)) AS rim_ast_made
    FROM shots sh
    GROUP BY 1, 2, 3
  ),
  tn AS (
    SELECT team_id, game_year, MIN(team_name) AS team_name
    FROM basketball_test.full_rosters GROUP BY team_id, game_year
  )
  SELECT o.game_year AS yr, tn.team_name,
         ROUND(100.0 * o.rim_att / o.fga, 1) AS rim_share_off,
         ROUND(100.0 * o.rim_made / o.rim_att, 1) AS rim_fg_off,
         ROUND(o.rim_pts::numeric / o.rim_att, 3) AS rim_pps_off,
         ROUND(100.0 * o.rim_ast_made / NULLIF(o.rim_made, 0), 1) AS rim_ast_off,
         ROUND(100.0 * d.rim_att / d.fga, 1) AS rim_share_def,
         ROUND(100.0 * d.rim_made / d.rim_att, 1) AS rim_fg_def,
         ROUND(d.rim_pts::numeric / d.rim_att, 3) AS rim_pps_def,
         ROUND(100.0 * d.rim_ast_made / NULLIF(d.rim_made, 0), 1) AS rim_ast_def,
         ROUND(o.rim_pts::numeric / o.rim_att - d.rim_pts::numeric / d.rim_att, 3) AS net_rim_pps,
         (o.rim_att - d.rim_att) AS rim_att_diff
  FROM sides o
  JOIN sides d ON d.game_year = o.game_year AND d.team_id = o.team_id
              AND d.type_lineup = 'defense'
  JOIN tn ON tn.team_id = o.team_id AND tn.game_year = o.game_year
  WHERE o.type_lineup = 'offense'
  ORDER BY yr, net_rim_pps DESC")
cat("== NET RIM PROFILE PER TEAM-SEASON ==\n")
print(res, row.names=FALSE)

cat("\n== VERIFY: teams per season (expect 14/14) ==\n")
print(table(res$yr))

dbDisconnect(con)
sink()
cat("done\n")

write.csv(res, file.path('output', 'team_interior_profiles.csv'), row.names = FALSE)

