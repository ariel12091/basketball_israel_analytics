readRenviron("app/.Renviron")
suppressMessages({library(DBI); library(RPostgres)})
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")

out <- file.path(Sys.getenv("SCRATCH"), "layup_team_fixed_out.txt")
sink(out)
options(width = 220)

# Season-scoped team names: (team_id, game_year) is the correct key
qb <- dbGetQuery(con, "
  WITH shots AS (
    SELECT d.game_id, d.id, d.team_id, d.parameters_type,
           d.parameters_points, d.parameters_made, s.game_year
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN basketball_test.schedule s ON s.game_id = d.game_id
    WHERE d.type = 'shot' AND d.type_lineup = 'offense'
  ),
  acts AS (
    SELECT DISTINCT game_id, id, parent_action_id, type
    FROM basketball_test.df_pts_poss_lineups_longer_mv
    WHERE type = 'assist'
  ),
  tn AS (
    SELECT team_id, game_year, MIN(team_name) AS team_name
    FROM basketball_test.full_rosters GROUP BY team_id, game_year
  )
  SELECT sh.game_year AS yr, tn.team_name,
         COUNT(*) AS fga,
         COUNT(*) FILTER (WHERE sh.parameters_type IN ('lay-up','dunk','allyhoop')) AS rim_att,
         ROUND(100.0 * COUNT(*) FILTER (WHERE sh.parameters_type IN ('lay-up','dunk','allyhoop'))
               / COUNT(*), 1) AS rim_share,
         ROUND(100.0 * COUNT(*) FILTER (WHERE sh.parameters_type = 'lay-up' AND sh.parameters_made = 'made')
               / NULLIF(COUNT(*) FILTER (WHERE sh.parameters_type = 'lay-up'), 0), 1) AS layup_fg,
         ROUND(100.0 * COUNT(*) FILTER (WHERE sh.parameters_type IN ('dunk','allyhoop') AND sh.parameters_made = 'made')
               / NULLIF(COUNT(*) FILTER (WHERE sh.parameters_type IN ('dunk','allyhoop')), 0), 1) AS dunk_fg,
         ROUND(100.0 * COUNT(*) FILTER (WHERE sh.parameters_type IN ('lay-up','dunk','allyhoop') AND sh.parameters_made = 'made')
               / NULLIF(COUNT(*) FILTER (WHERE sh.parameters_type IN ('lay-up','dunk','allyhoop')), 0), 1) AS rim_fg,
         ROUND(SUM(CASE WHEN sh.parameters_type IN ('lay-up','dunk','allyhoop') AND sh.parameters_made = 'made'
                        THEN sh.parameters_points ELSE 0 END)
               / NULLIF(COUNT(*) FILTER (WHERE sh.parameters_type IN ('lay-up','dunk','allyhoop')), 0), 3) AS rim_pps,
         ROUND(100.0 * COUNT(*) FILTER (WHERE sh.parameters_type IN ('lay-up','dunk','allyhoop')
                 AND sh.parameters_made = 'made'
                 AND EXISTS (SELECT 1 FROM acts a
                             WHERE a.game_id = sh.game_id AND a.parent_action_id = sh.id))
               / NULLIF(COUNT(*) FILTER (WHERE sh.parameters_type IN ('lay-up','dunk','allyhoop')
                 AND sh.parameters_made = 'made'), 0), 1) AS rim_ast_pct
  FROM shots sh
  JOIN tn ON tn.team_id = sh.team_id AND tn.game_year = sh.game_year
  GROUP BY 1, 2
  ORDER BY 1, rim_share DESC")
cat("== B-FIXED. TEAM OFFENSE RIM PROFILE (season-scoped names) ==\n")
print(qb, row.names=FALSE)

qc <- dbGetQuery(con, "
  WITH shots AS (
    SELECT d.game_id, d.id, d.team_id, d.parameters_type,
           d.parameters_made, s.game_year
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN basketball_test.schedule s ON s.game_id = d.game_id
    WHERE d.type = 'shot' AND d.type_lineup = 'defense'
  ),
  tn AS (
    SELECT team_id, game_year, MIN(team_name) AS team_name
    FROM basketball_test.full_rosters GROUP BY team_id, game_year
  )
  SELECT sh.game_year AS yr, tn.team_name,
         COUNT(*) AS opp_fga,
         COUNT(*) FILTER (WHERE sh.parameters_type IN ('lay-up','dunk','allyhoop')) AS rim_att,
         ROUND(100.0 * COUNT(*) FILTER (WHERE sh.parameters_type IN ('lay-up','dunk','allyhoop'))
               / COUNT(*), 1) AS rim_share,
         ROUND(100.0 * COUNT(*) FILTER (WHERE sh.parameters_type IN ('lay-up','dunk','allyhoop') AND sh.parameters_made = 'made')
               / NULLIF(COUNT(*) FILTER (WHERE sh.parameters_type IN ('lay-up','dunk','allyhoop')), 0), 1) AS rim_fg,
         ROUND(100.0 * COUNT(*) FILTER (WHERE sh.parameters_type = 'lay-up' AND sh.parameters_made = 'blocked')
               / NULLIF(COUNT(*) FILTER (WHERE sh.parameters_type = 'lay-up'), 0), 1) AS layup_blk_pct
  FROM shots sh
  JOIN tn ON tn.team_id = sh.team_id AND tn.game_year = sh.game_year
  GROUP BY 1, 2
  ORDER BY 1, rim_fg ASC")
cat("\n== C-FIXED. TEAM DEFENSE RIM PROFILE (season-scoped names) ==\n")
print(qc, row.names=FALSE)

# Verification: row count per season should be 14, exactly one Netanya per season
cat("\n== VERIFY: teams per season ==\n")
print(table(qb$yr))

dbDisconnect(con)
sink()
cat("done\n")
