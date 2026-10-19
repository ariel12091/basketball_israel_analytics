readRenviron("app/.Renviron")
suppressMessages({library(DBI); library(RPostgres)})
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")

out <- file.path(Sys.getenv("SCRATCH"), "layup_onoff_out.txt")
sink(out)
options(width = 260)

# Mirrors onoff_mv.sql: lineups_lookup (player x lineup_hash x is_on_verdict)
# joined to action MV by lineup_hash. Interior = shot-type proxy
# (lay-up/dunk/allyhoop). 2026 only.
res <- dbGetQuery(con, "
  WITH sched AS (
    SELECT DISTINCT game_id FROM basketball_test.schedule WHERE game_year = 2026
  ),
  base0 AS (
    SELECT DISTINCT player_id, team_id, lineup_hash,
           COALESCE(is_on_verdict, 0)::int AS is_on
    FROM basketball_test.lineups_lookup
  ),
  acts AS (
    SELECT d.lineup_hash, d.type_lineup,
           CASE WHEN d.type = 'shot' THEN 1 ELSE 0 END AS fga_flag,
           CASE WHEN d.type = 'shot' AND d.parameters_type IN ('lay-up','dunk','allyhoop')
                THEN 1 ELSE 0 END AS int_att_flag,
           CASE WHEN d.type = 'shot' AND d.parameters_type IN ('lay-up','dunk','allyhoop')
                     AND d.parameters_made = 'made'
                THEN 1 ELSE 0 END AS int_made_flag,
           CASE WHEN d.final_end_poss IS TRUE THEN 1 ELSE 0 END AS poss_flag
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN sched USING (game_id)
    WHERE d.type = 'shot' OR d.final_end_poss IS TRUE
  ),
  agg AS (
    SELECT b.player_id, b.team_id, b.is_on, a.type_lineup,
           SUM(a.fga_flag) AS fga,
           SUM(a.int_att_flag) AS int_att,
           SUM(a.int_made_flag) AS int_made,
           SUM(a.poss_flag) AS poss
    FROM base0 b
    JOIN acts a USING (lineup_hash)
    GROUP BY 1, 2, 3, 4
  ),
  pvt AS (
    SELECT player_id, team_id,
      SUM(poss)     FILTER (WHERE is_on = 1 AND type_lineup = 'offense') AS on_poss,
      SUM(poss)     FILTER (WHERE is_on = 0 AND type_lineup = 'offense') AS off_poss,
      SUM(fga)      FILTER (WHERE is_on = 1 AND type_lineup = 'offense') AS on_fga_o,
      SUM(int_att)  FILTER (WHERE is_on = 1 AND type_lineup = 'offense') AS on_int_att_o,
      SUM(int_made) FILTER (WHERE is_on = 1 AND type_lineup = 'offense') AS on_int_made_o,
      SUM(fga)      FILTER (WHERE is_on = 0 AND type_lineup = 'offense') AS off_fga_o,
      SUM(int_att)  FILTER (WHERE is_on = 0 AND type_lineup = 'offense') AS off_int_att_o,
      SUM(int_made) FILTER (WHERE is_on = 0 AND type_lineup = 'offense') AS off_int_made_o,
      SUM(fga)      FILTER (WHERE is_on = 1 AND type_lineup = 'defense') AS on_fga_d,
      SUM(int_att)  FILTER (WHERE is_on = 1 AND type_lineup = 'defense') AS on_int_att_d,
      SUM(int_made) FILTER (WHERE is_on = 1 AND type_lineup = 'defense') AS on_int_made_d,
      SUM(fga)      FILTER (WHERE is_on = 0 AND type_lineup = 'defense') AS off_fga_d,
      SUM(int_att)  FILTER (WHERE is_on = 0 AND type_lineup = 'defense') AS off_int_att_d,
      SUM(int_made) FILTER (WHERE is_on = 0 AND type_lineup = 'defense') AS off_int_made_d
    FROM agg
    GROUP BY 1, 2
  ),
  named AS (
    SELECT r.player_id, r.team_id,
           MIN(TRIM(r.firstname) || ' ' || TRIM(r.lastname)) AS player_name,
           MIN(r.team_name) AS team_name
    FROM basketball_test.full_rosters r
    WHERE r.game_year = 2026
    GROUP BY 1, 2
  )
  SELECT n.player_name, n.team_name,
         p.on_poss,
         ROUND(100.0 * p.on_int_att_o / NULLIF(p.on_fga_o, 0), 1)   AS int_share_on,
         ROUND(100.0 * p.off_int_att_o / NULLIF(p.off_fga_o, 0), 1) AS int_share_off,
         ROUND(100.0 * p.on_int_att_o / NULLIF(p.on_fga_o, 0)
             - 100.0 * p.off_int_att_o / NULLIF(p.off_fga_o, 0), 1) AS d_share,
         ROUND(100.0 * p.on_int_made_o / NULLIF(p.on_int_att_o, 0), 1)   AS int_fg_on,
         ROUND(100.0 * p.off_int_made_o / NULLIF(p.off_int_att_o, 0), 1) AS int_fg_off,
         ROUND(100.0 * p.on_int_made_o / NULLIF(p.on_int_att_o, 0)
             - 100.0 * p.off_int_made_o / NULLIF(p.off_int_att_o, 0), 1) AS d_fg,
         ROUND(100.0 * p.on_int_att_d / NULLIF(p.on_fga_d, 0)
             - 100.0 * p.off_int_att_d / NULLIF(p.off_fga_d, 0), 1) AS d_share_def,
         ROUND(100.0 * p.on_int_made_d / NULLIF(p.on_int_att_d, 0)
             - 100.0 * p.off_int_made_d / NULLIF(p.off_int_att_d, 0), 1) AS d_fg_def
  FROM pvt p
  JOIN named n USING (player_id, team_id)
  WHERE p.on_poss >= 500 AND p.off_poss >= 300
  ORDER BY d_share DESC")

cat("== 2026 PLAYER ON/OFF — TEAM INTERIOR-PROXY SHOT PROFILE ==\n")
cat("(int = lay-up/dunk/alley-oop tagged; share = % of team FGA; d_ = ON minus OFF)\n")
cat("(offense cols unless _def; min 500 ON / 300 OFF poss)\n\n")
cat("--- TOP 12: team takes MORE interior shots with player ON ---\n")
print(head(res, 12), row.names=FALSE)
cat("\n--- BOTTOM 12: team takes FEWER interior shots with player ON ---\n")
print(tail(res, 12), row.names=FALSE)

r2 <- res[order(-res$d_fg), ]
cat("\n--- TOP 10 by interior FG% lift (offense) ---\n")
print(head(r2, 10), row.names=FALSE)
cat("\n--- BOTTOM 10 by interior FG% lift ---\n")
print(tail(r2, 10), row.names=FALSE)

r3 <- res[order(res$d_fg_def), ]
cat("\n--- TOP 10 rim-protection proxy: opponent interior FG% drops most with player ON ---\n")
print(head(r3, 10), row.names=FALSE)

cat("\nrows (players passing threshold):", nrow(res), "\n")

dbDisconnect(con)
sink()
cat("done\n")

write.csv(res, file.path('output', 'layup_onoff_2026.csv'), row.names = FALSE)

