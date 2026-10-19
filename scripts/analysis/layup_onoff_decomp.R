# Decompose each player's ON/OFF team shot-mix shift (2026):
# which categories (lay-up / dunk+oop / 2pt jumper / 3pt) gain or lose share,
# and how much of the ON interior volume is the player's own shots.
readRenviron("app/.Renviron")
suppressMessages({library(DBI); library(RPostgres)})
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")

out <- file.path(Sys.getenv("SCRATCH"), "layup_onoff_decomp_out.txt")
sink(out)
options(width = 260)

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
    SELECT d.lineup_hash, d.player_id AS shooter_id,
           CASE WHEN d.type = 'shot' THEN 1 ELSE 0 END AS fga,
           CASE WHEN d.type = 'shot' AND d.parameters_type = 'lay-up' THEN 1 ELSE 0 END AS layup,
           CASE WHEN d.type = 'shot' AND d.parameters_type IN ('dunk','allyhoop') THEN 1 ELSE 0 END AS dunk,
           CASE WHEN d.type = 'shot' AND d.parameters_type = 'jump-shot' AND d.parameters_points = 2 THEN 1 ELSE 0 END AS mid,
           CASE WHEN d.type = 'shot' AND d.parameters_type = 'jump-shot' AND d.parameters_points = 3 THEN 1 ELSE 0 END AS three,
           CASE WHEN d.final_end_poss IS TRUE THEN 1 ELSE 0 END AS poss
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN sched USING (game_id)
    WHERE d.type_lineup = 'offense' AND (d.type = 'shot' OR d.final_end_poss IS TRUE)
  ),
  agg AS (
    SELECT b.player_id, b.team_id, b.is_on,
           SUM(a.fga) AS fga, SUM(a.layup) AS layup, SUM(a.dunk) AS dunk,
           SUM(a.mid) AS mid, SUM(a.three) AS three, SUM(a.poss) AS poss,
           SUM(a.fga)   FILTER (WHERE a.shooter_id = b.player_id) AS own_fga,
           SUM(a.layup + a.dunk) FILTER (WHERE a.shooter_id = b.player_id) AS own_int
    FROM base0 b
    JOIN acts a USING (lineup_hash)
    GROUP BY 1, 2, 3
  ),
  pvt AS (
    SELECT player_id, team_id,
      SUM(poss)  FILTER (WHERE is_on = 1) AS on_poss,
      SUM(poss)  FILTER (WHERE is_on = 0) AS off_poss,
      SUM(fga)   FILTER (WHERE is_on = 1) AS on_fga,
      SUM(fga)   FILTER (WHERE is_on = 0) AS off_fga,
      SUM(layup) FILTER (WHERE is_on = 1) AS on_layup,
      SUM(layup) FILTER (WHERE is_on = 0) AS off_layup,
      SUM(dunk)  FILTER (WHERE is_on = 1) AS on_dunk,
      SUM(dunk)  FILTER (WHERE is_on = 0) AS off_dunk,
      SUM(mid)   FILTER (WHERE is_on = 1) AS on_mid,
      SUM(mid)   FILTER (WHERE is_on = 0) AS off_mid,
      SUM(three) FILTER (WHERE is_on = 1) AS on_three,
      SUM(three) FILTER (WHERE is_on = 0) AS off_three,
      COALESCE(SUM(own_fga) FILTER (WHERE is_on = 1), 0) AS own_fga,
      COALESCE(SUM(own_int) FILTER (WHERE is_on = 1), 0) AS own_int
    FROM agg GROUP BY 1, 2
  ),
  named AS (
    SELECT r.player_id, r.team_id,
           MIN(TRIM(r.firstname) || ' ' || TRIM(r.lastname)) AS player_name,
           MIN(r.team_name) AS team_name
    FROM basketball_test.full_rosters r
    WHERE r.game_year = 2026
    GROUP BY 1, 2
  )
  SELECT n.player_name, n.team_name, p.on_poss,
    ROUND(100.0*(p.on_layup+p.on_dunk)/p.on_fga - 100.0*(p.off_layup+p.off_dunk)/p.off_fga, 1) AS d_int,
    ROUND(100.0*p.on_layup/p.on_fga - 100.0*p.off_layup/p.off_fga, 1) AS d_layup,
    ROUND(100.0*p.on_dunk /p.on_fga - 100.0*p.off_dunk /p.off_fga, 1) AS d_dunk,
    ROUND(100.0*p.on_mid  /p.on_fga - 100.0*p.off_mid  /p.off_fga, 1) AS d_mid,
    ROUND(100.0*p.on_three/p.on_fga - 100.0*p.off_three/p.off_fga, 1) AS d_3pt,
    ROUND(100.0*p.own_int/NULLIF(p.on_layup+p.on_dunk, 0), 1) AS own_pct_of_int,
    ROUND(100.0*p.own_int/p.on_fga, 1) AS own_int_share,
    ROUND(100.0*(p.on_layup+p.on_dunk-p.own_int)/(p.on_fga-p.own_fga)
        - 100.0*(p.off_layup+p.off_dunk)/p.off_fga, 1) AS d_int_teammates
  FROM pvt p
  JOIN named n USING (player_id, team_id)
  WHERE p.on_poss >= 500 AND p.off_poss >= 300
  ORDER BY d_int DESC")

cat("== 2026 ON/OFF SHOT-MIX DECOMPOSITION (share of team FGA, pp = ON-OFF) ==\n")
cat("d_int = d_layup + d_dunk; own_pct_of_int = player's own share of team interior attempts while ON\n")
cat("d_int_teammates = teammates-only interior share while ON minus team interior share OFF\n\n")
cat("--- TOP 14 (added interior) ---\n")
print(head(res, 14), row.names=FALSE)
cat("\n--- BOTTOM 14 (removed interior) ---\n")
print(tail(res, 14), row.names=FALSE)

write.csv(res, file.path("output", "layup_onoff_decomp_2026.csv"), row.names = FALSE)
cat("\nrows:", nrow(res), " -> output/layup_onoff_decomp_2026.csv\n")

dbDisconnect(con)
sink()
cat("done\n")
