# Player-level (2026): does Delta(rim/3pt generation share, ON-OFF) correlate
# with Delta offensive PPP? Extends layup_onoff.R (adds fg3 flags + team_score
# so PPP uses the exact onoff_mv.sql recipe). Same thresholds: >=500 ON / >=300 OFF.
readRenviron("app/.Renviron")
suppressMessages({library(DBI); library(RPostgres)})
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")

out <- file.path(Sys.getenv("SCRATCH"), "gen_player_onoff_corr_out.txt")
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
    SELECT d.lineup_hash, d.type_lineup,
           COALESCE(d.team_score, 0) AS pts,
           CASE WHEN d.type = 'shot' THEN 1 ELSE 0 END AS fga_flag,
           CASE WHEN d.type = 'shot' AND d.parameters_type IN ('lay-up','dunk','allyhoop')
                THEN 1 ELSE 0 END AS rim_att_flag,
           CASE WHEN d.type = 'shot' AND d.parameters_points = 3
                THEN 1 ELSE 0 END AS fg3_att_flag,
           CASE WHEN d.type = 'shot' AND d.parameters_type IN ('lay-up','dunk','allyhoop')
                     AND d.parameters_made = 'made'
                THEN 1 ELSE 0 END AS rim_made_flag,
           CASE WHEN d.type = 'shot' AND d.parameters_points = 3
                     AND d.parameters_made = 'made'
                THEN 1 ELSE 0 END AS fg3_made_flag,
           CASE WHEN d.final_end_poss IS TRUE THEN 1 ELSE 0 END AS poss_flag
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN sched USING (game_id)
    WHERE d.type = 'shot' OR d.final_end_poss IS TRUE OR COALESCE(d.team_score, 0) > 0
  ),
  agg AS (
    SELECT b.player_id, b.team_id, b.is_on, a.type_lineup,
           SUM(a.pts) AS pts,
           SUM(a.poss_flag) AS poss,
           SUM(a.fga_flag) AS fga,
           SUM(a.rim_att_flag) AS rim_att,
           SUM(a.fg3_att_flag) AS fg3_att,
           SUM(a.rim_made_flag) AS rim_made,
           SUM(a.fg3_made_flag) AS fg3_made
    FROM base0 b
    JOIN acts a USING (lineup_hash)
    GROUP BY 1, 2, 3, 4
  ),
  pvt AS (
    SELECT player_id, team_id,
      SUM(poss)     FILTER (WHERE is_on = 1 AND type_lineup = 'offense') AS on_poss,
      SUM(poss)     FILTER (WHERE is_on = 0 AND type_lineup = 'offense') AS off_poss,
      SUM(pts)      FILTER (WHERE is_on = 1 AND type_lineup = 'offense') AS on_pts,
      SUM(pts)      FILTER (WHERE is_on = 0 AND type_lineup = 'offense') AS off_pts,
      SUM(fga)      FILTER (WHERE is_on = 1 AND type_lineup = 'offense') AS on_fga,
      SUM(fga)      FILTER (WHERE is_on = 0 AND type_lineup = 'offense') AS off_fga,
      SUM(rim_att)  FILTER (WHERE is_on = 1 AND type_lineup = 'offense') AS on_rim_att,
      SUM(rim_att)  FILTER (WHERE is_on = 0 AND type_lineup = 'offense') AS off_rim_att,
      SUM(fg3_att)  FILTER (WHERE is_on = 1 AND type_lineup = 'offense') AS on_fg3_att,
      SUM(fg3_att)  FILTER (WHERE is_on = 0 AND type_lineup = 'offense') AS off_fg3_att,
      SUM(rim_made) FILTER (WHERE is_on = 1 AND type_lineup = 'offense') AS on_rim_made,
      SUM(rim_made) FILTER (WHERE is_on = 0 AND type_lineup = 'offense') AS off_rim_made,
      SUM(fg3_made) FILTER (WHERE is_on = 1 AND type_lineup = 'offense') AS on_fg3_made,
      SUM(fg3_made) FILTER (WHERE is_on = 0 AND type_lineup = 'offense') AS off_fg3_made
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
  SELECT n.player_name, n.team_name, p.*
  FROM pvt p
  JOIN named n USING (player_id, team_id)
  WHERE p.on_poss >= 500 AND p.off_poss >= 300")

adv <- dbGetQuery(con, "
  SELECT player_id, team_id, off_on_efg, off_off_efg, off_on_ts, off_off_ts
  FROM basketball_test.player_advanced_stats_mv
  WHERE game_year = 2026")
dbDisconnect(con)

res <- merge(res, adv, by = c("player_id", "team_id"), all.x = TRUE)
cat("adv join: matched", sum(!is.na(res$off_on_efg)), "of", nrow(res), "\n")

r <- within(res, {
  ppp_on  <- 100 * on_pts / on_poss
  ppp_off <- 100 * off_pts / off_poss
  d_ppp   <- ppp_on - ppp_off
  rim_share_on  <- 100 * on_rim_att / on_fga
  rim_share_off <- 100 * off_rim_att / off_fga
  d_rim_share   <- rim_share_on - rim_share_off
  fg3_share_on  <- 100 * on_fg3_att / on_fga
  fg3_share_off <- 100 * off_fg3_att / off_fga
  d_fg3_share   <- fg3_share_on - fg3_share_off
  d_good_share  <- d_rim_share + d_fg3_share
  d_rim_fg <- 100 * (on_rim_made / on_rim_att - off_rim_made / off_rim_att)
  d_fg3_fg <- 100 * (on_fg3_made / on_fg3_att - off_fg3_made / off_fg3_att)
  d_efg <- 100 * (off_on_efg - off_off_efg)
  d_ts  <- 100 * (off_on_ts - off_off_ts)
})

cat("== INVARIANT CHECKS ==\n")
cat("players passing threshold:", nrow(r), "(expect ~137)\n")
cat("league mean ppp_on (expect ~108-115):", round(mean(r$ppp_on), 1), "\n")
cat("mean d_rim_share (expect ~0):", round(mean(r$d_rim_share), 2), "\n\n")

cat("== PEARSON r: generation delta (rows) vs outcome delta (cols) ==\n")
preds <- c("d_rim_share","d_fg3_share","d_good_share","d_rim_fg","d_fg3_fg")
outcomes <- c("d_ppp","d_efg","d_ts")
print(t(sapply(preds, function(p) {
  unlist(lapply(outcomes, function(o) {
    ok <- is.finite(r[[p]]) & is.finite(r[[o]])
    setNames(round(cor(r[[p]][ok], r[[o]][ok]), 2), o)
  }))
})))

cat("\n== OLS: d_ppp ~ generation (shares) + finishing (fg) ==\n")
print(summary(lm(d_ppp ~ d_rim_share + d_fg3_share + d_rim_fg + d_fg3_fg, data = r)))
cat("\n== OLS: generation only ==\n")
print(summary(lm(d_ppp ~ d_rim_share + d_fg3_share, data = r)))
cat("\n== OLS: d_efg ~ generation only ==\n")
print(summary(lm(d_efg ~ d_rim_share + d_fg3_share, data = r)))

cat("\n== QUARTILES: players binned by d_good_share -> mean d_ppp ==\n")
q <- cut(r$d_good_share, quantile(r$d_good_share, 0:4/4), include.lowest = TRUE)
print(aggregate(cbind(d_ppp, d_good_share, d_rim_share, d_fg3_share) ~ q,
                data = r, FUN = function(x) round(mean(x), 2)))

ord <- r[order(-r$d_good_share),
         c("player_name","team_name","on_poss","d_rim_share","d_fg3_share",
           "d_good_share","d_ppp")]
num <- sapply(ord, is.numeric); ord[num] <- lapply(ord[num], round, 1)
cat("\n== TOP 15 generators (d_good_share) ==\n")
print(head(ord, 15), row.names = FALSE)
cat("\n== BOTTOM 15 ==\n")
print(tail(ord, 15), row.names = FALSE)

write.csv(r, file.path("output", "shotgen_player_onoff_2026.csv"), row.names = FALSE)
sink()
cat("done\n")
