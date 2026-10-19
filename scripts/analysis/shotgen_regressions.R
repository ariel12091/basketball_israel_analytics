# v2 regressions, offense-focused:
# (1) TEAM level: off_ppp / off_efg / off_ts ~ shot mix (28 team-seasons)
# (2) PLAYER level: d_ppp / d_efg / d_ts ~ generation (+finishing), tiers by
#     2026 OFFENSIVE rating rank (top4/mid4/bottom6). Reuses session CSVs.
readRenviron("app/.Renviron")
suppressMessages({library(DBI); library(RPostgres)})
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")

out <- file.path(Sys.getenv("SCRATCH"), "gen_regressions_v2_out.txt")
sink(out)
options(width = 220)

show <- function(m) {
  s <- summary(m)
  co <- round(coef(s), 3)
  colnames(co) <- c("est", "se", "t", "p")
  print(co)
  cat(sprintf("R2 = %.3f  adjR2 = %.3f  n = %d\n\n",
              s$r.squared, s$adj.r.squared, length(s$residuals) + m$rank))
}

# ---------- (1) TEAM LEVEL ----------
td <- read.csv(file.path("output", "shotgen_team_2025_2026.csv"))
cat("############ TEAM LEVEL (n = 28 team-seasons, 2025+2026) ############\n")
cat("Units: outcomes off_ppp (pts/100), off_efg, off_ts (both %, x100).\n")
cat("Predictors: shares in % of team FGA. factor(yr) = season intercept.\n\n")
for (o in c("off_ppp", "off_efg", "off_ts")) {
  cat("==", o, "~ rim_share + fg3_share + factor(yr) ==\n")
  show(lm(as.formula(paste(o, "~ rim_share + fg3_share + factor(yr)")), td))
  cat("==", o, "~ layup_share + dunk_share + fg3_share + factor(yr) ==\n")
  show(lm(as.formula(paste(o, "~ layup_share + dunk_share + fg3_share + factor(yr)")), td))
  cat("==", o, "~ xpps_mix (expected PPS of mix) + factor(yr) ==\n")
  show(lm(as.formula(paste(o, "~ xpps_mix + factor(yr)")), td))
}

# ---------- (2) PLAYER LEVEL ----------
r <- read.csv(file.path("output", "shotgen_player_onoff_2026.csv"))
rtg <- dbGetQuery(con, "
  SELECT team_id, team_name, off_ppp,
         dense_rank() OVER (ORDER BY off_ppp DESC) AS rk
  FROM basketball_test.team_ppp_ratings_mv
  WHERE game_year = 2026")
dbDisconnect(con)
rtg$tier <- cut(rtg$rk, c(0, 4, 8, 14), labels = c("top4", "mid4", "bottom6"))
cat("\n############ PLAYER LEVEL (n = 137, 2026; all offense-side) ############\n")
cat("== 2026 OFFENSIVE-RATING TIERS ==\n")
print(rtg[order(rtg$rk), ], row.names = FALSE)
r <- merge(r, rtg[, c("team_id", "tier")], by = "team_id")
r$tier <- factor(r$tier, levels = c("top4", "mid4", "bottom6"))
cat("players per tier:\n"); print(table(r$tier)); cat("\n")
cat("Units: all vars ON-OFF deltas. d_ppp pts/100; d_efg, d_ts pp;\n")
cat("shares pp of team FGA; d_rim_fg, d_fg3_fg pp of FG%.\n\n")

for (o in c("d_ppp", "d_efg", "d_ts")) {
  cat("========== OUTCOME:", o, "==========\n")
  cat("-- generation only --\n")
  show(lm(as.formula(paste(o, "~ d_rim_share + d_fg3_share")), r))
  cat("-- generation + finishing --\n")
  show(lm(as.formula(paste(o, "~ d_rim_share + d_fg3_share + d_rim_fg + d_fg3_fg")), r))
  cat("-- generation + offensive tier FE --\n")
  show(lm(as.formula(paste(o, "~ d_rim_share + d_fg3_share + tier")), r))
  cat("-- interaction: d_good_share x tier --\n")
  show(lm(as.formula(paste(o, "~ d_good_share * tier")), r))
}

cat("== per-tier r: d_good_share vs outcomes (offensive tiers) ==\n")
print(t(sapply(levels(r$tier), function(t) {
  dd <- r[r$tier == t, ]
  c(sapply(c("d_ppp","d_efg","d_ts"), function(o)
      round(cor(dd$d_good_share, dd[[o]]), 2)), n = nrow(dd))
})))

sink()
cat("done\n")
