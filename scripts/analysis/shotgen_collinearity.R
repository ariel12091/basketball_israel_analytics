# Multicollinearity diagnostics for the shot-mix regressions + corner-3 split.
# Team level: shares are compositional (layup+dunk+3pt+mid = 100% of FGA);
# mid_share is the omitted baseline, so each coef = "swap 1pp of mid for X".
# Corner 3: c3_share_fga is parquet-based (lags MV ~8 games); above-break =
# fg3_share - c3_share_fga (mixed denominators, approximation - flagged).
out <- file.path(Sys.getenv("SCRATCH"), "gen_collinearity_c3_out.txt")
sink(out)
options(width = 200)

vif <- function(df, vars) {
  sapply(vars, function(v) {
    r2 <- summary(lm(reformulate(setdiff(vars, v), v), data = df))$r.squared
    round(1 / (1 - r2), 2)
  })
}
show <- function(m, n) {
  s <- summary(m)
  co <- round(coef(s), 3); colnames(co) <- c("est","se","t","p")
  print(co)
  cat(sprintf("R2 = %.3f  adjR2 = %.3f  n = %d\n\n", s$r.squared, s$adj.r.squared, n))
}

td <- read.csv(file.path("output", "shotgen_team_2025_2026.csv"))
td$c3_share  <- td$c3_share_fga
td$ab3_share <- td$fg3_share - td$c3_share_fga

cat("############ TEAM LEVEL (n = 28) ############\n")
cat("== predictor correlation matrix (compositional shares) ==\n")
vars4 <- c("layup_share","dunk_share","c3_share","ab3_share","mid_share")
print(round(cor(td[, vars4]), 2))

cat("\n== VIFs ==\n")
cat("model A (rim_share + fg3_share):\n")
print(vif(td, c("rim_share","fg3_share")))
cat("model B (layup + dunk + fg3):\n")
print(vif(td, c("layup_share","dunk_share","fg3_share")))
cat("model C (layup + dunk + c3 + ab3):\n")
print(vif(td, c("layup_share","dunk_share","c3_share","ab3_share")))

for (o in c("off_ppp","off_efg","off_ts")) {
  cat("==", o, "~ layup + dunk + corner3 + abovebreak3 + factor(yr)  [baseline = mid] ==\n")
  show(lm(reformulate(c("layup_share","dunk_share","c3_share","ab3_share","factor(yr)"), o), td), 28)
}

cat("== sensitivity: c3 share of 3s instead of share of FGA (off_ppp) ==\n")
show(lm(off_ppp ~ fg3_share + c3_share_3s + factor(yr), td), 28)

r <- read.csv(file.path("output", "shotgen_player_onoff_2026.csv"))
cat("\n############ PLAYER LEVEL (n = 137) ############\n")
cat("== predictor correlations (ON-OFF deltas) ==\n")
pv <- c("d_rim_share","d_fg3_share","d_rim_fg","d_fg3_fg")
print(round(cor(r[, pv], use = "complete.obs"), 2))
cat("\n== VIFs, generation+finishing model ==\n")
print(vif(r, pv))
sink()
cat("done\n")
