# Fit the four-factor -> ORtg regression that produces FF_IMPACT_WEIGHTS
# (app/R/helpers.R). Rerun after each season; update the constants manually.
#
# Usage:  "Rscript.exe" scripts/fit_ff_impact_weights.R
# Reads app/.Renviron (readonly credentials). Prints pooled coefficients,
# per-season stability, and a team/opponent fixed-effects robustness check.

read_renviron <- function(path) {
  lines <- readLines(path)
  lines <- lines[grepl("=", lines, fixed = TRUE) & !grepl("^\\s*#", lines)]
  kv <- do.call(rbind, strsplit(lines, "=", fixed = TRUE))
  setNames(trimws(kv[, 2]), trimws(kv[, 1]))
}

env <- read_renviron(file.path("app", ".Renviron"))

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = env[["PG_HOST"]], port = as.integer(env[["PG_PORT"]]),
  dbname = env[["PG_DB"]], user = env[["PG_USER"]], password = env[["PG_PASS"]],
  sslmode = env[["PG_SSLMODE"]], connect_timeout = 15L, bigint = "numeric"
)

df <- DBI::dbGetQuery(con, "
  SELECT game_year, game_id, team_id, opp_team_id,
         off_ppp, off_efg, off_tov, off_oreb, off_ftr, off_poss
  FROM basketball_test.team_metrics_by_game_mv
  WHERE off_ppp IS NOT NULL AND off_efg IS NOT NULL AND off_tov IS NOT NULL
    AND off_oreb IS NOT NULL AND off_ftr IS NOT NULL
")
DBI::dbDisconnect(con)

cat("rows:", nrow(df), " seasons:", paste(sort(unique(df$game_year)), collapse = ", "), "\n\n")

m0 <- lm(off_ppp ~ off_efg + off_tov + off_oreb + off_ftr,
         data = df, weights = df$off_poss)
cat("=== Pooled OLS (weights source) ===\n")
print(round(coef(summary(m0)), 4))
cat("R-squared:", round(summary(m0)$r.squared, 4), "\n\n")

cat("=== Per-season stability ===\n")
for (yr in sort(unique(df$game_year))) {
  d <- df[df$game_year == yr, ]
  m <- lm(off_ppp ~ off_efg + off_tov + off_oreb + off_ftr, data = d, weights = d$off_poss)
  cat(yr, " n=", nrow(d), "  ",
      paste(sprintf("%s=%+.3f", names(coef(m))[-1], coef(m)[-1]), collapse = "  "), "\n", sep = "")
}

# Robustness: team x season + opponent fixed effects (coefficients should
# barely move; if they drift materially, investigate before updating weights).
df$team_season <- interaction(df$team_id, df$game_year)
df$opp_season <- interaction(df$opp_team_id, df$game_year)
m2 <- lm(off_ppp ~ off_efg + off_tov + off_oreb + off_ftr + team_season + opp_season,
         data = df, weights = df$off_poss)
cat("\n=== Fixed-effects check ===\n")
print(round(coef(m2)[c("off_efg", "off_tov", "off_oreb", "off_ftr")], 4))

cat("\nUpdate FF_IMPACT_WEIGHTS in app/R/helpers.R with the pooled coefficients (2dp).\n")
