# Run from repository root; --live also validates all three read adapters.
source("app/R/mod_team_insights.R", encoding = "UTF-8")
fixture <- expand.grid(game_id = as.character(1:4), unit = c("a", "b"), side = c("offense", "defense"), stringsAsFactors = FALSE)
for (k in insight_counts) fixture[[k]] <- 0
fixture$poss <- ifelse(fixture$game_id == "1", 100, 10)
fixture$pts <- fixture$poss * ifelse(fixture$unit == "a" & fixture$side == "offense", 1.2, 1)
fixture$fga <- fixture$poss; fixture$fgm <- fixture$poss * .4
fixture$minutes <- ifelse(fixture$side == "offense", 5, 0)
a <- insight_summary(fixture[fixture$unit == "a", ])
stopifnot(abs(a$net - 20) < 1e-8, a$minutes == 20, a$games == 4)
stopifnot(nrow(insight_candidates(fixture, 50)) == 2,
          nrow(insight_candidates(fixture, 200)) == 0)
stable <- insight_sensitivity(fixture, fixture$unit == "a")
stopifnot(stable$status == "Direction survives each game exclusion", all(abs(stable$range - 20) < 1e-8))
# One outlier creates a positive aggregate but the other games favor b.
unstable <- fixture
unstable$pts[unstable$unit == "a" & unstable$side == "offense"] <-
  ifelse(unstable$game_id[unstable$unit == "a" & unstable$side == "offense"] == "1", 150, 5)
stopifnot(insight_sensitivity(unstable, unstable$unit == "a")$status == "Direction changes or reaches zero")
# Unequal game sizes must produce a ratio of sums, not an average of ratings.
x <- fixture[fixture$unit == "a", ]
x$pts[x$side == "offense"] <- ifelse(x$game_id[x$side == "offense"] == "1", 150, 5)
stopifnot(abs(insight_summary(x)$off - 100 * 165 / 130) < 1e-8,
          is.na(insight_summary(fixture[FALSE, ])$net))
cat("Calculation checks passed\n")

if ("--live" %in% commandArgs(TRUE)) {
  readRenviron("app/.Renviron")
  con <- DBI::dbConnect(RPostgres::Postgres(), host = Sys.getenv("PG_HOST"),
    port = as.integer(Sys.getenv("PG_PORT", "6543")), dbname = Sys.getenv("PG_DB"),
    user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"), sslmode = "require")
  for (comp in c("il", "E", "U")) {
    relation <- if (comp == "il") "basketball_test.final_schedule_mv" else "euroleague.final_schedule_mv"
    clause <- if (comp == "il") "game_year = 2026" else paste0("game_year = 2025 AND competition = ", DBI::dbQuoteString(con, comp))
    teams <- DBI::dbGetQuery(con, paste("SELECT team_id, COUNT(*) AS n FROM", relation, "WHERE", clause, "GROUP BY team_id ORDER BY n DESC LIMIT 1"))
    params <- list(if (comp == "il") 2026L else 2025L, as.character(teams$team_id[1]))
    if (comp != "il") params <- c(params, list(comp))
    d <- DBI::dbGetQuery(con, insight_query(comp), params = params)
    stopifnot(nrow(d) > 0, all(is.finite(d$poss)), !anyDuplicated(d[c("unit", "game_id", "side")]))
    cat(comp, ":", nrow(d), "rows;", length(unique(d$game_id)), "games;", nrow(insight_candidates(d)), "eligible units\n")
  }
  DBI::dbDisconnect(con)
}
