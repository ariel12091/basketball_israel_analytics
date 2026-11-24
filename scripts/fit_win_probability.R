# Fit the win-probability model: docs/shared_definitions.md, "Win probability".
#
# Reads every possession of both leagues from basketball_test.win_prob_states()
# (the single definition of the model's inputs), fits the six constants by
# maximum likelihood, and checks the model out of sample: each season is
# predicted by a fit on all the others, and a side that fell below x% must win
# about x% of the time.
#
# Usage (from the repo root):
#   Rscript scripts/fit_win_probability.R           read-only, prints the fit
#   Rscript scripts/fit_win_probability.R --write   also stores it as the active
#                                                   row of win_prob_model (ETL role)

suppressMessages({library(DBI); library(RPostgres); library(data.table)})
write_fit <- "--write" %in% commandArgs(trailingOnly = TRUE)
readRenviron(if (write_fit) "etl/.Renviron" else "app/.Renviron")
con <- dbConnect(RPostgres::Postgres(), host = Sys.getenv("PG_HOST"),
  port = if (write_fit) 5432L else as.integer(Sys.getenv("PG_PORT")),
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"), connect_timeout = 15L, bigint = "numeric")
on.exit(dbDisconnect(con))

league_label <- c(il = "IL", euro = "")
ps <- rbindlist(lapply(names(league_label), function(lg) {
  s <- as.data.table(dbGetQuery(con, "SELECT * FROM basketball_test.win_prob_states($1, NULL)", params = list(lg)))
  s[is_final == FALSE, .(season = trimws(paste(league_label[[lg]], season)), game_id, quarter = period,
                         t = as.numeric(seconds_left), m = as.numeric(home_score - away_score),
                         s = ifelse(home_has_ball, 1, -1), neutral, home_won,
                         cnH = as.numeric(home_prior_net), cpH = as.numeric(home_prior_poss),
                         cnA = as.numeric(away_prior_net), cpA = as.numeric(away_prior_poss))]
}))

wp <- function(p, d) {
  dr <- 100 * (d$cnH / (d$cpH + p[["K"]]) - d$cnA / (d$cpA + p[["K"]]))
  mu <- ifelse(d$neutral, 0, p[["h"]]) + p[["k"]] * dr
  z <- (d$m + p[["b"]] * d$s + mu * d$t / 2400) / (p[["sigma"]] * sqrt((d$t + p[["c"]]) / 2400))
  pmin(pmax(pnorm(z), 1e-6), 1 - 1e-6)
}
fit <- function(d) {
  start <- c(sigma = 16, c = 30, h = 2, k = 1, K = 700, b = 0.5)
  f <- function(par) { q <- wp(par, d); -sum(ifelse(d$home_won, log(q), log(1 - q))) }
  o <- optim(start, f, method = "L-BFGS-B", lower = c(5, 1, -10, 0, 100, -3), upper = c(40, 600, 10, 5, 5000, 3),
             control = list(parscale = c(10, 30, 2, 1, 500, 0.5)))
  if (o$convergence != 0) stop("fit did not converge: ", o$message)
  o$par
}
ll <- function(q, y) -mean(y * log(q) + (1 - y) * log(1 - q))

out <- character()
say <- function(...) { line <- paste0(...); cat(line, "\n"); out <<- c(out, line) }
games <- uniqueN(ps[, .(season, game_id)])   # game ids are per league: count them by season
say("Possession states: ", nrow(ps), " in ", games, " games")
by_season <- ps[, .(games = uniqueN(game_id), states = .N), by = season][order(season)]
print(by_season)

# Out of sample: each season predicted by a fit on all the other seasons.
oos <- rbindlist(lapply(sort(unique(ps$season)), function(sn) {
  te <- ps[season == sn]; p <- fit(ps[season != sn])
  data.table(season = sn, game_id = te$game_id, y = as.numeric(te$home_won), q = wp(p, te))
}))
say("Out of sample log loss: ", paste(oos[, sprintf("%s %.3f", season[1], ll(q, y)), by = season]$V1, collapse = ", "))
oos[, qw := ifelse(y == 1, q, 1 - q)]
per <- oos[, .(low = min(qw), d2 = any(q < .02 | q > .98), d5 = any(q < .05 | q > .95), d10 = any(q < .1 | q > .9)),
           by = .(season, game_id)]
for (x in c(2, 5, 10)) {
  n <- per[get(paste0("d", x)) == TRUE, .N]; won <- per[low < x / 100, .N]
  say(sprintf("Comebacks out of sample: below %d%%, %d of %d games (%.1f%%)", x, won, n, 100 * won / n))
}

p <- fit(ps)
cat("\nFit on every season:\n"); print(signif(p, 6))

if (write_fit) {
  dbBegin(con)
  dbExecute(con, "UPDATE basketball_test.win_prob_model SET active = FALSE WHERE active")
  id <- dbGetQuery(con, "
    INSERT INTO basketball_test.win_prob_model (fitted_on, seasons, games, possession_states, sigma, c_seconds,
      home_advantage, rating_weight, rating_shrink_poss, ball_value, checks, active)
    VALUES (current_date, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, TRUE) RETURNING fit_id",
    params = list(paste(by_season$season, collapse = ", "), games, nrow(ps), p[["sigma"]], p[["c"]], p[["h"]],
                  p[["k"]], p[["K"]], p[["b"]], paste(out, collapse = "\n")))$fit_id
  dbCommit(con)
  cat("\nStored as the active fit, fit_id", id, "\n")
}
