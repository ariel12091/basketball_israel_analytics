# Fit the win-probability model: docs/shared_definitions.md, "Win probability".
#
# Read-only. Builds one state per possession for every Israeli and
# EuroLeague/EuroCup game, fits the six constants by maximum likelihood on all
# of them, and checks the model out of sample: each season is predicted by a fit
# on all the others, and a side that fell below x% must win about x% of the time.
# Writing a fit to the constants table comes with that table.
#
# Usage: Rscript scripts/fit_win_probability.R    (from the repo root)

suppressMessages({library(DBI); library(RPostgres); library(data.table)})
readRenviron("app/.Renviron")
con <- dbConnect(RPostgres::Postgres(), host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE"), connect_timeout = 15L, bigint = "numeric")

# Offensive event rows in event order. secs = time left in regulation, or in the
# current overtime period.
il_ev <- as.data.table(dbGetQuery(con, "
  SELECT d.game_id, d.id AS ord, d.team_id, d.quarter, d.end_game_seconds_remaining AS secs,
         d.team_score AS pts, d.final_end_poss AS pend
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  WHERE d.type_lineup = 'offense'"))
il_home <- as.data.table(dbGetQuery(con, "
  SELECT game_id, team_id AS home, opp_team_id AS away, has_won AS home_won, game_date,
         game_type = 35 AS neutral, 'IL ' || game_year AS season
  FROM basketball_test.final_schedule_mv WHERE is_home"))
el_ev <- as.data.table(dbGetQuery(con, "
  SELECT game_id, source_event_order AS ord, team_id, period AS quarter,
         CASE WHEN period <= 4 THEN 2400 - elapsed_seconds
              ELSE 2400 + 300 * (period - 4) - elapsed_seconds END AS secs,
         points AS pts, final_end_poss AS pend
  FROM euroleague.ribbon_events_v WHERE type_lineup = 'offense'"))
el_home <- as.data.table(dbGetQuery(con, "
  SELECT f.game_id, f.team_id AS home, f.opp_team_id AS away, f.has_won AS home_won, f.game_date,
         coalesce(s.phase = 'FF', FALSE) AS neutral, f.competition || ' ' || f.game_year AS season
  FROM euroleague.final_schedule f JOIN euroleague.schedule s USING (game_id) WHERE f.is_home"))
dbDisconnect(con)

build_states <- function(ev, home) {
  ev[, c("pts", "secs", "ord") := lapply(.SD, as.numeric), .SDcols = c("pts", "secs", "ord")]
  ev <- merge(ev, home, by = "game_id"); setorder(ev, game_id, ord)
  ev[is.na(pend), pend := FALSE]; ev[is.na(pts), pts := 0]
  ev[, pnum := cumsum(shift(as.integer(pend), fill = 0L)), by = game_id]
  ev[, `:=`(hcum = cumsum(pts * (team_id == home)), acum = cumsum(pts * (team_id == away))), by = game_id]
  ev[, `:=`(h_before = hcum - pts * (team_id == home), a_before = acum - pts * (team_id == away))]
  ps <- ev[, .(season = season[1], quarter = quarter[1], offense = team_id[.N], ended = any(pend),
               h0 = h_before[1], a0 = a_before[1], end_secs = secs[.N]),
           by = .(game_id, pnum, home, away, home_won, game_date, neutral)][ended == TRUE]
  ps[, `:=`(prev_end = shift(end_secs), prev_q = shift(quarter)), by = game_id]
  ps[, t := ifelse(is.na(prev_q) | prev_q != quarter, ifelse(quarter <= 4, 2400 - 600 * (quarter - 1), 300), prev_end)]
  ps[, `:=`(m = h0 - a0, s = ifelse(offense == home, 1, -1))]
  # pre-game rating inputs: net points and possessions in earlier games of the same season
  gt <- ev[, .(pf = sum(pts), poss = sum(pend)), by = .(game_id, season, team_id, game_date)]
  gt[, opp_pts := sum(pf) - pf, by = game_id]
  setorder(gt, team_id, season, game_date, game_id)
  gt[, `:=`(cn = cumsum(pf - opp_pts) - (pf - opp_pts), cp = cumsum(poss) - poss), by = .(season, team_id)]
  ps[gt, on = .(game_id, home = team_id), `:=`(cnH = i.cn, cpH = i.cp)]
  ps[gt, on = .(game_id, away = team_id), `:=`(cnA = i.cn, cpA = i.cp)]
  stopifnot(!anyNA(ps[, .(t, m, s, cnH, cnA, home_won)]))
  ps
}

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

ps <- rbind(build_states(il_ev, il_home), build_states(el_ev, el_home))
# game ids are per league (Israeli and EuroLeague ids overlap): count games by season
cat("Possession states:", nrow(ps), "in", uniqueN(ps[, .(season, game_id)]), "games\n")
print(ps[, .(games = uniqueN(game_id), states = .N), by = season][order(season)])

# Out of sample: each season predicted by a fit on all the other seasons.
oos <- rbindlist(lapply(sort(unique(ps$season)), function(sn) {
  te <- ps[season == sn]; p <- fit(ps[season != sn])
  data.table(season = sn, game_id = te$game_id, y = as.numeric(te$home_won), q = wp(p, te))
}))
cat("\nOut of sample, by season (log loss):\n")
print(oos[, .(log_loss = round(ll(q, y), 3)), by = season][order(season)])
oos[, qw := ifelse(y == 1, q, 1 - q)]
per <- oos[, .(low = min(qw), dipped2 = any(q < .02 | q > .98), dipped5 = any(q < .05 | q > .95), dipped10 = any(q < .1 | q > .9)),
           by = .(season, game_id)]
cat("\nComebacks (out of sample): a side that fell below x% should win about x% of the time\n")
for (x in c(2, 5, 10)) {
  n <- per[get(paste0("dipped", x)) == TRUE, .N]; won <- per[low < x / 100, .N]
  cat(sprintf("  below %2d%%: %4d games, came back %3d (%.1f%%)\n", x, n, won, 100 * won / n))
}

p <- fit(ps)
cat("\nFit on every season (the constants to store):\n")
print(round(p, 3))
