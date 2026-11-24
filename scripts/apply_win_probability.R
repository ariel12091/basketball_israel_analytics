# Create the win-probability owner (sql/migrations/2026-11-24_win_probability.sql):
# basketball_test.win_prob_model, win_prob_states(), game_win_probability().
#
# Dry run by default: one transaction, every gate, then ROLLBACK. Set
# CONFIRM_WIN_PROBABILITY=1 to COMMIT. The model table starts empty; write the
# first fit with scripts/fit_win_probability.R --write, then re-apply
# sql/security/enable_readonly_rls.sql (scripts/apply_db_security.R) so
# app_readonly can call the functions.
#
# Gates:
#   1. every game's final row equals the official final score (Israeli games
#      whose play-by-play does not add up are listed, not failed: 14 known);
#   2. game_win_probability() equals the formula computed here from the same
#      states (pnorm), on a sample of games of both leagues, with test
#      constants written inside the transaction;
#   3. one game answers in well under a second.
# WIN_PROB_DUMP=<file.rds> also saves every state of both leagues.

confirm_apply <- identical(Sys.getenv("CONFIRM_WIN_PROBABILITY", "0"), "1")
dump_to <- Sys.getenv("WIN_PROB_DUMP", "")
readRenviron("etl/.Renviron")
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L,
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = "require", bigint = "numeric")
on.exit(dbDisconnect(con))
q <- function(sql, params = NULL) dbGetQuery(con, sql, params = params)
x <- function(sql) invisible(dbExecute(con, sql, immediate = TRUE))

cat("mode:", if (confirm_apply) "APPLY (commits)" else "DRY RUN (rolls back)", "\n")
x("BEGIN")
ok <- FALSE
tryCatch({
  x("SET LOCAL lock_timeout = '5s'")
  x("SET LOCAL statement_timeout = '300s'")
  mig <- Sys.getenv("WIN_PROB_SQL", "sql/migrations/2026-11-24_win_probability.sql")
  x(paste(readLines(mig, encoding = "UTF-8"), collapse = "\n"))
  cat("objects created\n")

  # Gate 1: final rows reconcile with the official scores
  official <- rbind(
    cbind(league = "il", q("SELECT game_id, team_score AS home_points, opp_score AS away_points
                             FROM basketball_test.final_schedule_mv WHERE is_home")),
    cbind(league = "euro", q("SELECT game_id, team_points AS home_points, opp_points AS away_points
                               FROM euroleague.final_schedule WHERE is_home")))
  states <- list()
  for (lg in c("il", "euro")) {
    t0 <- Sys.time()
    s <- q("SELECT * FROM basketball_test.win_prob_states($1, NULL)", list(lg))
    cat(sprintf("%-4s states: %d rows, %d games, %.1fs\n", lg, nrow(s), length(unique(s$game_id)),
                as.numeric(difftime(Sys.time(), t0, units = "secs"))))
    states[[lg]] <- cbind(league = lg, s)
  }
  st <- do.call(rbind, states)
  fin <- merge(st[st$is_final, c("league", "game_id", "home_score", "away_score")], official, by = c("league", "game_id"))
  bad <- fin[fin$home_score != fin$home_points | fin$away_score != fin$away_points, ]
  print(table(league = fin$league, matches = !(paste(fin$league, fin$game_id) %in% paste(bad$league, bad$game_id))))
  if (any(bad$league == "euro")) stop("EuroLeague final rows differ from the official scores")
  if (sum(bad$league == "il") > 14) stop("more Israeli games than the 14 known differ from the official scores")
  if (nrow(bad)) { cat("Israeli games whose play-by-play does not reach the official score:\n"); print(bad) }
  if (anyNA(st[!st$is_final, c("seconds_left", "home_has_ball", "home_score", "away_score")])) stop("NA in a possession row")
  if (any(st$seconds_left < 0 | st$seconds_left > 2400)) stop("seconds_left outside 0-2400")
  if (nzchar(dump_to)) { saveRDS(st, dump_to); cat("states saved to", dump_to, "\n") }

  # Gate 2: the SQL formula equals R's, with test constants
  k <- list(sigma = 15.9, c = 14, h = 4.3, kw = 1.04, K = 518, b = 0.58)
  x(sprintf("INSERT INTO basketball_test.win_prob_model (fitted_on, seasons, games, possession_states, sigma, c_seconds,
             home_advantage, rating_weight, rating_shrink_poss, ball_value, checks, active)
             VALUES (current_date, 'gate test', 0, 0, %f, %f, %f, %f, %f, %f, 'dry-run gate', TRUE)",
            k$sigma, k$c, k$h, k$kw, k$K, k$b))
  set.seed(1)
  sample_games <- rbind(data.frame(league = "il", game_id = sample(unique(states$il$game_id), 5)),
                        data.frame(league = "euro", game_id = sample(unique(states$euro$game_id), 5)))
  worst <- 0
  for (i in seq_len(nrow(sample_games))) {
    lg <- sample_games$league[i]; g <- sample_games$game_id[i]
    w <- q("SELECT * FROM basketball_test.game_win_probability($1, $2)", list(lg, g))
    s <- states[[lg]][states[[lg]]$game_id == g, ]; s <- s[order(s$possession_no), ]
    r <- 100 * (s$home_prior_net / (s$home_prior_poss + k$K) - s$away_prior_net / (s$away_prior_poss + k$K))
    mu <- ifelse(s$neutral, 0, k$h) + k$kw * r
    z <- (s$home_score - s$away_score + k$b * ifelse(s$home_has_ball %in% TRUE, 1, -1) + mu * s$seconds_left / 2400) /
         (k$sigma * sqrt((s$seconds_left + k$c) / 2400))
    p <- ifelse(s$is_final, as.numeric(s$home_won), pnorm(z))
    if (nrow(w) != nrow(s)) stop("game ", g, ": row count differs")
    worst <- max(worst, abs(w$home_win_probability - p))
  }
  cat(sprintf("gate 2: SQL vs R formula on %d games, largest difference %.2e\n", nrow(sample_games), worst))
  if (worst > 1e-9) stop("SQL and R formulas disagree")

  # Gate 3: one game is fast
  t <- system.time(q("SELECT * FROM basketball_test.game_win_probability('il', $1)", list(sample_games$game_id[1])))[["elapsed"]]
  t2 <- system.time(q("SELECT * FROM basketball_test.game_win_probability('euro', $1)", list(sample_games$game_id[6])))[["elapsed"]]
  cat(sprintf("gate 3: one game %.2fs (il), %.2fs (euro)\n", t, t2))
  if (max(t, t2) > 2) stop("one game takes over 2 s")
  x("DELETE FROM basketball_test.win_prob_model WHERE seasons = 'gate test'")
  ok <- TRUE
}, error = function(e) cat("FAILED:", conditionMessage(e), "\n"))
if (ok && confirm_apply) { x("COMMIT"); cat("COMMITTED\n") } else { x("ROLLBACK"); cat("rolled back\n") }
if (!ok) quit(status = 1)
