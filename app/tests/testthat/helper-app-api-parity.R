# Parity proof for the app_api Game Logs readers (plan Task 5 Step 1), shared
# by two test files so the proof is written once:
#   test-app-api-sql-catalog-db.R  -- deploys sql/publication + sql/app_api
#                                     into a scratch pubtest_* schema as the
#                                     owner and runs it there (before apply)
#   test-app-api-game-logs-db.R    -- runs it against the applied app_api as
#                                     app_readonly (after apply)
#
# The reference is Shiny itself: its fetchers are lifted out of app/R/ by
# parse() and evaluated against the same connection, so the comparison uses
# Shiny's own SQL, not a copy of it.

# Shiny's Game Logs season pulls, bound to `con`.
shiny_game_logs_fetchers <- function(con) {
  env <- new.env(parent = globalenv())
  env$pg_pool <- con
  env$db_get_query <- function(pool, sql, params = NULL) {
    DBI::dbGetQuery(pool, sql, params = params)
  }
  env$cached_ref_query <- function(key, query_fun, ...) query_fun()
  lift <- function(path, wanted) {
    for (e in parse(path)) {
      if (is.call(e) && identical(as.character(e[[1]]), "<-") &&
          as.character(e[[2]])[[1]] %in% wanted) {
        eval(e, envir = env)
      }
    }
  }
  lift(repo_file("R", "svc_game_logs.R"),
       c("gl_query_schedule", "gl_query_lineup_totals", "gl_query_lineup_ff"))
  lift(repo_file("R", "global.R"), "fetch_scoreless_games")
  lift(repo_file("R", "global_euro.R"), "euro_fetch_game_log_rows")
  missing <- setdiff(c("gl_query_schedule", "gl_query_lineup_totals", "gl_query_lineup_ff",
                       "fetch_scoreless_games", "euro_fetch_game_log_rows"), ls(env))
  if (length(missing)) stop("Shiny fetchers not found: ", paste(missing, collapse = ", "))
  env
}

app_api_payload <- function(con, sql, params) {
  text <- DBI::dbGetQuery(con, sql, params = params)$p[[1]]
  list(text = text, value = jsonlite::fromJSON(text))
}

# One comparable form for a frame from either side: plain columns, dates as
# text, numbers as double, rows in key order.
parity_normalise <- function(df, keys) {
  df <- as.data.frame(df, stringsAsFactors = FALSE)
  for (col in names(df)) {
    x <- df[[col]]
    if (inherits(x, c("Date", "POSIXt"))) x <- format(x, "%Y-%m-%d")
    if (is.numeric(x)) x <- as.numeric(x)
    if (is.factor(x)) x <- as.character(x)
    df[[col]] <- x
  }
  df <- df[do.call(order, unname(as.list(df[keys]))), , drop = FALSE]
  rownames(df) <- NULL
  df
}

# Sums of `cols` by `keys`, NA keys kept as their own group. NA values count
# as zero, as in Shiny's gl_build_*_metrics (sum(..., na.rm = TRUE)).
parity_sum_by <- function(df, keys, cols) {
  if (!nrow(df)) {
    out <- df[0, c(keys, cols), drop = FALSE]
    return(out)
  }
  grp <- do.call(paste, c(lapply(df[keys], function(x) ifelse(is.na(x), "<NA>", x)), sep = "\r"))
  idx <- split(seq_len(nrow(df)), grp)
  out <- do.call(rbind, lapply(idx, function(i) {
    row <- df[i[[1]], keys, drop = FALSE]
    for (col in cols) row[[col]] <- sum(as.numeric(df[[col]][i]), na.rm = TRUE)
    row
  }))
  rownames(out) <- NULL
  out
}

PARITY_IL_KEYS <- c("game_id", "team_id", "type_lineup", "num_starters")
PARITY_SUMMARY_COLS <- c("total_poss", "total_pts", "fg2_made", "fg2_att",
                         "fg3_made", "fg3_att", "minutes")
# Four Factors column -> its name in the reader's rows.
PARITY_FF_COLS <- c(total_poss = "ff_total_poss", total_points = "total_points",
                    ts_poss_count = "ts_poss_count", oreb_count = "oreb_count",
                    oreb_opportunities = "oreb_opportunities", tov_count = "tov_count",
                    total_ft_attempts = "total_ft_attempts", total_fga = "total_fga",
                    total_fgm = "total_fgm", total_fg3_made = "total_fg3_made",
                    minutes = "ff_minutes")
PARITY_STATES <- c("published", "publishing", "failed", "stale")

# Compares one reader side (`rows` restricted to rows that carry `cols`) with
# the Shiny frame summed to the reader's keys.
expect_fact_parity <- function(rows, shiny, cols, label) {
  reader_cols <- unname(cols)
  shiny_cols <- if (is.null(names(cols))) cols else names(cols)
  # A reader row carries this side when any of its columns is non-NULL (the
  # other side of the FULL JOIN is all NULL). SQL's sum() of an all-NULL group
  # is NULL where Shiny's na.rm sum is 0, so NULL compares as 0.
  has_side <- Reduce(`|`, lapply(reader_cols, function(col) !is.na(rows[[col]])))
  present <- rows[has_side, c(PARITY_IL_KEYS, reader_cols), drop = FALSE]
  names(present) <- c(PARITY_IL_KEYS, shiny_cols)
  for (col in shiny_cols) present[[col]][is.na(present[[col]])] <- 0
  expected <- parity_sum_by(shiny, PARITY_IL_KEYS, shiny_cols)
  a <- parity_normalise(present, PARITY_IL_KEYS)
  b <- parity_normalise(expected, PARITY_IL_KEYS)
  expect_identical(nrow(a), nrow(b), label = paste(label, "row count"))
  expect_identical(a[PARITY_IL_KEYS], b[PARITY_IL_KEYS], label = paste(label, "keys"))
  for (col in shiny_cols) {
    tol <- if (col == "minutes") 1e-9 else 0
    expect_equal(a[[col]], b[[col]], tolerance = tol, label = paste(label, col))
  }
  # The starters split loses nothing: per (game, team, side) the split sums to
  # the unsplit Shiny total.
  side <- c("game_id", "team_id", "type_lineup")
  expect_equal(
    parity_normalise(parity_sum_by(present, side, shiny_cols), side),
    parity_normalise(parity_sum_by(shiny, side, shiny_cols), side),
    tolerance = 1e-9, label = paste(label, "unsplit totals"))
}

# fn_schema: where the readers live ("app_api", or the scratch schema).
expect_app_api_game_logs_parity <- function(con, fn_schema, il_season, euro_season) {
  q <- function(name) paste0(DBI::dbQuoteIdentifier(con, fn_schema), ".", name)
  shiny <- shiny_game_logs_fetchers(con)

  v <- DBI::dbGetQuery(con, paste0("SELECT * FROM ", q("version()")))
  expect_setequal(v$league, c("il", "euroleague", "eurocup"))
  expect_true(all(v$state %in% PARITY_STATES))
  expect_true(all(v$version >= 1))

  # ---- Israeli season ----
  il <- app_api_payload(con, paste0("SELECT ", q("game_logs_il($1)"), "::text AS p"),
                        list(as.integer(il_season)))
  p <- il$value
  rows <- as.data.frame(p$rows)
  expect_gt(nrow(rows), 0L)
  expect_true(p$state %in% PARITY_STATES)
  expect_gte(p$version, 1)
  expect_equal(p$row_count, nrow(rows))
  expect_equal(p$ctl_poss, sum(rows$total_poss, na.rm = TRUE))
  expect_equal(p$ctl_ff_poss, sum(rows$ff_total_poss, na.rm = TRUE))

  sched_shiny <- shiny$gl_query_schedule(il_season)
  sched <- as.data.frame(p$schedule)
  expect_equal(p$schedule_count, nrow(sched))
  expect_setequal(names(sched), names(sched_shiny))
  expect_equal(parity_normalise(sched[names(sched_shiny)], c("game_id", "team_id")),
               parity_normalise(sched_shiny, c("game_id", "team_id")),
               label = "schedule rows")

  expect_fact_parity(rows, shiny$gl_query_lineup_totals(il_season),
                     PARITY_SUMMARY_COLS, "Summary")
  expect_fact_parity(rows, shiny$gl_query_lineup_ff(il_season), PARITY_FF_COLS, "Four Factors")

  scoreless_shiny <- shiny$fetch_scoreless_games()
  scoreless_shiny <- scoreless_shiny[scoreless_shiny$game_id %in% sched_shiny$game_id, ,
                                     drop = FALSE]
  scoreless <- as.data.frame(p$scoreless)
  if (!nrow(scoreless)) scoreless <- data.frame(game_id = integer(0), team_id = integer(0))
  expect_equal(parity_normalise(scoreless, c("game_id", "team_id")),
               parity_normalise(scoreless_shiny[c("game_id", "team_id")],
                                c("game_id", "team_id")),
               label = "scoreless pairs")

  # ---- Empty season: the envelope and its metadata still arrive (R6) ----
  e <- app_api_payload(con, paste0("SELECT ", q("game_logs_il($1)"), "::text AS p"),
                       list(1900L))$value
  expect_length(e$rows, 0L)
  expect_equal(e$row_count, 0)
  expect_equal(e$ctl_poss, 0)
  expect_equal(e$schedule_count, 0)
  expect_true(e$state %in% PARITY_STATES)
  expect_gte(e$version, 1)

  sizes <- c(il = nchar(il$text, type = "bytes"))

  # ---- EuroLeague / EuroCup: native rows, equal to Shiny's frame as a set (R10) ----
  euro_keys <- c("game_id", "team_id", "own_starters", "opp_starters")
  for (comp in c("E", "U")) {
    eu <- app_api_payload(con, paste0("SELECT ", q("game_logs_euro($1, $2)"), "::text AS p"),
                          list(comp, as.integer(euro_season)))
    pe <- eu$value
    sizes[[paste0("euro_", comp)]] <- nchar(eu$text, type = "bytes")
    ref <- shiny$euro_fetch_game_log_rows(comp, euro_season)
    expect_true(pe$state %in% PARITY_STATES, info = comp)
    expect_equal(pe$league, if (comp == "E") "euroleague" else "eurocup")
    expect_equal(pe$row_count, nrow(ref), info = comp)
    expect_equal(pe$ctl_poss, sum(as.numeric(ref$off_poss)), info = comp)
    if (nrow(ref)) {
      got <- as.data.frame(pe$rows)
      expect_setequal(names(got), names(ref))
      a <- parity_normalise(got[names(ref)], euro_keys)
      b <- parity_normalise(ref, euro_keys)
      for (col in names(ref)) {
        expect_equal(a[[col]], b[[col]], tolerance = if (col == "minutes") 1e-9 else 0,
                     label = paste(comp, col))
      }
    }
  }
  expect_error(DBI::dbGetQuery(con, paste0("SELECT ", q("game_logs_euro('X', 2025)"))),
               "unknown competition")

  cat(sprintf("\n[API payload bytes] il/%s=%d euro_E/%s=%d euro_U/%s=%d\n",
              il_season, sizes[["il"]], euro_season, sizes[["euro_E"]],
              euro_season, sizes[["euro_U"]]))
  invisible(sizes)
}
