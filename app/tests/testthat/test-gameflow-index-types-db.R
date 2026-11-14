# RPostgres with bigint="numeric" reads EuroLeague IDs as doubles. Quoting
# those fixtures used to produce ::float8 predicates, full fact scans and
# large temp spills. Exercise that caller, not just integer-bound IDs.
test_that("Game Flow double-valued IDs retain indexed single-game reads", {
  skip_if_not(identical(Sys.getenv("RUN_DB_TESTS"), "1"), "RUN_DB_TESTS not enabled")
  skip_if_not(nzchar(Sys.getenv("PG_HOST")), "no database configured")
  con <- db_test_connect(RPostgres::Postgres(),
    host = Sys.getenv("PG_HOST"), port = 5432L,
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = "require", bigint = "numeric",
    connect_timeout = 15L)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbExecute(con, "SET default_transaction_read_only = on")
  DBI::dbExecute(con, "SET statement_timeout = '10s'")
  readers <- global_defs("RIBBON_SQL_ISRAEL", "RIBBON_SQL_EURO")
  canonical <- function(row) {
    for (key in c("lanes", "margin", "poss", "events")) {
      frame <- jsonlite::fromJSON(row[[key]][1])
      if (is.data.frame(frame) && nrow(frame)) {
        # SQL does not promise lane ordering; compare complete row multisets.
        keys <- vapply(seq_len(nrow(frame)), function(i)
          jsonlite::toJSON(frame[i, , drop = FALSE], digits = NA), character(1))
        frame <- frame[order(keys), , drop = FALSE]
        rownames(frame) <- NULL
      }
      row[[key]] <- list(frame)
    }
    row
  }
  for (league in c("il", "euroleague", "eurocup")) {
    ids <- switch(league, il = c(390L, 2L), euroleague = c(781L, 8L),
                  eurocup = c(671L, 966L))
    sql <- if (league == "il") readers$RIBBON_SQL_ISRAEL else readers$RIBBON_SQL_EURO
    # The old reader with INTEGER parameters is a safe result baseline.
    original <- gsub("(\\$[12])::(int4|bigint)", "\\1", sql)
    expected <- canonical(DBI::dbGetQuery(con, original, params = as.list(ids)))
    expect_equal(canonical(DBI::dbGetQuery(con, sql,
                 params = as.list(as.numeric(ids)))), expected, info = league)
    quoted <- gsub("$1", "?game_id", gsub("$2", "?team_id", sql, fixed = TRUE), fixed = TRUE)
    quoted <- DBI::sqlInterpolate(con, quoted,
      game_id = as.numeric(ids[1]), team_id = as.numeric(ids[2]))
    expect_equal(canonical(DBI::dbGetQuery(con, quoted)), expected, info = league)
    # EXPLAIN only: a regression must fail without executing its full scans.
    plan <- DBI::dbGetQuery(con, paste("EXPLAIN (FORMAT JSON)", quoted))[[1]][1]
    nodes <- list()
    visit <- function(node) {
      nodes[[length(nodes) + 1L]] <<- node
      for (child in node$Plans) visit(child)
    }
    visit(jsonlite::fromJSON(plan, simplifyVector = FALSE)[[1]]$Plan)
    facts <- c("lineup_totals_by_game", "action_team_context_actions",
               "matchup_segments_actions", "df_pts_poss_lineups_longer_mv")
    scans <- Filter(function(n) identical(n[["Node Type"]], "Seq Scan") &&
                      n[["Relation Name"]] %in% facts, nodes)
    expect_equal(length(scans), 0L, info = league)
  }
})
