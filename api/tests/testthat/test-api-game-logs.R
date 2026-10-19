# GET /v1/{league}/game-logs: for every Game Logs preset the endpoint's rows
# equal the Shiny golden (the frame the renderer hands DataTables), value for
# value and in the same order. Requests go through the real router
# (api_router()), on recorded query answers.

gl_api_env <- api_test_env()
gl_router <- api_router(gl_api_env)

gl_golden <- function(name) {
  readRDS(file.path(api_test_root, "app", "tests", "goldens", "out", "game_logs", paste0(name, ".rds")))
}

# A golden column's API key: the shot cells' display names become snake_case
# and the Gameflow link becomes the has_scores flag it was drawn from.
# Deliberately restates the rename contract (GL_API_KEYS in
# handlers_game_logs.R) instead of reading it, so a changed key fails here.
gl_api_key <- function(cols) {
  renames <- c("Off Shot" = "off_shot_fga", "Def Shot" = "def_shot_fga", gameflow = "has_scores")
  out <- renames[cols]
  unname(ifelse(is.na(out), cols, out))
}

gl_get <- function(league, qs = "") api_http(gl_router, sprintf("/v1/%s/game-logs", league), qs)
gl_json <- function(resp) jsonlite::fromJSON(resp$body, simplifyVector = TRUE)

# The payload the handler answers with, before serialisation (exact values).
gl_payload <- function(qs, league) {
  gl_api_env$gl_api_payload(gl_api_env$api_query(api_req(qs)), league)$payload
}

test_that("the preset loop's literal coverage set matches the presets", {
  # Ruling R5: the scanner sees literals only, so the presets' union is written
  # out here, plus the X ids the loop below claims.
  covers("GL-01", "GL-02", "GL-03", "GL-04", "GL-05", "GL-06", "GL-07", "X-09",
         "X-04", "X-05", "X-11", "X-12", "X-13")
  literal <- c("GL-01", "GL-02", "GL-03", "GL-04", "GL-05", "GL-06", "GL-07", "X-09")
  expect_setequal(literal, unique(unlist(lapply(GAME_LOGS_PRESETS, `[[`, "covers"))))
  expect_setequal(names(GL_PRESET_QUERIES), names(GAME_LOGS_PRESETS))
})

for (gl_preset_name in names(GAME_LOGS_PRESETS)) {
  local({
    name <- gl_preset_name
    preset <- GAME_LOGS_PRESETS[[name]]
    test_that(paste("Game Logs endpoint equals the golden:", name), {
      covers(preset$covers, "X-04", "X-05", "X-11", "X-12", "X-13")
      golden <- gl_golden(name)
      league <- preset_api_league(preset)
      qs <- preset_to_query(name)

      resp <- gl_get(league, qs)
      expect_identical(resp$status, 200L)
      body <- gl_json(resp)
      expect_null(body$totals)
      expect_identical(body$meta$league, league)
      expect_identical(body$meta$warnings, list())

      if (isTRUE(preset$empty)) {
        expect_null(golden)
        expect_length(body$rows, 0L)
        return(invisible())
      }

      # Exact values, before serialisation.
      rows <- gl_payload(qs, league)$rows
      expect_identical(nrow(rows), nrow(golden$data))
      cols <- setdiff(names(golden$data), "gameflow")
      for (col in cols) {
        expect_identical(rows[[gl_api_key(col)]], golden$data[[col]], info = paste(name, col))
      }
      expect_true(is.logical(rows$has_scores))
      expect_false(anyNA(rows$has_scores))
      # A Gameflow link is drawn exactly where has_scores is TRUE.
      expect_identical(rows$has_scores, grepl("ribbon-link", golden$data$gameflow, fixed = TRUE), info = name)

      # The JSON carries the same values (numbers unrounded, dates ISO).
      expect_identical(nrow(body$rows), nrow(golden$data))
      for (col in cols) {
        g <- golden$data[[col]]
        if (inherits(g, "Date")) g <- format(g)
        expect_equal(body$rows[[gl_api_key(col)]], g, tolerance = 1e-12, info = paste(name, col, "json"))
      }

      # Columns: visible in the golden's order, hidden = the golden's hidden set
      # plus the row ids; polarity from the renderer's heat maps.
      meta_cols <- body$meta$columns
      visible <- gl_api_key(setdiff(names(golden$data), golden$hidden))
      expect_identical(meta_cols$key[!meta_cols$hidden], visible, info = name)
      expect_setequal(meta_cols$key[meta_cols$hidden], c(gl_api_key(golden$hidden), "game_id", "team_id"))
      pol <- stats::setNames(meta_cols$polarity, meta_cols$key)
      expect_identical(pol[["def_ppp"]], "lower_better")
      expect_identical(pol[["off_ppp"]], "higher_better")
      if ("off_tov_pct" %in% meta_cols$key) {
        expect_identical(pol[["off_tov_pct"]], "lower_better")
        expect_identical(pol[["def_tov_pct"]], "higher_better")
      }
      pk <- stats::setNames(meta_cols$percentile_key, meta_cols$key)
      expect_identical(pk[["off_ppp"]], "pr_off_ppp")
      expect_true(is.na(pk[["net_rtg"]]))

      expect_identical(body$meta$default_sort$column,
                       c("game_date", if (league == "il") "gn" else "round_number"))
      expect_identical(body$meta$default_sort$direction, c("desc", "desc"))
    })
  })
}
rm(gl_preset_name)

test_that("column labels and definitions come from Shiny's table headers", {
  covers("X-16")
  cols <- gl_json(gl_get("il", "season=2026"))$meta$columns
  by_key <- function(k, f) cols[[f]][match(k, cols$key)]
  expect_identical(by_key("off_ppp", "label"), "Off PPP")
  expect_identical(by_key("has_scores", "label"), "Gameflow")
  expect_identical(by_key("off_shot_fga", "label"), "Off Shot")
  expect_identical(by_key("net_rtg", "definition"), gl_api_env$COLUMN_TOOLTIPS[["Net"]])
  expect_true(is.na(by_key("game_date", "definition")))
  expect_identical(by_key("game_date", "format"), "date")

  ff <- gl_json(gl_get("il", "season=2026&view=four_factors"))$meta$columns
  ff_key <- function(k, f) ff[[f]][match(k, ff$key)]
  expect_identical(ff_key("off_oreb_pct", "label"), "OREB%")
  expect_identical(ff_key("off_oreb_pct", "group"), "Offense")
  expect_identical(ff_key("def_ppp", "group"), "Defense")
  # A header cell's own title wins, as HEADER_TOOLTIP_JS gives it precedence.
  expect_identical(ff_key("off_oreb_pct", "definition"), gl_api_env$OFF_OREB_TOOLTIP)
  expect_identical(ff_key("def_oreb_pct", "definition"), gl_api_env$DEF_OREB_TOOLTIP)
  expect_identical(ff_key("def_efg_pct", "definition"), gl_api_env$COLUMN_TOOLTIPS[["eFG%"]])

  el <- gl_json(gl_get("euroleague", "season=2025"))$meta$columns
  expect_identical(el$label[match(c("round_number", "phase_label"), el$key)],
                   unname(unlist(gl_api_env$GL_LEAGUE$euroleague$header_labels)))
})

# The header cells' text as the browser receives it: the builder's HTML, with
# the last header row's <th> texts read off by a regex -- independent of the
# API's own tag walker (api_header_cells()).
gl_rendered_labels <- function(header, n) {
  html <- as.character(header)
  cells <- regmatches(html, gregexpr("<th[^>]*>[^<]*</th>", html))[[1]]
  texts <- sub("^<th[^>]*>([^<]*)</th>$", "\\1", cells)
  texts <- vapply(texts, function(x) as.character(htmltools::HTML(x)), character(1), USE.NAMES = FALSE)
  utils::tail(texts, n)
}

test_that("visible labels equal what Shiny's header draws, per league and view", {
  covers("X-16", "GL-01")
  cases <- list(
    list("il", "season=2026", "summary", list(has_shots = TRUE)),
    list("il", "season=2026&view=four_factors", "four_factors", list()),
    list("euroleague", "season=2025", "summary", list(has_shots = FALSE)),
    list("euroleague", "season=2025&view=four_factors", "four_factors", list())
  )
  for (cs in cases) {
    cols <- gl_json(gl_get(cs[[1]], cs[[2]]))$meta$columns
    vis <- cols[!cols$hidden, ]
    header <- do.call(gl_api_env$gl_header, c(list(cs[[1]], cs[[3]]), cs[[4]], list(show_gameflow = TRUE)))
    expect_identical(vis$label, gl_rendered_labels(header, nrow(vis)), info = paste(cs[[1]], cs[[3]]))
    # One decimal exactly where the tables round to one decimal.
    dec <- intersect(c(gl_api_env$GL_ROUND_1[[cs[[3]]]], "minutes"), cols$key)
    expect_setequal(cols$key[cols$format == "decimal_1"], dec)
  }
})

test_that("Tab 4 and Tab 11 draw their headers, rounding and heat from the shared constants", {
  covers("GL-01")
  # The API reads labels, rounding and polarity from GL_LEAGUE, GL_ROUND_1 and
  # GL_HEAT_REVERSE_*; this pins the renderers to the same constants, so a
  # label or rounding typed back into a renderer fails here.
  for (f in c("server_tab4.R", "server_tab11_euro_gamelogs.R")) {
    src <- paste(readLines(file.path(api_test_root, "app", "R", f), warn = FALSE), collapse = "\n")
    expect_false(grepl("gamelog_(summary|ff)_header\\(", src), info = f)
    expect_identical(lengths(regmatches(src, gregexpr("gl_header\\(", src))), 2L, info = f)
    expect_true(grepl("GL_ROUND_1$summary", src, fixed = TRUE), info = f)
    expect_true(grepl("GL_ROUND_1$four_factors", src, fixed = TRUE), info = f)
    expect_true(grepl("GL_HEAT_REVERSE_SUMMARY", src, fixed = TRUE), info = f)
    expect_true(grepl("GL_HEAT_REVERSE_FF", src, fixed = TRUE), info = f)
    expect_false(grepl("formatRound\\([^)]*c\\(\"", src), info = f)
  }
})

test_that("an empty result is a 200 with no rows and the resolved filters echoed", {
  covers("GL-02")
  resp <- gl_get("il", preset_to_query("il_summary_empty"))
  expect_identical(resp$status, 200L)
  body <- gl_json(resp)
  expect_length(body$rows, 0L)
  f <- body$meta$filters
  expect_identical(f$team, "2")
  expect_identical(f$gn$min_gn, 30L)
  expect_identical(f$gn$max_gn, 31L)
  expect_identical(f$season, 2026L)
  expect_identical(f$dates, c("2025-09-01", "2026-07-01"))
  expect_identical(body$meta$view, "summary")
  expect_true(nrow(body$meta$columns) > 0L)

  csv <- gl_get("il", paste0(preset_to_query("il_summary_empty"), "&format=csv"))
  expect_identical(csv$status, 200L)
  expect_length(utils::read.csv(text = csv$body, check.names = FALSE)[[1]], 0L)
})

test_that("team ids are league-local: an Israeli-only id matches no EuroLeague game", {
  covers("X-01")
  # 3 is HAPOEL TEL AVIV in the Israeli 2026 teams and appears in no EuroLeague
  # 2025 row as a team or an opponent, so a match could only come from the
  # other league's id space. Checked against the recorded data, not assumed.
  il_ids <- gl_api_env$fetch_teams_min(2026)$team_id
  el_rows <- gl_api_env$gl_default_readers()$euro_rows("E", "2025", gl_api_env$svc_data_version("euroleague"))
  expect_true(3 %in% il_ids)
  expect_false(3 %in% c(el_rows$team_id, el_rows$opp_team_id))

  resp <- gl_get("euroleague", "season=2025&teams=3")
  expect_identical(resp$status, 200L)
  expect_length(gl_json(resp)$rows, 0L)
  resp <- gl_get("euroleague", "season=2025&opponents=3")
  expect_identical(resp$status, 200L)
  expect_length(gl_json(resp)$rows, 0L)

  # A number both leagues use (2) selects the EuroLeague team that has it.
  rows <- gl_json(gl_get("euroleague", "season=2025&teams=2"))$rows
  el_name <- with(gl_api_env$euro_fetch_teams("E", 2025), team_name[team_id == 2])
  expect_gt(nrow(rows), 0L)
  expect_identical(unique(rows$team_id), 2L)
  expect_identical(unique(rows$team_name), el_name)
})

test_that("format=csv gives the visible columns in golden order, every row", {
  covers("X-14", "GL-08")
  for (name in c("il_summary_default", "il_ff_default", "el_summary_default", "el_ff_default")) {
    golden <- gl_golden(name)
    preset <- GAME_LOGS_PRESETS[[name]]
    resp <- gl_get(preset_api_league(preset), paste0(preset_to_query(name), "&format=csv"))
    expect_identical(resp$status, 200L, info = name)
    expect_match(resp$headers[["Content-Type"]], "^text/csv")
    expect_match(resp$headers[["Content-Disposition"]], "_game_logs_", fixed = TRUE)
    csv <- utils::read.csv(text = resp$body, check.names = FALSE, stringsAsFactors = FALSE)
    expect_identical(names(csv), gl_api_key(setdiff(names(golden$data), golden$hidden)), info = name)
    expect_identical(nrow(csv), nrow(golden$data), info = name)
    expect_identical(csv$team_name, golden$data$team_name, info = name)
  }
})

test_that("sort= orders by the named columns, NAs last; an unknown column is a 400", {
  covers("X-13")
  rows <- gl_payload("season=2026&sort=net_rtg:asc", "il")$rows
  expect_false(is.unsorted(rows$net_rtg[!is.na(rows$net_rtg)]))
  rows <- gl_payload("season=2026&sort=team_name:asc,game_date:desc", "il")$rows
  # Text compares byte-wise (C locale), whatever the server's locale.
  expect_identical(rows$team_name, sort(rows$team_name, method = "radix"))
  for (t in unique(rows$team_name)) {
    expect_false(is.unsorted(rev(as.numeric(rows$game_date[rows$team_name == t]))))
  }
  # NAs last in both directions.
  sorter <- gl_api_env$gl_api_sort
  df <- data.frame(x = c(2, NA, 1, 3), y = c("b", "a", "c", "d"))
  expect_identical(sorter(df, list(list(column = "x", direction = "asc")))$x, c(1, 2, 3, NA))
  expect_identical(sorter(df, list(list(column = "x", direction = "desc")))$x, c(3, 2, 1, NA))

  resp <- gl_get("il", "season=2026&sort=bogus:asc")
  expect_identical(resp$status, 400L)
  expect_identical(gl_json(resp)$param, "sort")
  # The other league's keys are not this league's columns.
  resp <- gl_get("euroleague", "season=2025&sort=gn:desc")
  expect_identical(resp$status, 400L)
  expect_identical(gl_json(resp)$param, "sort")
})

test_that("the real router answers bad bytes, bad names and unknown leagues as errors", {
  covers("X-20")
  resp <- gl_get("il", "season=%00")
  expect_identical(resp$status, 400L)
  expect_identical(gl_json(resp)$param, "season")
  resp <- gl_get("il", "foo=1")
  expect_identical(resp$status, 400L)
  expect_identical(gl_json(resp)$param, "foo")
  resp <- gl_get("il", "teams=1")
  expect_identical(resp$status, 400L)
  expect_identical(gl_json(resp)$param, "teams")
  resp <- gl_get("nba", "")
  expect_identical(resp$status, 404L)
  expect_identical(gl_json(resp)$param, "league")
  resp <- api_http(gl_router, "/v1/il/teams", "?season=%00")
  expect_identical(resp$status, 400L)
})
