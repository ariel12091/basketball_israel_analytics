library(testthat)

source(file.path("..", "..", "scripts", "backfill_basket_shooting_history.R"), local = TRUE)

test_that("accumulated-stat rows preserve exact shooting totals", {
  html <- paste0(
    "<table><tr><th>#</th><th>name</th></tr>",
    "<tr><td>1</td><td><a href='player.asp?PlayerId=21797'>ים&nbsp;מדר</a></td>",
    "<td>28</td><td>741</td><td>427</td><td>92/176</td><td>52.3%</td>",
    "<td>60/138</td><td>43.5%</td><td>63/74</td><td>85.1%</td></tr>",
    "<tr><td>2</td><td><a href=\"player.asp?PlayerId=123\">A &amp; B</a></td>",
    "<td>10</td><td>12:30</td><td>20.5</td><td>2/5</td><td>40%</td>",
    "<td>1/4</td><td>25%</td><td>3/3</td><td>100%</td></tr></table>"
  )
  out <- parse_basket_accumulate_html(html)

  expect_equal(out$basket_player_id, c(21797L, 123L))
  expect_equal(out$player_name, c("ים מדר", "A & B"))
  expect_equal(out$fg3_made, c(60L, 1L))
  expect_equal(out$fg3_attempted, c(138L, 4L))
  expect_equal(out$minutes[2], 12.5)
  expect_equal(out$points[2], 20.5)
})

test_that("profile labels produce season attributes", {
  html <- paste0(
    "<section><div>אזרחות:&nbsp; <a> ישראל (ISR)</a></div>",
    "<div>עמדה:&nbsp;גארד</div><div>גובה:&nbsp;1.90</div>",
    "<div>תאריך לידה: 21/12/2000</div></section>"
  )
  out <- parse_basket_player_profile_html(html)

  expect_equal(out$nationality_name, "ישראל")
  expect_equal(out$nationality_code, "ISR")
  expect_equal(out$position_name, "גארד")
  # The parse carries the Hebrew; English arrives in the post-download pass.
  expect_true(is.na(out$position_en))
  expect_equal(resolve_profile_positions(out)$position_en, "G")
  expect_equal(out$height_m, 1.9)
})

test_that("profile heights in centimeters are normalized to meters", {
  html <- "<div>אזרחות: USA (USA)</div><div>עמדה: סנטר גובה: 208</div>"
  out <- parse_basket_player_profile_html(html)
  expect_equal(out$height_m, 2.08)
})

test_that("invalid made-attempted totals fail loudly", {
  expect_error(parse_made_attempted("8/7", "FT"), "Invalid FT totals")
})

test_that("season selection is capped at five", {
  expect_equal(parse_seasons("2022:2026"), 2022:2026)
  expect_error(parse_seasons("2021:2026"), "At most five seasons")
})

test_that("an existing cache file prevents another request", {
  cache_dir <- tempfile("basket-cache-test-")
  dir.create(cache_dir)
  cache_file <- file.path(cache_dir, "page.html")
  writeLines("<html>cached</html>", cache_file, useBytes = TRUE)
  on.exit(unlink(cache_dir, recursive = TRUE), add = TRUE)

  result <- download_with_cache(
    "https://invalid.example.test/should-not-be-requested", cache_file
  )
  expect_true(result$cache_hit)
  expect_equal(readLines(result$path, warn = FALSE), "<html>cached</html>")
})

test_that("written-row comparison passes identical rows and names mismatches", {
  expected <- data.frame(
    game_year = c(2026L, 2026L), basket_player_id = c(1L, 2L),
    player_name = c("A", "B"), player_name_en = c("A en", "B en"),
    games = c(10L, 3L), minutes = c(12.5, 30),
    points = c(20, 4), fg2_made = c(2L, 1L), fg2_attempted = c(5L, 2L),
    fg3_made = c(1L, 0L), fg3_attempted = c(4L, 1L), ft_made = c(3L, 2L),
    ft_attempted = c(3L, 2L), source_url = "u", source_page = 1L,
    source_row = c(2L, 3L), source_content_md5 = strrep("a", 32),
    stringsAsFactors = FALSE
  )
  expect_length(compare_written_rows(expected[rev(seq_len(2)), ], expected), 0L)

  stored <- expected
  stored$fg3_attempted[2] <- 9L
  expect_match(compare_written_rows(stored, expected), "fg3_attempted differs on 1 rows")
  expect_match(compare_written_rows(stored[1, ], expected), "1 parsed rows absent")
  expect_match(compare_written_rows(expected, expected[1, ]), "1 SQL rows not in")
})

test_that("Hebrew positions translate to the site's English abbreviations", {
  expect_equal(
    translate_position(c("רכז", "פ. פורוורד", "פ.פורוורד", "פורוורד/סנטר", NA)),
    c("PG", "PF", "PF", "F-C", NA)
  )
  expect_error(translate_position("שחקן חדש"), "No English position")
})

# The separators are built from code points on purpose: a Hebrew maqaf and an
# ASCII hyphen are indistinguishable on screen, and a literal one in this file
# would be exactly as invisible here as it was in the page that found it.
test_that("compound positions resolve whatever separator the site used", {
  forward <- "פורוורד"
  centre <- "סנטר"
  separators <- c("-", "/", intToUtf8(0x05BE), intToUtf8(0x2013))
  expect_equal(
    translate_position(paste0(forward, separators, centre)),
    rep("F-C", length(separators))
  )
  expect_equal(translate_position(paste0(forward, " - ", centre)), "F-C")
})

test_that("an unknown position names every distinct value with its code points", {
  expect_error(
    translate_position(c("שחקן חדש", "רכז", "עמדה אחרת", "שחקן חדש")),
    "U[+]05E9"
  )
  message <- tryCatch(
    translate_position(c("שחקן חדש", "רכז", "עמדה אחרת", "שחקן חדש")),
    error = conditionMessage
  )
  expect_match(message, "שחקן חדש")
  expect_match(message, "עמדה אחרת")
  expect_match(message, "n=2")
})

test_that("positions resolve after the download, naming the player behind a gap", {
  profiles <- data.frame(
    game_year = c(2025L, 2025L),
    basket_player_id = c(17485L, 17486L),
    position_name = c("רכז", "עמדה אחרת"),
    position_en = NA_character_,
    stringsAsFactors = FALSE
  )
  expect_error(resolve_profile_positions(profiles), "2025 player 17486")

  profiles$position_name[2] <- paste0("פורוורד", intToUtf8(0x05BE), "סנטר")
  expect_equal(resolve_profile_positions(profiles)$position_en, c("PG", "F-C"))
})

accumulate_row <- function(id, name, fg3 = "1/4") {
  paste0(
    "<tr><td>1</td><td><a href='player.asp?PlayerId=", id, "'>", name, "</a></td>",
    "<td>10</td><td>100</td><td>20</td><td>2/5</td><td>40%</td>",
    "<td>", fg3, "</td><td>25%</td><td>3/3</td><td>100%</td></tr>"
  )
}

test_that("a season scrape attaches English names from the lang=en pages", {
  html <- list(
    he = c(paste0("<table>", accumulate_row(1, "אלף"), accumulate_row(2, "בית"), "</table>"),
           "<a href='stats-accumulate.asp'>stats</a><table></table>"),
    # English page order differs; the join is by PlayerId, not position.
    en = c(paste0("<table>", accumulate_row(2, "Bet"), accumulate_row(1, "Alef"), "</table>"),
           "<table></table>")
  )
  fetch <- function(year, page, lang = "he") {
    list(url = paste0("u-", lang, page), content_md5 = strrep("a", 32),
         fetched_at = Sys.time(), cache_hit = TRUE, html = html[[lang]][page])
  }
  out <- scrape_basket_season(2026L, pause_seconds = 0, fetch_page = fetch)
  expect_equal(out$players$player_name, c("אלף", "בית"))
  expect_equal(out$players$player_name_en, c("Alef", "Bet"))
})

test_that("English pages that disagree with the Hebrew table stop the run", {
  players <- parse_basket_accumulate_html(
    paste0("<table>", accumulate_row(1, "אלף"), accumulate_row(2, "בית"), "</table>")
  )
  other_id <- parse_basket_accumulate_html(
    paste0("<table>", accumulate_row(1, "Alef"), accumulate_row(3, "Gimel"), "</table>")
  )
  other_total <- parse_basket_accumulate_html(
    paste0("<table>", accumulate_row(1, "Alef"), accumulate_row(2, "Bet", "2/4"), "</table>")
  )
  expect_error(merge_english_names(players, other_id, 2026L), "disagree on PlayerIds")
  expect_error(merge_english_names(players, other_total, 2026L), "disagree on fg3_made")
})

test_that("a page that fails to parse is deleted from the cache", {
  cache_file <- tempfile(fileext = ".html")
  writeLines("<html>Access denied</html>", cache_file)
  on.exit(unlink(cache_file), add = TRUE)
  page <- list(html = "<html>Access denied</html>", path = cache_file)

  expect_error(parse_page_or_discard(page, parse_basket_player_profile_html),
               "deleted cached file")
  expect_false(file.exists(cache_file))
})

test_that("an empty page without site markup is a bad response, not the season end", {
  cache_file <- tempfile(fileext = ".html")
  writeLines("<html>challenge</html>", cache_file)
  on.exit(unlink(cache_file), add = TRUE)
  fetch <- function(year, page, lang = "he") {
    html <- if (page == 1L) paste0("<table>", accumulate_row(1, "A"), "</table>") else "<html>challenge</html>"
    list(url = "u", content_md5 = strrep("a", 32), fetched_at = Sys.time(),
         cache_hit = TRUE, path = if (page == 1L) NULL else cache_file, html = html)
  }
  failures <- new_failure_log()
  out <- scrape_basket_season(2026L, pause_seconds = 0, fetch_page = fetch,
                              failures = failures)

  # It must not be read as the end of the season: it is recorded, the bad cache
  # file is dropped so the next run refetches it, and the write is blocked.
  expect_match(paste(failure_frame(failures)$reason, collapse = " "),
               "does not look like a basket.co.il stats page")
  expect_equal(out$players$basket_player_id, 1L)
  expect_error(assert_no_download_failures(failures), "refusing to write")
  expect_false(file.exists(cache_file))
})

# Translation used to happen inside this parse, so one unmapped position killed
# the download mid-run. The parse now carries the Hebrew through untouched.
test_that("an unknown position neither fails the parse nor drops the cache", {
  cache_file <- tempfile(fileext = ".html")
  html <- "<div>אזרחות: USA (USA)</div><div>עמדה: שחקן חדש גובה: 2.00</div>"
  writeLines(html, cache_file)
  on.exit(unlink(cache_file), add = TRUE)

  profile <- parse_page_or_discard(list(html = html, path = cache_file),
                                   parse_basket_player_profile_html)
  expect_equal(profile$position_name, "שחקן חדש")
  expect_true(is.na(profile$position_en))
  expect_true(file.exists(cache_file))
})

profile_row <- function(year, player_id) {
  data.frame(
    position_name = "רכז", position_en = NA_character_, height_m = 1.9,
    nationality_name = "Israel", nationality_code = "ISR",
    game_year = as.integer(year), basket_player_id = as.integer(player_id),
    source_url = "u", source_content_md5 = strrep("a", 32),
    fetched_at = Sys.time(), cache_hit = TRUE, stringsAsFactors = FALSE
  )
}

test_that("a profile that will not download is recorded and the run continues", {
  failures <- new_failure_log()
  fetch <- function(year, player_id) {
    if (player_id == 2L) stop("Request failed after 5 attempts: profile 2")
    profile_row(year, player_id)
  }
  players <- data.frame(game_year = rep(2026L, 3), basket_player_id = 1:3)

  out <- scrape_basket_profiles(players, pause_seconds = 0, fetch_profile = fetch,
                                failures = failures)

  expect_equal(out$basket_player_id, c(1L, 3L))
  recorded <- failure_frame(failures)
  expect_equal(nrow(recorded), 1L)
  expect_equal(recorded$reference, "player 2")
  expect_match(recorded$reason, "Request failed")
})

test_that("a stats page that will not download does not end the season", {
  failures <- new_failure_log()
  fetch <- function(year, page, lang = "he") {
    if (identical(lang, "he") && page == 2L) stop("Request failed after 5 attempts")
    html <- if (page <= 3L) {
      paste0("<table>", accumulate_row(page, if (identical(lang, "he")) "א" else "A"), "</table>")
    } else "<a href='stats-accumulate.asp'>stats</a><table></table>"
    list(url = paste0("u-", lang, page), content_md5 = strrep("a", 32),
         fetched_at = Sys.time(), cache_hit = TRUE, html = html)
  }

  out <- scrape_basket_season(2026L, pause_seconds = 0, fetch_page = fetch,
                              failures = failures)

  expect_equal(out$players$basket_player_id, c(1L, 3L))
  expect_equal(nrow(failure_frame(failures)), 1L)
})

test_that("a season whose pages all fail gives up before exhausting max_pages", {
  failures <- new_failure_log()
  attempts <- 0L
  fetch <- function(year, page, lang = "he") {
    attempts <<- attempts + 1L
    stop("Request failed after 5 attempts")
  }

  out <- scrape_basket_season(2026L, max_pages = 50L, pause_seconds = 0,
                              fetch_page = fetch, failures = failures)

  expect_null(out$players)
  expect_lte(attempts, 4L)
  expect_match(paste(failure_frame(failures)$reason, collapse = " "), "Request failed")
})

test_that("recorded download failures refuse the write", {
  failures <- new_failure_log()
  expect_null(failure_frame(failures))
  expect_silent(assert_no_download_failures(failures))

  record_failure(failures, "profile", 2026L, "player 42", "Request failed after 5 attempts")
  expect_error(assert_no_download_failures(failures), "refusing to write")
  expect_error(assert_no_download_failures(failures), "player 42")
})
