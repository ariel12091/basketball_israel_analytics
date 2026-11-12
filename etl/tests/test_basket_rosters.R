library(testthat)

source(file.path("..", "..", "scripts", "basket_rosters.R"), local = TRUE)

# One roster entry as basket.co.il's team page prints it (2026-11-12,
# team.asp?TeamId=2112), Hebrew and English.
entry <- function(id, lang, num, name, desc_pos, height, dob) {
  paste0(
    '<a href="player.asp?PlayerId=', id, if (lang == "en") "&lang=en", '">',
    '<div class="box_role_data"><img src="x.jpg" alt="', name, '" /></div>',
    '<div class="box_role_data">',
    '<div class="box_role_data role_num ', lang, '">', num, '</div>',
    '<div class="box_role_data role_name ', lang, '">', sub(" ", "<br />", name), '</div>',
    '<div class="clear_fix"></div>',
    '<div class="box_role_data role_desc ', lang, '" style="margin-top: -8px;">',
    '<strong>', desc_pos, '&nbsp;|&nbsp;', height, '</strong><br />', dob, '</div></div></a>')
}

test_that("a roster entry gives jersey, name, position, height and date of birth", {
  he <- parse_basket_roster_html(paste0(
    "<div>", entry(26032, "he", 1, "יוג'ין ג'רמן", "גארד", "1.83", "02/12/1997"),
    entry(27214, "he", 21, "ברונו קאבוקלו", "סנטר", "2.08", "21/09/1995"),
    # The same player linked again elsewhere on the page (a stats table):
    '<a href="player.asp?PlayerId=26032">יוג\'ין ג\'רמן</a>', "</div>"), "he")
  expect_equal(he$basket_player_id, c(26032L, 27214L))
  expect_equal(he$jersey, c("1", "21"))
  expect_equal(he$player_name, c("יוג'ין ג'רמן", "ברונו קאבוקלו"))
  expect_equal(he$position_name, c("גארד", "סנטר"))
  expect_equal(he$height_m, c(1.83, 2.08))
  expect_equal(he$date_of_birth, as.Date(c("1997-12-02", "1995-09-21")))
})

test_that("line breaks inside an entry, as on the real page, still parse", {
  multi <- gsub("><", ">\r\n                <", entry(26053, "he", 7, "רומן סורקין", "סנטר", "2.08", "03/03/1996"), fixed = TRUE)
  r <- parse_basket_roster_html(multi, "he")
  expect_equal(nrow(r), 1L)
  expect_equal(r$height_m, 2.08)
  expect_equal(r$date_of_birth, as.Date("1996-03-03"))
})

test_that("the English page gives the English name and position code", {
  en <- parse_basket_roster_html(
    entry(26032, "en", 1, "Eugene German", "G", "1.83", "02/12/1997"), "en")
  expect_equal(en$player_name, "Eugene German")
  expect_equal(en$position_name, "G")
})

test_that("both languages join on the player id; Hebrew is the base", {
  he <- parse_basket_roster_html(paste0(
    entry(1, "he", 4, "א ב", "רכז", "1.80", "01/01/2000"),
    entry(2, "he", 5, "ג ד", "פורוורד-סנטר", "2.06", "02/02/2001")), "he")
  en <- parse_basket_roster_html(entry(1, "en", 4, "A B", "PG", "1.80", "01/01/2000"), "en")
  r <- merge_basket_roster(he, en, basket_team_id = 2112L, game_year = 2027L)
  expect_equal(r$basket_player_id, c(1L, 2L))
  expect_equal(r$player_name_en, c("A B", NA))
  # position_en: the English page's code, else the Hebrew word mapped.
  expect_equal(r$position_en, c("PG", "F-C"))
  expect_equal(r$basket_team_id, c(2112L, 2112L))
})

test_that("a page with no roster entries is an error, not an empty team", {
  expect_error(validate_basket_roster(data.frame(), 2112L), "no roster entries")
  bad <- parse_basket_roster_html(entry(1, "he", 4, "א ב", "רכז", "3.40", "01/01/2000"), "he")
  bad <- merge_basket_roster(bad, bad[0, ], basket_team_id = 2112L, game_year = 2027L)
  expect_error(validate_basket_roster(bad, 2112L), "height")
})

test_that("the season's team pages are read off the stats page's menu", {
  html <- paste0('<a href="team.asp?TeamId=2111">x</a><a href="team.asp?TeamId=2112">y</a>',
                 '<a href="team.asp?TeamId=2111">x</a>')
  expect_equal(basket_season_team_ids(html), c(2111L, 2112L))
  expect_error(basket_season_team_ids("<p></p>"), "no team pages")
})

test_that("both writers share one registration upsert; a stats row is never a roster row's to change", {
  stats_cols <- c("game_year", "basket_player_id", "player_name", "run_id")
  roster_cols <- c("game_year", "basket_player_id", "player_name", "height_m")
  stats <- registration_upsert_sql("stats", "stage_s", stats_cols)
  roster <- registration_upsert_sql("roster", "stage_r", roster_cols)
  expect_match(stats, "INSERT INTO basketball_test.basket_player_registration", fixed = TRUE)
  expect_match(stats, "ON CONFLICT (game_year, basket_player_id) DO UPDATE SET", fixed = TRUE)
  expect_match(stats, "source = EXCLUDED.source", fixed = TRUE)
  expect_match(stats, "'stats'", fixed = TRUE)
  expect_false(grepl("WHERE basket_player_registration.source", stats, fixed = TRUE))
  expect_match(roster, "'roster'", fixed = TRUE)
  expect_match(roster, "WHERE basket_player_registration.source = 'roster'", fixed = TRUE)
  expect_match(roster, "height_m = EXCLUDED.height_m", fixed = TRUE)
  # A stats row taking over a roster row drops the roster's facts, so the row
  # holds one source whichever writer ran first; the profile write refills them.
  expect_match(stats, "height_m = CASE WHEN basket_player_registration.source = 'roster' THEN NULL ELSE basket_player_registration.height_m END", fixed = TRUE)
  expect_match(stats, "jersey = CASE WHEN", fixed = TRUE)
  expect_false(grepl("CASE WHEN", roster, fixed = TRUE))
  # The matcher owns the link; no writer touches it.
  expect_false(any(grepl("identity_", c(stats, roster), fixed = TRUE)))
  expect_error(registration_upsert_sql("stats", "s", c("game_year", "identity_id")), "identity")
  expect_error(registration_upsert_sql("other", "s", stats_cols))
})

test_that("a team's Basket spelling comes from its players' stats rows, or stays NA", {
  rows <- data.frame(basket_team_id = c(2111L, 2111L, 2111L, 2112L),
                     basket_player_id = c(1L, 2L, 3L, 9L))
  stats <- data.frame(basket_player_id = c(1L, 2L, 3L),
                      basket_team_name = c("מכבי", "מכבי", "הפועל"))
  out <- roster_team_names(rows, stats)
  expect_equal(out$basket_team_name, c("מכבי", "מכבי", "מכבי", NA))
  # A new season before any stats page: every name NA, no error.
  expect_true(all(is.na(roster_team_names(rows, stats[0, ])$basket_team_name)))
})

test_that("a season's roster stops before writing on a player listed twice or a position with no code", {
  rows <- data.frame(game_year = 2027L, basket_player_id = c(1L, 2L, 1L),
                     basket_team_id = c(2111L, 2111L, 2112L), player_name = c("א", "ב", "א"),
                     position_name = "רכז", position_en = "PG", stringsAsFactors = FALSE)
  expect_error(validate_season_roster(rows), "player 1 .*2111.*2112")
  ok <- rows[1:2, ]
  expect_silent(validate_season_roster(ok))
  ok$position_en[2] <- NA
  expect_error(validate_season_roster(ok), "no English code")
})

test_that("a stats write without profiles may not take over roster rows", {
  expect_error(assert_profiles_for_takeover(with_profiles = FALSE, roster_rows = 3L), "--with-profiles")
  expect_silent(assert_profiles_for_takeover(with_profiles = FALSE, roster_rows = 0L))
  expect_silent(assert_profiles_for_takeover(with_profiles = TRUE, roster_rows = 3L))
})

test_that("a failed roster step does not stop the identity link or the name rewrite", {
  wf <- readLines(file.path("..", "..", ".github", "workflows", "basket-refresh.yml"), warn = FALSE)
  at <- grep("name: Scrape and write team rosters", wf, fixed = TRUE)
  expect_length(at, 1)
  step <- wf[at:(at + 4)]
  expect_true(any(grepl("continue-on-error: true", step, fixed = TRUE)))
})
