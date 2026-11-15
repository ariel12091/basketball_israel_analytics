# PREVIEW PROTOTYPE (react/logos, 2026-11-15) -- team logos and player
# headshots for the web app, written to web/src/data/images.json.
#
#   Rscript scripts/export_web_images.R
#
# Feasibility only, before the image rights are cleared: nothing is written to
# the database. If it goes ahead, the URLs move to their owners --
# season_teams.logo_url and basket_player_registration.photo_url (filled by
# scripts/basket_rosters.R, exposed through player_display_names_v) -- and this
# script and the JSON are deleted. Nothing here is typed by hand:
#
# - logos: basketball_test.schedule.team_icon_1/2, the team's most recent game
#   per season, keyed by the roster team id (schedule_team_dict).
# - photos, from two caches under exports/cache/basket_shooting_history/:
#   1. teams/<season>/<team>-he.html (scripts/basket_rosters.R; the current
#      season only): each roster card's photo, on that team;
#   2. profiles/<season>/<PlayerId>.html (the shooting-history backfill; every
#      season): the profile's player_img, on every team the person played for
#      that season.
#   The roster card wins where both exist. Each is keyed by (roster team id,
#   player id) through the Basket registration's verified identity and
#   player_display_names_v, which cover the app's seasons (2025-2027); 2022-23
#   profiles hold only the site placeholder.
#
# The URLs point at basket.co.il; nothing is downloaded or re-hosted.
#
#   IMAGES_OUT=<path>  write somewhere other than web/src/data/images.json

suppressPackageStartupMessages({
  library(DBI)
  library(jsonlite)
})

if (!exists("parse_basket_profile_photo")) source("scripts/basket_rosters.R")

readRenviron("etl/.Renviron")
con <- dbConnect(RPostgres::Postgres(),
  host = Sys.getenv("PG_HOST"), port = 5432L, dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
  sslmode = "require", bigint = "numeric")
invisible(dbExecute(con, "SET default_transaction_read_only = on"))

# --- logos --------------------------------------------------------------------
logos <- dbGetQuery(con, "
  WITH sides AS (
    SELECT game_year, game_date, id, team1 AS team_id_schedule, team_icon_1 AS icon
    FROM basketball_test.schedule
    UNION ALL
    SELECT game_year, game_date, id, team2, team_icon_2
    FROM basketball_test.schedule
  )
  SELECT DISTINCT ON (s.game_year, d.team_id_rosters)
         s.game_year, d.team_id_rosters AS team_id, s.icon
  FROM sides s
  JOIN basketball_test.schedule_team_dict d
    ON d.game_year = s.game_year AND d.team_id_schedule = s.team_id_schedule
  WHERE NULLIF(btrim(s.icon), '') IS NOT NULL
  ORDER BY s.game_year, d.team_id_rosters, s.game_date DESC NULLS LAST, s.id DESC")
logos$url <- basket_absolute_url(logos$icon)

teams <- dbGetQuery(con, "SELECT game_year, team_id FROM basketball_test.season_teams")
missing_logo <- merge(teams, logos, all.x = TRUE)
missing_logo <- missing_logo[is.na(missing_logo$url), c("game_year", "team_id")]
if (nrow(missing_logo)) {
  message("no logo for ", nrow(missing_logo), " season team(s): ",
          paste(sprintf("%d/%d", missing_logo$game_year, missing_logo$team_id), collapse = ", "))
}

# --- photos -------------------------------------------------------------------
read_page <- function(p) paste(readLines(p, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
cache_root <- file.path("exports", "cache", "basket_shooting_history")
pages <- list.files(file.path(cache_root, "teams"), pattern = "^[0-9]+-he\\.html$",
                    recursive = TRUE, full.names = TRUE)
photos <- do.call(rbind, lapply(pages, function(p) {
  out <- parse_basket_roster_photos(read_page(p))
  if (!nrow(out)) return(NULL)
  out$game_year <- as.integer(basename(dirname(p)))
  out$basket_team_id <- as.integer(sub("-he\\.html$", "", basename(p)))
  out
}))
if (is.null(photos)) photos <- data.frame(basket_player_id = integer(), photo_url = character(),
  game_year = integer(), basket_team_id = integer())

# The team comes from the page (the registration's basket_team_id is blank on
# stats-page rows); the registration gives the person, the view the player id.
team_map <- dbGetQuery(con, "
  SELECT game_year, team_id_schedule AS basket_team_id, team_id_rosters AS team_id
  FROM basketball_test.schedule_team_dict")
ids <- dbGetQuery(con, "
  SELECT r.game_year, r.basket_player_id, v.team_id, v.player_id
  FROM basketball_test.basket_player_registration r
  JOIN basketball_test.player_display_names_v v
    ON v.game_year = r.game_year AND v.identity_id = r.identity_id
  WHERE r.identity_match_status = 'verified'")
roster_mapped <- merge(merge(photos, team_map, by = c("game_year", "basket_team_id")),
                       ids, by = c("game_year", "basket_player_id", "team_id"))
message(sprintf("roster photos: %d on cached team pages, %d mapped to a (team, player)",
                nrow(photos), nrow(roster_mapped)))

# Profiles: no team on the page, so every (team, player) of that person.
profile_files <- list.files(file.path(cache_root, "profiles"), pattern = "^[0-9]+\\.html$",
                            recursive = TRUE, full.names = TRUE)
profiles <- data.frame(
  game_year = as.integer(basename(dirname(profile_files))),
  basket_player_id = as.integer(sub("\\.html$", "", basename(profile_files))),
  photo_url = vapply(profile_files, function(p) parse_basket_profile_photo(read_page(p)),
                     character(1), USE.NAMES = FALSE))
profiles <- profiles[!is.na(profiles$photo_url), , drop = FALSE]
profile_mapped <- merge(profiles, ids, by = c("game_year", "basket_player_id"))
message(sprintf("profile photos: %d real photos cached, %d mapped to a (team, player)",
                nrow(profiles), nrow(profile_mapped)))

cols <- c("game_year", "team_id", "player_id", "photo_url")
mapped <- rbind(roster_mapped[cols], profile_mapped[cols])  # roster first: it wins
mapped <- mapped[!duplicated(mapped[c("game_year", "team_id", "player_id")]), , drop = FALSE]
players <- dbGetQuery(con, "
  SELECT game_year, count(*) AS players FROM basketball_test.player_display_names_v
  WHERE display_name IS NOT NULL GROUP BY 1 ORDER BY 1")
players$with_photo <- vapply(players$game_year, function(y) sum(mapped$game_year == y), integer(1))
print(players, row.names = FALSE)
# (Not on.exit(): at top level it fires at once when the file is source()d.)
dbDisconnect(con)

# --- write --------------------------------------------------------------------
by_season <- function(df, key, value) {
  lapply(split(df, df$game_year), function(s) {
    s <- s[order(s[[key]]), , drop = FALSE]
    stats::setNames(as.list(s[[value]]), s[[key]])
  })
}
logos <- logos[!is.na(logos$url), , drop = FALSE]
mapped$key <- sprintf("%d:%d", as.integer(mapped$team_id), as.integer(mapped$player_id))

out <- list(
  note = "PREVIEW PROTOTYPE, generated by scripts/export_web_images.R; do not edit.",
  logos = by_season(logos, "team_id", "url"),
  photos = by_season(mapped, "key", "photo_url")
)
path <- Sys.getenv("IMAGES_OUT", file.path("web", "src", "data", "images.json"))
con_out <- file(path, open = "wb")
writeLines(toJSON(out, auto_unbox = TRUE, pretty = TRUE), con_out, sep = "\n")
close(con_out)
message("wrote ", path)
