# basket.co.il team rosters: position, height and date of birth for every
# registered player of the current season, including players the season-stat
# pages (stats-accumulate.asp) do not list yet -- mostly mid-season arrivals
# (2026-11-12: 54 of 2026-27's 234 players had no height without them).
#
#   Rscript scripts/basket_rosters.R --season=2027 [--write]
#
# The season's team pages are read off the stats page's menu (the scraper's
# page 1, usually cached), then each team page is fetched in Hebrew and in
# English -- politely: one request at a time, --pause-seconds (default 5)
# between real downloads, the scraper's own user agent, every page cached
# under <cache-dir>/teams/<season>/. The menu lists the CURRENT season's teams,
# so run this for the current season only.
#
# Safe default: fetch, parse and validate only. --write upserts into
# basketball_test.basket_player_registration as source = 'roster' (never
# deletes, never touches a player the stats pages list) with each team's
# Basket spelling, which etl/basket_identity_match.R then needs.

if (!exists("BASKET_POSITION_EN")) local({
  here <- "scripts"
  for (f in rev(sys.frames())) if (!is.null(f$ofile)) {
    here <- dirname(f$ofile)
    break
  }
  source(file.path(here, "backfill_basket_shooting_history.R"), local = parent.frame(2))
})


basket_team_url <- function(team_id, lang = "he") {
  paste0("https://basket.co.il/team.asp?TeamId=", as.integer(team_id),
         if (identical(lang, "en")) "&lang=en" else "")
}

team_cache_path <- function(cache_dir, game_year, team_id, lang) {
  file.path(cache_dir, "teams", as.character(as.integer(game_year)),
            sprintf("%d-%s.html", as.integer(team_id), lang))
}

# The season's team pages, from the site menu every page carries.
basket_season_team_ids <- function(html) {
  hits <- regmatches(html, gregexpr("team\\.asp\\?TeamId=([0-9]+)", html, perl = TRUE))[[1]]
  ids <- sort(unique(as.integer(sub(".*TeamId=", "", hits))))
  if (!length(ids)) stop("no team pages linked from the stats page")
  ids
}

decode_html_text <- function(x) {
  x <- gsub("<br\\s*/?>", " ", x, perl = TRUE, ignore.case = TRUE)
  x <- gsub("<[^>]+>", " ", x, perl = TRUE)
  x <- gsub("&nbsp;", " ", x, fixed = TRUE)
  x <- gsub("&#39;", "'", x, fixed = TRUE)
  x <- gsub("&quot;", "\"", x, fixed = TRUE)
  x <- gsub("&amp;", "&", x, fixed = TRUE)
  trimws(gsub("\\s+", " ", x, perl = TRUE))
}

div_text <- function(block, class) {
  m <- regmatches(block, regexec(
    paste0('(?s)<div class="box_role_data ', class, '[^"]*"[^>]*>(.*?)</div>'), block, perl = TRUE))[[1]]
  if (length(m) < 2) NA_character_ else decode_html_text(m[2])
}

# One row per roster entry: links to a player that carry the role_name block.
# The same player linked elsewhere on the page (stats tables) is skipped.
parse_basket_roster_html <- function(html, lang = c("he", "en")) {
  lang <- match.arg(lang)
  # (?s): the real page breaks lines inside every entry.
  pattern <- '(?s)<a href="player\\.asp\\?PlayerId=([0-9]+)[^"]*">((?:(?!</a>).)*?role_name(?:(?!</a>).)*?)</a>'
  hits <- regmatches(html, gregexpr(pattern, html, perl = TRUE))[[1]]
  empty <- data.frame(basket_player_id = integer(), jersey = character(),
                      player_name = character(), position_name = character(),
                      height_m = numeric(), date_of_birth = as.Date(character()),
                      stringsAsFactors = FALSE)
  if (!length(hits)) return(empty)
  rows <- lapply(hits, function(a) {
    id <- as.integer(sub('(?s)^<a href="player\\.asp\\?PlayerId=([0-9]+).*$', "\\1", a, perl = TRUE))
    desc_raw <- regmatches(a, regexec('(?s)role_desc[^"]*"[^>]*>(.*?)</div>', a, perl = TRUE))[[1]]
    strong <- regmatches(a, regexec("(?s)<strong>(.*?)</strong>", a, perl = TRUE))[[1]]
    pos_h <- if (length(strong) > 1) strsplit(decode_html_text(strong[2]), "\\s*\\|\\s*")[[1]] else c(NA, NA)
    dob <- if (length(desc_raw) > 1) regmatches(desc_raw[2], regexpr("[0-9]{2}/[0-9]{2}/[0-9]{4}", desc_raw[2])) else character()
    data.frame(
      basket_player_id = id,
      jersey = div_text(a, "role_num"),
      player_name = div_text(a, "role_name"),
      position_name = if (length(pos_h) >= 1) pos_h[1] else NA_character_,
      height_m = suppressWarnings(as.numeric(if (length(pos_h) >= 2) pos_h[2] else NA)),
      date_of_birth = if (length(dob)) as.Date(dob, "%d/%m/%Y") else as.Date(NA),
      stringsAsFactors = FALSE
    )
  })
  out <- do.call(rbind, rows)
  out[!duplicated(out$basket_player_id), , drop = FALSE]
}

# Hebrew page as the base, English name and position code joined on the id;
# a player on the Hebrew page only gets his Hebrew position mapped.
merge_basket_roster <- function(he, en, basket_team_id, game_year) {
  en_small <- if (nrow(en)) data.frame(basket_player_id = en$basket_player_id,
    player_name_en = en$player_name, position_code = en$position_name,
    stringsAsFactors = FALSE) else data.frame(basket_player_id = integer(),
    player_name_en = character(), position_code = character())
  r <- merge(he, en_small, by = "basket_player_id", all.x = TRUE, sort = TRUE)
  mapped <- if (nrow(r)) map_position(r$position_name) else character()
  data.frame(
    game_year = rep(as.integer(game_year), nrow(r)),
    basket_player_id = r$basket_player_id,
    basket_team_id = rep(as.integer(basket_team_id), nrow(r)),
    jersey = r$jersey,
    player_name = r$player_name,
    player_name_en = r$player_name_en,
    position_name = r$position_name,
    position_en = ifelse(is.na(r$position_code), mapped, r$position_code),
    height_m = r$height_m,
    date_of_birth = r$date_of_birth,
    stringsAsFactors = FALSE
  )
}

validate_basket_roster <- function(r, team_id) {
  if (!nrow(r)) stop(sprintf("team %d: no roster entries on the page", team_id))
  if (anyDuplicated(r$basket_player_id)) stop(sprintf("team %d: a player listed twice", team_id))
  if (any(is.na(r$player_name) | !nzchar(r$player_name))) stop(sprintf("team %d: a roster entry without a name", team_id))
  h <- r$height_m[!is.na(r$height_m)]
  if (length(h) && (min(h) < 1.5 || max(h) > 2.4))
    stop(sprintf("team %d: height out of range (%.2f-%.2f m)", team_id, min(h), max(h)))
  invisible(TRUE)
}

# The whole season's rows, before anything is written: a player on two team
# pages would make the upsert touch one key twice and fail the transaction,
# and a position with no English code would fail the table's CHECK. Both stop
# here, naming what to look at.
validate_season_roster <- function(rows) {
  key <- paste(rows$game_year, rows$basket_player_id)
  twice <- unique(key[duplicated(key)])
  if (length(twice)) {
    detail <- vapply(twice, function(k) sprintf("player %s on teams %s",
      rows$basket_player_id[key == k][1], paste(rows$basket_team_id[key == k], collapse = ", ")),
      character(1))
    stop("a player is listed on more than one team page: ", paste(detail, collapse = "; "))
  }
  missing <- !is.na(rows$position_name) & is.na(rows$position_en)
  if (any(missing)) {
    stop("positions with no English code (add them to BASKET_POSITION_EN): ",
         paste(unique(rows$position_name[missing]), collapse = ", "))
  }
  invisible(TRUE)
}

fetch_team_page <- function(game_year, team_id, lang, cache_dir, refresh_cache,
                            pause_seconds, max_attempts, initial_backoff) {
  url <- basket_team_url(team_id, lang)
  response <- download_with_cache(url, team_cache_path(cache_dir, game_year, team_id, lang),
    refresh_cache = refresh_cache, max_attempts = max_attempts,
    initial_backoff = initial_backoff)
  # Polite: a pause after every real download, none after a cache hit.
  if (!response$cache_hit) Sys.sleep(pause_seconds)
  list(url = url, md5 = unname(tools::md5sum(response$path)), fetched_at = response$fetched_at,
       cache_hit = response$cache_hit, html = read_downloaded_html(response$path))
}

# Each team's name as the season-stat pages spell it -- the identity matcher
# maps that spelling through schedule_team_dict -- taken from the players a
# team page and the stats pages both list (most common spelling). NA when the
# team shares no player with the stats pages, e.g. a new season before its
# first stats page; the matcher then reports those rows unmatched.
roster_team_names <- function(rows, stats) {
  shared <- merge(rows[c("basket_team_id", "basket_player_id")],
                  stats[c("basket_player_id", "basket_team_name")], by = "basket_player_id")
  pick <- vapply(split(shared$basket_team_name, shared$basket_team_id), function(x) {
    counts <- table(x)
    names(counts)[which.max(counts)]
  }, character(1))
  rows$basket_team_name <- unname(pick[as.character(rows$basket_team_id)])
  rows
}

ROSTER_REGISTRATION_COLUMNS <- c("game_year", "basket_player_id", "player_name",
  "player_name_en", "basket_team_name", "basket_team_id", "jersey", "position_name",
  "position_en", "height_m", "date_of_birth", "source_url", "source_content_md5", "fetched_at")

# Upserts the roster rows into basket_player_registration as source = 'roster'.
# A player the stats pages list keeps his stats row untouched.
write_basket_rosters <- function(con, rows, game_year) {
  DBI::dbWithTransaction(con, {
    DBI::dbExecute(con, "SET LOCAL statement_timeout = '120s'")
    stats <- DBI::dbGetQuery(con, paste(
      "SELECT basket_player_id, basket_team_name FROM", BASKET_REGISTRATION_TABLE,
      "WHERE game_year = $1 AND source = 'stats' AND basket_team_name IS NOT NULL"),
      params = list(as.integer(game_year)))
    rows <- roster_team_names(rows, stats)
    DBI::dbExecute(con, paste0("CREATE TEMP TABLE basket_roster_stage (LIKE ", BASKET_REGISTRATION_TABLE,
                               " INCLUDING DEFAULTS) ON COMMIT DROP"))
    DBI::dbAppendTable(con, DBI::Id(table = "basket_roster_stage"), rows[ROSTER_REGISTRATION_COLUMNS])
    upserted <- DBI::dbExecute(con, registration_upsert_sql("roster", "basket_roster_stage",
                                                            ROSTER_REGISTRATION_COLUMNS))
    check <- DBI::dbGetQuery(con, paste(
      "SELECT count(*) FILTER (WHERE r.source = 'roster') AS roster_rows,",
      "count(*) FILTER (WHERE r.source = 'stats') AS stats_rows,",
      "count(*) FILTER (WHERE r.source = 'roster' AND r.basket_team_name IS NULL) AS no_team_name,",
      "count(*) FILTER (WHERE r.source = 'roster' AND r.height_m IS DISTINCT FROM s.height_m) AS height_diffs",
      "FROM", BASKET_REGISTRATION_TABLE, "r JOIN basket_roster_stage s USING (game_year, basket_player_id)"))
    if (check$roster_rows + check$stats_rows != nrow(rows)) {
      stop("read back ", check$roster_rows + check$stats_rows, " registrations for ", nrow(rows), " parsed")
    }
    if (check$roster_rows != upserted) stop("upserted ", upserted, " rows but ", check$roster_rows, " are roster rows")
    if (check$height_diffs > 0) stop(check$height_diffs, " roster rows read back with a different height")
    if (check$no_team_name > 0) {
      warning(check$no_team_name, " roster rows have no Basket team spelling (their team shares no ",
              "player with the stats pages); the identity matcher will report them unmatched")
    }
    sprintf("season %d: %d team-page players; %d written as roster rows, %d already stats rows",
            game_year, nrow(rows), check$roster_rows, check$stats_rows)
  })
}

main_rosters <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  known <- grepl("^--(?:season|pause-seconds|cache-dir|max-attempts|initial-backoff)=", args, perl = TRUE) |
    args %in% c("--write", "--refresh-cache")
  if (any(!known)) stop("Unknown argument(s): ", paste(args[!known], collapse = ", "))
  season <- as.integer(option_value(args, "season", NA))
  if (is.na(season)) stop("--season=YYYY is required (the current season)")
  pause_seconds <- as.numeric(option_value(args, "pause-seconds", "5"))
  cache_dir <- option_value(args, "cache-dir", "exports/cache/basket_shooting_history")
  max_attempts <- as.integer(option_value(args, "max-attempts", "5"))
  initial_backoff <- as.numeric(option_value(args, "initial-backoff", "15"))
  refresh_cache <- "--refresh-cache" %in% args
  write <- "--write" %in% args

  menu <- fetch_basket_page(season, 1L, cache_dir = cache_dir, refresh_cache = FALSE,
                            max_attempts = max_attempts, initial_backoff = initial_backoff)
  team_ids <- basket_season_team_ids(menu$html)
  cat(sprintf("season %d: %d team pages (%s)\n", season, length(team_ids), paste(range(team_ids), collapse = "-")))
  downloads <- 0L
  rows <- do.call(rbind, lapply(team_ids, function(id) {
    he <- fetch_team_page(season, id, "he", cache_dir, refresh_cache, pause_seconds, max_attempts, initial_backoff)
    en <- fetch_team_page(season, id, "en", cache_dir, refresh_cache, pause_seconds, max_attempts, initial_backoff)
    downloads <<- downloads + !he$cache_hit + !en$cache_hit
    r <- merge_basket_roster(parse_basket_roster_html(he$html, "he"),
                             parse_basket_roster_html(en$html, "en"), id, season)
    validate_basket_roster(r, id)
    r$source_url <- he$url
    r$source_content_md5 <- he$md5
    r$fetched_at <- as.POSIXct(he$fetched_at)
    r
  }))
  cat(sprintf("downloads: %d (others from cache); roster rows: %d; with a height: %d; with an English name: %d\n",
              downloads, nrow(rows), sum(!is.na(rows$height_m)), sum(!is.na(rows$player_name_en))))
  validate_season_roster(rows)
  if (!write) {
    cat("Dry run only; pass --write to upsert into", BASKET_REGISTRATION_TABLE, "\n")
    return(invisible(rows))
  }
  con <- connect_etl_db()
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  cat(write_basket_rosters(con, rows, season), "\n")
  invisible(rows)
}

if (sys.nframe() == 0L) main_rosters()
