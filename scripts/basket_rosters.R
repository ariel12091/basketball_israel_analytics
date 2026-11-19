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

# Each roster entry's headshot URL, from the same entries as
# parse_basket_roster_html(): the <img> inside the card's box_role_data div.
# Relative paths are made absolute against basket.co.il; the site's
# placeholder (img/default_player.jpg) counts as no photo.
parse_basket_roster_photos <- function(html) {
  pattern <- '(?s)<a href="player\\.asp\\?PlayerId=([0-9]+)[^"]*">((?:(?!</a>).)*?role_name(?:(?!</a>).)*?)</a>'
  hits <- regmatches(html, gregexpr(pattern, html, perl = TRUE))[[1]]
  empty <- data.frame(basket_player_id = integer(), photo_url = character(),
                      stringsAsFactors = FALSE)
  if (!length(hits)) return(empty)
  id <- as.integer(sub('(?s)^<a href="player\\.asp\\?PlayerId=([0-9]+).*$', "\\1", hits, perl = TRUE))
  src <- vapply(hits, function(a) {
    m <- regmatches(a, regexec('(?s)box_role_data"[^>]*>\\s*<img[^>]*\\ssrc="([^"]+)"', a, perl = TRUE))[[1]]
    if (length(m) < 2) NA_character_ else m[2]
  }, character(1), USE.NAMES = FALSE)
  src[grepl("default_player", src, fixed = TRUE)] <- NA_character_
  out <- data.frame(basket_player_id = id, photo_url = basket_absolute_url(src),
                    stringsAsFactors = FALSE)
  out <- out[!is.na(out$photo_url), , drop = FALSE]
  out[!duplicated(out$basket_player_id), , drop = FALSE]
}

# A player profile page's headshot (player.asp?PlayerId=N, cached by the
# shooting-history backfill under profiles/<season>/<PlayerId>.html): the
# <img> in its player_img block, the photo of that season's player record.
# NA when the block is missing or holds the site placeholder.
parse_basket_profile_photo <- function(html) {
  m <- regmatches(html, regexec(
    '(?s)class="player_img"[^>]*>\\s*<img[^>]*\\ssrc="([^"]+)"', html, perl = TRUE))[[1]]
  if (length(m) < 2 || grepl("default_player", m[2], fixed = TRUE)) return(NA_character_)
  basket_absolute_url(m[2])
}

# basket.co.il serves its images under absolute, root-relative and
# page-relative paths (logos in schedule, photos on team pages); "..//pics"
# (news menus) resolves to the root as well.
basket_absolute_url <- function(x) {
  x <- sub("^(\\.\\./)+/*", "/", trimws(x), perl = TRUE)
  out <- x
  root <- !is.na(x) & startsWith(x, "/")
  rel <- !is.na(x) & !grepl("^https?://", x) & !root
  out[root] <- paste0("https://basket.co.il", x[root])
  out[rel] <- paste0("https://basket.co.il/", x[rel])
  out[!is.na(out) & !nzchar(out)] <- NA_character_
  # One spelling: www. redirects (301) to the bare host, and
  # "basket.co.il//pics" (2025 logos) collapses to one slash.
  out <- sub("^https?://(www\\.)?basket\\.co\\.il", "https://basket.co.il", out, perl = TRUE)
  sub("^(https?://[^/]+)//+", "\\1/", out, perl = TRUE)
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

# Profile-page links of the season's team-page-only players: their other
# registrations, which etl/basket_person_ids.R joins into one person. Only the
# links are kept -- a roster row's facts stay the team page's. A failed fetch
# is skipped with a warning; that row's links stay as they were.
roster_profile_links <- function(keys, pause_seconds = 5,
                                 fetch_profile = fetch_basket_player_profile) {
  profiles <- scrape_basket_profiles(keys, pause_seconds = pause_seconds,
                                     fetch_profile = fetch_profile)
  if (is.null(profiles)) profiles <- data.frame(game_year = integer(), basket_player_id = integer(),
                                                linked_basket_player_ids = character())
  missed <- setdiff(paste(keys$game_year, keys$basket_player_id),
                    paste(profiles$game_year, profiles$basket_player_id))
  if (length(missed)) {
    warning("no profile for ", length(missed), " team-page players (", paste(missed, collapse = ", "),
            "); their links stay as they were")
  }
  profiles[c("game_year", "basket_player_id", "linked_basket_player_ids")]
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
    # The roster columns' types and nothing else: LIKE would copy NOT NULL
    # source, which the upsert itself supplies.
    DBI::dbExecute(con, paste0("CREATE TEMP TABLE basket_roster_stage ON COMMIT DROP AS SELECT ",
                               paste(ROSTER_REGISTRATION_COLUMNS, collapse = ", "), " FROM ",
                               BASKET_REGISTRATION_TABLE, " WITH NO DATA"))
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

# Headshots for basket_player_registration.photo_url (2026-11-19), read off
# the cache the two scrapers fill: each team page's roster cards
# (teams/<season>/<team>-he.html) and each profile page
# (profiles/<season>/<PlayerId>.html). One photo per registration: a roster
# card first (the card on the player's own team page when he has two), else
# the profile's. Only found photos are written, so a photo the site stops
# showing is kept.
read_cached_page <- function(path) paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")

basket_cached_photos <- function(cache_dir, seasons) {
  rows <- list()
  for (season in as.integer(seasons)) {
    teams <- list.files(file.path(cache_dir, "teams", season), pattern = "^[0-9]+-he\\.html$",
                        full.names = TRUE)
    for (f in teams) {
      out <- parse_basket_roster_photos(read_cached_page(f))
      if (nrow(out)) rows[[length(rows) + 1]] <- data.frame(
        game_year = season, basket_player_id = out$basket_player_id,
        basket_team_id = as.integer(sub("-he\\.html$", "", basename(f))),
        photo_url = out$photo_url, photo_source = "roster", stringsAsFactors = FALSE)
    }
    profiles <- list.files(file.path(cache_dir, "profiles", season), pattern = "^[0-9]+\\.html$",
                           full.names = TRUE)
    if (length(profiles)) {
      url <- vapply(profiles, function(f) parse_basket_profile_photo(read_cached_page(f)),
                    character(1), USE.NAMES = FALSE)
      keep <- !is.na(url)
      if (any(keep)) rows[[length(rows) + 1]] <- data.frame(
        game_year = season,
        basket_player_id = as.integer(sub("\\.html$", "", basename(profiles[keep]))),
        basket_team_id = NA_integer_, photo_url = url[keep], photo_source = "profile",
        stringsAsFactors = FALSE)
    }
  }
  if (!length(rows)) return(data.frame(game_year = integer(), basket_player_id = integer(),
    basket_team_id = integer(), photo_url = character(), photo_source = character()))
  do.call(rbind, rows)
}

# The photo each registration gets from a staged basket_photo_stage: one per
# (game_year, basket_player_id), in the order described above.
BASKET_PHOTO_PICK_SQL <- paste(
  "SELECT DISTINCT ON (s.game_year, s.basket_player_id) s.game_year, s.basket_player_id, s.photo_url",
  "FROM basket_photo_stage s",
  "JOIN", BASKET_REGISTRATION_TABLE, "r USING (game_year, basket_player_id)",
  "ORDER BY s.game_year, s.basket_player_id, (s.photo_source = 'roster') DESC,",
  "(s.basket_team_id IS NOT DISTINCT FROM r.basket_team_id) DESC, s.basket_team_id")

# Inside the caller's transaction; returns the number of registrations changed.
write_basket_photos <- function(con, photos) {
  DBI::dbExecute(con, paste("CREATE TEMP TABLE basket_photo_stage (game_year int4, basket_player_id int4,",
                            "basket_team_id int4, photo_url text, photo_source text) ON COMMIT DROP"))
  if (nrow(photos)) DBI::dbAppendTable(con, DBI::Id(table = "basket_photo_stage"),
                                       photos[c("game_year", "basket_player_id", "basket_team_id",
                                                "photo_url", "photo_source")])
  DBI::dbExecute(con, paste0(
    "UPDATE ", BASKET_REGISTRATION_TABLE, " r SET photo_url = p.photo_url, updated_at = now() ",
    "FROM (", BASKET_PHOTO_PICK_SQL, ") p ",
    "WHERE r.game_year = p.game_year AND r.basket_player_id = p.basket_player_id ",
    "AND r.photo_url IS DISTINCT FROM p.photo_url"))
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

  # Profile links of the team-page-only players, one polite request each.
  keys <- DBI::dbGetQuery(con, paste("SELECT game_year, basket_player_id FROM", BASKET_REGISTRATION_TABLE,
                                     "WHERE game_year = $1 AND source = 'roster' ORDER BY basket_player_id"),
                          params = list(season))
  cat(sprintf("profile links: %d team-page players, %.0f s apart\n", nrow(keys), pause_seconds))
  fetch <- function(game_year, basket_player_id) fetch_basket_player_profile(
    game_year, basket_player_id, cache_dir = cache_dir, refresh_cache = refresh_cache,
    max_attempts = max_attempts, initial_backoff = initial_backoff)
  links <- roster_profile_links(keys, pause_seconds = pause_seconds, fetch_profile = fetch)
  written <- DBI::dbWithTransaction(con, write_basket_links(con, links))
  cat(sprintf("profile links written for %d team-page players\n", written))
  # Headshots from the pages just fetched (team pages) and the profiles the
  # shooting-history scrape cached before this step (basket-refresh.yml).
  photos <- basket_cached_photos(cache_dir, season)
  set <- DBI::dbWithTransaction(con, write_basket_photos(con, photos))
  cat(sprintf("photos: %d cached (%d team cards), %d registrations changed\n",
              nrow(photos), sum(photos$photo_source == "roster"), set))
  invisible(rows)
}

if (sys.nframe() == 0L) main_rosters()
