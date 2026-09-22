# Scrape basket.co.il accumulated player shooting totals by season.
#
# Safe default: fetch, parse and validate only. Pass --write to upsert into the
# tables created by 2026-09-22_basket_shooting_history.sql.

BASKET_SHOOTING_PARSER_VERSION <- "basket-accumulate-v3"
BASKET_SHOOTING_BASE_URL <- "https://basket.co.il/stats-accumulate.asp"
BASKET_SHOOTING_USER_AGENT <- paste(
  "on_off_israel_pbp/1.0",
  "(historical basketball statistics research; low-rate cached backfill)"
)

# Hebrew profile position -> the abbreviation basket.co.il's English profile
# prints for the same player. Verified 2026-09-22 against one English profile
# per label (e.g. PlayerId 12734: רכז / PG). An unlisted label stops the run.
BASKET_POSITION_EN <- c(
  "גארד" = "G", "רכז" = "PG", "גארד-פורוורד" = "G-F", "ס.פורוורד" = "SF",
  "פ.פורוורד" = "PF", "פ. פורוורד" = "PF", "פורוורד" = "F",
  "פורוורד-סנטר" = "F-C", "פורוורד/סנטר" = "F-C", "סנטר" = "C"
)

# The site writes one compound position with several separators that all render
# as a hyphen: ASCII "-", "/", and the Hebrew maqaf U+05BE. Folding them before
# the lookup keeps punctuation out of the table while an unrecognised *word*
# still fails closed.
BASKET_POSITION_SEPARATORS <- c(
  "\u05be",  # Hebrew maqaf, the one that stopped a run
  "\u2010", "\u2011", "\u2012",
  "\u2013", "\u2014"
)

normalize_position_name <- function(position_name) {
  out <- position_name
  for (separator in BASKET_POSITION_SEPARATORS) {
    out <- gsub(separator, "-", out, fixed = TRUE)
  }
  out <- gsub("[[:space:]]+", " ", out)
  trimws(gsub(" *- *", "-", out))
}

map_position <- function(position_name) {
  unname(BASKET_POSITION_EN[normalize_position_name(position_name)])
}

# Name every distinct unknown, not just the first, and print its code points:
# the maqaf that stopped a 30-minute run was invisible in the error text.
describe_positions <- function(values) {
  counts <- table(values)
  paste(vapply(names(counts), function(value) {
    sprintf("'%s' (n=%d, %s)", value, counts[[value]],
            paste(sprintf("U+%04X", utf8ToInt(value)), collapse = " "))
  }, character(1)), collapse = "; ")
}

unknown_position_error <- function(detail) {
  structure(class = c("basket_unknown_position", "error", "condition"), list(
    message = paste0("No English position for ", detail,
                     "; verify each on an English profile and add it to ",
                     "BASKET_POSITION_EN"),
    call = NULL
  ))
}

translate_position <- function(position_name) {
  out <- map_position(position_name)
  unknown <- !is.na(position_name) & is.na(out)
  if (any(unknown)) stop(unknown_position_error(describe_positions(position_name[unknown])))
  out
}

# Resolution runs once, after every page is on disk, so a lookup gap can never
# interrupt the download. Naming the season and a player id makes the offending
# profile reachable without grepping the cache.
resolve_profile_positions <- function(profiles) {
  if (is.null(profiles) || !nrow(profiles)) return(profiles)
  mapped <- map_position(profiles$position_name)
  unknown <- !is.na(profiles$position_name) & is.na(mapped)
  if (any(unknown)) {
    values <- profiles$position_name[unknown]
    detail <- vapply(sort(unique(values)), function(value) {
      first <- which(unknown & profiles$position_name == value)[1]
      sprintf("%s [%s player %s]", describe_positions(values[values == value]),
              profiles$game_year[first], profiles$basket_player_id[first])
    }, character(1))
    stop(unknown_position_error(paste(detail, collapse = "; ")))
  }
  profiles$position_en <- mapped
  profiles
}

# Parse a fetched page; if that fails, delete its cache file so a bad response
# (an error or anti-bot page served as 200) is downloaded again next run instead
# of failing forever.
parse_page_or_discard <- function(page, parse) {
  tryCatch(parse(page$html), error = function(e) {
    discarded <- !is.null(page$path) && file.exists(page$path) && unlink(page$path) == 0L
    stop(conditionMessage(e), if (discarded) {
      paste0(" [deleted cached file ", page$path, "; rerun to download it again]")
    }, call. = FALSE)
  })
}

# A real past-the-end page is a full site page with no player rows; a page with
# no rows and none of the site's stats markup is a bad response, not the end.
parse_accumulate_page <- function(html) {
  rows <- parse_basket_accumulate_html(html)
  if (!nrow(rows) && !grepl("stats-accumulate", html, fixed = TRUE)) {
    stop("Page has no player rows and does not look like a basket.co.il stats page")
  }
  rows
}

# Anything that goes wrong while fetching is collected here instead of ending
# the run: the download always reaches the end, and the collected entries then
# decide whether the write may proceed.
new_failure_log <- function() {
  log <- new.env(parent = emptyenv())
  log$entries <- list()
  log
}

record_failure <- function(log, kind, game_year, reference, reason) {
  if (is.null(log)) return(invisible(NULL))
  log$entries[[length(log$entries) + 1L]] <- data.frame(
    kind = kind, game_year = as.integer(game_year),
    reference = as.character(reference),
    reason = trimws(substr(gsub("[\r\n]+", " ", reason), 1L, 300L)),
    stringsAsFactors = FALSE
  )
  invisible(NULL)
}

failure_frame <- function(log) {
  if (is.null(log) || !length(log$entries)) return(NULL)
  do.call(rbind, log$entries)
}

failure_lines <- function(log) {
  frame <- failure_frame(log)
  if (is.null(frame)) return("download failures: none")
  c(sprintf("download failures: %d", nrow(frame)),
    sprintf("  [%s] %s %s: %s", frame$kind, frame$game_year, frame$reference,
            frame$reason))
}

assert_no_download_failures <- function(log) {
  frame <- failure_frame(log)
  if (is.null(frame)) return(invisible(NULL))
  stop(sprintf(
    "%d download failure(s); refusing to write. The cache keeps everything that did download, so a rerun resumes from it:\n%s",
    nrow(frame), paste(failure_lines(log)[-1], collapse = "\n")
  ), call. = FALSE)
}

format_bytes <- function(bytes) {
  units <- c("B", "KB", "MB", "GB", "TB")
  index <- 1L
  bytes <- as.numeric(bytes)
  while (bytes >= 1024 && index < length(units)) {
    bytes <- bytes / 1024
    index <- index + 1L
  }
  sprintf("%.1f %s", bytes, units[index])
}

new_sanity_monitor <- function(cache_dir, checkpoint_requests = 100L,
                               stop_after_checkpoint = FALSE) {
  dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
  report_path <- file.path(
    cache_dir,
    paste0("sanity-", format(Sys.time(), "%Y%m%d-%H%M%S"), ".log")
  )
  monitor <- new.env(parent = emptyenv())
  monitor$cache_dir <- cache_dir
  monitor$report_path <- report_path
  monitor$checkpoint_requests <- checkpoint_requests
  monitor$checkpoint_emitted <- FALSE
  monitor$stop_after_checkpoint <- stop_after_checkpoint
  monitor$network_attempts <- 0L
  monitor$network_successes <- 0L
  monitor$cache_hits <- 0L
  monitor$downloaded_bytes <- 0
  monitor$accumulate_pages <- 0L
  monitor$shooting_rows <- 0L
  monitor$english_names <- 0L
  monitor$fg2_attempted <- 0
  monitor$fg3_attempted <- 0
  monitor$ft_attempted <- 0
  monitor$profile_rows <- 0L
  monitor$profile_missing_position <- 0L
  monitor$profile_missing_height <- 0L
  monitor$profile_missing_nationality <- 0L
  monitor$positions <- character()
  monitor$heights <- numeric()
  monitor$nationalities <- character()
  monitor
}

# Free bytes on the volume holding `path`, or NA when it cannot be determined.
free_disk_bytes <- function(path) {
  path <- normalizePath(path, winslash = "/", mustWork = TRUE)
  out <- tryCatch(suppressWarnings(if (.Platform$OS.type == "windows") {
    system2("powershell", c("-NoProfile", "-Command",
                            sprintf("(Get-PSDrive %s).Free", substr(path, 1L, 1L))),
            stdout = TRUE, stderr = FALSE)
  } else {
    fields <- strsplit(tail(system2("df", c("-Pk", shQuote(path)), stdout = TRUE), 1L),
                       "[[:space:]]+")[[1]]
    as.numeric(fields[4]) * 1024
  }), error = function(e) NA)
  suppressWarnings(as.numeric(trimws(out[1])))
}

cache_size_bytes <- function(cache_dir) {
  files <- list.files(cache_dir, recursive = TRUE, full.names = TRUE)
  files <- files[file.exists(files) & !dir.exists(files)]
  if (!length(files)) return(0)
  sum(file.info(files)$size, na.rm = TRUE)
}

write_sanity_report <- function(monitor, label, extra = character()) {
  position_summary <- if (length(monitor$positions)) {
    paste(head(sort(unique(monitor$positions)), 12L), collapse = ", ")
  } else "none parsed"
  nationality_summary <- if (length(monitor$nationalities)) {
    paste(head(sort(unique(monitor$nationalities)), 12L), collapse = ", ")
  } else "none parsed"
  height_summary <- if (length(monitor$heights)) {
    sprintf("%.2f-%.2f m", min(monitor$heights), max(monitor$heights))
  } else "none parsed"
  lines <- c(
    paste0("=== ", label, " @ ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %z"), " ==="),
    sprintf("network attempts: %d", monitor$network_attempts),
    sprintf("network successes: %d", monitor$network_successes),
    sprintf("cache hits: %d", monitor$cache_hits),
    sprintf("downloaded bytes: %s", format_bytes(monitor$downloaded_bytes)),
    sprintf("cache size: %s", format_bytes(cache_size_bytes(monitor$cache_dir))),
    sprintf("free disk space: %s", {
      free <- free_disk_bytes(monitor$cache_dir)
      if (is.na(free)) "unknown" else format_bytes(free)
    }),
    sprintf("parsed accumulated pages: %d", monitor$accumulate_pages),
    sprintf("parsed player-seasons: %d", monitor$shooting_rows),
    sprintf("English names matched: %d", monitor$english_names),
    sprintf("parsed attempts: 2PT=%.0f, 3PT=%.0f, FT=%.0f",
            monitor$fg2_attempted, monitor$fg3_attempted, monitor$ft_attempted),
    sprintf("parsed profiles: %d", monitor$profile_rows),
    sprintf(
      "profile missing fields: position=%d, height=%d, nationality=%d",
      monitor$profile_missing_position, monitor$profile_missing_height,
      monitor$profile_missing_nationality
    ),
    paste("observed positions:", position_summary),
    paste("observed height range:", height_summary),
    paste("observed nationalities:", nationality_summary),
    extra,
    ""
  )
  cat(paste(lines, collapse = "\n"))
  cat("Sanity report:", monitor$report_path, "\n")
  cat(lines, sep = "\n", file = monitor$report_path, append = TRUE)
  invisible(lines)
}

maybe_emit_request_checkpoint <- function(monitor) {
  if (!is.null(monitor) && !monitor$checkpoint_emitted &&
      monitor$network_attempts >= monitor$checkpoint_requests) {
    monitor$checkpoint_emitted <- TRUE
    write_sanity_report(monitor, paste0(
      "SANITY CHECK AFTER FIRST ", monitor$checkpoint_requests, " HTTP REQUESTS"
    ))
    if (isTRUE(monitor$stop_after_checkpoint)) {
      condition <- structure(
        list(
          message = paste0(
            "Stopped cleanly after the ", monitor$checkpoint_requests,
            "-request sanity checkpoint; rerun without --stop-after-checkpoint to resume from cache"
          ),
          call = NULL
        ),
        class = c("basket_checkpoint_stop", "condition")
      )
      stop(condition)
    }
  }
}

validate_scraped_data <- function(players, pages, profiles = NULL, label = "scrape") {
  problems <- character()
  if (!nrow(players)) problems <- c(problems, "no player-season rows")
  if (anyDuplicated(players[c("game_year", "basket_player_id")])) {
    problems <- c(problems, "duplicate player-season keys")
  }
  if (is.null(players$player_name_en) || anyNA(players$player_name_en) ||
      any(!nzchar(trimws(players$player_name_en)))) {
    problems <- c(problems, "player-seasons without an English name")
  }
  for (prefix in c("fg2", "fg3", "ft")) {
    made <- players[[paste0(prefix, "_made")]]
    attempted <- players[[paste0(prefix, "_attempted")]]
    if (anyNA(made) || anyNA(attempted) || any(made < 0 | attempted < made)) {
      problems <- c(problems, paste0("invalid ", prefix, " made/attempted totals"))
    }
  }
  page_keys <- paste(pages$game_year, pages$page_number)
  player_page_keys <- paste(players$game_year, players$source_page)
  if (any(!player_page_keys %in% page_keys)) {
    problems <- c(problems, "player rows reference an absent source page")
  }
  if (!is.null(profiles)) {
    if (anyDuplicated(profiles[c("game_year", "basket_player_id")])) {
      problems <- c(problems, "duplicate profile keys")
    }
    profile_keys <- paste(profiles$game_year, profiles$basket_player_id)
    player_keys <- paste(players$game_year, players$basket_player_id)
    if (any(!profile_keys %in% player_keys)) {
      problems <- c(problems, "profiles reference an absent player-season")
    }
    if (any(!is.na(profiles$position_name) & is.na(profiles$position_en))) {
      problems <- c(problems, "profiles with a Hebrew position but no English position")
    }
    if (any(!is.na(profiles$height_m) &
            (profiles$height_m < 0.5 | profiles$height_m > 3))) {
      problems <- c(problems, "profile height outside 0.5-3.0 metres")
    }
  }
  if (length(problems)) stop(label, " sanity check failed: ", paste(problems, collapse = "; "))
  c(
    sprintf("validated player-seasons: %d", nrow(players)),
    sprintf("validated source pages: %d", nrow(pages)),
    sprintf("validated profiles: %d", if (is.null(profiles)) 0L else nrow(profiles)),
    sprintf("total attempts: 2PT=%d, 3PT=%d, FT=%d",
            sum(players$fg2_attempted), sum(players$fg3_attempted),
            sum(players$ft_attempted))
  )
}

html_decode <- function(x) {
  named <- c(
    "&nbsp;" = " ", "&#160;" = " ", "&amp;" = "&", "&quot;" = "\"",
    "&#39;" = "'", "&apos;" = "'", "&lt;" = "<", "&gt;" = ">"
  )
  for (entity in names(named)) x <- gsub(entity, named[[entity]], x, fixed = TRUE)

  numeric_entities <- unique(regmatches(
    x, gregexpr("&#(?:[0-9]+|x[0-9A-Fa-f]+);", x, perl = TRUE)
  )[[1]])
  numeric_entities <- numeric_entities[nzchar(numeric_entities)]
  for (entity in numeric_entities) {
    body <- substring(entity, 3L, nchar(entity) - 1L)
    code <- if (startsWith(body, "x")) {
      strtoi(substring(body, 2L), base = 16L)
    } else {
      suppressWarnings(as.integer(body))
    }
    if (!is.na(code)) x <- gsub(entity, intToUtf8(code), x, fixed = TRUE)
  }
  x
}

html_text <- function(x) {
  x <- gsub("(?is)<(?:script|style)\\b[^>]*>.*?</(?:script|style)>", " ", x, perl = TRUE)
  x <- gsub("(?i)<br\\s*/?>", " ", x, perl = TRUE)
  x <- gsub("(?s)<[^>]+>", " ", x, perl = TRUE)
  x <- html_decode(x)
  trimws(gsub("[[:space:]]+", " ", x, perl = TRUE))
}

extract_matches <- function(pattern, x) {
  hit <- gregexpr(pattern, x, perl = TRUE)[[1]]
  if (identical(hit, -1L)) character() else regmatches(x, list(hit))[[1]]
}

parse_number <- function(x, integer = FALSE, field = "number") {
  value <- gsub(",", "", trimws(x), fixed = TRUE)
  value <- gsub("[^0-9.+-]", "", value, perl = TRUE)
  out <- suppressWarnings(as.numeric(value))
  if (!nzchar(value) || is.na(out)) stop("Could not parse ", field, " from '", x, "'")
  if (integer && out != floor(out)) stop(field, " is not an integer: '", x, "'")
  if (integer) as.integer(out) else out
}

parse_minutes <- function(x) {
  value <- trimws(x)
  if (grepl("^[0-9]+:[0-9]{2}$", value)) {
    bits <- as.numeric(strsplit(value, ":", fixed = TRUE)[[1]])
    return(bits[1] + bits[2] / 60)
  }
  parse_number(value, field = "minutes")
}

parse_made_attempted <- function(x, field) {
  value <- gsub("[[:space:]]", "", x, perl = TRUE)
  bits <- strsplit(value, "/", fixed = TRUE)[[1]]
  if (length(bits) != 2L) stop("Could not parse ", field, " made/attempted from '", x, "'")
  made <- parse_number(bits[1], integer = TRUE, field = paste(field, "made"))
  attempted <- parse_number(bits[2], integer = TRUE, field = paste(field, "attempted"))
  if (made < 0L || attempted < made) stop("Invalid ", field, " totals: '", x, "'")
  c(made = made, attempted = attempted)
}

empty_shooting_rows <- function() {
  data.frame(
    basket_player_id = integer(), player_name = character(), games = integer(),
    minutes = numeric(), points = numeric(), fg2_made = integer(),
    fg2_attempted = integer(), fg3_made = integer(), fg3_attempted = integer(),
    ft_made = integer(), ft_attempted = integer(), source_row = integer(),
    stringsAsFactors = FALSE
  )
}

parse_basket_accumulate_html <- function(html) {
  rows <- extract_matches("(?is)<tr\\b[^>]*>.*?</tr>", html)
  parsed <- vector("list", length(rows))
  used <- 0L

  for (row_number in seq_along(rows)) {
    row <- rows[[row_number]]
    id_hit <- regexec("(?i)player\\.asp\\?[^\"'>]*PlayerId=([0-9]+)", row, perl = TRUE)
    id_parts <- regmatches(row, id_hit)[[1]]
    if (length(id_parts) != 2L) next

    raw_cells <- extract_matches("(?is)<t[dh]\\b[^>]*>.*?</t[dh]>", row)
    cells <- vapply(raw_cells, html_text, character(1))
    if (length(cells) < 11L) {
      stop("Player row ", row_number, " has ", length(cells), " cells; expected at least 11")
    }

    fg2 <- parse_made_attempted(cells[6], "2PT")
    fg3 <- parse_made_attempted(cells[8], "3PT")
    ft <- parse_made_attempted(cells[10], "FT")
    used <- used + 1L
    parsed[[used]] <- data.frame(
      basket_player_id = as.integer(id_parts[2]),
      player_name = cells[2],
      games = parse_number(cells[3], integer = TRUE, field = "games"),
      minutes = parse_minutes(cells[4]),
      points = parse_number(cells[5], field = "points"),
      fg2_made = unname(fg2["made"]), fg2_attempted = unname(fg2["attempted"]),
      fg3_made = unname(fg3["made"]), fg3_attempted = unname(fg3["attempted"]),
      ft_made = unname(ft["made"]), ft_attempted = unname(ft["attempted"]),
      source_row = row_number,
      stringsAsFactors = FALSE
    )
  }

  if (used == 0L) return(empty_shooting_rows())
  out <- do.call(rbind, parsed[seq_len(used)])
  if (anyDuplicated(out$basket_player_id)) {
    stop("A page contains duplicate Basket PlayerId values")
  }
  out
}

basket_accumulate_url <- function(game_year, page_number, lang = "he") {
  stopifnot(length(game_year) == 1L, length(page_number) == 1L,
            lang %in% c("he", "en"))
  query <- c(
    StatsBoard = 0, c = page_number, cYear = game_year, lang = lang, local = 0,
    maxYear = game_year, minYear = game_year, sType = "TO", selectedTeam = 0,
    stats_options = 1
  )
  paste0(
    BASKET_SHOOTING_BASE_URL, "?",
    paste0(names(query), "=", utils::URLencode(as.character(query), reserved = TRUE),
           collapse = "&")
  )
}

read_downloaded_html <- function(path) {
  raw <- readBin(path, what = "raw", n = file.info(path)$size)
  text <- rawToChar(raw)
  utf8 <- iconv(text, from = "UTF-8", to = "UTF-8", sub = NA)
  if (!is.na(utf8)) return(utf8)
  converted <- iconv(text, from = "windows-1255", to = "UTF-8", sub = "byte")
  if (is.na(converted)) stop("Could not decode downloaded HTML as UTF-8 or windows-1255")
  converted
}

download_with_cache <- function(url, cache_path, refresh_cache = FALSE,
                                max_attempts = 5L, initial_backoff = 2,
                                user_agent = BASKET_SHOOTING_USER_AGENT,
                                monitor = NULL) {
  if (!refresh_cache && file.exists(cache_path) && file.info(cache_path)$size > 0) {
    if (!is.null(monitor)) monitor$cache_hits <- monitor$cache_hits + 1L
    return(list(path = cache_path, cache_hit = TRUE,
                fetched_at = file.info(cache_path)$mtime))
  }
  if (max_attempts < 1L) stop("max_attempts must be positive")
  dir.create(dirname(cache_path), recursive = TRUE, showWarnings = FALSE)
  old_options <- options(timeout = max(60, getOption("timeout", 60)))
  on.exit(options(old_options), add = TRUE)
  last_error <- NULL

  for (attempt in seq_len(max_attempts)) {
    if (!is.null(monitor)) monitor$network_attempts <- monitor$network_attempts + 1L
    temporary <- tempfile(pattern = "basket-download-", tmpdir = dirname(cache_path),
                          fileext = ".html")
    result <- tryCatch(
      suppressWarnings(utils::download.file(
        url, temporary, method = "libcurl", mode = "wb", quiet = TRUE,
        headers = c(`User-Agent` = user_agent)
      )),
      error = identity
    )
    valid <- !inherits(result, "error") && identical(result, 0L) &&
      file.exists(temporary) && file.info(temporary)$size > 0
    if (valid) {
      copied <- file.copy(temporary, cache_path, overwrite = TRUE, copy.mode = FALSE)
      unlink(temporary)
      if (!copied) stop("Downloaded but could not populate cache: ", cache_path)
      if (!is.null(monitor)) {
        monitor$network_successes <- monitor$network_successes + 1L
        monitor$downloaded_bytes <- monitor$downloaded_bytes + file.info(cache_path)$size
      }
      return(list(path = cache_path, cache_hit = FALSE, fetched_at = Sys.time()))
    }
    last_error <- if (inherits(result, "error")) conditionMessage(result) else {
      paste("download.file returned status", result)
    }
    if (file.exists(temporary)) unlink(temporary)
    if (attempt < max_attempts) {
      delay <- min(60, initial_backoff * 2^(attempt - 1L))
      delay <- delay + stats::runif(1L, 0, delay * 0.25)
      message(sprintf(
        "Request failed (attempt %d/%d); retrying in %.1fs: %s",
        attempt, max_attempts, delay, url
      ))
      Sys.sleep(delay)
    }
  }
  stop("Request failed after ", max_attempts, " attempts: ", url,
       "; last error: ", last_error)
}

accumulate_cache_path <- function(cache_dir, game_year, page_number, lang = "he") {
  folder <- if (identical(lang, "he")) "accumulate" else paste0("accumulate-", lang)
  file.path(cache_dir, folder, as.character(as.integer(game_year)),
            sprintf("page-%03d.html", as.integer(page_number)))
}

profile_cache_path <- function(cache_dir, game_year, basket_player_id) {
  file.path(cache_dir, "profiles", as.character(as.integer(game_year)),
            paste0(as.integer(basket_player_id), ".html"))
}

fetch_basket_page <- function(game_year, page_number,
                              cache_dir = "exports/cache/basket_shooting_history",
                              refresh_cache = FALSE, max_attempts = 5L,
                              initial_backoff = 2, monitor = NULL, lang = "he") {
  url <- basket_accumulate_url(game_year, page_number, lang = lang)
  response <- download_with_cache(
    url, accumulate_cache_path(cache_dir, game_year, page_number, lang = lang),
    refresh_cache = refresh_cache, max_attempts = max_attempts,
    initial_backoff = initial_backoff, monitor = monitor
  )
  list(
    url = url,
    content_md5 = unname(tools::md5sum(response$path)),
    fetched_at = response$fetched_at,
    cache_hit = response$cache_hit,
    path = response$path,
    html = read_downloaded_html(response$path)
  )
}

basket_player_url <- function(basket_player_id) {
  paste0("https://basket.co.il/player.asp?PlayerId=", as.integer(basket_player_id))
}

first_capture <- function(pattern, text, group = 1L) {
  hit <- regexec(pattern, text, perl = TRUE)
  parts <- regmatches(text, hit)[[1]]
  if (length(parts) <= group) NA_character_ else trimws(parts[group + 1L])
}

parse_basket_player_profile_html <- function(html) {
  text <- html_text(html)
  nationality_name <- first_capture(
    "אזרחות:[[:space:]]*(.*?)[[:space:]]*\\(([A-Z]{3})\\)", text, 1L
  )
  nationality_code <- first_capture(
    "אזרחות:[[:space:]]*(.*?)[[:space:]]*\\(([A-Z]{3})\\)", text, 2L
  )
  # The site's own team string, sponsor included, bounded by the next label.
  team_name <- first_capture(
    "קבוצה:[[:space:]]*(.*?)[[:space:]]*אזרחות:", text, 1L
  )
  # dd/mm/yyyy: 695 of 1160 cached profiles have a day > 12 and none has a
  # month > 12, so day-first is the site's format, not an assumption.
  birth_text <- first_capture(
    "תאריך לידה:[[:space:]]*([0-9]{2}/[0-9]{2}/[0-9]{4})", text, 1L
  )
  date_of_birth <- if (is.na(birth_text)) {
    as.Date(NA)
  } else {
    as.Date(birth_text, format = "%d/%m/%Y")
  }
  position_name <- first_capture(
    "עמדה:[[:space:]]*(.*?)[[:space:]]*גובה:", text, 1L
  )
  height_text <- first_capture(
    "גובה:[[:space:]]*([0-9]+(?:[.,][0-9]+)?)", text, 1L
  )
  height_m <- if (is.na(height_text)) NA_real_ else {
    as.numeric(sub(",", ".", height_text, fixed = TRUE))
  }
  if (!is.na(height_m) && height_m > 3) height_m <- height_m / 100
  blank_to_na <- function(x) if (is.na(x) || !nzchar(trimws(x))) NA_character_ else trimws(x)
  position_name <- blank_to_na(position_name)
  nationality_name <- blank_to_na(nationality_name)
  nationality_code <- blank_to_na(nationality_code)
  team_name <- blank_to_na(team_name)
  if (is.na(position_name) && is.na(height_m) && is.na(nationality_name)) {
    stop("Player page contains none of position, height, or nationality")
  }
  data.frame(
    position_name = position_name,
    # Filled by resolve_profile_positions() once the download is complete.
    position_en = NA_character_,
    height_m = height_m,
    nationality_name = nationality_name,
    nationality_code = nationality_code,
    basket_team_name = team_name,
    date_of_birth = date_of_birth,
    stringsAsFactors = FALSE
  )
}

fetch_basket_player_profile <- function(
    game_year, basket_player_id,
    cache_dir = "exports/cache/basket_shooting_history",
    refresh_cache = FALSE, max_attempts = 5L, initial_backoff = 2,
    monitor = NULL) {
  url <- basket_player_url(basket_player_id)
  response <- download_with_cache(
    url, profile_cache_path(cache_dir, game_year, basket_player_id),
    refresh_cache = refresh_cache, max_attempts = max_attempts,
    initial_backoff = initial_backoff, monitor = monitor
  )
  profile <- parse_page_or_discard(
    list(html = read_downloaded_html(response$path), path = response$path),
    parse_basket_player_profile_html
  )
  profile$game_year <- game_year
  profile$basket_player_id <- basket_player_id
  profile$source_url <- url
  profile$source_content_md5 <- unname(tools::md5sum(response$path))
  profile$fetched_at <- response$fetched_at
  profile$cache_hit <- response$cache_hit
  profile
}

scrape_basket_profiles <- function(players, pause_seconds = 1,
                                   fetch_profile = fetch_basket_player_profile,
                                   monitor = NULL, failures = NULL) {
  keys <- unique(players[c("game_year", "basket_player_id")])
  profiles <- vector("list", nrow(keys))
  for (i in seq_len(nrow(keys))) {
    profiles[[i]] <- tryCatch(
      fetch_profile(keys$game_year[i], keys$basket_player_id[i]),
      error = function(e) {
        record_failure(failures, "profile", keys$game_year[i],
                       paste("player", keys$basket_player_id[i]),
                       conditionMessage(e))
        NULL
      }
    )
    # A failure still went to the network, so it still earns the pause.
    used_network <- is.null(profiles[[i]]) || !isTRUE(profiles[[i]]$cache_hit[1])
    if (!is.null(monitor) && !is.null(profiles[[i]])) {
      monitor$profile_rows <- monitor$profile_rows + 1L
      monitor$profile_missing_position <- monitor$profile_missing_position +
        as.integer(is.na(profiles[[i]]$position_name[1]))
      monitor$profile_missing_height <- monitor$profile_missing_height +
        as.integer(is.na(profiles[[i]]$height_m[1]))
      monitor$profile_missing_nationality <- monitor$profile_missing_nationality +
        as.integer(is.na(profiles[[i]]$nationality_name[1]))
      monitor$positions <- c(
        monitor$positions, profiles[[i]]$position_name[!is.na(profiles[[i]]$position_name)]
      )
      monitor$heights <- c(
        monitor$heights, profiles[[i]]$height_m[!is.na(profiles[[i]]$height_m)]
      )
      monitor$nationalities <- c(
        monitor$nationalities,
        profiles[[i]]$nationality_code[!is.na(profiles[[i]]$nationality_code)]
      )
    }
    if (!is.null(monitor)) maybe_emit_request_checkpoint(monitor)
    if (i < nrow(keys) && used_network && pause_seconds > 0) {
      Sys.sleep(pause_seconds)
    }
  }
  do.call(rbind, profiles)
}

# Attach English names from the lang=en accumulated pages of the same season.
# The English pages must list exactly the same PlayerIds with identical
# totals; anything else means they are not the same table and stops the run.
merge_english_names <- function(players, english, game_year) {
  label <- paste("Season", game_year, "English pages")
  if (anyDuplicated(english$basket_player_id)) {
    stop(label, " list a PlayerId more than once")
  }
  missing <- setdiff(players$basket_player_id, english$basket_player_id)
  extra <- setdiff(english$basket_player_id, players$basket_player_id)
  if (length(missing) || length(extra)) {
    stop(label, " disagree on PlayerIds: ", length(missing), " only in Hebrew, ",
         length(extra), " only in English (e.g. ", c(missing, extra)[1], ")")
  }
  matched <- english[match(players$basket_player_id, english$basket_player_id), ]
  for (column in c("games", "fg2_made", "fg2_attempted", "fg3_made",
                   "fg3_attempted", "ft_made", "ft_attempted")) {
    differs <- players[[column]] != matched[[column]]
    if (any(differs)) {
      stop(label, " disagree on ", column, " for ", sum(differs),
           " players (e.g. PlayerId ", players$basket_player_id[differs][1], ")")
    }
  }
  players$player_name_en <- matched$player_name
  players
}

# A page that fails is not the end of the season, so the loop records it and
# moves on. It does give up after BASKET_MAX_CONSECUTIVE_PAGE_FAILURES, because
# a site that is simply down would otherwise retry every page to max_pages.
BASKET_MAX_CONSECUTIVE_PAGE_FAILURES <- 3L

scrape_basket_season <- function(game_year, max_pages = 50L, pause_seconds = 1,
                                 fetch_page = fetch_basket_page, monitor = NULL,
                                 failures = NULL) {
  all_rows <- list()
  pages <- list()
  fingerprints <- character()
  consecutive_failures <- 0L

  for (page_number in seq_len(max_pages)) {
    fetched <- tryCatch({
      page <- fetch_page(game_year, page_number)
      list(page = page, rows = parse_page_or_discard(page, parse_accumulate_page))
    }, error = function(e) {
      record_failure(failures, "stats page", game_year,
                     paste("page", page_number), conditionMessage(e))
      NULL
    })
    if (is.null(fetched)) {
      consecutive_failures <- consecutive_failures + 1L
      if (consecutive_failures >= BASKET_MAX_CONSECUTIVE_PAGE_FAILURES) {
        record_failure(failures, "season", game_year, "pages",
                       sprintf("gave up after %d consecutive page failures",
                               consecutive_failures))
        break
      }
      if (pause_seconds > 0) Sys.sleep(pause_seconds)
      next
    }
    consecutive_failures <- 0L
    page <- fetched$page
    rows <- fetched$rows
    if (!is.null(monitor)) {
      monitor$accumulate_pages <- monitor$accumulate_pages + 1L
      monitor$shooting_rows <- monitor$shooting_rows + nrow(rows)
      monitor$fg2_attempted <- monitor$fg2_attempted + sum(rows$fg2_attempted)
      monitor$fg3_attempted <- monitor$fg3_attempted + sum(rows$fg3_attempted)
      monitor$ft_attempted <- monitor$ft_attempted + sum(rows$ft_attempted)
      maybe_emit_request_checkpoint(monitor)
    }
    pages[[length(pages) + 1L]] <- data.frame(
      game_year = game_year, page_number = page_number, source_url = page$url,
      content_md5 = page$content_md5, fetched_at = page$fetched_at,
      cache_hit = isTRUE(page$cache_hit), parsed_rows = nrow(rows),
      stringsAsFactors = FALSE
    )

    if (nrow(rows) == 0L) {
      if (page_number == 1L) {
        record_failure(failures, "season", game_year, "page 1",
                       "returned no player rows")
      }
      break
    }

    fingerprint <- paste(sort(rows$basket_player_id), collapse = ",")
    if (fingerprint %in% fingerprints) {
      pages[[length(pages)]]$parsed_rows <- 0L
      break
    }
    fingerprints <- c(fingerprints, fingerprint)
    rows$game_year <- game_year
    rows$source_url <- page$url
    rows$source_page <- page_number
    rows$source_content_md5 <- page$content_md5
    rows$fetched_at <- page$fetched_at
    all_rows[[length(all_rows) + 1L]] <- rows
    if (page_number < max_pages && !isTRUE(page$cache_hit) && pause_seconds > 0) {
      Sys.sleep(pause_seconds)
    }
  }

  players <- do.call(rbind, all_rows)
  if (is.null(players)) {
    return(list(players = NULL, pages = do.call(rbind, pages)))
  }
  if (anyDuplicated(players[c("game_year", "basket_player_id")])) {
    record_failure(failures, "season", game_year, "player ids",
                   "a PlayerId appears on more than one page")
  }

  english_pages <- sort(unique(players$source_page))
  english <- do.call(rbind, lapply(english_pages, function(page_number) {
    tryCatch({
      page <- fetch_page(game_year, page_number, lang = "en")
      if (page_number < max(english_pages) && !isTRUE(page$cache_hit) &&
          pause_seconds > 0) {
        Sys.sleep(pause_seconds)
      }
      # A requested English page must carry rows: these pages had rows in Hebrew.
      parse_page_or_discard(page, function(html) {
        rows <- parse_basket_accumulate_html(html)
        if (!nrow(rows)) stop("English page ", page_number, " has no player rows")
        rows
      })
    }, error = function(e) {
      record_failure(failures, "english page", game_year,
                     paste("page", page_number), conditionMessage(e))
      NULL
    })
  }))
  # A disagreement between the Hebrew and English tables is a correctness
  # problem, not a transient one, but it is still recorded rather than thrown:
  # any recorded failure blocks the write, and the download runs to the end.
  players <- tryCatch(
    merge_english_names(players, english, game_year),
    error = function(e) {
      record_failure(failures, "english names", game_year, "merge",
                     conditionMessage(e))
      players$player_name_en <- NA_character_
      players
    }
  )
  if (!is.null(monitor)) {
    monitor$english_names <- monitor$english_names + sum(!is.na(players$player_name_en))
    maybe_emit_request_checkpoint(monitor)
  }
  list(players = players, pages = do.call(rbind, pages))
}

parse_seasons <- function(value) {
  if (grepl("^[0-9]{4}:[0-9]{4}$", value)) {
    ends <- as.integer(strsplit(value, ":", fixed = TRUE)[[1]])
    values <- seq.int(ends[1], ends[2])
  } else {
    values <- as.integer(strsplit(value, ",", fixed = TRUE)[[1]])
  }
  if (!length(values) || anyNA(values)) stop("Invalid --seasons value: ", value)
  values <- sort(unique(values))
  if (length(values) > 5L) {
    stop("At most five seasons may be requested per run; received ", length(values))
  }
  values
}

option_value <- function(args, name, default) {
  prefix <- paste0("--", name, "=")
  hits <- args[startsWith(args, prefix)]
  if (!length(hits)) return(default)
  if (length(hits) > 1L) stop("Option supplied more than once: --", name)
  substring(hits, nchar(prefix) + 1L)
}

connect_etl_db <- function() {
  if (!requireNamespace("DBI", quietly = TRUE) || !requireNamespace("RPostgres", quietly = TRUE)) {
    stop("--write requires the DBI and RPostgres packages")
  }
  readRenviron("etl/.Renviron")
  DBI::dbConnect(
    RPostgres::Postgres(), host = Sys.getenv("PG_HOST"),
    port = as.integer(Sys.getenv("PG_PORT")), dbname = Sys.getenv("PG_DB"),
    user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
    sslmode = Sys.getenv("PG_SSLMODE"), connect_timeout = 15L, bigint = "numeric"
  )
}

database_preflight <- function(con) {
  required <- c(
    "basketball_test.basket_shooting_import_runs",
    "basketball_test.basket_shooting_source_pages",
    "basketball_test.basket_player_season"
  )
  present <- vapply(required, function(relation) {
    value <- DBI::dbGetQuery(
      con, "SELECT to_regclass($1)::text AS relation", params = list(relation)
    )$relation[[1]]
    !is.na(value) && nzchar(value)
  }, logical(1))
  if (any(!present)) {
    stop("Database preflight failed; missing: ", paste(required[!present], collapse = ", "))
  }
  # pg_attribute, not information_schema: the latter hides ungranted columns.
  required_columns <- list(
    c("basketball_test.basket_player_season", "player_name_en"),
    c("basketball_test.basket_player_season", "position_en"),
    c("basketball_test.basket_player_season", "profile_fetched_at")
  )
  column_present <- vapply(required_columns, function(rc) {
    DBI::dbGetQuery(con, paste(
      "SELECT count(*)::integer AS n FROM pg_attribute",
      "WHERE attrelid = to_regclass($1) AND attname = $2 AND NOT attisdropped"
    ), params = list(rc[1], rc[2]))$n[[1]] == 1L
  }, logical(1))
  if (any(!column_present)) {
    stop("Database preflight failed; apply ",
         "sql/migrations/2026-09-23_basket_shooting_history_consolidate.sql first (missing: ",
         paste(vapply(required_columns[!column_present], paste, character(1),
                      collapse = "."), collapse = ", "), ")")
  }
  size <- DBI::dbGetQuery(
    con, "SELECT pg_database_size(current_database())::numeric AS bytes"
  )$bytes[[1]]
  c("database migrations present (incl. English columns)",
    paste("database size before backfill:", format_bytes(size)))
}

SHOOTING_KEY_COLUMNS <- c("game_year", "basket_player_id")
SHOOTING_VALUE_COLUMNS <- c(
  "player_name", "player_name_en", "games", "minutes", "points", "fg2_made", "fg2_attempted",
  "fg3_made", "fg3_attempted", "ft_made", "ft_attempted", "source_url",
  "source_page", "source_row", "source_content_md5"
)

# Row-level comparison of SQL rows against the parsed rows they came from.
# Pure, so it is testable offline; returns a character vector of problems.
compare_written_rows <- function(stored, expected) {
  keys <- function(df) paste(df$game_year, df$basket_player_id)
  problems <- character()
  missing <- setdiff(keys(expected), keys(stored))
  extra <- setdiff(keys(stored), keys(expected))
  if (length(missing)) {
    problems <- c(problems, sprintf("%d parsed rows absent from SQL (e.g. %s)",
                                    length(missing), missing[1]))
  }
  if (length(extra)) {
    problems <- c(problems, sprintf("%d SQL rows not in the parsed batch (e.g. %s)",
                                    length(extra), extra[1]))
  }
  joined <- merge(expected, stored, by = SHOOTING_KEY_COLUMNS,
                  suffixes = c(".parsed", ".sql"))
  for (column in SHOOTING_VALUE_COLUMNS) {
    parsed <- joined[[paste0(column, ".parsed")]]
    sql <- joined[[paste0(column, ".sql")]]
    differs <- if (is.numeric(parsed)) {
      xor(is.na(parsed), is.na(sql)) |
        (!is.na(parsed) & !is.na(sql) & abs(parsed - as.numeric(sql)) > 1e-9)
    } else {
      xor(is.na(parsed), is.na(sql)) |
        (!is.na(parsed) & !is.na(sql) & parsed != as.character(sql))
    }
    if (any(differs)) {
      problems <- c(problems, sprintf(
        "%s differs on %d rows (e.g. %s %s: parsed '%s', SQL '%s')",
        column, sum(differs), joined$game_year[differs][1],
        joined$basket_player_id[differs][1], parsed[differs][1], sql[differs][1]
      ))
    }
  }
  problems
}

validate_written_rows <- function(con, run_id, expected, label) {
  stored <- DBI::dbGetQuery(con, paste(
    "SELECT", paste(c(SHOOTING_KEY_COLUMNS, SHOOTING_VALUE_COLUMNS), collapse = ", "),
    "FROM basketball_test.basket_player_season WHERE run_id = $1"
  ), params = list(run_id))
  problems <- compare_written_rows(stored, expected)
  if (length(problems)) {
    stop(label, " failed: ", paste(problems, collapse = "; "))
  }
  c(
    sprintf("SQL rows read back for run: %d", nrow(stored)),
    sprintf("row-level match on %d columns: OK", length(SHOOTING_VALUE_COLUMNS)),
    sprintf("rows cover seasons: %s",
            paste(sort(unique(stored$game_year)), collapse = ", "))
  )
}

validate_written_scrape <- function(con, run_id, players, profiles = NULL) {
  stored <- DBI::dbGetQuery(con, paste(
    "SELECT count(*)::integer AS rows,",
    "coalesce(sum(fg2_attempted), 0)::numeric AS fg2a,",
    "coalesce(sum(fg3_attempted), 0)::numeric AS fg3a,",
    "coalesce(sum(ft_attempted), 0)::numeric AS fta",
    "FROM basketball_test.basket_player_season WHERE run_id = $1"
  ), params = list(run_id))
  profile_counts <- DBI::dbGetQuery(con, paste(
    "SELECT count(profile_fetched_at)::integer AS rows,",
    "count(*) FILTER (WHERE position_name IS NOT NULL",
    "                   AND position_en IS NULL)::integer AS untranslated",
    "FROM basketball_test.basket_player_season WHERE run_id = $1"
  ), params = list(run_id))
  stored_profiles <- profile_counts$rows[[1]]
  if (profile_counts$untranslated[[1]] > 0L) {
    stop("Post-backfill check failed: ", profile_counts$untranslated[[1]],
         " SQL profiles have a Hebrew position but no position_en")
  }
  expected_profiles <- if (is.null(profiles)) 0L else nrow(profiles)
  matches <- c(
    stored$rows[[1]] == nrow(players),
    as.numeric(stored$fg2a[[1]]) == sum(players$fg2_attempted),
    as.numeric(stored$fg3a[[1]]) == sum(players$fg3_attempted),
    as.numeric(stored$fta[[1]]) == sum(players$ft_attempted),
    stored_profiles == expected_profiles
  )
  if (!all(matches)) {
    stop("Post-backfill reconciliation failed: SQL rows/totals differ from parsed data")
  }
  row_lines <- validate_written_rows(
    con, run_id, players, "Post-backfill row-level reconciliation"
  )
  size <- DBI::dbGetQuery(
    con, "SELECT pg_database_size(current_database())::numeric AS bytes"
  )$bytes[[1]]
  c(
    paste("reconciled import run:", format(run_id, scientific = FALSE)),
    sprintf("SQL player-seasons: %d", stored$rows[[1]]),
    sprintf("SQL profiles: %d", stored_profiles),
    sprintf("SQL attempts: 2PT=%s, 3PT=%s, FT=%s",
            stored$fg2a[[1]], stored$fg3a[[1]], stored$fta[[1]]),
    row_lines,
    paste("database size after backfill:", format_bytes(size))
  )
}

write_scrape <- function(con, seasons, players, pages, profiles = NULL,
                         checkpoint_rows = 100L, on_row_checkpoint = NULL) {
  years_sql <- paste(as.integer(seasons), collapse = ",")
  run <- DBI::dbGetQuery(con, paste0(
    "INSERT INTO basketball_test.basket_shooting_import_runs ",
    "(requested_game_years, source_base_url, parser_version, status, requested_pages) ",
    "VALUES (ARRAY[", years_sql, "]::integer[], $1, $2, 'running', $3) RETURNING run_id"
  ), params = list(BASKET_SHOOTING_BASE_URL, BASKET_SHOOTING_PARSER_VERSION, nrow(pages)))
  run_id <- run$run_id[[1]]

  tryCatch({
    DBI::dbWithTransaction(con, {
      pages$run_id <- run_id
      DBI::dbExecute(con, paste(
        "CREATE TEMP TABLE basket_shooting_pages_stage",
        "(LIKE basketball_test.basket_shooting_source_pages INCLUDING DEFAULTS)",
        "ON COMMIT DROP"
      ))
      DBI::dbAppendTable(con, DBI::Id(table = "basket_shooting_pages_stage"),
                         pages[c("run_id", "game_year", "page_number", "source_url",
                                 "content_md5", "fetched_at", "cache_hit", "parsed_rows")])
      DBI::dbExecute(con, paste(
        "INSERT INTO basketball_test.basket_shooting_source_pages",
        "SELECT * FROM basket_shooting_pages_stage"
      ))

      players$run_id <- run_id
      DBI::dbExecute(con, paste(
        "CREATE TEMP TABLE basket_shooting_players_stage",
        "(LIKE basketball_test.basket_player_season INCLUDING DEFAULTS)",
        "ON COMMIT DROP"
      ))
      stage_columns <- c(
        "game_year", "basket_player_id", "player_name", "player_name_en", "games",
        "minutes", "points",
        "fg2_made", "fg2_attempted", "fg3_made", "fg3_attempted", "ft_made",
        "ft_attempted", "source_url", "source_page", "source_row",
        "source_content_md5", "fetched_at", "run_id"
      )
      upsert_players <- function(rows) {
        DBI::dbExecute(con, "TRUNCATE basket_shooting_players_stage")
        DBI::dbAppendTable(con, DBI::Id(table = "basket_shooting_players_stage"),
                           rows[stage_columns])
        DBI::dbExecute(con, paste(
          "INSERT INTO basketball_test.basket_player_season",
          paste0("(", paste(stage_columns, collapse = ", "), ")"),
          "SELECT", paste(stage_columns, collapse = ", "),
          "FROM basket_shooting_players_stage",
          "ON CONFLICT (game_year, basket_player_id) DO UPDATE SET",
          "player_name = EXCLUDED.player_name,",
          "player_name_en = EXCLUDED.player_name_en, games = EXCLUDED.games,",
          "minutes = EXCLUDED.minutes, points = EXCLUDED.points,",
          "fg2_made = EXCLUDED.fg2_made, fg2_attempted = EXCLUDED.fg2_attempted,",
          "fg3_made = EXCLUDED.fg3_made, fg3_attempted = EXCLUDED.fg3_attempted,",
          "ft_made = EXCLUDED.ft_made, ft_attempted = EXCLUDED.ft_attempted,",
          "source_url = EXCLUDED.source_url, source_page = EXCLUDED.source_page,",
          "source_row = EXCLUDED.source_row,",
          "source_content_md5 = EXCLUDED.source_content_md5,",
          "fetched_at = EXCLUDED.fetched_at, run_id = EXCLUDED.run_id, updated_at = now()"
        ))
      }
      # Row checkpoint: write the first N rows, read them back and compare
      # every value before writing the rest. A mismatch stops inside the
      # transaction, so nothing is committed.
      players <- players[order(players$game_year, players$source_page,
                               players$source_row), , drop = FALSE]
      first <- seq_len(min(checkpoint_rows, nrow(players)))
      upsert_players(players[first, , drop = FALSE])
      checkpoint_lines <- validate_written_rows(
        con, run_id, players[first, , drop = FALSE],
        paste0("Sanity check after first ", length(first), " SQL rows")
      )
      if (!is.null(on_row_checkpoint)) on_row_checkpoint(length(first), checkpoint_lines)
      if (nrow(players) > length(first)) upsert_players(players[-first, , drop = FALSE])

      if (!is.null(profiles) && nrow(profiles)) {
        profiles$run_id <- run_id
        # Profiles now live on the same row as the shooting totals, which the
        # upsert above has already created, so this fills columns rather than
        # inserting. The staging table is explicit: the target's NOT NULL
        # shooting columns make LIKE unusable here.
        DBI::dbExecute(con, paste(
          "CREATE TEMP TABLE basket_profiles_stage (",
          "game_year integer, basket_player_id integer, position_name text,",
          "position_en text, height_m numeric, nationality_name text,",
          "nationality_code text, basket_team_name text, date_of_birth date,",
          "source_url text, source_content_md5 text,",
          "fetched_at timestamptz, cache_hit boolean",
          ") ON COMMIT DROP"
        ))
        profile_columns <- c(
          "game_year", "basket_player_id", "position_name", "position_en", "height_m",
          "nationality_name", "nationality_code", "basket_team_name",
          "date_of_birth", "source_url",
          "source_content_md5", "fetched_at", "cache_hit"
        )
        DBI::dbAppendTable(con, DBI::Id(table = "basket_profiles_stage"),
                           profiles[profile_columns])
        updated <- DBI::dbExecute(con, paste(
          "UPDATE basketball_test.basket_player_season t SET",
          "position_name = s.position_name, position_en = s.position_en,",
          "height_m = s.height_m, nationality_name = s.nationality_name,",
          "nationality_code = s.nationality_code,",
          "basket_team_name = s.basket_team_name,",
          "date_of_birth = s.date_of_birth,",
          "profile_source_url = s.source_url,",
          "profile_source_content_md5 = s.source_content_md5,",
          "profile_fetched_at = s.fetched_at, profile_cache_hit = s.cache_hit,",
          "updated_at = now()",
          "FROM basket_profiles_stage s",
          "WHERE t.game_year = s.game_year",
          "AND t.basket_player_id = s.basket_player_id"
        ))
        if (updated != nrow(profiles)) {
          stop("Profile write touched ", updated, " rows for ", nrow(profiles),
               " parsed profiles; every profile must match a player-season row")
        }
      }
    })
    cache_hits <- sum(pages$cache_hit)
    if (!is.null(profiles)) cache_hits <- cache_hits + sum(profiles$cache_hit)
    DBI::dbExecute(con, paste(
      "UPDATE basketball_test.basket_shooting_import_runs",
      "SET status = 'completed', finished_at = now(), fetched_pages = $2,",
      "cache_hits = $3, parsed_rows = $4",
      "WHERE run_id = $1"
    ), params = list(run_id, nrow(pages), cache_hits, nrow(players)))
  }, error = function(e) {
    DBI::dbExecute(con, paste(
      "UPDATE basketball_test.basket_shooting_import_runs",
      "SET status = 'failed', finished_at = now(), error_message = $2 WHERE run_id = $1"
    ), params = list(run_id, substr(conditionMessage(e), 1L, 4000L)))
    stop(e)
  })
  run_id
}

main <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  known <- grepl(paste0(
    "^--(?:seasons|max-pages|pause-seconds|profile-pause-seconds|cache-dir|",
    "max-attempts|initial-backoff|checkpoint-requests|checkpoint-rows|min-free-disk-mb)="
  ), args) | args %in% c(
    "--write", "--with-profiles", "--refresh-cache", "--stop-after-checkpoint"
  )
  if (any(!known)) stop("Unknown argument(s): ", paste(args[!known], collapse = ", "))
  seasons <- parse_seasons(option_value(args, "seasons", "2022:2026"))
  max_pages <- as.integer(option_value(args, "max-pages", "50"))
  pause_seconds <- as.numeric(option_value(args, "pause-seconds", "1"))
  profile_pause_seconds <- as.numeric(option_value(args, "profile-pause-seconds", "1"))
  cache_dir <- option_value(args, "cache-dir", "exports/cache/basket_shooting_history")
  max_attempts <- as.integer(option_value(args, "max-attempts", "5"))
  initial_backoff <- as.numeric(option_value(args, "initial-backoff", "2"))
  checkpoint_requests <- as.integer(option_value(args, "checkpoint-requests", "100"))
  checkpoint_rows <- as.integer(option_value(args, "checkpoint-rows", "100"))
  min_free_disk_mb <- as.numeric(option_value(args, "min-free-disk-mb", "1024"))
  write <- "--write" %in% args
  with_profiles <- "--with-profiles" %in% args
  refresh_cache <- "--refresh-cache" %in% args
  stop_after_checkpoint <- "--stop-after-checkpoint" %in% args
  if (is.na(max_pages) || max_pages < 1L) stop("--max-pages must be a positive integer")
  if (is.na(pause_seconds) || pause_seconds < 0) stop("--pause-seconds must be non-negative")
  if (is.na(profile_pause_seconds) || profile_pause_seconds < 0) {
    stop("--profile-pause-seconds must be non-negative")
  }
  if (!nzchar(cache_dir)) stop("--cache-dir must not be empty")
  if (is.na(max_attempts) || max_attempts < 1L) stop("--max-attempts must be positive")
  if (is.na(initial_backoff) || initial_backoff < 0) {
    stop("--initial-backoff must be non-negative")
  }
  if (is.na(checkpoint_requests) || checkpoint_requests < 1L) {
    stop("--checkpoint-requests must be positive")
  }
  if (is.na(checkpoint_rows) || checkpoint_rows < 1L) {
    stop("--checkpoint-rows must be positive")
  }
  if (is.na(min_free_disk_mb) || min_free_disk_mb < 0) {
    stop("--min-free-disk-mb must be non-negative")
  }

  if (write && stop_after_checkpoint) {
    stop("--stop-after-checkpoint cannot be combined with --write")
  }
  monitor <- new_sanity_monitor(
    cache_dir, checkpoint_requests,
    stop_after_checkpoint = stop_after_checkpoint
  )
  # A full disk makes every download fail mid-run; refuse to start instead.
  free_disk <- free_disk_bytes(cache_dir)
  if (is.na(free_disk)) {
    warning("Could not determine free disk space; skipping the disk check")
  } else if (free_disk < min_free_disk_mb * 1024^2) {
    stop("Only ", format_bytes(free_disk), " free on the cache volume; need at least ",
         min_free_disk_mb, " MB (--min-free-disk-mb). Free some space and rerun.")
  }
  preflight <- c(
    paste("seasons:", paste(seasons, collapse = ", ")),
    paste("maximum accumulated-page requests:", length(seasons) * max_pages),
    paste("profiles enabled:", with_profiles),
    paste("cache directory:", normalizePath(cache_dir, winslash = "/", mustWork = TRUE)),
    paste("HTTP checkpoint:", checkpoint_requests, "requests"),
    paste("SQL row checkpoint:", checkpoint_rows, "rows")
  )
  if (write) {
    preflight_con <- connect_etl_db()
    database_lines <- tryCatch(
      database_preflight(preflight_con),
      finally = DBI::dbDisconnect(preflight_con)
    )
    preflight <- c(preflight, database_lines)
  }
  write_sanity_report(monitor, "PREFLIGHT BEFORE SCRAPE/BACKFILL", preflight)

  page_fetcher <- function(year, page, lang = "he") fetch_basket_page(
    year, page, cache_dir = cache_dir, refresh_cache = refresh_cache,
    max_attempts = max_attempts, initial_backoff = initial_backoff,
    monitor = monitor, lang = lang
  )
  profile_fetcher <- function(year, player_id) fetch_basket_player_profile(
    year, player_id, cache_dir = cache_dir, refresh_cache = refresh_cache,
    max_attempts = max_attempts, initial_backoff = initial_backoff,
    monitor = monitor
  )

  failures <- new_failure_log()
  results <- lapply(seasons, function(year) {
    cat("Fetching Basket season", year, "...\n")
    scrape_basket_season(
      year, max_pages = max_pages, pause_seconds = pause_seconds,
      fetch_page = page_fetcher, monitor = monitor, failures = failures
    )
  })
  players <- do.call(rbind, lapply(results, `[[`, "players"))
  pages <- do.call(rbind, lapply(results, `[[`, "pages"))
  if (is.null(players)) stop("No player-seasons downloaded from any season")
  cat(sprintf("Fetched %d player-seasons from %d pages (%s).\n",
              nrow(players), nrow(pages), paste(seasons, collapse = ", ")))
  profiles <- NULL
  if (with_profiles) {
    cat("Fetching", nrow(players), "season-player profile pages...\n")
    profiles <- scrape_basket_profiles(
      players, pause_seconds = profile_pause_seconds,
      fetch_profile = profile_fetcher, monitor = monitor, failures = failures
    )
    cat("Fetched", if (is.null(profiles)) 0L else nrow(profiles),
        "season-player profiles.\n")
    # Every page is on disk by now, so an unmapped position costs a rerun off
    # the cache rather than the download it used to abort.
    profiles <- tryCatch(resolve_profile_positions(profiles), error = function(e) {
      record_failure(failures, "position", NA_integer_, "lookup",
                     conditionMessage(e))
      profiles
    })
  }

  download_failures <- failure_lines(failures)
  cat(paste(download_failures, collapse = "\n"), "\n")
  if (!is.null(failure_frame(failures))) {
    write_sanity_report(monitor, "DOWNLOAD COMPLETE WITH FAILURES", download_failures)
    assert_no_download_failures(failures)
  }

  before_write <- c(validate_scraped_data(
    players, pages, profiles, label = "Pre-backfill"
  ), download_failures)
  if (!monitor$checkpoint_emitted) {
    before_write <- c(before_write, sprintf(
      "100-request checkpoint not reached (%d real HTTP attempts; remaining data came from cache or the run was smaller)",
      monitor$network_attempts
    ))
  }
  write_sanity_report(monitor, "FINAL SCRAPE CHECK BEFORE SQL BACKFILL", before_write)

  if (!write) {
    cat("Dry run only; pass --write to upsert.\n")
    return(invisible(list(players = players, pages = pages, profiles = profiles)))
  }
  con <- connect_etl_db()
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  run_id <- write_scrape(
    con, seasons, players, pages, profiles, checkpoint_rows = checkpoint_rows,
    on_row_checkpoint = function(n, lines) write_sanity_report(
      monitor, paste0("SANITY CHECK AFTER FIRST ", n, " SQL ROWS"), lines
    )
  )
  after_write <- validate_written_scrape(con, run_id, players, profiles)
  write_sanity_report(monitor, "POST-BACKFILL SQL RECONCILIATION", after_write)
  cat("Completed import run", format(run_id, scientific = FALSE), "\n")
  invisible(run_id)
}

if (sys.nframe() == 0L) {
  tryCatch(
    main(),
    basket_checkpoint_stop = function(e) cat(conditionMessage(e), "\n")
  )
}
