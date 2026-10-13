# Schedule IDs belong to a different provider namespace from PBP IDs.
# Match by season as well as ID, using the existing canonical dictionary.
map_pbp_schedule_teams <- function(schedule_rows, team_dictionary) {
  required <- c('game_year', 'team1', 'team2')
  if (!all(required %in% names(schedule_rows))) {
    stop('PBP schedule is missing season/team IDs', call. = FALSE)
  }
  map_cols <- c('game_year', 'team_id_schedule', 'team_id_rosters')
  if (!all(map_cols %in% names(team_dictionary))) {
    stop('PBP team dictionary is missing mapping columns', call. = FALSE)
  }
  keys <- paste(as.integer(team_dictionary$game_year), as.integer(team_dictionary$team_id_schedule), sep = ':')
  if (anyDuplicated(keys)) stop('PBP team dictionary has duplicate season/team keys', call. = FALSE)
  for (side in 1:2) {
    schedule_key <- paste(as.integer(schedule_rows$game_year), as.integer(schedule_rows[[paste0('team', side)]]), sep = ':')
    mapped <- as.integer(team_dictionary$team_id_rosters[match(schedule_key, keys)])
    if (anyNA(mapped) || any(mapped <= 0L)) {
      stop(sprintf('Missing schedule-to-PBP team mapping for %s',
                   paste(schedule_key[is.na(mapped) | mapped <= 0L], collapse = ', ')), call. = FALSE)
    }
    schedule_rows[[paste0('pbp_team', side)]] <- mapped
  }
  schedule_rows
}

# PBP source selection has no database side effects. The same cleaner and
# schedule checks run before accepting either the primary feed or its archive.
normalize_pbp_payload <- function(raw) {
  if (is.null(raw$result) && !is.null(raw$actions$actions)) {
    raw <- list(result = list(gameInfo = raw$actions$gameInfo,
                             actions = raw$actions$actions,
                             success = raw$actions$success,
                             boxscore = raw$boxscore))
  }
  raw
}

validate_pbp_source <- function(raw, game_id, clean_fn, expected = NULL) {
  raw <- normalize_pbp_payload(raw)
  gi <- raw$result$gameInfo
  actual_id <- suppressWarnings(as.integer(gi$gameId))
  if (length(actual_id) != 1L || is.na(actual_id) || actual_id != game_id) {
    stop('PBP payload does not identify the requested game', call. = FALSE)
  }
  if (identical(raw$result$success, FALSE)) {
    stop('PBP provider reported an unsuccessful response', call. = FALSE)
  }
  if (!isTRUE(as.logical(gi$gameFinished))) {
    stop('PBP game is not marked finished', call. = FALSE)
  }
  team_ids <- suppressWarnings(as.integer(c(gi$homeTeam$id, gi$awayTeam$id)))
  if (length(team_ids) != 2L || anyNA(team_ids) || any(team_ids <= 0L) || anyDuplicated(team_ids)) {
    stop('PBP must identify two distinct teams', call. = FALSE)
  }
  for (side in c('homeTeam', 'awayTeam')) {
    players <- gi[[side]]$players
    n_players <- if (is.data.frame(players)) nrow(players) else length(players)
    if (n_players < 5L) stop('PBP roster has fewer than five players on a side', call. = FALSE)
  }
  actions <- clean_fn(raw)
  if (!is.data.frame(actions) || !nrow(actions)) {
    stop('PBP contains no usable actions', call. = FALSE)
  }
  if (anyNA(actions$id) || anyDuplicated(actions$id)) {
    stop('PBP cleaned action IDs are missing or duplicated', call. = FALSE)
  }
  if (!all(1:4 %in% actions$quarter)) {
    stop('PBP is missing a regulation quarter after known corrections', call. = FALSE)
  }
  if (!is.null(expected)) {
    cols <- c('pbp_team1', 'pbp_team2', 'score_team1', 'score_team2')
    if (!all(cols %in% names(expected)) || nrow(expected) != 1L) {
      stop('Expected PBP schedule must contain one team/score row', call. = FALSE)
    }
    ids <- as.integer(c(expected$pbp_team1, expected$pbp_team2))
    scores <- as.numeric(c(expected$score_team1, expected$score_team2))
    if (anyNA(ids) || anyNA(scores) || any(scores < 0) || !setequal(ids, team_ids)) {
      stop('PBP teams do not match a usable schedule row', call. = FALSE)
    }
    got <- vapply(ids, function(team) sum(actions$team_score[actions$team_id == team], na.rm = TRUE), numeric(1))
    if (!identical(as.numeric(got), scores)) {
      stop(sprintf('PBP scoring totals %s do not match schedule %s',
                   paste(got, collapse = '-'), paste(scores, collapse = '-')), call. = FALSE)
    }
  }
  embedded_pbp_box(raw, game_id)
  raw
}

fetch_pbp_from_sources <- function(game_id, pbp_url, clean_fn, expected = NULL,
                                   log_msg = message,
                                   read_fn = function(url) jsonlite::read_json(url, simplifyVector = TRUE)) {
  game_id <- suppressWarnings(as.integer(game_id))
  if (length(game_id) != 1L || is.na(game_id) || game_id <= 0L) {
    stop('A positive game ID is required', call. = FALSE)
  }
  # Preserve existing explicit overrides (including game 402), but derive the
  # normal archive URL for every game without a game-specific code branch.
  override <- Sys.getenv(sprintf('PBP_GAME_%d_FALLBACK_URL', game_id), unset = '')
  primary <- if (nzchar(override)) override else as.character(pbp_url)[1]
  archive <- sprintf('https://basket.co.il/livestats/data/game-%d.json', game_id)
  urls <- unique(c(primary, archive))
  urls <- urls[!is.na(urls) & nzchar(urls)]
  errors <- character()
  for (url in urls) {
    candidate <- tryCatch(
      validate_pbp_source(read_fn(url), game_id, clean_fn, expected),
      error = function(e) {
        errors <<- c(errors, sprintf('%s: %s', url, conditionMessage(e)))
        NULL
      }
    )
    if (!is.null(candidate)) {
      if (length(errors)) {
        log_msg(sprintf('PBP fallback accepted for game %d: %s', game_id, url))
      }
      attr(candidate, 'pbp_source_url') <- url
      return(candidate)
    }
    log_msg(sprintf('PBP source rejected for game %d: %s', game_id, tail(errors, 1L)))
  }
  stop(sprintf('No valid PBP source for game %d. %s', game_id,
               paste(errors, collapse = ' | ')), call. = FALSE)
}

# Basket archives include the box score, so selecting one also supplies
# starters without another request to a failed/corrupt primary provider.
embedded_pbp_box <- function(pbp, game_id) {
  box <- pbp$result$boxscore
  if (is.null(box)) return(NULL)
  gi <- box$gameInfo
  if (!identical(suppressWarnings(as.integer(gi$gameId)), as.integer(game_id))) {
    stop('Archived box score does not identify the requested game', call. = FALSE)
  }
  pbp_teams <- as.integer(c(pbp$result$gameInfo$homeTeam$id, pbp$result$gameInfo$awayTeam$id))
  box_teams <- as.integer(c(gi$homeTeamId, gi$awayTeamId))
  if (!identical(pbp_teams, box_teams) || is.null(box$homeTeam$players) || is.null(box$awayTeam$players)) {
    stop('Archived box score teams/players do not match PBP', call. = FALSE)
  }
  list(result = list(boxscore = box))
}
