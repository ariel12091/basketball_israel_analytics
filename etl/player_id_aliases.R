# Player identity alias support.
#
# Some upstream game payloads assign more than one player_id to the same
# real player within a team/season, while others reuse an id for a different
# player in only some games. Keep both corrections explicit and data-driven
# so ETL and retroactive backfills use the same mapping.

default_player_id_aliases <- function() {
  same_team_reason <-
    "same team/season duplicate display name with split lineup identity"
  tibble::tibble(
    game_year = c(2026L, 2026L, 2025L, 2026L, 2026L, 2026L),
    team_id = c(15L, 7L, 13L, 13L, 9L, 6L),
    alias_player_id = c(2136L, 2143L, 27817L, 2046L, 2052L, 1982L),
    canonical_player_id = c(1251L, 1262L, 3206L, 1165L, 1110L, 1143L),
    player_name = c(
      "SAGIV DVIR",
      "AMIR DANON",
      "ALON DANIELI",
      "BEN ALTSHULER",
      "NOAM AVIVI",
      "DJ BURNS"
    ),
    reason = c(
      same_team_reason,
      same_team_reason,
      same_team_reason,
      "cross-team season re-mint: id 2046 (Maccabi Raanana) is the same person as canonical 1165 (Galil Elion)",
      "cross-team season re-mint: id 2052 (Bnei Herzliya) is the same person as canonical 1110 (Galil Elion)",
      "cross-team season re-mint: id 1982 (Bnei Herzliya) is the same person as canonical 1143 (Rishon Lezion)"
    ),
    # Same-team duplicates pollute that team's lineups/on-off and must be
    # scrubbed from base data; cross-team re-mints are identity-dictionary
    # merges only (Tab 5) — base data deliberately keeps the split ids, so
    # the alias-residue safeguard must not treat their rows as corruption.
    canonicalize_base = c(TRUE, TRUE, TRUE, FALSE, FALSE, FALSE)
  )
}

retired_default_player_id_aliases <- function() {
  tibble::tibble(
    game_year = c(2026L, 2026L),
    team_id = c(5L, 12L),
    alias_player_id = c(2152L, 1277L),
    canonical_player_id = c(2060L, 1183L),
    reason = c(
      "retired: Holon 2060 is a reused provider id, not a season-wide duplicate of 2152",
      "retired: Kiryat Ata 1183 is a reused provider id, not a season-wide duplicate of 1277"
    )
  )
}

default_player_id_game_overrides <- function() {
  dplyr::bind_rows(
    tibble::tibble(
      game_id = c(165L, 199L, 356L, 361L, 383L, 384L, 385L),
      game_year = 2026L,
      team_id = 5L,
      alias_player_id = 2060L,
      canonical_player_id = 2152L,
      player_name = "J'VON MCCORMICK",
      reason = "provider reused Josh Hagins player_id for J'Von McCormick in this game"
    ),
    tibble::tibble(
      game_id = c(
        51L, 57L, 62L, 72L, 78L, 84L, 90L, 97L, 102L, 114L,
        119L, 126L, 130L, 142L, 148L, 155L, 160L, 167L,
        208L, 226L, 293L
      ),
      game_year = 2026L,
      team_id = 12L,
      alias_player_id = 1183L,
      canonical_player_id = 1277L,
      player_name = "ITAY ZLOTOLOV",
      reason = "provider reused DeAndre Williams player_id for Itay Zlotolov in this game"
    )
  )
}

ensure_player_id_aliases_table <- function(pg, schema = SCHEMA) {
  DBI::dbExecute(
    pg,
    sprintf(
      'CREATE TABLE IF NOT EXISTS "%s"."player_id_aliases" (
         game_year int NOT NULL,
         team_id int NOT NULL,
         alias_player_id int NOT NULL,
         canonical_player_id int NOT NULL,
         player_name text,
         reason text,
         active boolean NOT NULL DEFAULT true,
         canonicalize_base boolean NOT NULL DEFAULT true,
         created_at timestamptz NOT NULL DEFAULT now(),
         updated_at timestamptz NOT NULL DEFAULT now(),
         PRIMARY KEY (game_year, team_id, alias_player_id),
         CHECK (alias_player_id <> canonical_player_id)
       )',
      schema
    )
  )

  # Self-migration for tables created before the flag existed.
  DBI::dbExecute(
    pg,
    sprintf(
      'ALTER TABLE "%s"."player_id_aliases"
         ADD COLUMN IF NOT EXISTS canonicalize_base boolean NOT NULL DEFAULT true',
      schema
    )
  )

  DBI::dbExecute(
    pg,
    sprintf(
      'CREATE INDEX IF NOT EXISTS player_id_aliases_canonical_idx
         ON "%s"."player_id_aliases" (game_year, team_id, canonical_player_id)
         WHERE active',
      schema
    )
  )

  invisible(TRUE)
}

ensure_player_id_game_overrides_table <- function(pg, schema = SCHEMA) {
  DBI::dbExecute(
    pg,
    sprintf(
      'CREATE TABLE IF NOT EXISTS "%s"."player_id_game_overrides" (
         game_id int NOT NULL,
         game_year int NOT NULL,
         team_id int NOT NULL,
         alias_player_id int NOT NULL,
         canonical_player_id int NOT NULL,
         player_name text,
         reason text,
         active boolean NOT NULL DEFAULT true,
         created_at timestamptz NOT NULL DEFAULT now(),
         updated_at timestamptz NOT NULL DEFAULT now(),
         PRIMARY KEY (game_id, team_id, alias_player_id),
         CHECK (alias_player_id <> canonical_player_id)
       )',
      schema
    )
  )

  DBI::dbExecute(
    pg,
    sprintf(
      'CREATE INDEX IF NOT EXISTS player_id_game_overrides_lookup_idx
         ON "%s"."player_id_game_overrides" (game_year, team_id, alias_player_id, game_id)
         WHERE active',
      schema
    )
  )

  invisible(TRUE)
}

ensure_player_id_corrections_tables <- function(pg, schema = SCHEMA) {
  ensure_player_id_aliases_table(pg, schema)
  ensure_player_id_game_overrides_table(pg, schema)
  invisible(TRUE)
}

seed_default_player_id_aliases <- function(pg, schema = SCHEMA, aliases = default_player_id_aliases()) {
  ensure_player_id_aliases_table(pg, schema)
  retired_aliases <- retired_default_player_id_aliases()

  if (nrow(aliases)) {
    if (!"canonicalize_base" %in% names(aliases)) aliases$canonicalize_base <- TRUE
    sql <- sprintf(
      'INSERT INTO "%s"."player_id_aliases"
         (game_year, team_id, alias_player_id, canonical_player_id, player_name, reason, canonicalize_base)
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       ON CONFLICT (game_year, team_id, alias_player_id) DO UPDATE
       SET canonical_player_id = EXCLUDED.canonical_player_id,
           player_name = EXCLUDED.player_name,
           reason = EXCLUDED.reason,
           active = true,
           canonicalize_base = EXCLUDED.canonicalize_base,
           updated_at = now()',
      schema
    )

    for (i in seq_len(nrow(aliases))) {
      DBI::dbExecute(
        pg,
        sql,
        params = list(
          as.integer(aliases$game_year[[i]]),
          as.integer(aliases$team_id[[i]]),
          as.integer(aliases$alias_player_id[[i]]),
          as.integer(aliases$canonical_player_id[[i]]),
          as.character(aliases$player_name[[i]]),
          as.character(aliases$reason[[i]]),
          isTRUE(aliases$canonicalize_base[[i]])
        )
      )
    }
  }

  if (nrow(retired_aliases)) {
    retire_sql <- sprintf(
      'UPDATE "%s"."player_id_aliases"
          SET active = false,
              reason = $5,
              updated_at = now()
        WHERE game_year = $1
          AND team_id = $2
          AND alias_player_id = $3
          AND canonical_player_id = $4',
      schema
    )

    for (i in seq_len(nrow(retired_aliases))) {
      DBI::dbExecute(
        pg,
        retire_sql,
        params = list(
          as.integer(retired_aliases$game_year[[i]]),
          as.integer(retired_aliases$team_id[[i]]),
          as.integer(retired_aliases$alias_player_id[[i]]),
          as.integer(retired_aliases$canonical_player_id[[i]]),
          as.character(retired_aliases$reason[[i]])
        )
      )
    }
  }

  invisible(nrow(aliases))
}

seed_default_player_id_game_overrides <- function(pg, schema = SCHEMA, overrides = default_player_id_game_overrides()) {
  ensure_player_id_game_overrides_table(pg, schema)
  if (!nrow(overrides)) return(invisible(0L))

  sql <- sprintf(
    'INSERT INTO "%s"."player_id_game_overrides"
       (game_id, game_year, team_id, alias_player_id, canonical_player_id, player_name, reason)
     VALUES ($1, $2, $3, $4, $5, $6, $7)
     ON CONFLICT (game_id, team_id, alias_player_id) DO UPDATE
     SET game_year = EXCLUDED.game_year,
         canonical_player_id = EXCLUDED.canonical_player_id,
         player_name = EXCLUDED.player_name,
         reason = EXCLUDED.reason,
         active = true,
         updated_at = now()',
    schema
  )

  for (i in seq_len(nrow(overrides))) {
    DBI::dbExecute(
      pg,
      sql,
      params = list(
        as.integer(overrides$game_id[[i]]),
        as.integer(overrides$game_year[[i]]),
        as.integer(overrides$team_id[[i]]),
        as.integer(overrides$alias_player_id[[i]]),
        as.integer(overrides$canonical_player_id[[i]]),
        as.character(overrides$player_name[[i]]),
        as.character(overrides$reason[[i]])
      )
    )
  }

  invisible(nrow(overrides))
}

load_player_id_aliases <- function(pg, schema = SCHEMA, game_years = NULL, team_ids = NULL) {
  ensure_player_id_corrections_tables(pg, schema)
  if (!exists("ensure_player_identity_dictionary", mode = "function")) {
    stop(
      "player_identity_dictionary.R must be sourced before loading player ID corrections",
      call. = FALSE
    )
  }
  ensure_player_identity_dictionary(pg, schema)

  sql <- sprintf(
    'SELECT
       game_id,
       game_year,
       team_id,
       alias_player_id,
       canonical_player_id,
       player_name,
       reason,
       correction_scope
     FROM "%s"."player_identity_aliases_v"
     ORDER BY game_year, team_id, alias_player_id, game_id NULLS FIRST',
    schema
  )
  aliases <- DBI::dbGetQuery(pg, sql)

  if (!nrow(aliases)) return(aliases)
  if (!is.null(game_years) && length(game_years)) {
    aliases <- aliases[aliases$game_year %in% as.integer(game_years), , drop = FALSE]
  }
  if (!is.null(team_ids) && length(team_ids)) {
    aliases <- aliases[aliases$team_id %in% as.integer(team_ids), , drop = FALSE]
  }
  aliases
}

normalize_player_id_aliases <- function(aliases) {
  if (is.null(aliases) || !nrow(aliases)) return(aliases)
  if (!"game_id" %in% names(aliases)) aliases$game_id <- NA_integer_
  aliases |>
    dplyr::mutate(
      game_id = suppressWarnings(as.integer(game_id)),
      game_year = suppressWarnings(as.integer(game_year)),
      team_id = suppressWarnings(as.integer(team_id)),
      alias_player_id = suppressWarnings(as.integer(alias_player_id)),
      canonical_player_id = suppressWarnings(as.integer(canonical_player_id))
    )
}

canonicalize_player_column <- function(df, aliases, column = "player_id") {
  if (is.null(df) || !nrow(df) || !nrow(aliases) || !(column %in% names(df))) return(df)
  if (!all(c("game_year", "team_id") %in% names(df))) return(df)

  aliases <- normalize_player_id_aliases(aliases)
  original <- df[[column]]
  lookup <- data.frame(
    .row_id = seq_len(nrow(df)),
    game_id = if ("game_id" %in% names(df)) suppressWarnings(as.integer(df$game_id)) else NA_integer_,
    game_year = suppressWarnings(as.integer(df$game_year)),
    team_id = suppressWarnings(as.integer(df$team_id)),
    alias_player_id = suppressWarnings(as.integer(original)),
    stringsAsFactors = FALSE
  )

  scoped_aliases <- aliases[!is.na(aliases$game_id), , drop = FALSE]
  season_aliases <- aliases[is.na(aliases$game_id), , drop = FALSE]

  mapped <- lookup
  if (nrow(scoped_aliases)) {
    mapped <- dplyr::left_join(
      mapped,
      scoped_aliases[, c("game_id", "game_year", "team_id", "alias_player_id", "canonical_player_id"), drop = FALSE] |>
        dplyr::rename(scoped_canonical_player_id = canonical_player_id),
      by = c("game_id", "game_year", "team_id", "alias_player_id")
    )
  } else {
    mapped$scoped_canonical_player_id <- NA_integer_
  }
  if (nrow(season_aliases)) {
    mapped <- dplyr::left_join(
      mapped,
      season_aliases[, c("game_year", "team_id", "alias_player_id", "canonical_player_id"), drop = FALSE] |>
        dplyr::rename(season_canonical_player_id = canonical_player_id),
      by = c("game_year", "team_id", "alias_player_id")
    )
  } else {
    mapped$season_canonical_player_id <- NA_integer_
  }

  mapped <- mapped[order(mapped$.row_id), , drop = FALSE]
  canonical <- dplyr::coalesce(
    mapped$scoped_canonical_player_id,
    mapped$season_canonical_player_id
  )
  has_map <- is.finite(canonical) & !is.na(canonical)
  if (!any(has_map)) return(df)

  if (is.character(original)) {
    replacement <- as.character(original)
    replacement[has_map] <- as.character(as.integer(canonical[has_map]))
  } else {
    replacement <- suppressWarnings(as.integer(original))
    replacement[has_map] <- as.integer(canonical[has_map])
  }
  df[[column]] <- replacement
  df
}

canonicalize_actions_player_ids <- function(actions_df, aliases) {
  if (is.null(actions_df) || !nrow(actions_df) || !nrow(aliases)) return(actions_df)
  for (col in intersect(c("player_id", "parameters_player_in", "parameters_player_out", "parameters_player", "parameters_fouled_on"), names(actions_df))) {
    actions_df <- canonicalize_player_column(actions_df, aliases, col)
  }
  actions_df
}

canonicalize_starter_player_ids <- function(starters_df, aliases) {
  if (is.null(starters_df) || !nrow(starters_df) || !nrow(aliases)) return(starters_df)
  starters_df <- canonicalize_player_column(starters_df, aliases, "player_id")
  if (!all(c("game_id", "team_id", "player_id") %in% names(starters_df))) return(starters_df)

  if (!"starter" %in% names(starters_df)) starters_df$starter <- FALSE
  key_cols <- c("game_id", "team_id", "player_id")
  if ("game_year" %in% names(starters_df)) {
    key_cols <- c("game_id", "game_year", "team_id", "player_id")
  }
  starters_df |>
    dplyr::mutate(starter = dplyr::coalesce(as.logical(starter), FALSE)) |>
    dplyr::group_by(dplyr::across(dplyr::all_of(key_cols))) |>
    dplyr::summarise(starter = any(starter, na.rm = TRUE), .groups = "drop")
}

first_present_value <- function(x) {
  if (is.logical(x)) return(any(x, na.rm = TRUE))
  if (is.numeric(x) || is.integer(x)) {
    x <- x[!is.na(x)]
    return(if (length(x)) x[[1]] else NA)
  }
  x_chr <- as.character(x)
  x_chr <- x_chr[!is.na(x_chr) & nzchar(trimws(x_chr))]
  if (length(x_chr)) x_chr[[1]] else NA_character_
}

canonicalize_roster_player_ids <- function(roster_df, aliases) {
  if (is.null(roster_df) || !nrow(roster_df) || !nrow(aliases)) return(roster_df)
  roster_df <- canonicalize_player_column(roster_df, aliases, "player_id")
  if (!all(c("game_id", "team_id", "player_id") %in% names(roster_df))) return(roster_df)

  non_key_cols <- setdiff(names(roster_df), c("game_id", "team_id", "player_id"))
  roster_df |>
    dplyr::group_by(game_id, team_id, player_id) |>
    dplyr::summarise(
      dplyr::across(dplyr::all_of(non_key_cols), first_present_value),
      .groups = "drop"
    )
}

player_aliases_touched <- function(df, aliases, columns = "player_id") {
  if (is.null(df) || !nrow(df) || !nrow(aliases)) return(FALSE)
  if (!all(c("game_year", "team_id") %in% names(df))) return(FALSE)

  aliases <- normalize_player_id_aliases(aliases)
  scoped_aliases <- aliases[!is.na(aliases$game_id), , drop = FALSE]
  season_aliases <- aliases[is.na(aliases$game_id), , drop = FALSE]

  for (col in intersect(columns, names(df))) {
    probe <- data.frame(
      game_id = if ("game_id" %in% names(df)) suppressWarnings(as.integer(df$game_id)) else NA_integer_,
      game_year = suppressWarnings(as.integer(df$game_year)),
      team_id = suppressWarnings(as.integer(df$team_id)),
      alias_player_id = suppressWarnings(as.integer(df[[col]])),
      stringsAsFactors = FALSE
    )
    probe <- probe[is.finite(probe$game_year) & is.finite(probe$team_id) & is.finite(probe$alias_player_id), , drop = FALSE]
    if (!nrow(probe)) next
    if (nrow(scoped_aliases)) {
      scoped_probe <- probe[is.finite(probe$game_id) & !is.na(probe$game_id), , drop = FALSE]
      if (nrow(scoped_probe)) {
        hit <- dplyr::semi_join(
          dplyr::distinct(scoped_probe),
          scoped_aliases[, c("game_id", "game_year", "team_id", "alias_player_id"), drop = FALSE],
          by = c("game_id", "game_year", "team_id", "alias_player_id")
        )
        if (nrow(hit)) return(TRUE)
      }
    }
    if (nrow(season_aliases)) {
      hit <- dplyr::semi_join(
        dplyr::distinct(probe),
        season_aliases[, c("game_year", "team_id", "alias_player_id"), drop = FALSE],
        by = c("game_year", "team_id", "alias_player_id")
      )
      if (nrow(hit)) return(TRUE)
    }
  }
  FALSE
}

empty_player_alias_residue_frame <- function() {
  data.frame(
    source_table = character(),
    source_column = character(),
    correction_scope = character(),
    game_year = integer(),
    team_id = integer(),
    alias_player_id = integer(),
    canonical_player_id = integer(),
    rows = integer(),
    games = integer(),
    game_ids = character(),
    stringsAsFactors = FALSE
  )
}

player_alias_dataframe_residue_summary <- function(df, aliases, source_table, columns = "player_id") {
  if (is.null(df) || !nrow(df) || is.null(aliases) || !nrow(aliases)) {
    return(empty_player_alias_residue_frame())
  }
  if (!all(c("game_year", "team_id") %in% names(df))) {
    stop(sprintf(
      "%s is missing game_year/team_id, so player_id alias residue cannot be verified",
      source_table
    ), call. = FALSE)
  }

  aliases <- normalize_player_id_aliases(aliases)
  columns <- intersect(columns, names(df))
  if (!length(columns)) return(empty_player_alias_residue_frame())

  hits <- list()
  for (col in columns) {
    probe <- data.frame(
      source_table = source_table,
      source_column = col,
      game_id = if ("game_id" %in% names(df)) suppressWarnings(as.integer(df$game_id)) else NA_integer_,
      game_year = suppressWarnings(as.integer(df$game_year)),
      team_id = suppressWarnings(as.integer(df$team_id)),
      alias_player_id = suppressWarnings(as.integer(df[[col]])),
      stringsAsFactors = FALSE
    )
    probe <- probe[
      is.finite(probe$game_year) &
        is.finite(probe$team_id) &
        is.finite(probe$alias_player_id),
      ,
      drop = FALSE
    ]
    if (!nrow(probe)) next

    scoped_aliases <- aliases[!is.na(aliases$game_id), , drop = FALSE]
    if (nrow(scoped_aliases) && any(is.finite(probe$game_id))) {
      scoped_hit <- dplyr::inner_join(
        probe[is.finite(probe$game_id), , drop = FALSE],
        scoped_aliases[, c("game_id", "game_year", "team_id", "alias_player_id", "canonical_player_id"), drop = FALSE] |>
          dplyr::mutate(correction_scope = "game"),
        by = c("game_id", "game_year", "team_id", "alias_player_id")
      )
      if (nrow(scoped_hit)) hits[[length(hits) + 1L]] <- scoped_hit
    }

    season_aliases <- aliases[is.na(aliases$game_id), , drop = FALSE]
    if (nrow(season_aliases)) {
      season_hit <- dplyr::inner_join(
        probe,
        season_aliases[, c("game_year", "team_id", "alias_player_id", "canonical_player_id"), drop = FALSE] |>
          dplyr::mutate(correction_scope = "season"),
        by = c("game_year", "team_id", "alias_player_id")
      )
      if (nrow(season_hit)) hits[[length(hits) + 1L]] <- season_hit
    }
  }

  if (!length(hits)) return(empty_player_alias_residue_frame())

  dplyr::bind_rows(hits) |>
    dplyr::mutate(game_ids_chr = dplyr::if_else(is.na(game_id), "", as.character(game_id))) |>
    dplyr::group_by(
      source_table,
      source_column,
      correction_scope,
      game_year,
      team_id,
      alias_player_id,
      canonical_player_id
    ) |>
    dplyr::summarise(
      rows = dplyr::n(),
      games = dplyr::n_distinct(game_id[!is.na(game_id)]),
      game_ids = paste(sort(unique(game_ids_chr[nzchar(game_ids_chr)])), collapse = ","),
      .groups = "drop"
    ) |>
    dplyr::arrange(source_table, source_column, game_year, team_id, alias_player_id) |>
    as.data.frame()
}

assert_no_player_alias_dataframe_residue <- function(
  df,
  aliases,
  source_table,
  columns = "player_id",
  log_msg = message
) {
  residue <- player_alias_dataframe_residue_summary(df, aliases, source_table, columns)
  if (!nrow(residue)) return(invisible(residue))

  for (i in seq_len(nrow(residue))) {
    log_msg(sprintf(
      "  Alias residue pre-write guard: %s.%s has %d active alias row(s) for game_year=%d team_id=%d alias_player_id=%d -> canonical_player_id=%d (game_ids=%s)",
      residue$source_table[[i]],
      residue$source_column[[i]],
      as.integer(residue$rows[[i]]),
      as.integer(residue$game_year[[i]]),
      as.integer(residue$team_id[[i]]),
      as.integer(residue$alias_player_id[[i]]),
      as.integer(residue$canonical_player_id[[i]]),
      residue$game_ids[[i]]
    ), "ERROR")
  }

  stop(
    sprintf("Active player_id alias residue remains in staged %s", source_table),
    call. = FALSE
  )
}

player_alias_residue_summary <- function(pg, schema = SCHEMA, game_ids = NULL) {
  ensure_player_id_corrections_tables(pg, schema)

  ids_sql <- NULL
  if (!is.null(game_ids)) {
    game_ids <- sort(unique(as.integer(game_ids)))
    game_ids <- game_ids[is.finite(game_ids)]
    if (length(game_ids)) ids_sql <- paste(game_ids, collapse = ",")
  }

  full_rosters_filter <- if (!is.null(ids_sql)) sprintf("AND fr.game_id IN (%s)", ids_sql) else ""
  lineups_filter <- if (!is.null(ids_sql)) sprintf("AND ll.game_id IN (%s)", ids_sql) else ""

  DBI::dbGetQuery(
    pg,
    sprintf(
      'WITH aliases AS (
         -- Only aliases meant to be scrubbed from base data. Sync-only
         -- cross-team merges (canonicalize_base = false) keep their split
         -- ids in base/derived tables by design and are not residue.
         SELECT NULL::int AS game_id, game_year, team_id, alias_player_id
           FROM "%s"."player_id_aliases"
          WHERE active AND canonicalize_base
         UNION ALL
         SELECT game_id, game_year, team_id, alias_player_id
           FROM "%s"."player_id_game_overrides"
          WHERE active
       ),
       season_aliases AS (
         SELECT game_year, team_id, alias_player_id
           FROM aliases
          WHERE game_id IS NULL
       ),
       hits AS (
         SELECT
           \'full_rosters\'::text AS source_table,
           fr.game_year,
           fr.team_id,
           fr.player_id AS alias_player_id,
           fr.game_id
         FROM "%s"."full_rosters" fr
         JOIN aliases a
           ON a.game_year = fr.game_year
          AND a.team_id = fr.team_id
          AND a.alias_player_id = fr.player_id
          AND (a.game_id IS NULL OR a.game_id = fr.game_id)
         WHERE TRUE
           %s
         UNION ALL
         SELECT
           \'lineups_lookup\'::text AS source_table,
           ll.game_year,
           ll.team_id,
           ll.player_id AS alias_player_id,
           ll.game_id
         FROM "%s"."lineups_lookup" ll
         JOIN aliases a
           ON a.game_year = ll.game_year
          AND a.team_id = ll.team_id
          AND a.alias_player_id = ll.player_id
          AND (a.game_id IS NULL OR a.game_id = ll.game_id)
         WHERE TRUE
           %s
         UNION ALL
         SELECT
           \'lineups_lookup_on\'::text AS source_table,
           llo.game_year,
           llo.team_id,
           llo.player_id AS alias_player_id,
           NULL::int AS game_id
         FROM "%s"."lineups_lookup_on" llo
         JOIN season_aliases a
           ON a.game_year = llo.game_year
          AND a.team_id = llo.team_id
          AND a.alias_player_id = llo.player_id
         UNION ALL
         SELECT
           \'sub_lineups_stats\'::text AS source_table,
           ss.game_year,
           ss.team_id,
           a.alias_player_id,
           NULL::int AS game_id
         FROM "%s"."sub_lineups_stats" ss
         JOIN season_aliases a
           ON a.game_year = ss.game_year
          AND a.team_id = ss.team_id
          AND ss.player_ids && ARRAY[a.alias_player_id]::int4[]
       )
       SELECT
         source_table,
         game_year,
         team_id,
         alias_player_id,
         count(*)::int AS rows,
         count(DISTINCT game_id)::int AS games
       FROM hits
       GROUP BY source_table, game_year, team_id, alias_player_id
       ORDER BY source_table, game_year, team_id, alias_player_id',
      schema,
      schema,
      schema, full_rosters_filter,
      schema, lineups_filter,
      schema,
      schema
    )
  )
}

player_alias_base_residue_summary <- function(pg, schema = SCHEMA, game_ids) {
  ensure_player_id_corrections_tables(pg, schema)

  game_ids <- sort(unique(as.integer(game_ids)))
  game_ids <- game_ids[is.finite(game_ids)]
  if (!length(game_ids)) {
    return(data.frame(
      source_table = character(),
      source_column = character(),
      correction_scope = character(),
      game_year = integer(),
      team_id = integer(),
      alias_player_id = integer(),
      canonical_player_id = integer(),
      rows = integer(),
      games = integer(),
      game_ids = character(),
      stringsAsFactors = FALSE
    ))
  }
  ids_sql <- paste(game_ids, collapse = ",")

  DBI::dbGetQuery(
    pg,
    sprintf(
      'WITH aliases AS (
         SELECT
           NULL::int AS game_id,
           game_year,
           team_id,
           alias_player_id,
           canonical_player_id,
           \'season\'::text AS correction_scope
         FROM "%s"."player_id_aliases"
         WHERE active
         UNION ALL
         SELECT
           game_id,
           game_year,
           team_id,
           alias_player_id,
           canonical_player_id,
           \'game\'::text AS correction_scope
         FROM "%s"."player_id_game_overrides"
         WHERE active
       ),
       hits AS (
         SELECT
           \'full_rosters\'::text AS source_table,
           \'player_id\'::text AS source_column,
           a.correction_scope,
           fr.game_year,
           fr.team_id,
           fr.player_id AS alias_player_id,
           a.canonical_player_id,
           fr.game_id
         FROM "%s"."full_rosters" fr
         JOIN aliases a
           ON a.game_year = fr.game_year
          AND a.team_id = fr.team_id
          AND a.alias_player_id = fr.player_id
          AND (a.game_id IS NULL OR a.game_id = fr.game_id)
         WHERE fr.game_id IN (%s)
         UNION ALL
         SELECT
           \'actions_clean\'::text AS source_table,
           \'player_id\'::text AS source_column,
           a.correction_scope,
           s.game_year,
           ac.team_id,
           ac.player_id AS alias_player_id,
           a.canonical_player_id,
           ac.game_id
         FROM "%s"."actions_clean" ac
         JOIN "%s"."schedule" s
           ON s.game_id = ac.game_id
         JOIN aliases a
           ON a.game_year = s.game_year
          AND a.team_id = ac.team_id
          AND a.alias_player_id = ac.player_id
          AND (a.game_id IS NULL OR a.game_id = ac.game_id)
         WHERE ac.game_id IN (%s)
         UNION ALL
         SELECT
           \'lineups_lookup\'::text AS source_table,
           \'player_id\'::text AS source_column,
           a.correction_scope,
           ll.game_year,
           ll.team_id,
           ll.player_id AS alias_player_id,
           a.canonical_player_id,
           ll.game_id
         FROM "%s"."lineups_lookup" ll
         JOIN aliases a
           ON a.game_year = ll.game_year
          AND a.team_id = ll.team_id
          AND a.alias_player_id = ll.player_id
          AND (a.game_id IS NULL OR a.game_id = ll.game_id)
         WHERE ll.game_id IN (%s)
       )
       SELECT
         source_table,
         source_column,
         correction_scope,
         game_year,
         team_id,
         alias_player_id,
         canonical_player_id,
         count(*)::int AS rows,
         count(DISTINCT game_id)::int AS games,
         string_agg(DISTINCT game_id::text, \',\' ORDER BY game_id::text) AS game_ids
       FROM hits
       GROUP BY
         source_table,
         source_column,
         correction_scope,
         game_year,
         team_id,
         alias_player_id,
         canonical_player_id
       ORDER BY source_table, game_year, team_id, alias_player_id',
      schema,
      schema,
      schema, ids_sql,
      schema, schema, ids_sql,
      schema, ids_sql
    )
  )
}

assert_no_player_alias_base_residue <- function(pg, schema = SCHEMA, game_ids, log_msg = message) {
  residue <- player_alias_base_residue_summary(pg, schema, game_ids)
  if (!nrow(residue)) return(invisible(residue))

  for (i in seq_len(nrow(residue))) {
    log_msg(sprintf(
      "  Alias residue guard: %s.%s has %d active alias row(s) for game_year=%d team_id=%d alias_player_id=%d -> canonical_player_id=%d (game_ids=%s)",
      residue$source_table[[i]],
      residue$source_column[[i]],
      as.integer(residue$rows[[i]]),
      as.integer(residue$game_year[[i]]),
      as.integer(residue$team_id[[i]]),
      as.integer(residue$alias_player_id[[i]]),
      as.integer(residue$canonical_player_id[[i]]),
      residue$game_ids[[i]]
    ), "ERROR")
  }

  stop(
    sprintf(
      "Active player_id alias residue remains in base tables for game_id(s): %s",
      paste(sort(unique(as.integer(game_ids))), collapse = ", ")
    ),
    call. = FALSE
  )
}

affected_player_alias_game_ids <- function(pg, schema = SCHEMA) {
  ensure_player_id_corrections_tables(pg, schema)

  DBI::dbGetQuery(
    pg,
    sprintf(
      'WITH season_aliases AS (
         SELECT game_year, team_id, alias_player_id
           FROM "%s"."player_id_aliases"
          WHERE active
       ),
       game_overrides AS (
         SELECT game_id, game_year, team_id, alias_player_id
           FROM "%s"."player_id_game_overrides"
          WHERE active
       )
       SELECT DISTINCT game_id
         FROM (
           SELECT fr.game_id
             FROM "%s"."full_rosters" fr
             JOIN season_aliases a
               ON a.game_year = fr.game_year
              AND a.team_id = fr.team_id
              AND a.alias_player_id = fr.player_id
           UNION
           SELECT ll.game_id
             FROM "%s"."lineups_lookup" ll
             JOIN season_aliases a
               ON a.game_year = ll.game_year
              AND a.team_id = ll.team_id
              AND a.alias_player_id = ll.player_id
           UNION
           SELECT fr.game_id
             FROM "%s"."full_rosters" fr
             JOIN game_overrides a
               ON a.game_year = fr.game_year
              AND a.team_id = fr.team_id
              AND a.alias_player_id = fr.player_id
           UNION
           SELECT ll.game_id
             FROM "%s"."lineups_lookup" ll
             JOIN game_overrides a
               ON a.game_year = ll.game_year
              AND a.team_id = ll.team_id
              AND a.alias_player_id = ll.player_id
           UNION
           SELECT game_id
             FROM game_overrides
         ) g
        ORDER BY game_id',
      schema, schema, schema, schema, schema, schema
    )
  )$game_id
}

cleanup_player_alias_lineup_derivatives <- function(pg, schema = SCHEMA, game_ids, log_msg = message) {
  game_ids <- sort(unique(as.integer(game_ids)))
  game_ids <- game_ids[is.finite(game_ids)]
  if (!length(game_ids)) {
    return(invisible(list(lineups = 0L, lineups_on = 0L, sub_lineups = 0L, sub_stats = 0L)))
  }

  aliases <- load_player_id_aliases(pg, schema)
  if (!nrow(aliases)) {
    return(invisible(list(lineups = 0L, lineups_on = 0L, sub_lineups = 0L, sub_stats = 0L)))
  }

  ids_sql <- paste(game_ids, collapse = ",")
  old_lineups <- DBI::dbGetQuery(
    pg,
    sprintf(
      'WITH aliases AS (
         SELECT NULL::int AS game_id, game_year, team_id, alias_player_id
           FROM "%s"."player_id_aliases"
          WHERE active
         UNION ALL
         SELECT NULL::int AS game_id, game_year, team_id, canonical_player_id AS alias_player_id
           FROM "%s"."player_id_aliases"
          WHERE NOT active
            AND reason LIKE \'retired:%%\'
         UNION ALL
         SELECT game_id, game_year, team_id, alias_player_id
           FROM "%s"."player_id_game_overrides"
          WHERE active
       )
       SELECT DISTINCT ll.team_id, ll.game_year, ll.lineup_hash, a.alias_player_id
         FROM "%s"."lineups_lookup" ll
         JOIN aliases a
           ON a.game_year = ll.game_year
          AND a.team_id = ll.team_id
          AND a.alias_player_id = ll.player_id
          AND (a.game_id IS NULL OR a.game_id = ll.game_id)
        WHERE ll.game_id IN (%s)
          AND ll.lineup_hash IS NOT NULL',
      schema, schema, schema, schema, ids_sql
    )
  )

  lineups_on_deleted <- 0L
  sub_lineups_deleted <- 0L
  sub_stats_deleted <- 0L

  if (nrow(old_lineups)) {
    old_lineups <- dplyr::distinct(old_lineups)
    old_lineup_hashes <- dplyr::distinct(old_lineups, team_id, game_year, lineup_hash)
    values_sql <- paste(
      sprintf(
        "(%d,%d,%s)",
        as.integer(old_lineup_hashes$team_id),
        as.integer(old_lineup_hashes$game_year),
        as.character(DBI::dbQuoteLiteral(pg, old_lineup_hashes$lineup_hash))
      ),
      collapse = ","
    )
    alias_values_sql <- paste(
      sprintf(
        "(%d,%d,%s,%d)",
        as.integer(old_lineups$team_id),
        as.integer(old_lineups$game_year),
        as.character(DBI::dbQuoteLiteral(pg, old_lineups$lineup_hash)),
        as.integer(old_lineups$alias_player_id)
      ),
      collapse = ","
    )

    old_sub_stats <- DBI::dbGetQuery(
      pg,
      sprintf(
        'WITH old_lineups(team_id, game_year, lineup_hash, alias_player_id) AS (VALUES %s)
         SELECT DISTINCT s.team_id, s.game_year, s.sub_lineup_hash, o.alias_player_id
           FROM "%s"."sub_lineups" s
           JOIN old_lineups o
             ON s.team_id = o.team_id
            AND s.game_year = o.game_year
            AND s.lineup_hash = o.lineup_hash
          WHERE s.player_ids && ARRAY[o.alias_player_id]::int4[]
         UNION
         SELECT DISTINCT o.team_id, o.game_year, o.lineup_hash AS sub_lineup_hash, o.alias_player_id
           FROM old_lineups o',
        alias_values_sql, schema
      )
    )

    lineups_on_deleted <- DBI::dbExecute(
      pg,
      sprintf(
        'WITH old_lineups(team_id, game_year, lineup_hash) AS (VALUES %s)
         DELETE FROM "%s"."lineups_lookup_on" l
          USING old_lineups o
          WHERE l.team_id = o.team_id
            AND l.game_year = o.game_year
            AND l.lineup_hash = o.lineup_hash',
        values_sql, schema
      )
    )

    sub_lineups_deleted <- DBI::dbExecute(
      pg,
      sprintf(
        'WITH old_lineups(team_id, game_year, lineup_hash) AS (VALUES %s)
         DELETE FROM "%s"."sub_lineups" s
          USING old_lineups o
          WHERE s.team_id = o.team_id
            AND s.game_year = o.game_year
            AND s.lineup_hash = o.lineup_hash',
        values_sql, schema
      )
    )

    if (nrow(old_sub_stats)) {
      old_sub_stats <- dplyr::distinct(old_sub_stats)
      stats_values_sql <- paste(
        sprintf(
          "(%d,%d,%s,%d)",
          as.integer(old_sub_stats$team_id),
          as.integer(old_sub_stats$game_year),
          as.character(DBI::dbQuoteLiteral(pg, old_sub_stats$sub_lineup_hash)),
          as.integer(old_sub_stats$alias_player_id)
        ),
        collapse = ","
      )

      sub_stats_deleted <- DBI::dbExecute(
        pg,
        sprintf(
          'WITH old_sub_stats(team_id, game_year, sub_lineup_hash, alias_player_id) AS (VALUES %s)
           DELETE FROM "%s"."sub_lineups_stats" ss
            USING old_sub_stats o
            WHERE ss.team_id = o.team_id
              AND ss.game_year = o.game_year
              AND ss.sub_lineup_hash = o.sub_lineup_hash
              AND ss.player_ids && ARRAY[o.alias_player_id]::int4[]',
          stats_values_sql, schema
        )
      )
    }
  }

  if (nrow(old_lineups) || lineups_on_deleted || sub_lineups_deleted || sub_stats_deleted) {
    log_msg(sprintf(
      "  player alias cleanup: %d old lineup hash(es), %d lineups_lookup_on rows, %d sub_lineups rows, %d scoped sub_lineups_stats rows",
      nrow(old_lineup_hashes), as.integer(lineups_on_deleted), as.integer(sub_lineups_deleted), as.integer(sub_stats_deleted)
    ))
  }

  invisible(list(
    lineups = if (exists("old_lineup_hashes")) nrow(old_lineup_hashes) else 0L,
    lineups_on = as.integer(lineups_on_deleted),
    sub_lineups = as.integer(sub_lineups_deleted),
    sub_stats = as.integer(sub_stats_deleted)
  ))
}
