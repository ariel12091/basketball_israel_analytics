# Link basket.co.il season registrations to play-by-play player identities.
#
# Safe default: match and report only. Pass --write to fill the identity
# columns on basketball_test.basket_player_season.
#
# Only seasons present on both sides can match. Basket covers 2022-2026, the
# play-by-play side 2025 onward, so the overlap is 2025-2026 and asking for a
# season outside it is an error rather than an empty run.
#
# basket_player_id is a per-season registration id, re-minted each year in
# ascending blocks (2025: 17478-21774, 2026: 21777-25995, no overlap between
# any two seasons). Matching is therefore strictly per season, and a person
# appearing in several seasons is several unrelated registrations here.

BASKET_IDENTITY_SEASONS <- 2025:2026

# Punctuation that separates or decorates name parts on one side but not the
# other: "T.J Leaf" vs "TJ LEAF", "Justin Edler-Davis" vs "EDLER DAVIS",
# Gabriel "Iffe" Lundberg vs GABRIEL LUNDBERG. The Hebrew maqaf is here for the
# same reason it is in the shooting backfill's position lookup: it renders as a
# hyphen and is not one.
NAME_PUNCTUATION <- c(
  # Dashes that all render alike: Hebrew maqaf, then the Unicode family.
  "\u05be", "\u2010", "\u2011",
  "\u2012", "\u2013", "\u2014",
  "-", ".", "'", "\"",
  # Curly quotes, plus the Hebrew geresh and gershayim.
  "\u2018", "\u2019", "\u201c",
  "\u201d", "\u05f3", "\u05f4",
  "(", ")", ","
)

# A quoted nickname sits between the given and family name and never appears on
# the play-by-play side, so it is removed before the punctuation fold.
NICKNAME_PATTERN <- paste0(
  "[\"\u201c\u2018']",
  "[^\"\u201d\u2019']+",
  "[\"\u201d\u2019']"
)

normalize_person_name <- function(x) {
  out <- toupper(trimws(as.character(x)))
  out <- gsub(NICKNAME_PATTERN, " ", out, perl = TRUE)
  for (mark in NAME_PUNCTUATION) out <- gsub(mark, " ", out, fixed = TRUE)
  out <- gsub("[[:space:]]+", " ", out)
  out <- trimws(out)
  out[!nzchar(out)] <- NA_character_
  out
}

db_query <- function(con, sql, params = NULL) {
  # The Supabase transaction pooler drops unnamed prepared statements between
  # statements, so parameterless reads go out as simple queries.
  if (is.null(params)) {
    DBI::dbGetQuery(con, sql, immediate = TRUE)
  } else {
    DBI::dbGetQuery(con, sql, params = params)
  }
}

assert_seasons_matchable <- function(seasons) {
  outside <- setdiff(seasons, BASKET_IDENTITY_SEASONS)
  if (length(outside)) {
    stop("No play-by-play data to match against for season(s): ",
         paste(sort(outside), collapse = ", "),
         ". Basket covers 2022-2026 and the play-by-play side 2025 onward.")
  }
  invisible(seasons)
}

# One row per Basket season registration, with the team already resolved
# through schedule_team_dict. A registration whose team string is unmapped is
# returned with team_id NA and reported rather than silently dropped.
fetch_basket_registrations <- function(con, seasons) {
  rows <- db_query(con, sprintf("
    SELECT s.game_year, s.basket_player_id, s.player_name, s.player_name_en,
           s.basket_team_name, d.team_id_rosters AS team_id
    FROM basketball_test.basket_player_season s
    LEFT JOIN basketball_test.schedule_team_dict d
      ON d.game_year = s.game_year
     AND d.team_name_basket = s.basket_team_name
    WHERE s.game_year IN (%s)
    ORDER BY s.game_year, s.basket_player_id",
    paste(as.integer(seasons), collapse = ", ")))
  rows$norm_en <- normalize_person_name(rows$player_name_en)
  rows$norm_he <- normalize_person_name(rows$player_name)
  rows
}

# One row per (season, team, identity) with both name forms. display_name is
# the play-by-play name; the Hebrew comes from the roster the identity resolves
# through. canonical_player_id is the segev id every roster and PBP fact keys
# on -- it is exactly one per (identity, season, team), verified across
# 2025-2027 -- and is what makes a written mapping joinable.
fetch_pbp_identities <- function(con, seasons) {
  rows <- db_query(con, sprintf("
    SELECT DISTINCT r.game_year, r.team_id, r.identity_id, r.canonical_player_id,
           r.display_name,
           btrim(f.firstnamelocal) || ' ' || btrim(f.lastnamelocal) AS name_he
    FROM basketball_test.resolved_player_identity_v r
    -- INNER, and on game_id: resolved_player_identity_v fans every active
    -- season mapping across all of that team-season's games, so a player with
    -- a single stray roster row appears in every game of a team he never
    -- played for. Requiring the roster row for that same game keeps the pool
    -- to real appearances -- which is what let Rishon Lezion's DJ Burns into
    -- Bnei Herzliya's candidate pool and made their Burns ambiguous.
    JOIN basketball_test.full_rosters f
      ON f.game_year = r.game_year
     AND f.game_id = r.game_id
     AND f.team_id = r.team_id
     AND f.player_id = r.source_player_id
    WHERE r.game_year IN (%s)",
    paste(as.integer(seasons), collapse = ", ")))
  rows$norm_en <- normalize_person_name(rows$display_name)
  rows$norm_he <- normalize_person_name(rows$name_he)
  rows
}

# Candidates for one registration: identities on the same team-season whose
# English or Hebrew name normalizes to the same string. Scoping to the team is
# what keeps same-named players apart; a mid-season transfer is one identity
# present on both its teams, so the team scope never hides it.
match_registration <- function(registration, identities) {
  if (is.na(registration$team_id)) return(identities[0, , drop = FALSE])
  pool <- identities[identities$game_year == registration$game_year &
                       identities$team_id == registration$team_id, , drop = FALSE]
  if (!nrow(pool)) return(pool)
  hit_en <- !is.na(registration$norm_en) & !is.na(pool$norm_en) &
    pool$norm_en == registration$norm_en
  hit_he <- !is.na(registration$norm_he) & !is.na(pool$norm_he) &
    pool$norm_he == registration$norm_he
  matched <- pool[hit_en | hit_he, , drop = FALSE]
  if (!nrow(matched)) return(matched)
  matched$matched_on <- ifelse(
    (hit_en & hit_he)[hit_en | hit_he], "name_both",
    ifelse(hit_en[hit_en | hit_he], "name_en", "name_he")
  )
  dedupe_by_canonical(matched)
}

# canonical_player_id, not identity_id, is the unit of a person here: the alias
# machinery already collapses duplicate identity records onto one canonical id
# (D.J. BURNS and DJ BURNS are both canonical 1143). Several identities sharing
# a canonical id are one player and not an ambiguity; several *canonical* ids
# are genuinely different people.
dedupe_by_canonical <- function(matched) {
  if (!nrow(matched)) return(matched)
  matched <- matched[order(matched$canonical_player_id,
                           match(matched$matched_on,
                                 c("name_both", "name_en", "name_he",
                                   "surname_team"))), , drop = FALSE]
  matched[!duplicated(matched$canonical_player_id), , drop = FALSE]
}

surname_of <- function(normalized) {
  parts <- strsplit(normalized, " ", fixed = TRUE)
  vapply(parts, function(p) if (!length(p)) NA_character_ else p[length(p)],
         character(1))
}

# Second pass, for registrations no exact name match reached. Within the mapped
# team only, a unique surname is a strong proposal but not proof: the given
# names differ for real reasons -- transliteration (JEMAYA/Jamiya, CHEN/Hen),
# a nickname stored as the given name (IFFE LUNDBERG for Gabriel "Iffe"
# Lundberg), and outright typos (ISAHIAH). It is offered for review rather than
# accepted, because a surname is also exactly what two brothers share.
match_registration_by_surname <- function(registration, identities) {
  if (is.na(registration$team_id)) return(identities[0, , drop = FALSE])
  pool <- identities[identities$game_year == registration$game_year &
                       identities$team_id == registration$team_id, , drop = FALSE]
  if (!nrow(pool)) return(pool)
  wanted <- surname_of(registration$norm_en)
  if (is.na(wanted) || !nzchar(wanted)) return(pool[0, , drop = FALSE])
  hit <- !is.na(pool$norm_en) & surname_of(pool$norm_en) == wanted
  matched <- pool[hit, , drop = FALSE]
  if (!nrow(matched)) return(matched)
  matched$matched_on <- "surname_team"
  matched <- dedupe_by_canonical(matched)
  # Ambiguous on surname is no better than unmatched; leave it for a human.
  if (nrow(matched) != 1L) return(matched[0, , drop = FALSE])
  matched
}

# Classify every registration: verified (exactly one identity), ambiguous
# (several), unmapped_team, or unmatched. Only 'verified' and 'ambiguous'
# produce rows; the other two are reported.
match_basket_identities <- function(registrations, identities) {
  results <- lapply(seq_len(nrow(registrations)), function(i) {
    registration <- registrations[i, , drop = FALSE]
    candidates <- match_registration(registration, identities)
    exact <- nrow(candidates) > 0L
    if (!exact) {
      candidates <- match_registration_by_surname(registration, identities)
    }
    status <- if (is.na(registration$team_id)) {
      "unmapped_team"
    } else if (!nrow(candidates)) {
      "unmatched"
    } else if (!exact) {
      "proposal"
    } else if (nrow(candidates) == 1L) {
      "verified"
    } else {
      "ambiguous"
    }
    if (!nrow(candidates)) {
      return(data.frame(
        game_year = registration$game_year,
        basket_player_id = registration$basket_player_id,
        team_id = registration$team_id,
        player_name = registration$player_name,
        player_name_en = registration$player_name_en,
        basket_team_name = registration$basket_team_name,
        identity_id = NA_real_, canonical_player_id = NA_integer_,
        display_name = NA_character_, matched_on = NA_character_,
        status = status, stringsAsFactors = FALSE
      ))
    }
    data.frame(
      game_year = registration$game_year,
      basket_player_id = registration$basket_player_id,
      team_id = registration$team_id,
      player_name = registration$player_name,
      player_name_en = registration$player_name_en,
      basket_team_name = registration$basket_team_name,
      identity_id = candidates$identity_id,
      canonical_player_id = candidates$canonical_player_id,
      display_name = candidates$display_name,
      matched_on = candidates$matched_on,
      status = status, stringsAsFactors = FALSE
    )
  })
  do.call(rbind, results)
}

match_summary <- function(matches) {
  per_registration <- matches[!duplicated(
    matches[c("game_year", "basket_player_id")]), , drop = FALSE]
  counts <- table(per_registration$game_year, per_registration$status)
  lines <- c("registrations by season and status:")
  for (season in rownames(counts)) {
    parts <- vapply(colnames(counts), function(status) {
      sprintf("%s=%d", status, counts[season, status])
    }, character(1))
    total <- sum(counts[season, ])
    verified <- if ("verified" %in% colnames(counts)) counts[season, "verified"] else 0L
    lines <- c(lines, sprintf("  %s  n=%d  %s  (%.1f%% verified)",
                              season, total, paste(parts, collapse = " "),
                              100 * verified / total))
  }
  lines
}

report_unresolved <- function(matches, limit = 40L) {
  unresolved <- matches[matches$status %in% c("unmatched", "unmapped_team",
                                              "ambiguous"), , drop = FALSE]
  if (!nrow(unresolved)) return("unresolved registrations: none")
  unresolved <- unresolved[order(unresolved$status, unresolved$game_year,
                                 unresolved$basket_player_id), , drop = FALSE]
  shown <- utils::head(unresolved, limit)
  c(sprintf("unresolved registrations: %d (showing %d)",
            nrow(unresolved), nrow(shown)),
    sprintf("  [%s] %s %s  %s / %s  (%s)%s",
            shown$status, shown$game_year, shown$basket_player_id,
            shown$player_name_en, shown$player_name, shown$basket_team_name,
            ifelse(is.na(shown$display_name), "",
                   paste0("  -> ", shown$display_name))))
}

# The link is written onto the Basket row, not into player_identity_map: this
# is a lookup layer answering questions about segev players, not another source
# of players to resolve. Only 'verified' and 'proposal' carry an identity;
# 'ambiguous' and 'unmatched' record that the matcher ran and did not conclude,
# which the schema distinguishes from NULL (never attempted).
write_basket_identity_links <- function(con, matches) {
  resolved <- matches[matches$status %in% c("verified", "proposal"), , drop = FALSE]
  unresolved <- matches[matches$status %in% c("ambiguous", "unmatched",
                                              "unmapped_team"), , drop = FALSE]
  unresolved <- unresolved[!duplicated(
    unresolved[c("game_year", "basket_player_id")]), , drop = FALSE]
  DBI::dbWithTransaction(con, {
    DBI::dbExecute(con, paste(
      "CREATE TEMP TABLE basket_identity_stage (",
      "game_year integer, basket_player_id integer, identity_id bigint,",
      "canonical_player_id integer, identity_match_status text,",
      "identity_matched_on text",
      ") ON COMMIT DROP"
    ))
    stage <- rbind(
      data.frame(
        game_year = as.integer(resolved$game_year),
        basket_player_id = as.integer(resolved$basket_player_id),
        identity_id = resolved$identity_id,
        canonical_player_id = as.integer(resolved$canonical_player_id),
        identity_match_status = resolved$status,
        identity_matched_on = resolved$matched_on,
        stringsAsFactors = FALSE
      ),
      data.frame(
        game_year = as.integer(unresolved$game_year),
        basket_player_id = as.integer(unresolved$basket_player_id),
        identity_id = NA_real_,
        canonical_player_id = NA_integer_,
        # unmapped_team means the matcher could not even look; from the row's
        # point of view that is the same as having looked and found nothing.
        identity_match_status = ifelse(unresolved$status == "ambiguous",
                                       "ambiguous", "unmatched"),
        identity_matched_on = NA_character_,
        stringsAsFactors = FALSE
      )
    )
    if (!nrow(stage)) return("no rows to link")
    DBI::dbAppendTable(con, DBI::Id(table = "basket_identity_stage"), stage)
    written <- DBI::dbExecute(con, "
      UPDATE basketball_test.basket_player_season t
      SET identity_id = s.identity_id,
          canonical_player_id = s.canonical_player_id,
          identity_match_status = s.identity_match_status,
          identity_matched_on = s.identity_matched_on,
          identity_matched_at = now(),
          updated_at = now()
      FROM basket_identity_stage s
      WHERE t.game_year = s.game_year
        AND t.basket_player_id = s.basket_player_id")
    if (written != nrow(stage)) {
      stop("Linked ", written, " rows for ", nrow(stage),
           " matched registrations; every registration must exist")
    }
    sprintf("linked %d registration(s): %d verified, %d proposal, %d ambiguous, %d unmatched",
            written, sum(stage$identity_match_status == "verified"),
            sum(stage$identity_match_status == "proposal"),
            sum(stage$identity_match_status == "ambiguous"),
            sum(stage$identity_match_status == "unmatched"))
  })
}

# Read the links back and confirm they say what was matched, row by row.
validate_written_links <- function(con, matches, seasons) {
  stored <- db_query(con, sprintf("
    SELECT game_year, basket_player_id, identity_id, canonical_player_id,
           identity_match_status, identity_matched_on
    FROM basketball_test.basket_player_season
    WHERE game_year IN (%s)", paste(as.integer(seasons), collapse = ", ")))
  expected <- matches[matches$status %in% c("verified", "proposal"), , drop = FALSE]
  problems <- character()

  stored_resolved <- stored[!is.na(stored$identity_id), , drop = FALSE]
  if (nrow(stored_resolved) != nrow(expected)) {
    problems <- c(problems, sprintf("%d linked rows in SQL vs %d resolved matches",
                                    nrow(stored_resolved), nrow(expected)))
  }
  key <- function(game_year, basket_player_id, identity_id) {
    paste(game_year, basket_player_id, format(identity_id, scientific = FALSE))
  }
  missing <- setdiff(
    key(expected$game_year, expected$basket_player_id, expected$identity_id),
    key(stored_resolved$game_year, stored_resolved$basket_player_id,
        stored_resolved$identity_id)
  )
  if (length(missing)) {
    problems <- c(problems, sprintf("%d resolved match(es) absent from SQL",
                                    length(missing)))
  }
  unlinked <- sum(is.na(stored$identity_match_status))
  if (unlinked > 0L) {
    problems <- c(problems, sprintf("%d row(s) left with no match status",
                                    unlinked))
  }
  if (length(problems)) {
    stop("Post-write reconciliation failed: ", paste(problems, collapse = "; "))
  }
  status <- table(stored$identity_match_status)
  c(sprintf("linked rows read back: %d", nrow(stored)),
    sprintf("status: %s", paste(sprintf("%s=%d", names(status), as.integer(status)),
                                collapse = ", ")),
    sprintf("seasons covered: %s",
            paste(sort(unique(stored$game_year)), collapse = ", ")))
}

main <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  known <- grepl("^--seasons=", args) | args %in% c("--write")
  if (any(!known)) stop("Unknown argument(s): ", paste(args[!known], collapse = ", "))
  seasons_arg <- sub("^--seasons=", "",
                     args[grepl("^--seasons=", args)][1])
  seasons <- if (is.na(seasons_arg)) {
    BASKET_IDENTITY_SEASONS
  } else if (grepl("^[0-9]{4}:[0-9]{4}$", seasons_arg)) {
    ends <- as.integer(strsplit(seasons_arg, ":", fixed = TRUE)[[1]])
    seq.int(ends[1], ends[2])
  } else {
    as.integer(strsplit(seasons_arg, ",", fixed = TRUE)[[1]])
  }
  if (anyNA(seasons)) stop("Invalid --seasons value: ", seasons_arg)
  assert_seasons_matchable(seasons)
  write <- "--write" %in% args

  source("scripts/backfill_basket_shooting_history.R", local = TRUE)
  con <- connect_etl_db()
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  registrations <- fetch_basket_registrations(con, seasons)
  identities <- fetch_pbp_identities(con, seasons)
  cat(sprintf("%d Basket registrations, %d play-by-play identity-team-seasons\n",
              nrow(registrations), nrow(identities)))

  matches <- match_basket_identities(registrations, identities)
  cat(paste(match_summary(matches), collapse = "\n"), "\n\n")
  cat(paste(report_unresolved(matches), collapse = "\n"), "\n\n")

  if (!write) {
    cat("Dry run only; pass --write to link on basket_player_season.\n")
    return(invisible(matches))
  }
  cat(write_basket_identity_links(con, matches), "\n")
  cat(paste(validate_written_links(con, matches, seasons), collapse = "\n"), "\n")
  invisible(matches)
}

if (sys.nframe() == 0L) main()
