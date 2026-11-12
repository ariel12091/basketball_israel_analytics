# One basket.co.il person across seasons (2026-11-12). basket.co.il re-mints a
# player's PlayerId every season, but his profile page links his other
# registrations (basket_player_registration.linked_basket_player_ids). The
# registrations those links join -- in either direction, through ids before
# 2022 as well -- are one person; basket_person_id is the smallest PlayerId
# among them, the earliest registration basket.co.il knows. Later seasons do
# not move it; a newly read profile that reveals an older registration does
# (every member then takes the older id), so join on it within a run, never
# store it elsewhere as permanent.
#
# A link that joins two different dates of birth is not trusted: the run stops
# and names the registrations.
#
#   Rscript etl/basket_person_ids.R            dry run: counts and changed keys
#   Rscript etl/basket_person_ids.R --write    update basket_person_id

# registrations: game_year, basket_player_id, date_of_birth,
# linked_basket_player_ids (list of integer vectors; NULL = profile not read).
assign_basket_person_ids <- function(registrations) {
  own <- as.integer(registrations$basket_player_id)
  links <- registrations$linked_basket_player_ids
  from <- rep(own, lengths(links))
  to <- as.integer(unlist(links))
  nodes <- unique(c(own, to))
  label <- setNames(nodes, as.character(nodes))
  if (length(from)) {
    ends <- as.character(c(from, to))
    repeat {
      m <- pmin(label[as.character(from)], label[as.character(to)])
      best <- tapply(c(m, m), ends, min)
      new <- label
      new[names(best)] <- pmin(label[names(best)], best)
      if (identical(new, label)) break
      label <- new
    }
  }
  person <- unname(label[as.character(own)])
  dob <- registrations$date_of_birth
  known <- !is.na(dob)
  clash <- tapply(dob[known], person[known], function(d) length(unique(d)) > 1)
  if (any(clash)) {
    detail <- vapply(names(clash)[clash], function(p) {
      hit <- person == as.integer(p) & known
      paste(sprintf("%s (%s)", own[hit], format(dob[hit])), collapse = ", ")
    }, character(1))
    stop("basket.co.il links join different dates of birth: ", paste(detail, collapse = "; "))
  }
  data.frame(game_year = as.integer(registrations$game_year), basket_player_id = own,
             basket_person_id = person)
}

# Stored keys whose registrations would no longer share one key. A split means
# links have gone missing (for instance a profile page whose layout changed),
# not that two people were told apart, so the write refuses it.
person_splits <- function(stored, new) {
  m <- merge(stored[c("basket_player_id", "stored")], new[c("basket_player_id", "basket_person_id")],
             by = "basket_player_id")
  m <- m[!is.na(m$stored), ]
  if (!nrow(m)) return(integer(0))
  n <- tapply(m$basket_person_id, m$stored, function(k) length(unique(k)))
  sort(as.integer(names(n)[n > 1]))
}

parse_pg_int_array <- function(x) {
  lapply(x, function(v) {
    if (is.na(v)) return(NULL)
    inner <- gsub("[{}]", "", v)
    if (!nzchar(inner)) integer() else as.integer(strsplit(inner, ",", fixed = TRUE)[[1]])
  })
}

main_person_ids <- function() {
  args <- commandArgs(trailingOnly = TRUE)
  flags <- c("--write", "--allow-split")
  if (any(!args %in% flags)) stop("Unknown argument(s): ", paste(setdiff(args, flags), collapse = ", "))
  write <- "--write" %in% args
  source("scripts/backfill_basket_shooting_history.R", local = TRUE)
  con <- connect_etl_db()
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  regs <- DBI::dbGetQuery(con, paste(
    "SELECT game_year, basket_player_id, date_of_birth, basket_person_id,",
    "linked_basket_player_ids::text AS links FROM", BASKET_REGISTRATION_TABLE))
  regs$linked_basket_player_ids <- parse_pg_int_array(regs$links)
  out <- assign_basket_person_ids(regs)
  out$stored <- as.integer(regs$basket_person_id)
  changed <- is.na(out$stored) | out$stored != out$basket_person_id
  sizes <- table(table(out$basket_person_id))
  cat(sprintf("%d registrations, %d persons; registrations per person: %s\n", nrow(out),
              length(unique(out$basket_person_id)),
              paste(sprintf("%s x%s", names(sizes), sizes), collapse = ", ")))
  cat(sprintf("keys to set or change: %d (%d were set before)\n", sum(changed), sum(changed & !is.na(out$stored))))
  moved <- out[changed & !is.na(out$stored), ]
  if (nrow(moved)) print(head(moved, 40), row.names = FALSE)
  splits <- person_splits(out, out)
  if (length(splits) && !"--allow-split" %in% args) {
    stop(length(splits), " persons would split (stored keys ", paste(head(splits, 20), collapse = ", "),
         "): their links have gone missing -- check the profile parser; --allow-split to accept")
  }
  if (!write) {
    cat("Dry run only; pass --write to update basket_person_id.\n")
    return(invisible(out))
  }
  DBI::dbWithTransaction(con, {
    DBI::dbExecute(con, paste("CREATE TEMP TABLE basket_person_stage",
      "(game_year integer, basket_player_id integer, basket_person_id integer) ON COMMIT DROP"))
    DBI::dbAppendTable(con, DBI::Id(table = "basket_person_stage"), out[changed, 1:3])
    n <- DBI::dbExecute(con, paste(
      "UPDATE", BASKET_REGISTRATION_TABLE, "r SET basket_person_id = s.basket_person_id, updated_at = now()",
      "FROM basket_person_stage s WHERE r.game_year = s.game_year AND r.basket_player_id = s.basket_player_id"))
    if (n != sum(changed)) stop("updated ", n, " rows for ", sum(changed), " changed keys")
    back <- DBI::dbGetQuery(con, paste("SELECT basket_player_id, basket_person_id FROM", BASKET_REGISTRATION_TABLE))
    m <- merge(out[1:3], back, by = "basket_player_id", suffixes = c("", ".db"))
    if (nrow(m) != nrow(out) || any(m$basket_person_id != m$basket_person_id.db)) stop("read-back differs")
    cat(sprintf("updated %d keys; read back %d registrations, all equal\n", n, nrow(m)))
  })
  invisible(out)
}

if (sys.nframe() == 0L) main_person_ids()
