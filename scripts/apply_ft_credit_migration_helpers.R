# Pure text logic for scripts/apply_ft_credit_migration.R. No database access
# and no side effects at source time, so it can be unit-tested
# (app/tests/testthat/test-ft-credit-staging.R).

STAGE_SUFFIX <- "__ftc_next"

# Append `suffix` to every occurrence of `ident` that is a complete identifier:
# `foo` is rewritten, `foo_archive` and `my_foo` are not.
suffix_identifier <- function(sql, ident, suffix = STAGE_SUFFIX) {
  pattern <- paste0("(?<![A-Za-z0-9_])\\Q", ident, "\\E(?![A-Za-z0-9_])")
  gsub(pattern, paste0(ident, suffix), sql, perl = TRUE)
}

# Rewrite a matview definition so it builds <name>__ftc_next with its own index
# names. `live_indexes` are the index names on the live view; the file must
# create exactly those. Stops if the file is not what it expects.
stage_matview_sql <- function(sql, schema, name, live_indexes) {
  qualified <- sprintf("%s.%s", schema, name)
  count <- function(ident, text) {
    hits <- gregexpr(paste0("(?<![A-Za-z0-9_])\\Q", ident, "\\E(?![A-Za-z0-9_])"), text, perl = TRUE)[[1]]
    sum(hits > 0)
  }
  # Drop statements in the file target the live object: remove them.
  lines <- strsplit(sql, "\n", fixed = TRUE)[[1]]
  sql <- paste(lines[!grepl("^\\s*DROP (MATERIALIZED VIEW|TABLE|INDEX)", lines)], collapse = "\n")

  create_stmt <- sprintf("CREATE MATERIALIZED VIEW %s", qualified)
  if (count(create_stmt, sql) != 1L) stop("expected exactly one '", create_stmt, "'")

  created <- regmatches(sql, gregexpr("CREATE\\s+(UNIQUE\\s+)?INDEX\\s+(IF\\s+NOT\\s+EXISTS\\s+)?[A-Za-z0-9_]+", sql, perl = TRUE))[[1]]
  created <- sub(".*\\s", "", created)
  not_created <- setdiff(live_indexes, created)
  not_live <- setdiff(created, live_indexes)
  if (length(not_created)) stop("live index not created by the file: ", paste(not_created, collapse = ", "))
  if (length(not_live)) stop("the file creates an index the live view does not have: ", paste(not_live, collapse = ", "))

  for (i in live_indexes) sql <- suffix_identifier(sql, i)
  sql <- suffix_identifier(sql, qualified)

  # The result must build only staged objects.
  staged <- paste0(c(qualified, live_indexes), STAGE_SUFFIX)
  left <- Filter(function(ident) count(ident, sql) > 0L, c(qualified, live_indexes))
  absent <- Filter(function(ident) count(ident, sql) == 0L, staged)
  if (length(left)) stop("still names the live object: ", paste(left, collapse = ", "))
  if (length(absent)) stop("staged name missing after rewrite: ", paste(absent, collapse = ", "))
  sql
}
