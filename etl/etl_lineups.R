library(DBI)
library(dplyr)
library(dbplyr)
library(purrr)
library(tidyr)
library(digest)

SCHEMA <- "basketball_test"

game_ids <- sched_subset$game_id


# ============================================================
# STEP 0: helper functions for sub-lineups (same as before)
# ============================================================

make_subs_for_one <- function(lineup_df, ks = c(2, 3, 4)) {
  # lineup_df must have: lineup_hash, team_id, player_id, game_year
  lineup_hash <- unique(lineup_df$lineup_hash)
  team_id     <- unique(lineup_df$team_id)
  game_year   <- unique(lineup_df$game_year)
  
  ids <- sort(unique(as.integer(lineup_df$player_id)))
  out <- list()
  
  for (k in ks) {
    if (length(ids) < k) next
    
    m <- t(combn(ids, k))   # rows = sub-lineups
    n <- nrow(m)
    
    # sub_lineup_id as "id1_id2_..."
    sub_ids <- apply(
      m,
      1L,
      function(v) paste(sort(as.integer(v)), collapse = "_")
    )
    
    sub_hash <- vapply(
      sub_ids,
      digest::digest,
      FUN.VALUE = character(1L),
      algo = "md5",
      USE.NAMES = FALSE
    )
    
    out[[length(out) + 1L]] <- tibble::tibble(
      lineup_hash     = lineup_hash,      # recycled to length n
      team_id         = team_id,          # recycled
      game_year       = game_year,        # recycled
      sub_lineup_id   = sub_ids,          # length n
      sub_lineup_hash = sub_hash,         # length n
      sub_size        = rep.int(k, n)     # length n
      # NOTE: no player_ids here – it's a GENERATED column in Postgres
    )
  }
  
  if (!length(out)) {
    return(tibble::tibble(
      lineup_hash     = character(0),
      team_id         = integer(0),
      game_year       = integer(0),
      sub_lineup_id   = character(0),
      sub_lineup_hash = character(0),
      sub_size        = integer(0)
    ))
  }
  
  dplyr::bind_rows(out)
}

build_sub_lineups_all <- function(players_df, ks = c(2, 3, 4)) {
  # players_df expected cols: lineup_hash, team_id, player_id, game_year
  if (!nrow(players_df)) {
    return(tibble::tibble(
      lineup_hash     = character(0),
      team_id         = integer(0),
      game_year       = integer(0),
      sub_lineup_id   = character(0),
      sub_lineup_hash = character(0),
      sub_size        = integer(0)
    ))
  }
  
  groups <- players_df %>%
    dplyr::group_by(lineup_hash, team_id, game_year) %>%
    dplyr::group_split()
  
  purrr::map_dfr(groups, make_subs_for_one, ks = ks)
}

# ============================================================
# SET YOUR TEST GAME IDS HERE
# ============================================================

game_ids <- c(
  # put real game_ids from basketball_test.lineups_lookup
  # e.g. 302501, 302502
)

# ============================================================
# STEP 1: pull ON lineups for these games from lineups_lookup
# ============================================================

lineups_src <- tbl(pg, dbplyr::in_schema(SCHEMA, "lineups_lookup"))



src_rows <- lineups_src %>%
  filter(
    game_id %in% !!game_ids,
    is_on_verdict == 1
  ) %>%
  select(
    lineup_hash,
    team_id,
    player_id,
    game_year
  ) %>%
  distinct() %>%
  arrange(lineup_hash, player_id) %>%
  collect()

src_rows
nrow(src_rows)

# If nrow(src_rows) == 0, fix game_ids / source first.

# ============================================================
# STEP 2: find which of these already exist in lineups_lookup_on
# ============================================================

ll_on <- tbl(pg, in_schema(SCHEMA, "lineups_lookup_on"))

existing_rows <- ll_on %>%
  filter(
    lineup_hash %in% !!unique(src_rows$lineup_hash),
    game_year   %in% !!unique(src_rows$game_year)
  ) %>%
  select(
    lineup_hash,
    team_id,
    player_id,
    game_year
  ) %>%
  distinct() %>%
  collect()

existing_rows
nrow(existing_rows)

lineups_src %>%
  filter(lineup_hash == "00814106a97c1b9f60fd776c843b60ae") %>%
  view()

# ============================================================
# STEP 3: keep only truly new rows for lineups_lookup_on
# ============================================================

new_rows_on <- dplyr::anti_join(
  src_rows,
  existing_rows,
  by = c("lineup_hash", "team_id", "player_id", "game_year")
)

new_rows_on
nrow(new_rows_on)

# If this is 0 → nothing new to insert.

# ============================================================
# STEP 4: INSERT new rows into lineups_lookup_on (TEST STEP)
# ============================================================
# When you are happy with new_rows_on, run this.

if (nrow(new_rows_on) > 0) {
  DBI::dbWriteTable(
    pg,
    DBI::Id(schema = SCHEMA, table = "lineups_lookup_on"),
    new_rows_on,
    append    = TRUE,
    row.names = FALSE
  )
}

# You can verify:
tbl(pg,  "lineups_lookup_on") %>%
  filter(lineup_hash %in% !!unique(new_rows_on$lineup_hash)) %>% collect()

# ============================================================
# STEP 5: identify “new” (lineup_hash, team_id, game_year) keys
# ============================================================

new_keys <- new_rows_on %>%
  distinct(lineup_hash, team_id, game_year)

new_keys
nrow(new_keys)

# ============================================================
# STEP 6: build players_new for those keys
# ============================================================

players_new <- new_rows_on %>%
  semi_join(new_keys, by = c("lineup_hash", "team_id", "game_year")) %>%
  arrange(lineup_hash, player_id)

players_new
nrow(players_new)

# ============================================================
# STEP 7: build all 2/3/4-man sub-lineups in R
# ============================================================

sub_lineups_table <- build_sub_lineups_all(players_new, ks = c(2, 3, 4))

sub_lineups_table
nrow(sub_lineups_table)

Check a single lineup_hash to inspect combinations:
  sub_lineups_table %>% filter(lineup_hash == sub_lineups_table$lineup_hash[1])

# ============================================================
# STEP 8: prepare final frame for INSERT into sub_lineups
# ============================================================

sub_line <- sub_lineups_table %>%
  mutate(created_at = Sys.time()) %>%
  rename(
    num_lineup = sub_size,
    lineup_id  = sub_lineup_id
  ) %>%
  select(
    team_id,
    lineup_hash,
    sub_lineup_hash,
    lineup_id,
    num_lineup,
    game_year,
    created_at
  )

sub_line
nrow(sub_line)

# ============================================================
# STEP 9: INSERT into basketball_test.sub_lineups
# ============================================================
# Remember: player_ids column is GENERATED in Postgres,
# so we do NOT include it here.

if (nrow(sub_line) > 0) {
  DBI::dbWriteTable(
    pg,
    DBI::Id(schema = SCHEMA, table = "sub_lineups"),
    sub_line,
    append    = TRUE,
    row.names = FALSE
  )
}

# Optional: quick sanity checks
# tbl(pg, in_schema(SCHEMA, "sub_lineups")) %>%
#   filter(lineup_hash %in% !!unique(sub_line$lineup_hash)) %>% head() %>% collect()
