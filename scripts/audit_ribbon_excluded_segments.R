#!/usr/bin/env Rscript

# Read-only post-anchor audit for ribbon Workstream D. This reproduces the
# reader's valid-five guard across every Israeli game and classifies the
# gameplay segments it excludes by the number of players on the recorded hash.
#
# The guard being reproduced is RIBBON_SQL_ISRAEL's `excluded_segments`
# (app/R/global.R): a segment is drawn only when its (team_id, lineup_hash)
# resolves, within the game's game_year, to exactly five distinct players in
# basketball_test.lineups_lookup_on. Everything else with gameplay and
# non-zero seconds is a hole in the lanes.
#
# Section 4 re-runs the reader's own predicate per game, verbatim, and
# reconciles it against this script's set-based reproduction. If those two
# disagree, the numbers below describe this script, not the ribbon.

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

readRenviron("app/.Renviron")

pg <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT", "6543")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"),
  connect_timeout = 15L
)
on.exit(dbDisconnect(pg), add = TRUE)

# final_schedule_mv is built from sched_long: TWO rows per game_id, one per
# team. Joining it directly fans every segment row out twofold. The grouping
# below would absorb that (game_year is identical on both rows, and MIN/MAX/
# BOOL_OR are duplicate-insensitive), but any later SUM would silently double.
# Collapse it to one row per game up front instead.
sql <- "
WITH game_year AS (
  SELECT DISTINCT game_id, game_year
  FROM basketball_test.final_schedule_mv
),
segs AS (
  SELECT
    d.game_id,
    f.game_year,
    d.team_id,
    d.segment_id,
    d.lineup_hash,
    MIN(d.segment_start_elapsed_seconds) AS start_elapsed,
    MAX(d.segment_end_elapsed_seconds) AS end_elapsed,
    MAX(d.segment_seconds) AS segment_seconds,
    BOOL_OR(d.type IS DISTINCT FROM 'substitution') AS has_gameplay
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN game_year f USING (game_id)
  GROUP BY d.game_id, f.game_year, d.team_id, d.segment_id, d.lineup_hash
  HAVING MAX(d.segment_seconds) > 0
),
lineup_counts AS (
  SELECT game_year, team_id, lineup_hash,
         COUNT(DISTINCT player_id)::integer AS player_count
  FROM basketball_test.lineups_lookup_on
  GROUP BY game_year, team_id, lineup_hash
),
-- lineups_lookup is the SOURCE of lineups_lookup_on: ETL Phase 3 copies the
-- is_on_verdict = 1 rows across (etl/etl_full.R, Phase 3). Carrying both
-- counts separates a source defect (the feed put six players on the floor)
-- from a propagation gap (the source says five, the derived table has none).
source_counts AS (
  SELECT game_id, team_id, lineup_hash,
         COUNT(DISTINCT player_id) FILTER (WHERE is_on_verdict = 1)::integer
           AS source_on_players
  FROM basketball_test.lineups_lookup
  GROUP BY game_id, team_id, lineup_hash
),
excluded AS (
  SELECT s.*,
         COALESCE(l.player_count, 0) AS player_count,
         COALESCE(sc.source_on_players, -1) AS source_on_players
  FROM segs s
  LEFT JOIN lineup_counts l
    ON l.game_year = s.game_year
   AND l.team_id = s.team_id
   AND l.lineup_hash = s.lineup_hash
  LEFT JOIN source_counts sc
    ON sc.game_id = s.game_id
   AND sc.team_id = s.team_id
   AND sc.lineup_hash = s.lineup_hash
  WHERE s.has_gameplay
    AND COALESCE(l.player_count, 0) <> 5
)
SELECT *
FROM excluded
ORDER BY game_id, team_id, start_elapsed, segment_id
"

excluded <- dbGetQuery(pg, sql)

cat("=== 1. Summary ===\n")
print(data.frame(
  excluded_segments = nrow(excluded),
  team_games = nrow(unique(excluded[c("game_id", "team_id")])),
  games = length(unique(excluded$game_id)),
  excluded_seconds = sum(excluded$segment_seconds)
), row.names = FALSE)

agg2 <- function(df, by) {
  keys <- df[by]
  split_key <- interaction(keys, drop = TRUE, sep = "\r")
  out <- do.call(rbind, lapply(split(df, split_key), function(g) {
    cbind(
      g[1, by, drop = FALSE],
      data.frame(
        excluded_segments = nrow(g),
        excluded_seconds = sum(g$segment_seconds)
      )
    )
  }))
  out[order(-out$excluded_seconds, out[[by[1]]]), , drop = FALSE]
}

cat("\n=== 2. By game ===\n")
by_game <- agg2(excluded, "game_id")
by_game$team_games <- vapply(by_game$game_id, function(g) {
  length(unique(excluded$team_id[excluded$game_id == g]))
}, integer(1))
print(by_game, row.names = FALSE)

cat("\n=== 3a. By game and team ===\n")
print(agg2(excluded, c("game_id", "team_id")), row.names = FALSE)

cat("\n=== 3b. By recorded player count ===\n")
by_count <- agg2(excluded, "player_count")
by_count <- by_count[order(by_count$player_count), , drop = FALSE]
print(by_count, row.names = FALSE)

cat("\n=== 3c. By game and recorded player count ===\n")
print(agg2(excluded, c("game_id", "player_count")), row.names = FALSE)

# --- 3d. Alias resolution ------------------------------------------------
# A lineup_hash is computed from the player-id list, so the same five people
# hash differently in the raw and canonical id spaces. A scoped alias backfill
# that rewrites lineups_lookup_on without rewriting lineups_lookup / the MV
# leaves the ribbon joining a raw hash to a canonical one. That looks exactly
# like missing data and is not. Resolve it explicitly before classifying.
alias_sql <- "
WITH gyv AS (
  SELECT DISTINCT game_id, game_year FROM basketball_test.final_schedule_mv
),
segs AS (
  SELECT d.game_id, f.game_year, d.team_id, d.lineup_hash,
         BOOL_OR(d.type IS DISTINCT FROM 'substitution') AS has_gameplay
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN gyv f USING (game_id)
  GROUP BY 1,2,3,4 HAVING MAX(d.segment_seconds) > 0
),
lc AS (
  SELECT game_year, team_id, lineup_hash, COUNT(DISTINCT player_id) AS n
  FROM basketball_test.lineups_lookup_on GROUP BY 1,2,3
),
unresolved AS (
  SELECT DISTINCT s.game_year, s.team_id, s.lineup_hash
  FROM segs s
  LEFT JOIN lc ON lc.game_year = s.game_year AND lc.team_id = s.team_id
              AND lc.lineup_hash = s.lineup_hash
  WHERE s.has_gameplay AND COALESCE(lc.n, 0) = 0
),
raw_sets AS (
  SELECT u.game_year, u.team_id, u.lineup_hash,
         ARRAY_AGG(DISTINCT l.player_id ORDER BY l.player_id) AS pids
  FROM unresolved u
  JOIN basketball_test.lineups_lookup l
    ON l.game_year = u.game_year AND l.team_id = u.team_id
   AND l.lineup_hash = u.lineup_hash AND l.is_on_verdict = 1
  GROUP BY 1,2,3
),
canon AS (
  SELECT r.*,
         (SELECT ARRAY_AGG(DISTINCT c ORDER BY c) FROM (
            SELECT COALESCE((
              SELECT MIN(m.canonical_player_id)
              FROM basketball_test.player_identity_map m
              WHERE m.active
                AND m.source_player_id = p
                AND (m.game_year IS NULL OR m.game_year = r.game_year)
                AND (m.team_id IS NULL OR m.team_id = r.team_id)
            ), p) AS c
            FROM unnest(r.pids) AS p) t) AS canon_pids
  FROM raw_sets r
),
on_sets AS (
  SELECT game_year, team_id, lineup_hash,
         ARRAY_AGG(DISTINCT player_id ORDER BY player_id) AS pids
  FROM basketball_test.lineups_lookup_on GROUP BY 1,2,3
)
SELECT c.game_year, c.team_id, c.lineup_hash,
       c.pids::text AS raw_set, c.canon_pids::text AS canonical_set,
       (c.canon_pids IS DISTINCT FROM c.pids) AS alias_applied,
       (SELECT o.lineup_hash FROM on_sets o
         WHERE o.game_year = c.game_year AND o.team_id = c.team_id
           AND o.pids = c.canon_pids LIMIT 1) AS alias_twin_hash
FROM canon c
ORDER BY c.game_year, c.team_id, c.lineup_hash
"
alias_res <- dbGetQuery(pg, alias_sql)
cat("\n=== 3d. Alias resolution for hashes absent from lineups_lookup_on ===\n")
if (nrow(alias_res)) print(alias_res, row.names = FALSE) else cat("(none)\n")

excluded$alias_twin <- alias_res$alias_twin_hash[
  match(paste(excluded$game_year, excluded$team_id, excluded$lineup_hash),
        paste(alias_res$game_year, alias_res$team_id, alias_res$lineup_hash))]

cat("\n=== 3e. Cause class ===\n")
# The reader excludes a segment when the DERIVED table (lineups_lookup_on)
# fails to give it five players. What that means is decided by two things:
# whether the SOURCE (lineups_lookup, is_on_verdict = 1) holds a valid five,
# and whether the same five resolve under canonical ids.
excluded$cause <- ifelse(
  !is.na(excluded$alias_twin),
  "alias id-space mismatch (resolves under canonical ids)",
  ifelse(excluded$source_on_players == 5,
         "propagation gap (source valid five, no alias twin)",
         ifelse(excluded$source_on_players > 5,
                "source over-attribution (>5 on the floor)",
                ifelse(excluded$source_on_players < 0,
                       "hash absent from lineups_lookup",
                       "source under-attribution (<5 on the floor)"))))
print(agg2(excluded, "cause"), row.names = FALSE)

cat("\n=== 3f. Cause class by team-game ===\n")
print(agg2(excluded, c("game_id", "team_id", "cause")), row.names = FALSE)

cat("\n=== 3g. Derived vs source player count ===\n")
print(agg2(excluded, c("player_count", "source_on_players")), row.names = FALSE)

# --- 4. Parity against the reader's own predicate -------------------------
# Verbatim from RIBBON_SQL_ISRAEL: single-game scope, EXISTS-form guard,
# lineup_players gated on cardinality = 5 within the game's game_year.
reader_sql <- "
WITH gy AS (
  SELECT game_year FROM basketball_test.final_schedule_mv
  WHERE game_id = $1 LIMIT 1
),
segs AS (
  SELECT team_id, segment_id, lineup_hash,
         BOOL_OR(type IS DISTINCT FROM 'substitution') AS has_gameplay
  FROM basketball_test.df_pts_poss_lineups_longer_mv
  WHERE game_id = $1
  GROUP BY team_id, segment_id, lineup_hash
  HAVING MAX(segment_seconds) > 0
),
lineup_players AS (
  SELECT l.team_id, l.lineup_hash
  FROM basketball_test.lineups_lookup_on l
  WHERE l.game_year = (SELECT game_year FROM gy)
  GROUP BY l.team_id, l.lineup_hash
  HAVING cardinality(ARRAY_AGG(DISTINCT l.player_id)) = 5
)
SELECT COUNT(*)::integer AS excluded_segments
FROM segs s
WHERE s.has_gameplay
  AND NOT EXISTS (SELECT 1 FROM lineup_players lp
                   WHERE lp.lineup_hash = s.lineup_hash
                     AND lp.team_id = s.team_id)
"

cat("\n=== 4. Reader parity (per affected game) ===\n")
parity <- do.call(rbind, lapply(by_game$game_id, function(g) {
  reader_n <- dbGetQuery(pg, reader_sql, params = list(as.integer(g)))$excluded_segments
  data.frame(
    game_id = g,
    audit_segments = by_game$excluded_segments[by_game$game_id == g],
    reader_segments = reader_n
  )
}))
parity$match <- parity$audit_segments == parity$reader_segments
print(parity, row.names = FALSE)
cat(sprintf("\nParity: %d/%d games agree with the reader's own predicate.\n",
            sum(parity$match), nrow(parity)))

# --- 5. Measure variants -------------------------------------------------
# The plan's headline baseline (19 team-games, 4675s) does not agree with its
# own per-game table (8 team-games, 4596s). These variants loosen one
# predicate at a time, so a future comparison can quote a measure that exists.
variant_sql <- "
WITH game_year AS (
  SELECT DISTINCT game_id, game_year FROM basketball_test.final_schedule_mv
),
segs AS (
  SELECT d.game_id, f.game_year, d.team_id, d.segment_id, d.lineup_hash,
         MAX(d.segment_seconds) AS segment_seconds,
         BOOL_OR(d.type IS DISTINCT FROM 'substitution') AS has_gameplay
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN game_year f USING (game_id)
  GROUP BY 1,2,3,4,5
),
lineup_counts AS (
  SELECT game_year, team_id, lineup_hash,
         COUNT(DISTINCT player_id)::integer AS player_count
  FROM basketball_test.lineups_lookup_on GROUP BY 1,2,3
),
j AS (
  SELECT s.*, COALESCE(l.player_count, 0) AS player_count
  FROM segs s LEFT JOIN lineup_counts l
    ON l.game_year = s.game_year AND l.team_id = s.team_id
   AND l.lineup_hash = s.lineup_hash
  WHERE COALESCE(l.player_count, 0) <> 5
)
SELECT variant, COUNT(*) AS segments,
       COUNT(DISTINCT (game_id, team_id)) AS team_games,
       COUNT(DISTINCT game_id) AS games,
       COALESCE(SUM(segment_seconds), 0)::numeric AS seconds
FROM (
  SELECT 'reader (gameplay, seconds>0)' AS variant, * FROM j
    WHERE has_gameplay AND segment_seconds > 0
  UNION ALL
  SELECT 'any segment, seconds>0', * FROM j WHERE segment_seconds > 0
  UNION ALL
  SELECT 'gameplay, any seconds', * FROM j WHERE has_gameplay
  UNION ALL
  SELECT 'any segment, any seconds', * FROM j
) v
GROUP BY variant ORDER BY seconds DESC
"
cat("\n=== 5. Measure variants (baseline reconciliation) ===\n")
print(dbGetQuery(pg, variant_sql), row.names = FALSE)

cat("\n=== 6. Details ===\n")
print(excluded, row.names = FALSE)
