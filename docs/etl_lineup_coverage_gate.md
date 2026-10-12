# ETL gameplay lineup coverage gate

The Israeli ETL checks cleaned source gameplay against the final
possessions-within-stints result before writing pws and committing the game.
The full runner calls this after overtime recovery; the legacy etl_update
entry point calls the same guard. The EuroLeague/EuroCup loader has a separate
snapshot validation and transaction path and is unchanged.

A gameplay action must appear exactly once, preserve its source quarter and
kind, have a segment and an opposing team, and reference valid five-player
lineups for both sides. The guard counts actual distinct ON players and checks
their canonical MD5 hash; a non-null hash or n_on=5 alone is insufficient.
Coverage is compared to actions_clean, so an event lost before pws cannot
silently disappear from the validation input.

Substitution resets, quarter/clock markers, timeouts and official-review
records are administrative rows and do not themselves require lineup coverage.
The gameplay types are listed in etl/lineup_coverage.R. A newly introduced
provider gameplay type must be added to that list.

On failure, the full runner rolls back the game's staged base tables. A new
failed game is excluded from successful_ids and is not published to
etl_processed_games; the next ordinary run retries it. A failed forced retry
of an already-published game preserves its previous snapshot and processed
marker. No quarter labels or players are guessed by this guard.

The error log contains representative action IDs, quarters, clocks, teams and
reasons. A CSV containing every offending action is written under
etl/logs/data_quality/details/lineup_coverage_game_<id>.csv, which the existing
GitHub data-quality artifact upload includes.

## Game 417 regression

The fixture was captured from the public Segevstats feed and the database on
2026-10-12. It contains 16 made baskets whose offensive coverage is incomplete
at the Q2/Q3 openings, plus two correctly covered late-Q3 events. The guard
rejects the incomplete events and accepts the covered ones. The provider's
Q3 five-player IN declarations (4170541–4170550) are labelled Q2, 10:00;
this both overwrites Q2 opening states and leaves Q3's initial states empty.

The guard prevents future incomplete games from being committed. It does not
repair game 417's existing published data. The scoped
period-label correction is now in KNOWN_PERIOD_LABEL_CORRECTIONS: only
4170541–4170550 move from Q2 to Q3. An explicit game_ids=417 reprocess is
still required to replace the existing published data.

Tests: app/tests/testthat/test-lineup-coverage-gate.R. They cover the real
incident, healthy regulation/OT coverage, missing and duplicate mappings,
malformed lineups, source-period/type drift, administrative events, diagnostics,
and placement before pws insertion and transaction commit.

The game 417 correction tests also replay the unchanged production lineup and
stint SQL against frozen CTE inputs using a read-only database connection. They
check the original failure, restored five-player Q2/Q3 openings, complete
gameplay coverage and the 85–86 scoring totals without changing application
tables. This integration test runs when RUN_DB_TESTS=1.
