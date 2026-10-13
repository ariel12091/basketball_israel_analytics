# Dynamic PBP source fallback

The Israeli ETL tries its scheduled PBP URL first. If fetching or source validation fails, it tries:

`https://basket.co.il/livestats/data/game-{game_id}.json`

This works for every game ID, without adding a game-specific branch. The accepted source URL and primary rejection reason are written to the ETL log.

Both candidates must identify the requested game and two distinct teams, mark the game finished, provide at least five roster entries per side, and pass the existing action cleaner and known corrections. Cleaned actions must have unique, non-missing IDs and cover all four regulation quarters. The nightly caller also supplies its schedule row: team IDs and scoring-event totals must match it. If neither candidate passes, the game remains failed and retryable; no arbitrary duplicate-collapse rule is added.

Basket's archive envelope is normalized to the existing result/gameInfo/actions shape. Its embedded box score supplies starters when an archive is selected. The embedded box score must identify the same game and home/away teams and contain both player lists. Existing downstream roster, lineup, and publication checks still apply.

An explicit override remains available as `PBP_GAME_<game_id>_FALLBACK_URL`, including the existing game-402 variable. An override is tried first, with the derived Basket URL available if it fails validation.

Implementation:

- [Source selection and validation](../etl/pbp_source_fallback.R)
- [PBP and box wrappers](../etl/etl_onoff.R)
- [Nightly integration](../etl/etl_full.R)
- [Regression tests](../app/tests/testthat/test-pbp-source-fallback.R)

Verification on 2026-10-13: an offline replay of the live game-414 primary and archive payloads reproduced the primary duplicate-ID failure and selected the archive. The result had 668 clean actions, 30 roster entries, ten starters, and team scoring totals 80–89 matching the schedule. All 25 focused regression assertions and R parse checks passed. This verification did not write to the database or publish code to GitHub.
