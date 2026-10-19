# Dynamic PBP source fallback

**Since 2026-10-18 basket.co.il is the main source.** The Israeli ETL first tries:

`https://basket.co.il/livestats/data/game-{game_id}.json`

If fetching or source validation fails, it falls back to its scheduled Segev PBP URL (`stats.segevstats.com`). Segev was the primary until the 2026-10-18 box-score comparison (`scripts/compare_official_boxscores.R`) found Segev's own box score failing to reconcile with the official final score for games 402 and 414, while basket.co.il's matched it and the app exactly.

This works for every game ID, without adding a game-specific branch. The accepted source URL and every rejection reason are written to the ETL log. basket.co.il's archive returns 404 for many older games, so any historical reload still depends on the Segev fallback.

Both candidates must identify the requested game and two distinct teams, be finished, provide at least five roster entries per side, and pass the existing action cleaner and known corrections. Cleaned actions must have unique, non-missing IDs and cover all four regulation quarters. The nightly caller reads schedule_team_dict once per write run and maps schedule team IDs to PBP IDs by season. The original schedule IDs remain unchanged. The mapped PBP IDs and scoring-event totals must match the selected source. If neither candidate passes, the game remains failed and retryable; no arbitrary duplicate-collapse rule is added.

"Finished" means `gameFinished` is true, or, because both providers often never set it (games 405, 417, 424 and 651 in 2026-27), the feed's last period marker closes the fourth period or an overtime (`end-of-quarter` with quarter >= 4, or `end-of-game`) and the schedule's final score is not a tie. The scoring totals must then still equal that score. A feed whose last marker opens an overtime, stops before the fourth period, or comes with no schedule row is refused. Until 2026-10-19 the flag alone was required, which refused game 424 and failed the nightly runs of 2026-10-17 and 2026-10-18.

Basket's archive envelope is normalized to the existing result/gameInfo/actions shape. Its embedded box score supplies starters when the archive is selected; when Segev is selected, starters come from Segev's box-score endpoint as before. The embedded box score must identify the same game and home/away teams and contain both player lists. Existing downstream roster, lineup, and publication checks still apply.

An explicit override remains available as `PBP_GAME_<game_id>_FALLBACK_URL`, including the existing game-402 variable. An override is tried first, then the derived Basket URL, then Segev.

Verification on 2026-10-18 (commit `0e6dd8b`): a read-only replay of the 33 processed 2026-27 games through the new order with main's real cleaner accepted 29 from the archive, each with its embedded box score. Game 648's archive is flagged not finished and fell back to Segev; 405, 417 and 651 are flagged not finished by both sources today (they were loaded earlier and are not re-fetched by the nightly run).

Implementation:

- [Source selection and validation](../etl/pbp_source_fallback.R)
- [PBP and box wrappers](../etl/etl_onoff.R)
- [Nightly integration](../etl/etl_full.R)
- [Regression tests](../app/tests/testthat/test-pbp-source-fallback.R)

Verification on 2026-10-13: an offline replay of the live game-414 primary and archive payloads reproduced the primary duplicate-ID failure and selected the archive. The result had 668 clean actions, 30 roster entries, ten starters, and team scoring totals 80–89 matching the schedule. All 33 focused regression assertions and R parse checks passed. This verification did not write to the database or publish code to GitHub.

The follow-up replay uses the actual game-414 schedule row (season 2027, schedule IDs 2122/2124) mapped to PBP IDs 14/17. An earlier validator incorrectly compared these two namespaces directly; regression coverage now includes season-aware mapping and missing/duplicate dictionary keys.
