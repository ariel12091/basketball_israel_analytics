import "server-only";
import type { RawWinProb } from "../rules/gameflow";
import { DeadlineError, type Query } from "./db";
import { logEvent } from "../lib/logEvent";

/** The win-probability read's own statement timeout: a slow read gives up
 *  early and leaves the rest of the request's SQL budget to the page. */
export const WIN_PROB_TIMEOUT_MS = 3000;

/**
 * Win probability after every possession (shared_definitions.md, Win
 * probability), from the one owner; 'euro' covers EuroLeague and EuroCup.
 * Runs in a savepoint under a short timeout and is always rolled back to it
 * (the snapshot is read-only, so only the SET LOCAL is undone): an error or a
 * timeout costs only the win chances, never the page. An expired request
 * deadline still fails the request, as every other read does.
 */
export async function readWinProb(q: Query, league: "il" | "euro", game: number): Promise<RawWinProb[]> {
  await q("SAVEPOINT win_prob", []);
  try {
    await q(`SET LOCAL statement_timeout = '${WIN_PROB_TIMEOUT_MS}ms'`, []);
    return await q<RawWinProb>(
      "SELECT possession_no, is_final, period, seconds_left, home_team_id, away_team_id, home_score, away_score, home_win_probability FROM basketball_test.game_win_probability($1::text, $2::int4)",
      [league, game],
    );
  } catch (e) {
    if (e instanceof DeadlineError) throw e;
    logEvent("win_prob_unavailable", { league, code: (e as { code?: string }).code }, "warn");
    return [];
  } finally {
    await q("ROLLBACK TO SAVEPOINT win_prob", []);
  }
}
