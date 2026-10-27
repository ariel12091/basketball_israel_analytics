// The URL of each table tab's first answer for a page request, shared by the
// tab pages (which read it on the server) and app/[league]/error.tsx (which
// renders the same tab without it when that read failed).
import type { League } from "../rules/gameLogs";
import {
  gameLogsPath,
  LINEUP_QUERY_DEFAULTS,
  lineupsPagePath,
  onoffPath,
  PLAYER_QUERY_DEFAULTS,
  playerStatsPath,
  queryFromSearch,
  TEAM_QUERY_DEFAULTS,
  teamRatingsPath,
  type SearchParams,
} from "./tabQuery";
import ui from "../data/shinyUi.json";

export const TAB_FIRST_PATH = {
  "game-logs": (l: League, sp: SearchParams) =>
    gameLogsPath(l, queryFromSearch(l, sp)),
  onoff: (l: League, sp: SearchParams) => {
    const d =
      l === "il" ? ui.onoffDefaults.israel : ui.onoffDefaults.euroleague;
    return onoffPath(
      l,
      queryFromSearch(l, sp, {
        min_all: String(d.minAll),
        min_on: String(d.minOn),
      }),
    );
  },
  // The client starts with auto on, so the first table uses the auto minimum.
  lineups: (l: League, sp: SearchParams) =>
    lineupsPagePath(l, queryFromSearch(l, sp, LINEUP_QUERY_DEFAULTS), true),
  "team-ratings": (l: League, sp: SearchParams) =>
    teamRatingsPath(l, queryFromSearch(l, sp, TEAM_QUERY_DEFAULTS)),
  "player-stats": (l: League, sp: SearchParams) =>
    playerStatsPath(l, queryFromSearch(l, sp, PLAYER_QUERY_DEFAULTS)),
};
export type TablePage = keyof typeof TAB_FIRST_PATH;

/** Thrown by a tab page whose server read failed. The response is then an
 * error (500), which Vercel's CDN does not keep, instead of a 200 without
 * its table that next.config.ts would let the CDN share for a minute. */
export class FirstReadFailed extends Error {
  constructor() {
    super("First table read failed");
    this.name = "FirstReadFailed";
  }
}
