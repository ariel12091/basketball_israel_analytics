import "server-only";
import type { League } from "../rules/gameLogs";
import { parseLineups } from "./lineupParams";
import { getLineupRoster, getLineups } from "./lineupsRepo";
export function lineupsAnswer(league: League, search: URLSearchParams) {
  return getLineups(parseLineups(search, league));
}
export function lineupRosterAnswer(league: League, search: URLSearchParams) {
  return getLineupRoster(parseLineups(search, league));
}
