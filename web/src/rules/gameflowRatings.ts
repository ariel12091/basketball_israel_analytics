// Gameflow stint ratings (docs/shared_definitions.md, "Gameflow stint
// rating"): Net, ORtg and DRtg for one player, a stint or a group of
// teammates, summed from the readers' credited events (R6: events follow
// credit, as On/Off counts). Counts come from SQL; only the rates are derived
// here, unrounded (display rounds with ratingLabel()).
import type { GameflowEvent, Side, Window } from "./gameflow";

export type StintRatings = {
  offPoss: number;
  offPts: number;
  defPoss: number;
  defPts: number;
  ortg: number | null;
  drtg: number | null;
  net: number | null;
};

const rate = (pts: number, poss: number) => (poss ? (100 * pts) / poss : null);

/**
 * Ratings over `side`'s rows whose credited five holds every one of
 * `players` (lane keys), within `window` when given (both ends inclusive).
 */
export function stintRatings(
  events: GameflowEvent[],
  side: Side,
  players: string[],
  window?: Window,
): StintRatings {
  let offPoss = 0,
    offPts = 0,
    defPoss = 0,
    defPts = 0;
  for (const e of events) {
    if (e.side !== side) continue;
    if (window && (e.elapsed < window.start || e.elapsed > window.end)) continue;
    if (!players.every((p) => e.players.includes(p))) continue;
    if (e.off) {
      offPoss += Number(e.poss_end);
      offPts += e.pts;
    } else {
      defPoss += Number(e.poss_end);
      defPts += e.pts;
    }
  }
  const ortg = rate(offPts, offPoss),
    drtg = rate(defPts, defPoss);
  return {
    offPoss,
    offPts,
    defPoss,
    defPts,
    ortg,
    drtg,
    net: ortg == null || drtg == null ? null : ortg - drtg,
  };
}
