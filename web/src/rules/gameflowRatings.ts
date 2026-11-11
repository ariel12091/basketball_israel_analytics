// Gameflow stint ratings (docs/shared_definitions.md, "Gameflow stint
// rating"): Net, ORtg and DRtg for one player, a stint or a group of
// teammates, summed from the readers' credited events (R6: events follow
// credit, as On/Off counts). Counts come from SQL; only the rates are derived
// here, unrounded (display rounds with ratingLabel()).
import { ratingLabel, type GameflowEvent, type Side, type Window } from "./gameflow";
import { rRound } from "./round";

/** Net as shown: ratingLabel() with a plus sign when positive. */
export const netLabel = (v: number | null) =>
  v != null && rRound(v, 1) > 0 ? `+${ratingLabel(v)}` : ratingLabel(v);

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
  return windowsRatings(events, side, players, window ? [window] : null);
}

/**
 * The same over several windows (a lineup's stints): an event counts once
 * when it falls in any of them, so a shared window edge is not doubled.
 * `null` means the whole game.
 */
export function windowsRatings(
  events: GameflowEvent[],
  side: Side,
  players: string[],
  windows: Window[] | null,
): StintRatings {
  let offPoss = 0,
    offPts = 0,
    defPoss = 0,
    defPts = 0;
  for (const e of events) {
    if (e.side !== side) continue;
    if (
      windows &&
      !windows.some((w) => e.elapsed >= w.start && e.elapsed <= w.end)
    )
      continue;
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
