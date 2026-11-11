// What a Gameflow answer says about one game, computed once on the server so
// the page and the API/chat read the same values (rule 1): the runs with
// their explanation lines (gameflowRuns.ts) and each player's whole-game
// credited ratings (gameflowRatings.ts). Nothing here re-derives a count.
import type { RibbonModel, Side } from "./gameflow";
import { detectRuns, runExplanation, type Run } from "./gameflowRuns";
import { stintRatings } from "./gameflowRatings";

export type ExplainedRun = Run & { lines: string[] };
export type PlayerRating = {
  side: Side;
  team: string;
  player_key: string;
  player: string;
  /** Floor time and +/- as the lanes show them. */
  secs: number;
  pm: number | null;
  off_poss: number;
  off_pts: number;
  def_poss: number;
  def_pts: number;
  /** Unrounded; null with no possession or no events. */
  ortg: number | null;
  drtg: number | null;
  net: number | null;
};

export function gameflowSummary(
  model: RibbonModel,
  names: Record<Side, string>,
): { runs: ExplainedRun[]; players: PlayerRating[] } {
  const runs = detectRuns(model.steps).map((r) => ({
    ...r,
    lines: runExplanation(model, r, names),
  }));
  const players = model.totals.map((t) => {
    const label =
      model.lanes.find((l) => l.side === t.side && l.player_key === t.player_key)
        ?.player_label ?? t.player_key;
    const r = stintRatings(model.events, t.side, [t.player_key]),
      rated = model.events.length > 0;
    return {
      side: t.side,
      team: names[t.side],
      player_key: t.player_key,
      player: label,
      secs: t.secs,
      pm: t.pm,
      off_poss: r.offPoss,
      off_pts: r.offPts,
      def_poss: r.defPoss,
      def_pts: r.defPts,
      ortg: rated ? r.ortg : null,
      drtg: rated ? r.drtg : null,
      net: rated ? r.net : null,
    };
  });
  return { runs, players };
}
