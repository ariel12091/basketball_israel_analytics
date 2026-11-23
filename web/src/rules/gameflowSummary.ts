// What a Gameflow answer says about one game, computed once on the server so
// the page and the API/chat read the same values (rule 1): the runs with
// their explanation lines (gameflowRuns.ts) and each player's whole-game
// credited ratings (gameflowRatings.ts). Nothing here re-derives a count.
import type { RibbonModel, Side } from "./gameflow";
import { detectRuns, runExplanation, surnameOf, type Run } from "./gameflowRuns";
import { stintRatings } from "./gameflowRatings";
import { factorsSentence, namedFactors, profileLine, runFactors, type RunPriors } from "./gameflowRunCause";

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
  /** Israeli league: player priors and heights (m) by `${side}:${player}`,
   *  for the run explanation judged against the players (D43 otherwise). */
  priors?: { priors: RunPriors; heights: Map<string, number> } | null,
): { runs: ExplainedRun[]; players: PlayerRating[] } {
  const label = new Map(model.lanes.map((l) => [`${l.side}:${l.player_key}`, l.player_label]));
  const surname = (side: Side, player: string) =>
    surnameOf(label.get(`${side}:${player}`) ?? player);
  const runs = detectRuns(model.steps).map((r) => {
    const results = priors
      ? factorsSentence(namedFactors(runFactors(model.events, r, priors.priors, priors.heights, surname), r.net), names)
      : undefined;
    const lines = runExplanation(model, r, names, undefined, undefined, results);
    // What the main fives usually do, before the cause (which stays last).
    const profile = priors
      ? profileLine(model, r, names, priors.priors, priors.heights, surname)
      : null;
    if (profile) lines.splice(-1, 0, profile);
    return { ...r, lines };
  });
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
