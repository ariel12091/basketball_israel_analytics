import { describe, it, expect } from "vitest";
import { gameflowSummary } from "./gameflowSummary";
import { ribbonModel, type RawRibbon } from "./gameflow";
import { detectRuns, runExplanation } from "./gameflowRuns";
import { stintRatings } from "./gameflowRatings";
import fixture673 from "./__fixtures__/gameflow-673.json";
import fixture440 from "./__fixtures__/gameflow-440.json";

const names = { own: "Maccabi Tel-Aviv", opp: "Hapoel Tel-Aviv" };
const model = ribbonModel(fixture673 as unknown as RawRibbon, 2, "il", 673);
const s = gameflowSummary(model, names);

describe("gameflowSummary", () => {
  it("carries the runs with their explanation, from the same rules", () => {
    const runs = detectRuns(model.steps);
    expect(s.runs).toHaveLength(runs.length);
    s.runs.forEach((r, i) => {
      expect(r).toMatchObject(runs[i]);
      expect(r.lines).toEqual(runExplanation(model, runs[i], names));
    });
    expect(s.runs.find((r) => r.defining)!.lines[0]).toMatch(/^Maccabi Tel-Aviv 37-5/);
  });
  it("rates every player over the whole game, both teams", () => {
    const dib = s.players.find((p) => p.player.includes("DIBARTOLOMEO"))!;
    expect(dib).toMatchObject({
      side: "own",
      team: "Maccabi Tel-Aviv",
      off_poss: 47,
      off_pts: 62,
      def_poss: 49,
      def_pts: 38,
    });
    expect(dib.net).toBeCloseTo(stintRatings(model.events, "own", [dib.player_key]).net!, 9);
    expect(new Set(s.players.map((p) => p.side))).toEqual(new Set(["own", "opp"]));
    expect(new Set(s.players.map((p) => p.player_key)).size).toBe(s.players.length);
  });
  it("has no runs and no ratings without the data for them", () => {
    const m440 = ribbonModel(fixture440 as unknown as RawRibbon, 2, "il", 440);
    expect(gameflowSummary(m440, names).runs).toEqual([]);
    const bare = ribbonModel({ ...(fixture673 as unknown as RawRibbon), events: null }, 2, "il", 673);
    expect(gameflowSummary(bare, names).players.every((p) => p.net == null)).toBe(true);
  });
});
