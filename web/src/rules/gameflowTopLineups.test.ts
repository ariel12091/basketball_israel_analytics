import { describe, it, expect } from "vitest";
import { gameLineups, type Lane, type Score, type Dictionary } from "./gameflow";

// Two own lineups (A: 0-300 and 500-600, B: 300-500), one opp lineup.
const lane = (
  side: Lane["side"],
  player: string,
  lineup: string,
  start: number,
  end: number,
): Lane => ({
  side,
  player_key: player,
  player_label: player,
  start_elapsed: start,
  end_elapsed: end,
  lineup_key: lineup,
  is_starter: false,
});
const lanes: Lane[] = [
  // Every member of a lineup carries its windows: counted once, not per player.
  ...["1", "2"].flatMap((p) => [
    lane("own", p, "A", 0, 300),
    lane("own", p, "A", 500, 600),
  ]),
  lane("own", "1", "B", 300, 500),
  lane("own", "3", "B", 300, 500),
  lane("opp", "9", "Z", 0, 600),
];
const dict: Dictionary = [
  { side: "own", lineup_key: "A", members: "1 | 2" },
  { side: "own", lineup_key: "B", members: "1 | 3" },
  { side: "opp", lineup_key: "Z", members: "9" },
];
// Own leads 10-4 at 300, 14-12 at 500, 20-12 at 600.
const steps: Score[] = [
  { elapsed: 300, margin: 6, order_key: 1, own: 10 },
  { elapsed: 500, margin: 2, order_key: 2, own: 14 },
  { elapsed: 600, margin: 8, order_key: 3, own: 20 },
];
const model = { lanes, steps, dict };

describe("gameLineups", () => {
  it("lists a side's lineups by minutes, each window counted once", () => {
    const out = gameLineups(model, "own");
    expect(out.map((g) => [dict[g.dictIndex].lineup_key, g.duration])).toEqual([
      ["A", 400],
      ["B", 200],
    ]);
    expect(out[0].windows).toEqual([
      { start: 0, end: 300 },
      { start: 500, end: 600 },
    ]);
  });
  it("carries the floor +/- signed for its side", () => {
    expect(gameLineups(model, "own").map((g) => g.pm)).toEqual([12, -4]);
    expect(gameLineups(model, "opp").map((g) => g.pm)).toEqual([-8]);
  });
  it("keeps only the top N", () => {
    expect(gameLineups(model, "own", 1)).toHaveLength(1);
  });
});
