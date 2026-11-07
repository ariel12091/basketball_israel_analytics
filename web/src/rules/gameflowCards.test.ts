import { describe, it, expect } from "vitest";
import { points, windowsSummary, type Score } from "./gameflow";

// Own scores 2 at 60s, opp 3 at 120s, own 2 at 200s, opp 2 at 300s.
const steps: Score[] = [
  { elapsed: 60, margin: 2, own: 2, order_key: 1 },
  { elapsed: 120, margin: -1, own: 2, order_key: 2 },
  { elapsed: 200, margin: 1, own: 4, order_key: 3 },
  { elapsed: 300, margin: -1, own: 4, order_key: 4 },
];

describe("windowsSummary (Gameflow player cards)", () => {
  it("sums floor time and points window by window", () => {
    const s = windowsSummary(
      "own",
      [
        { start: 0, end: 100 },
        { start: 150, end: 250 },
      ],
      steps,
    );
    expect(s).toEqual({ secs: 200, pm: "+4", pf: 4, pa: 0 });
  });
  it("one window equals points() for that window", () => {
    const p = points("opp", 0, 300, steps),
      s = windowsSummary("opp", [{ start: 0, end: 300 }], steps);
    expect([s.pf, s.pa]).toEqual([p.pf, p.pa]);
    expect(s.pm).toBe("+1");
  });
  it("no score data leaves the points blank", () => {
    expect(windowsSummary("own", [{ start: 0, end: 60 }], [])).toEqual({
      secs: 60,
      pm: "",
      pf: null,
      pa: null,
    });
  });
});
