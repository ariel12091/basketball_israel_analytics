import { describe, it, expect } from "vitest";
import {
  detectRuns,
  periodClock,
  runLineups,
  runStandouts,
} from "./gameflowRuns";
import { ribbonModel, type RawRibbon, type Score } from "./gameflow";
import fixture673 from "./__fixtures__/gameflow-673.json";
import fixture440 from "./__fixtures__/gameflow-440.json";

const model673 = ribbonModel(fixture673 as unknown as RawRibbon, 2, "il", 673);
const model440 = ribbonModel(fixture440 as unknown as RawRibbon, 2, "il", 440);
const at = (m: typeof model673, t: number) => periodClock(t, m.bounds);

// Steps from a list of [elapsed, ownPts, oppPts] scoring increments.
const steps = (rows: [number, number, number][]): Score[] => {
  let own = 0,
    opp = 0;
  return rows.map(([elapsed, a, b], i) => {
    own += a;
    opp += b;
    return { elapsed, order_key: i, own, margin: own - opp };
  });
};

describe("periodClock", () => {
  it("reads the clock left in the period", () => {
    const b = model673.bounds;
    expect(periodClock(0, b)).toBe("Q1 10:00");
    expect(periodClock(1655, b)).toBe("Q3 2:25");
    expect(periodClock(2249, b)).toBe("Q4 2:31");
    expect(periodClock(600, b)).toBe("Q1 0:00");
  });
});

describe("detectRuns", () => {
  it("finds game 673's three runs, the defining one first by net", () => {
    const runs = detectRuns(model673.steps);
    expect(runs).toHaveLength(3);
    const defining = runs.find((r) => r.defining)!;
    expect(defining).toMatchObject({ side: "own", pts: 37, oppPts: 5, net: 32 });
    expect([at(model673, defining.start), at(model673, defining.end)]).toEqual([
      "Q3 2:25",
      "Q4 2:31",
    ]);
    const label = (r: (typeof runs)[number]) =>
      `${r.side} ${r.pts}-${r.oppPts} ${at(model673, r.start)} ${at(model673, r.end)}`;
    expect(runs.map(label)).toEqual([
      "own 16-6 Q1 3:45 Q1 0:48",
      "opp 30-4 Q2 2:50 Q3 4:18",
      "own 37-5 Q3 2:25 Q4 2:31",
    ]);
    expect(runs.filter((r) => r.defining)).toHaveLength(1);
  });

  it("gives the margins just before and at the end of the run", () => {
    const d = detectRuns(model673.steps).find((r) => r.defining)!;
    expect(d.toMargin - d.fromMargin).toBe(32);
  });

  it("finds none in game 440", () => {
    expect(detectRuns(model440.steps)).toEqual([]);
  });

  it("needs a net of 10 with the other team at 6 or fewer", () => {
    expect(detectRuns(steps([[10, 4, 0], [20, 0, 6], [30, 6, 0], [40, 6, 0]]))).toHaveLength(1);
    expect(detectRuns(steps([[10, 3, 0], [20, 3, 0], [30, 3, 0]]))).toEqual([]);
    expect(
      detectRuns(steps([[10, 8, 0], [20, 0, 7], [30, 8, 0], [40, 7, 0]])).some(
        (r) => r.oppPts === 7,
      ),
    ).toBe(false);
  });

  it("keeps the shorter window when nets tie", () => {
    // 10-0 in 20 s, and the same 10-0 stretched by a 2-2 exchange first.
    const runs = detectRuns(
      steps([[10, 2, 0], [20, 0, 2], [100, 5, 0], [120, 5, 0]]),
    );
    expect(runs).toHaveLength(1);
    expect([runs[0].start, runs[0].end]).toEqual([100, 120]);
  });
});

describe("runLineups", () => {
  const d = detectRuns(model673.steps).find((r) => r.defining)!;
  const lu = runLineups(model673, d);
  const side = (s: "own" | "opp") => lu.find((l) => l.side === s)!;
  it("names the change that put each five on the floor", () => {
    expect(side("own").onSince).toBe(1566);
    expect(side("own").cameIn.join(" ")).toContain("DIBARTOLOMEO");
    expect(side("own").wentOut.join(" ")).toContain("BRISSETT");
  });
  it("lists the other team's change too", () => {
    // Hapoel changed at Q3 3:54 too, then again at Q3 3:01: the five on the
    // floor at Maccabi's first point is the 3:01 one.
    const opp = side("opp");
    expect(at(model673, opp.onSince)).toBe("Q3 3:01");
    expect(opp.cameIn).toEqual(["OZ BLAYZER"]);
    expect(opp.wentOut).toEqual(["TOMER GINAT"]);
  });
  it("counts the changes during the run", () => {
    expect(side("own").changesDuring).toBe(3);
    expect(side("own").stayedOn).toBe(false);
  });
  it("puts the run team first", () => {
    expect(lu[0].side).toBe("own");
  });
});

describe("runStandouts", () => {
  const d = detectRuns(model673.steps).find((r) => r.defining)!;
  const s = runStandouts(model673.events, d);
  it("finds the hot three-point shooting and the cold two-point shooting", () => {
    // Maccabi 9/12 from three (3/17 otherwise); Hapoel 1/6 inside the arc
    // (15/28 otherwise), a 37-point gap. Hapoel's 0/6 from three also
    // qualifies, but only one three-point line is kept; its 8 turnovers in 19
    // possessions (8 in 55) rank third.
    expect(s).toEqual([
      { side: "own", metric: "fg3", inRun: { made: 9, att: 12 }, rest: { made: 3, att: 17 }, variance: true },
      { side: "opp", metric: "fg2", inRun: { made: 1, att: 6 }, rest: { made: 15, att: 28 }, variance: false },
    ]);
  });
  it("rests every number on enough attempts", () => {
    for (const x of s) {
      const min = x.metric === "tov" || x.metric === "rtg" ? 8 : 6;
      expect(x.inRun.att).toBeGreaterThanOrEqual(min);
      expect(x.rest.att).toBeGreaterThanOrEqual(min);
    }
  });
  it("shows nothing on a short stretch", () => {
    expect(runStandouts(model673.events, { ...d, end: d.start + 30 })).toEqual([]);
  });
});
