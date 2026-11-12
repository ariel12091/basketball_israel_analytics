import { describe, it, expect } from "vitest";
import {
  detectRuns,
  periodClock,
  windowLabel,
  runExplanation,
  runLineupGroups,
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

describe("windowLabel", () => {
  const b = model673.bounds;
  it("names a window by period and game clock, as runs are named", () => {
    expect(windowLabel({ start: 375, end: 552 }, b)).toBe("Q1 3:45 – Q1 0:48");
    expect(windowLabel({ start: 1655, end: 2249 }, b)).toBe("Q3 2:25 – Q4 2:31");
  });
  it("starts a window on a period line in the period it opens", () => {
    expect(windowLabel({ start: 0, end: 600 }, b)).toBe("Q1 10:00 – Q1 0:00");
    expect(windowLabel({ start: 1200, end: 1566 }, b)).toBe("Q3 10:00 – Q3 3:54");
  });
});

describe("periodClock", () => {
  it("reads the clock left in the period", () => {
    const b = model673.bounds;
    expect(periodClock(0, b)).toBe("Q1 10:00");
    expect(periodClock(1655, b)).toBe("Q3 2:25");
    expect(periodClock(2249, b)).toBe("Q4 2:31");
    expect(periodClock(600, b)).toBe("Q1 0:00");
  });
  it("reads a moment on a period line as the next period's start when asked", () => {
    const b = model673.bounds;
    expect(periodClock(1200, b, true)).toBe("Q3 10:00");
    expect(periodClock(0, b, true)).toBe("Q1 10:00");
    expect(periodClock(2400, b, true)).toBe("Q4 0:00");
  });
});

// A Q1 run by the opening fives, from hand-built lanes and steps.
const opening = (() => {
  const own = ["1", "2", "3", "4", "5"],
    opp = ["6", "7", "8", "9", "10"];
  const lane = (side: "own" | "opp", p: string, s: number, e: number) => ({
    team_id: side === "own" ? 2 : 3,
    player_id: Number(p),
    player_label: `P${p}`,
    start_elapsed: s,
    end_elapsed: e,
    lineup_key: side,
  });
  const raw = {
    lanes: [
      ...own.map((p) => lane("own", p, 0, 600)),
      ...opp.map((p) => lane("opp", p, 0, 600)),
    ],
    margin: steps([[30, 3, 0], [60, 3, 0], [90, 0, 2], [120, 3, 0], [150, 3, 0]]),
    n_periods: 4,
  } as unknown as RawRibbon;
  return ribbonModel(raw, 2, "il", 1);
})();

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

describe("runExplanation", () => {
  const names = { own: "Maccabi Tel-Aviv", opp: "Hapoel Tel-Aviv" };
  const runs = detectRuns(model673.steps);
  const lines = (r: (typeof runs)[number]) => runExplanation(model673, r, names);
  it("reads window, lineups, results and cause in order", () => {
    const l = lines(runs.find((r) => r.defining)!);
    expect(l[0]).toBe(
      "Maccabi Tel-Aviv 37-5 · Q3 2:25 – Q4 2:31 · from down 20 to up 12",
    );
    expect(l[1]).toBe(
      "Maccabi Tel-Aviv: this five had been on since Q3 3:54 (JOHN DIBARTOLOMEO in for OSHAE BRISSETT); 3 changes during the run",
    );
    expect(l[2]).toBe(
      "Hapoel Tel-Aviv: this five had been on since Q3 3:01 (OZ BLAYZER in for TOMER GINAT); 5 changes during the run",
    );
    // Percentages lead, counts follow (user 2026-11-11).
    expect(l[3]).toBe(
      "Maccabi Tel-Aviv 75% from three (9/12), 18% the rest of the game (3/17) — shooting variance",
    );
    expect(l[4]).toBe(
      "Hapoel Tel-Aviv 17% inside the arc (1/6), 54% the rest of the game (15/28)",
    );
    // The strongest result that is not from three leads; the threes follow;
    // both teams changed just before, so neither lineup is credited.
    expect(l[5]).toBe(
      "Estimated cause: Hapoel Tel-Aviv's two-point shooting (17% vs 54% the rest of the game), with Maccabi Tel-Aviv's hot three-point shooting on top; both teams also changed lineups just before (Maccabi Tel-Aviv at Q3 3:54, Hapoel Tel-Aviv at Q3 3:01), so no lineup is credited. A timing match, not proof.",
    );
    expect(l).toHaveLength(6);
  });
  it("calls an opening five the starting five, never a substitution", () => {
    const [run] = detectRuns(opening.steps);
    expect(run).toMatchObject({ side: "own", pts: 12, oppPts: 2 });
    const lu = runLineups(opening, run);
    expect(lu.every((l) => l.cameIn.length === 0 && l.wentOut.length === 0)).toBe(true);
    const l = runExplanation(opening, run, { own: "A", opp: "B" }, []);
    expect(l[1]).toBe("A: the starting five; stayed on throughout");
    expect(l[2]).toBe("B: the starting five; stayed on throughout");
    expect(l.at(-1)).toBe(
      "Estimated cause: neither team changed lineups in the 2 minutes before the run, and no result stood out against the rest of the game.",
    );
  });
  it("credits only the team whose change came shortly before the run", () => {
    const d = runs.find((r) => r.defining)!;
    const lu = runLineups(model673, d);
    // Move Hapoel's change far back: only Maccabi's 3:54 change is close.
    const far = lu.map((x) => (x.side === "opp" ? { ...x, onSince: d.start - 400 } : x));
    expect(runExplanation(model673, d, names, [], far).at(-1)).toBe(
      "Estimated cause: Maccabi Tel-Aviv's lineup change at Q3 3:54 lines up with the run. A timing match, not proof.",
    );
    const oppOnly = lu.map((x) => (x.side === "own" ? { ...x, onSince: d.start - 400 } : x));
    expect(runExplanation(model673, d, names, [], oppOnly).at(-1)).toBe(
      "Estimated cause: Hapoel Tel-Aviv's lineup change at Q3 3:01 lines up with the run. A timing match, not proof.",
    );
    // A result and one team's change: both named.
    const twos = runStandouts(model673.events, d).filter((x) => !x.variance);
    expect(runExplanation(model673, d, names, twos, far).at(-1)).toBe(
      "Estimated cause: Hapoel Tel-Aviv's two-point shooting (17% vs 54% the rest of the game); Maccabi Tel-Aviv's lineup change at Q3 3:54 also lines up with the run. A timing match, not proof.",
    );
  });
  it("reads the margin from the run team's side", () => {
    const hapoel = runs.find((r) => r.side === "opp")!;
    expect(lines(hapoel)[0]).toMatch(/^Hapoel Tel-Aviv 30-4 · Q2 2:50 – Q3 4:18 · from (down|up|level)/);
  });
  it("names three-point shooting as the cause when only it stood out", () => {
    const r = runs.find((r) => r.defining)!;
    const three = [
      { side: "own" as const, metric: "fg3" as const, inRun: { made: 9, att: 12 }, rest: { made: 3, att: 17 }, variance: true },
    ];
    expect(runExplanation(model673, r, names, three).at(-1)).toBe(
      "Estimated cause: Maccabi Tel-Aviv's hot three-point shooting; both teams also changed lineups just before (Maccabi Tel-Aviv at Q3 3:54, Hapoel Tel-Aviv at Q3 3:01), so no lineup is credited. A timing match, not proof.",
    );
    const lu = runLineups(model673, r).map((x) => ({ ...x, onSince: r.start - 400 }));
    expect(runExplanation(model673, r, names, three, lu).at(-1)).toBe(
      "Estimated cause: Maccabi Tel-Aviv's hot three-point shooting; neither team changed lineups in the 2 minutes before the run.",
    );
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
  it("never offers points per possession as a cause", () => {
    for (const r of detectRuns(model673.steps))
      expect(runStandouts(model673.events, r).map((x) => String(x.metric))).not.toContain("rtg");
  });
  it("words turnovers and offensive rebounds as rates with counts", () => {
    const r = detectRuns(model673.steps).find((x) => x.defining)!;
    const names = { own: "A", opp: "B" };
    const l = runExplanation(model673, r, names, [
      { side: "opp", metric: "tov", inRun: { made: 8, att: 19 }, rest: { made: 8, att: 55 }, variance: false },
      { side: "own", metric: "oreb", inRun: { made: 4, att: 10 }, rest: { made: 5, att: 20 }, variance: false },
    ]);
    expect(l[3]).toBe("B turned it over on 42% of possessions (8/19), 15% the rest of the game (8/55)");
    expect(l[4]).toBe("A rebounded 40% of its missed shots (4/10), 25% the rest of the game (5/20)");
  });
  it("rests every number on enough attempts", () => {
    for (const x of s) {
      const min = x.metric === "tov" ? 8 : 6;
      expect(x.inRun.att).toBeGreaterThanOrEqual(min);
      expect(x.rest.att).toBeGreaterThanOrEqual(min);
    }
  });
  it("shows nothing on a short stretch", () => {
    expect(runStandouts(model673.events, { ...d, end: d.start + 30 })).toEqual([]);
  });
});

describe("runLineupGroups", () => {
  const d = detectRuns(model673.steps).find((r) => r.defining)!;
  const groups = runLineupGroups(model673, d);
  it("lists both teams' lineups, run team first", () => {
    expect(groups[0].side).toBe("own");
    expect(new Set(groups.map((g) => g.side))).toEqual(new Set(["own", "opp"]));
  });
  it("counts only the time inside the run", () => {
    for (const side of ["own", "opp"] as const) {
      const secs = groups.filter((g) => g.side === side).reduce((t, g) => t + g.duration, 0);
      expect(secs).toBeCloseTo(d.end - d.start, 6);
    }
    for (const g of groups)
      for (const w of g.windows) {
        expect(w.start).toBeGreaterThanOrEqual(d.start);
        expect(w.end).toBeLessThanOrEqual(d.end);
      }
  });
  it("orders each team's lineups by time in the run", () => {
    for (const side of ["own", "opp"] as const) {
      const t = groups.filter((g) => g.side === side).map((g) => g.duration);
      expect(t).toEqual([...t].sort((a, b) => b - a));
    }
  });
});
