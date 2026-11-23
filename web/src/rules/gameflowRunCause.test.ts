import { describe, it, expect } from "vitest";
import {
  factorsSentence,
  factorReason,
  lineupProfile,
  mainFives,
  namedFactors,
  poissonBinomialTail,
  runFactors,
  type Factor,
  type PlayerPrior,
  type RunPriors,
} from "./gameflowRunCause";
import type { GameflowEvent, Side } from "./gameflow";
import type { Run } from "./gameflowRuns";
import { runExplanation } from "./gameflowRuns";
import { ribbonModel, type RawRibbon } from "./gameflow";
import fixture673 from "./__fixtures__/gameflow-673.json";

const LEAGUE = {
  fg2: 0.53,
  fg3: 0.35,
  ft: 0.73,
  tovPlayer: 0.038,
  tovTeam: 0.19,
  orebPlayer: 0.024,
  drebPlayer: 0.062,
  orebMiss: 0.32,
  ppp: 1.09,
};
const run: Run = {
  side: "own",
  start: 100,
  end: 300,
  pts: 14,
  oppPts: 2,
  net: 12,
  defining: false,
  fromMargin: 0,
  toMargin: 12,
};
const OWN = ["1", "2", "3", "4", "5"],
  OPP = ["6", "7", "8", "9", "10"];
let key = 0;
// One counted event and its mirror (the defending five), as the readers return both.
function ev(
  side: Side,
  elapsed: number,
  kind: GameflowEvent["kind"],
  made: boolean | null,
  actor: string | null,
  possEnd = true,
): GameflowEvent[] {
  const k = ++key,
    other: Side = side === "own" ? "opp" : "own";
  const base = { elapsed, order_key: k, pts: 0, kind, made, poss_end: possEnd };
  return [
    { ...base, side, off: true, players: side === "own" ? OWN : OPP, actor },
    {
      ...base,
      side: other,
      off: false,
      players: side === "own" ? OPP : OWN,
      actor,
    },
  ];
}
const prior = (over: Partial<PlayerPrior> = {}): PlayerPrior => ({
  fg2: [0, 0],
  fg3: [0, 0],
  ft: [0, 0],
  tov: [0, 0],
  oreb: [0, 0],
  dreb: [0, 0],
  ...over,
});
const priorsOf = (players: Record<string, PlayerPrior>): RunPriors => ({
  players: new Map(Object.entries(players)),
  league: LEAGUE,
});
const surname = (_s: Side, p: string) => `P${p}`;
const factor = (f: Partial<Factor>): Factor => ({
  end: "run",
  side: "own",
  kind: "fg2",
  obs: 0,
  n: 6,
  expected: 0,
  pts: 0,
  p: 1,
  who: [],
  ...f,
});

describe("poissonBinomialTail", () => {
  it("is the exact tail of the count, each attempt with its own chance", () => {
    expect(poissonBinomialTail([0.5, 0.5, 0.5], 3, true)).toBeCloseTo(
      0.125,
      12,
    );
    expect(poissonBinomialTail([0.5, 0.5, 0.5], 0, false)).toBeCloseTo(
      0.125,
      12,
    );
    // P(X >= 1) = 1 - 0.8 * 0.4
    expect(poissonBinomialTail([0.2, 0.6], 1, true)).toBeCloseTo(0.68, 12);
    expect(poissonBinomialTail([0.2, 0.6], 2, true)).toBeCloseTo(0.12, 12);
    expect(poissonBinomialTail([], 0, true)).toBe(1);
  });
});

describe("runFactors", () => {
  it("judges each shot against its shooter, pulled toward the league", () => {
    const events = [1, 2, 3, 4].flatMap((i) =>
      ev("own", 100 + i, "fg2", true, "1"),
    );
    const good = runFactors(
      events,
      run,
      priorsOf({ "own:1": prior({ fg2: [450, 500] }) }),
      new Map(),
      surname,
    );
    const none = runFactors(events, run, priorsOf({}), new Map(), surname);
    const g = good.find((f) => f.kind === "fg2" && f.end === "run")!,
      n = none.find((f) => f.kind === "fg2" && f.end === "run")!;
    // (450 + 55 * 0.53) / (500 + 55) per shot, against the league's 0.53.
    expect(g.expected).toBeCloseTo((4 * (450 + 55 * 0.53)) / 555, 10);
    expect(n.expected).toBeCloseTo(4 * 0.53, 10);
    expect(g.pts).toBeLessThan(n.pts);
    expect(n.pts).toBeCloseTo(2 * (4 - 4 * 0.53), 10);
  });
  it("counts the other team's misses and turnovers toward the run team", () => {
    const events = [
      ...[1, 2, 3, 4, 5].flatMap((i) => ev("opp", 110 + i, "fg3", false, "6")),
      ...[1, 2, 3].flatMap((i) => ev("opp", 150 + i, "tov", null, "7")),
      ...ev("opp", 400, "tov", null, "7"), // after the run: not counted
    ];
    const f = runFactors(events, run, priorsOf({}), new Map(), surname);
    const threes = f.find((x) => x.end === "other" && x.kind === "fg3")!,
      tov = f.find((x) => x.end === "other" && x.kind === "tov")!;
    expect(threes.pts).toBeCloseTo(3 * 5 * 0.35, 10);
    expect(threes.p).toBeCloseTo(0.65 ** 5, 10);
    expect(tov.obs).toBe(3);
    expect(tov.n).toBe(8);
    expect(tov.expected).toBeCloseTo(8 * 0.19, 10);
    expect(tov.pts).toBeCloseTo(1.09 * (3 - 8 * 0.19), 10);
    expect(tov.who).toEqual([["P7", 3]]);
  });
  it("expects more turnovers from a five that turns it over more", () => {
    const events = [1, 2, 3, 4, 5, 6, 7, 8].flatMap((i) =>
      ev("opp", 110 + i, "fg2", false, "6"),
    );
    const careless = Object.fromEntries(
      OPP.map((p) => [`opp:${p}`, prior({ tov: [60, 600] })]),
    );
    const a = runFactors(
        events,
        run,
        priorsOf(careless),
        new Map(),
        surname,
      ).find((x) => x.kind === "tov" && x.end === "other")!,
      b = runFactors(events, run, priorsOf({}), new Map(), surname).find(
        (x) => x.kind === "tov" && x.end === "other",
      )!;
    expect(a.expected).toBeGreaterThan(b.expected);
  });
  it("keeps 0.6 of the five's turnover edge over the league (TOV_FIVE_WEIGHT)", () => {
    const events = [1, 2, 3, 4].flatMap((i) => ev("opp", 110 + i, "fg2", false, "6"));
    // Players at twice the league rate, with a record far bigger than K.
    const twice = Object.fromEntries(OPP.map((p) => [`opp:${p}`, prior({ tov: [76e5, 1e8] })]));
    const f = runFactors(events, run, priorsOf(twice), new Map(), surname).find(
      (x) => x.kind === "tov" && x.end === "other",
    )!;
    expect(f.expected / 4).toBeCloseTo(LEAGUE.tovTeam * (1 + 0.6 * (2 - 1)), 4);
  });
  it("expects more offensive rebounds from a five of rebounders, and from a taller five", () => {
    const events = [1, 2, 3, 4, 5, 6].flatMap((i) =>
      ev("own", 100 + i, "fg2", false, "1"),
    );
    const rebounders = Object.fromEntries(
      OWN.map((p) => [`own:${p}`, prior({ oreb: [60, 600] })]),
    );
    const oreb = (pr: RunPriors, h = new Map<string, number>()) =>
      runFactors(events, run, pr, h, surname).find(
        (x) => x.kind === "oreb" && x.end === "run",
      )!;
    const base = oreb(priorsOf({}));
    expect(base.expected).toBeCloseTo(6 * 0.32, 10);
    expect(oreb(priorsOf(rebounders)).expected).toBeGreaterThan(base.expected);
    const tall = new Map([
      ...OWN.map((p) => [`own:${p}`, 2.05] as const),
      ...OPP.map((p) => [`opp:${p}`, 1.95] as const),
    ]);
    expect(oreb(priorsOf({}), tall).expected).toBeGreaterThan(base.expected);
  });
});

describe("namedFactors", () => {
  it("names nothing without a lead worth 2.5 points and unusual", () => {
    expect(
      namedFactors([
        factor({ pts: 2.4, p: 0.01 }),
        factor({ kind: "tov", pts: 2, p: 0.01 }),
      ]),
    ).toEqual([]);
    expect(namedFactors([factor({ pts: 4, p: 0.2 })])).toEqual([]);
  });
  it("lets a smaller factor lead a small run: a fifth of its net margin", () => {
    // 673's 16-6: Maccabi rebounded 4 of 5 misses, worth 2.4 of a 10-point run.
    const reb = factor({ kind: "oreb", obs: 4, n: 5, pts: 2.36, p: 0.063 }),
      second = factor({
        end: "other",
        side: "opp",
        kind: "tov",
        pts: 1.6,
        p: 0.05,
      });
    expect(namedFactors([reb], 10)).toEqual([reb]);
    expect(namedFactors([reb])).toEqual([]);
    expect(namedFactors([reb], 20)).toEqual([]);
    // The second bar never sits above the lead bar.
    const small = factor({ kind: "oreb", pts: 1.7, p: 0.04 }),
      tov = factor({
        kind: "tov",
        end: "other",
        side: "opp",
        pts: 1.6,
        p: 0.04,
      });
    expect(namedFactors([small, tov], 8)).toEqual([small, tov]);
    expect(namedFactors([second], 10)).toEqual([]);
  });
  it("needs four shots for a shooting factor", () => {
    expect(namedFactors([factor({ n: 3, pts: 3, p: 0.05 })])).toEqual([]);
  });
  it("adds second clauses from 1.5 points, and a team that barely turned it over", () => {
    const lead = factor({ pts: 3, p: 0.02 }),
      second = factor({
        end: "other",
        side: "opp",
        kind: "oreb",
        pts: 1.6,
        p: 0.05,
      }),
      small = factor({ kind: "ft", pts: 1.4, p: 0.01 }),
      clean = factor({
        kind: "tov",
        n: 9,
        obs: 0,
        expected: 2.1,
        pts: 1.2,
        p: 0.13,
      });
    expect(namedFactors([lead, second, small, clean])).toEqual([
      lead,
      second,
      clean,
    ]);
  });
  it("names every factor that qualifies, biggest first", () => {
    // 673's 37-5: four qualify, all named.
    const fs = [
      factor({ kind: "tov", side: "opp", end: "other", n: 19, pts: 4.82, p: 0.016 }),
      factor({ kind: "fg3", pts: 13.9, p: 0.007 }),
      factor({ kind: "fg2", side: "opp", end: "other", pts: 4.89, p: 0.054 }),
      factor({ kind: "fg3", side: "opp", end: "other", pts: 6.6, p: 0.064 }),
    ];
    expect(namedFactors(fs).map((f) => `${f.side}:${f.kind}`)).toEqual(["own:fg3", "opp:fg3", "opp:fg2", "opp:tov"]);
  });
});

describe("factorsSentence", () => {
  it("says what each team did, with whole expected numbers from 2 up", () => {
    const named = [
      factor({
        end: "other",
        side: "opp",
        kind: "tov",
        obs: 12,
        n: 23,
        expected: 4.7,
        pts: 8,
        who: [
          ["Sanogo", 4],
          ["Amir", 3],
        ],
      }),
      factor({
        end: "other",
        side: "opp",
        kind: "fg3",
        obs: 1,
        n: 10,
        expected: 3.5,
        pts: 7.6,
      }),
      factor({ kind: "oreb", obs: 0, n: 9, expected: 1.96, pts: 2 }),
    ];
    expect(factorsSentence(named, { own: "Maccabi", opp: "Holon" })).toBe(
      "Holon had 12 turnovers in 23 possessions (expected 5; Sanogo 4, Amir 3) and went 1/10 from three (expected 4); Maccabi rebounded none of 9 misses (expected 2)",
    );
    expect(
      factorsSentence([factor({ kind: "tov", obs: 0, n: 9, expected: 1.6 })], {
        own: "M",
        opp: "O",
      }),
    ).toBe("M had no turnovers in 9 possessions (expected 1.6)");
    expect(factorsSentence([], { own: "M", opp: "O" })).toBeNull();
  });
});

describe("runExplanation with the players' priors (Israeli league)", () => {
  const model = ribbonModel(fixture673 as unknown as RawRibbon, 2, "il", 673);
  const r: Run = {
    side: "own",
    start: 375,
    end: 552,
    pts: 16,
    oppPts: 6,
    net: 10,
    defining: false,
    fromMargin: 0,
    toMargin: 10,
  };
  const names = { own: "Maccabi", opp: "Hapoel" };
  it("replaces the rest-of-game standouts with the factors sentence", () => {
    const lines = runExplanation(
      model,
      r,
      names,
      undefined,
      undefined,
      "Hapoel had 5 turnovers in 9 possessions (expected 2)",
    );
    expect(lines.some((l) => l.includes("the rest of the game"))).toBe(false);
    expect(lines.at(-1)).toMatch(
      /^Estimated cause: Hapoel had 5 turnovers in 9 possessions \(expected 2\)[;.]/,
    );
  });
  it("says so when nothing stood out", () => {
    expect(
      runExplanation(model, r, names, undefined, undefined, null).at(-1),
    ).toMatch(
      /^Estimated cause: no single result stood out against what these players usually do[;.]/,
    );
  });
  it("keeps the standout lines when no priors were read (EuroLeague, D43)", () => {
    const lines = runExplanation(model, r, names);
    expect(lines.at(-1)).toMatch(/^Estimated cause: /);
    expect(lines.at(-1)).not.toMatch(/what these players usually do/);
  });
});


describe("factor reasons (Gameflow lineup profile)", () => {
  const H = (side: Side, five: string[], hs: number[]) =>
    new Map(five.map((p, i) => [`${side}:${p}`, hs[i]] as [string, number]));
  const avg = lineupProfile("own", OWN, priorsOf({}), new Map());
  it("says nothing of an average five", () => {
    for (const k of ["fg2", "fg3", "ft", "tov", "oreb"] as const)
      expect(factorReason(k, avg, avg)).toBeNull();
  });
  it("names a small five, and a defense weak on its glass, for offensive rebounds", () => {
    const small = lineupProfile("own", OWN, priorsOf({}), H("own", OWN, [1.9, 1.92, 1.95, 1.98, 2.01]));
    expect(small.tallest).toBe(2.01);
    expect(factorReason("oreb", small, avg)).toBe("a five no taller than 2.01 m");
    const thin = prior({ dreb: [20, 1000] });
    const def = lineupProfile("opp", OPP, priorsOf(Object.fromEntries(OPP.map((p) => [`opp:${p}`, thin]))), new Map());
    expect(factorReason("oreb", small, def)).toBe(
      "a five no taller than 2.01 m and against a weak defensive-rebounding five",
    );
    // Size is left out unless every height is known.
    expect(lineupProfile("own", OWN, priorsOf({}), H("own", OWN.slice(0, 4), [1.9, 1.9, 1.9, 1.9])).tallest).toBeNull();
  });
  it("names a turnover-prone five for turnovers and shooters for threes only", () => {
    const sloppy = prior({ tov: [60, 1000], fg3: [45, 100] });
    const p = lineupProfile("own", OWN, priorsOf(Object.fromEntries(OWN.map((x) => [`own:${x}`, sloppy]))), new Map());
    expect(factorReason("tov", p, avg)).toBe("a turnover-prone five");
    expect(factorReason("fg3", p, avg)).toBe("good three-point shooters");
    expect(factorReason("fg2", p, avg)).toBeNull();
  });
  it("profiles a team's five only when it played half the run and a minute", () => {
    const model = ribbonModel(fixture673 as unknown as RawRibbon, 2, "il", 673);
    const fives = mainFives(model, { ...run, start: 0, end: 400 });
    expect([...fives.values()].every((f) => f.length === 5)).toBe(true);
    expect(fives.size).toBeGreaterThan(0);
    expect(mainFives(model, { ...run, start: 0, end: 30 }).size).toBe(0);
  });
  it("puts the reason inside the clause's brackets", () => {
    const f = factor({ end: "other", side: "opp", kind: "oreb", obs: 0, n: 7, expected: 2.1, pts: 2.3, p: 0.05, why: "a five no taller than 2.01 m" });
    expect(factorsSentence([f], { own: "A", opp: "B" })).toBe(
      "B rebounded none of 7 misses (expected 2, a five no taller than 2.01 m)",
    );
  });
});
