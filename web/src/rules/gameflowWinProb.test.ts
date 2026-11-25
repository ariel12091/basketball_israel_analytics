import { describe, it, expect } from "vitest";
import { ribbonModel, type RawRibbon, type RawWinProb } from "./gameflow";
import { detectRuns, runCandidates } from "./gameflowRuns";
import { runSwing, winChanceAt, winLabels, winPct, winSeries, swingPoints } from "./gameflowWinProb";
import fixture673 from "./__fixtures__/gameflow-673.json";
import fixture440 from "./__fixtures__/gameflow-440.json";
import fixture1147 from "./__fixtures__/gameflow-1147.json";

const raw = (f: unknown) => f as RawRibbon;
const games = [
  { name: "673", f: raw(fixture673), team: 2, league: "il", game: 673 },
  { name: "440", f: raw(fixture440), team: 2, league: "il", game: 440 },
  { name: "1147", f: raw(fixture1147), team: 9, league: "euro", game: 1147 },
];
const row = (o: Partial<RawWinProb>): RawWinProb => ({
  possession_no: 1,
  is_final: false,
  period: 1,
  seconds_left: 2400,
  home_team_id: 2,
  away_team_id: 3,
  home_score: 0,
  away_score: 0,
  home_win_probability: 0.6,
  ...o,
});

describe("winSeries", () => {
  it("reads the page team's side of every possession, ending on the result", () => {
    const s = winSeries(raw(fixture673).win_prob, 2);
    expect(s.length).toBe(149);
    expect(s.at(-1)).toMatchObject({ final: true, own: 1 });
    expect(s.every((p) => p.own >= 0 && p.own <= 1)).toBe(true);
  });
  it("flips the chance for the away team", () => {
    const rows = raw(fixture673).win_prob!,
      home = winSeries(rows, 2),
      away = winSeries(rows, 3);
    away.forEach((p, i) => {
      expect(p.own).toBeCloseTo(1 - home[i].own, 12);
      expect(p.ownScore).toBe(home[i].oppScore);
    });
  });
  it("puts each row on the ribbon's clock, overtime included", () => {
    const s = winSeries(
      [
        row({ possession_no: 2, period: 2, seconds_left: 1500 }),
        row({ possession_no: 1 }),
        row({ possession_no: 3, period: 5, seconds_left: 120 }),
      ],
      2,
    );
    expect(s.map((p) => p.elapsed)).toEqual([0, 900, 2580]);
  });
  it("is empty without rows", () => {
    expect(winSeries(undefined, 2)).toEqual([]);
    expect(winSeries(null, 2)).toEqual([]);
  });
  it("is on the model", () => {
    const m = ribbonModel(raw(fixture673), 2, "il", 673);
    expect(m.winProb.length).toBe(149);
    expect(ribbonModel({ ...raw(fixture673), win_prob: undefined }, 2, "il", 673).winProb).toEqual([]);
  });
});

describe("winChanceAt", () => {
  const s = winSeries(raw(fixture673).win_prob, 2);
  it("is the pre-game chance at tip-off and the result after the end", () => {
    expect(winChanceAt(s, 0)).toBe(s[0]);
    expect(winChanceAt(s, 1e9)).toBe(s.at(-1));
  });
  it("is null with no series", () => {
    expect(winChanceAt([], 100)).toBeNull();
  });
});

describe("runSwing", () => {
  it("reads the run's start and end at the run's own scores", () => {
    for (const g of games) {
      const m = ribbonModel(g.f, g.team, g.league, g.game);
      // Every candidate the ranking reads, not only the three shown.
      const runs = runCandidates(m.steps);
      expect(runs.length, g.name).toBeGreaterThan(detectRuns(m.steps).length);
      // A run that starts or stops inside a same-second scoring burst (a
      // free-throw trip: no possession starts mid-trip) reads the state
      // before the second's first score or after its last one.
      const scored = m.steps.filter((x) => x.own != null),
        before = (t: number) => Number(scored.filter((x) => x.elapsed < t).at(-1)?.margin ?? 0),
        after = (t: number) => Number(scored.filter((x) => x.elapsed <= t).at(-1)?.margin ?? 0);
      for (const r of runs) {
        const pts = swingPoints(m.winProb, r)!,
          from = pts.from.ownScore - pts.from.oppScore,
          to = pts.to.ownScore - pts.to.oppScore;
        // The one known gap (646 run ends checked): in 1147 two free throws
        // at 2067 s are followed by no possession start before the 3 at
        // 2093 s, so the state at margin 10 does not exist in the model's
        // rows; the run reads the 2063 s state (margin 8).
        if (g.name === "1147" && r.start === 2093) expect(from).toBe(8);
        else expect([r.fromMargin, before(r.start)], `${g.name} ${r.start} from`).toContain(from);
        expect([r.toMargin, after(r.end)], `${g.name} ${r.end} to`).toContain(to);
      }
    }
  });
  it("is from the run team's side", () => {
    const m = ribbonModel(raw(fixture673), 2, "il", 673);
    for (const r of detectRuns(m.steps)) {
      const sw = runSwing(m.winProb, r)!,
        pts = swingPoints(m.winProb, r)!,
        side = (p: { own: number }) => (r.side === "own" ? p.own : 1 - p.own);
      expect(sw.from).toBeCloseTo(side(pts.from), 12);
      expect(sw.to).toBeCloseTo(side(pts.to), 12);
      expect(sw.to).toBeGreaterThan(sw.from);
    }
  });
  it("is null with no series", () => {
    expect(runSwing([], { side: "own", start: 10, end: 20 })).toBeNull();
  });
});

describe("winPct", () => {
  const p = (own: number, final = false) => ({ elapsed: 0, own, ownScore: 0, oppScore: 0, final });
  it("rounds to a whole percent and never shows 0 or 100 before the end", () => {
    expect(winPct(p(0.71))).toBe("71%");
    expect(winPct(p(0.004))).toBe("<1%");
    expect(winPct(p(0.996))).toBe(">99%");
    expect(winPct(p(1, true))).toBe("100%");
    expect(winPct(p(0, true))).toBe("0%");
  });
});

describe("winLabels", () => {
  const s = winSeries(raw(fixture673).win_prob, 2);
  it("gives both teams' chance at a moment, adding up to 100", () => {
    const w = winLabels(s, 1200)!;
    expect(Number(w.own.replace("%", "")) + Number(w.opp.replace("%", ""))).toBe(100);
    expect(winLabels(s, 1e9)).toEqual({ own: "100%", opp: "0%" });
  });
  it("is null with no series", () => {
    expect(winLabels([], 10)).toBeNull();
  });
});
