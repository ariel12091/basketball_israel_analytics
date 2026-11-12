import { describe, it, expect } from "vitest";
import { readdirSync, readFileSync } from "node:fs";
import {
  ribbonModel,
  periodBounds,
  mergeWindows,
  sharedWindows,
  shortLabels,
  completeMargin,
  points,
  marginScale,
  clock,
  ratingSeries,
  ratingAt,
  windowSeries,
  RATING_WINDOW,
  type PossEnd,
  type Score,
  windowDetail,
  type RawRibbon,
} from "./gameflow";
const directory = "test/goldens/gameflow",
  files = readdirSync(directory).filter((f) => f.endsWith(".json"));
describe("Gameflow Shiny reference models", () => {
  it("requires all ten real Shiny query and transform fixtures", () =>
    expect(files).toHaveLength(10));
  for (const file of files)
    it(file, () => {
      const {
        case: c,
        raw,
        expected,
      } = JSON.parse(readFileSync(`${directory}/${file}`, "utf8"));
      const model = ribbonModel(raw, c.team_id, c.league, c.game_id);
      for (const key of [
        "lanes",
        "bars",
        "margin",
        "dict",
        "totals",
        "details",
        "health",
      ] as const)
        expect(model[key], key).toEqual(expected[key]);
      expect(
        model.bars.map((b) =>
          model.bounds.map((end, i) => {
            const start = Math.max(
                b.start_elapsed,
                i ? model.bounds[i - 1] : 0,
              ),
              to = Math.min(b.end_elapsed, end);
            return to <= start
              ? null
              : windowDetail(
                  model.lanes,
                  model.steps,
                  model.dict,
                  b,
                  start,
                  to,
                );
          }),
        ),
      ).toEqual(expected.periodDetails);
    });
});
describe("Gameflow edge cases and shared-player windows", () => {
  it("uses nominal regulation/OT bounds and closes an incomplete feed only at the reached quarter", () => {
    expect(periodBounds(6)).toEqual([600, 1200, 1800, 2400, 2700, 3000]);
    expect(
      completeMargin(
        [{ elapsed: 961, margin: 7, order_key: 1 }],
        periodBounds(2),
      ),
    ).toEqual([
      { elapsed: 0, margin: 0 },
      { elapsed: 961, margin: 7 },
      { elapsed: 1200, margin: 7 },
    ]);
  });
  it("uses the final same-second score, nets rescinded baskets, and mirrors opponent perspective", () => {
    const steps = [
      { elapsed: 10, margin: 2, own: 2, order_key: 1 },
      { elapsed: 10, margin: 3, own: 3, order_key: 2 },
      { elapsed: 20, margin: 1, own: 1, order_key: 3 },
    ];
    expect(points("own", 0, 20, steps)).toEqual({ pm: 1, pf: 1, pa: 0 });
    expect(points("opp", 0, 20, steps)).toEqual({ pm: -1, pf: 0, pa: 1 });
    expect(points("own", 0, 20, [])).toEqual({ pm: null, pf: null, pa: null });
  });
  it("intersects multiple players across sides, merges adjacent windows and keeps duplicate names distinct by id", () => {
    const raw: RawRibbon = {
      lanes: [
        {
          team_id: 1,
          player_id: 7,
          player_label: "Same Name",
          start_elapsed: 0,
          end_elapsed: 100,
          lineup_key: "a",
        },
        {
          team_id: 1,
          player_id: 7,
          player_label: "Same Name",
          start_elapsed: 100,
          end_elapsed: 200,
          lineup_key: "b",
        },
        {
          team_id: 2,
          player_id: 7,
          player_label: "Same Name",
          start_elapsed: 50,
          end_elapsed: 120,
          lineup_key: "c",
        },
      ],
      margin: [],
      n_periods: 4,
    };
    const model = ribbonModel(raw, 1, "il", 1);
    expect(sharedWindows(model.bars, ["own:7", "opp:7"])).toEqual([
      { start: 50, end: 120 },
    ]);
    expect(
      mergeWindows([
        { start: 0, end: 10 },
        { start: 10, end: 20 },
      ]),
    ).toEqual([{ start: 0, end: 20 }]);
  });
  it("retains names, disambiguates compact surname collisions and floors clock labels", () => {
    expect(
      shortLabels(
        ["FIRST SMITH", "SECOND SMITH", "JONES, Name"],
        ["own", "own", "opp"],
      ),
    ).toEqual(["F. SMITH", "S. SMITH", "JONES"]);
    expect(clock(59.99)).toBe("0:59");
    expect(marginScale([{ elapsed: 0, margin: 30 }]).interval).toBe(10);
  });
});
describe("ratingSeries", () => {
  // 25 possessions each, alternating: the team scores 2 on each of its own,
  // the opponent 1 on each of its own, and an and-one free throw lands after
  // the team's last possession end.
  const poss: PossEnd[] = [],
    steps: Score[] = [];
  let own = 0,
    opp = 0,
    key = 0;
  for (let i = 1; i <= 25; i++) {
    own += 2;
    steps.push({ elapsed: 4 * i, order_key: ++key, own, margin: own - opp });
    poss.push({ elapsed: 4 * i, order_key: key, off: true, own, opp });
    opp += 1;
    steps.push({ elapsed: 4 * i + 2, order_key: ++key, own, margin: own - opp });
    poss.push({ elapsed: 4 * i + 2, order_key: key, off: false, own, opp });
  }
  steps.push({ elapsed: 101, order_key: ++key, own: own + 1, margin: own + 1 - opp });
  const s = ratingSeries(poss, steps);
  it("starts at the first possession and ends on each box-score rating", () => {
    expect(ratingAt(s, 3)).toEqual({ elapsed: 0, own: null, opp: null });
    expect(ratingAt(s, 4)).toMatchObject({ own: 200, opp: null });
    expect(ratingAt(s, 6)).toMatchObject({ own: 200, opp: 100 });
    const last = s[s.length - 1];
    expect(last.own).toBeCloseTo((100 * 51) / 25, 10);
    expect(last.opp).toBe(100);
  });
  it("last N counts each team's latest possessions, from the first", () => {
    const w = windowSeries(poss);
    expect(ratingAt(w, 4)).toMatchObject({ own: 200, opp: null });
    expect(ratingAt(w, 6)).toMatchObject({ own: 200, opp: 100 });
    const k = RATING_WINDOW + 1;
    expect(ratingAt(w, 4 * k)).toMatchObject({ own: 200 });
    expect(ratingAt(w, 4 * k + 2)).toMatchObject({ opp: 100 });
    // The trailing free throw follows the last end: the window ignores it.
    expect(w[w.length - 1].own).toBe(200);
  });
  it("is absent from the model when the reader sends no possessions", () => {
    const raw = { lanes: [], margin: [], n_periods: 4 };
    expect(ribbonModel(raw, 1, "euroleague", 1).ratings).toBeNull();
  });
});

describe("lineup names by height", () => {
  const raw = JSON.parse(readFileSync("src/rules/__fixtures__/gameflow-673.json", "utf8")) as RawRibbon;
  const height: Record<string, [number, string]> = {
    "JOHN DIBARTOLOMEO": [1.83, "G"], "YAM MADAR": [1.9, "G"], "KEATON WALLACE": [1.91, "G"],
    "JIMMY CLARK III": [1.91, "G"], "JAYLEN HOARD": [2.03, "F"], "ROMAN SORKIN": [2.08, "C"], "TJ LEAF": [2.08, "F-C"],
  };
  const withHeights = {
    ...raw,
    heights: (raw.lanes ?? [])
      .filter((l) => height[l.player_label])
      .map((l) => ({ team_id: l.team_id, player_id: l.player_id, height_m: height[l.player_label][0], position_en: height[l.player_label][1] })),
  } as RawRibbon;
  const members = (r: RawRibbon) => ribbonModel(r, 2, "il", 673).dict.map((d) => d.members);
  it("orders each lineup shortest to tallest; position breaks ties; unknown last", () => {
    const m = members(withHeights).find((x) => x.includes("TJ LEAF") && x.includes("DANIEL THEIS") && x.includes("JIMMY CLARK III"))!;
    // 1.83, 1.91 (G, G: by name), 2.08 F-C, then Theis with no height.
    expect(m).toBe("JOHN DIBARTOLOMEO | JIMMY CLARK III | KEATON WALLACE | TJ LEAF | DANIEL THEIS");
    // A lineup whose five all have a height: strictly shortest to tallest.
    const known = members(withHeights).find((x) => x.split(" | ").every((n) => height[n]))!;
    const hs = known.split(" | ").map((n) => height[n][0]);
    expect(hs).toEqual([...hs].sort((a, b) => a - b));
  });
  it("stays alphabetical without heights (EuroLeague, the stored goldens)", () => {
    for (const m of members(raw)) expect(m.split(" | ")).toEqual([...m.split(" | ")].sort((a, b) => a.localeCompare(b)));
  });
});
