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
