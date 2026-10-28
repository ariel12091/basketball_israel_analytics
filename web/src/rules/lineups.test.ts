import { describe, it, expect } from "vitest";
import {
  localLineups,
  lineupPage,
  lineupAutoMin,
  lineupTableRows,
  rankLineups,
  euroLineupRates,
  pivotLineupGames,
} from "./lineups";
import { loadLineup, presets } from "../../test/lineupGoldens";
import type { Row } from "./gameLogs";
describe("Lineups: helpers.R membership and totals", () => {
  it("keeps defensive modal counts when unclassified MV rows follow them", () => {
    expect(
      pivotLineupGames([
        { game_id: 356, type_lineup: "defense", poss: 18, pts: 17 },
        { game_id: 356, type_lineup: "offense", poss: 20, pts: 33 },
        { game_id: 356, type_lineup: null, poss: 0, pts: 0 },
        { game_id: 999, type_lineup: null, poss: 0, pts: 0 },
      ]),
    ).toEqual([
      { game_id: 356, def_poss: 18, def_pts: 17, off_poss: 20, off_pts: 33 },
    ]);
  });
  const p = { on: [1], any: [2, 3], off: [4], count: 1, exact: true };
  it("combines all, exactly k-of, and exclusions", () => {
    const rows = [
      { team_id: 2, player_ids: "{1,2}" },
      { team_id: 2, player_ids: "{1,2,3}" },
      { team_id: 2, player_ids: "{1,2,4}" },
    ];
    expect(localLineups(rows, "2", p)).toEqual([rows[0]]);
    expect(localLineups(rows, "", p)).toEqual(rows);
  });
  it("auto threshold uses 150th row and can lower to zero", () => {
    expect(lineupAutoMin([])).toBeNull();
    expect(lineupAutoMin([{ total_poss: 999 }])).toBe(0);
    expect(
      lineupAutoMin(
        Array.from({ length: 200 }, (_, i) => ({ total_poss: 200 - i })),
      ),
    ).toBe(60);
  });
  it("filters before total and derives rates from additive counts", () => {
    const rows = [
      {
        off_poss: 10,
        off_pts: 20,
        def_poss: 10,
        def_pts: 10,
        total_poss: 20,
        minutes: 5,
        net_rtg: 100,
      },
      {
        off_poss: 100,
        off_pts: 100,
        def_poss: 100,
        def_pts: 130,
        total_poss: 200,
        minutes: 50,
        net_rtg: -30,
      },
    ];
    const total = lineupTableRows(rows, "summary", 0)[0];
    expect(total.off_ppp).toBeCloseTo((120 / 110) * 100);
    expect(total.minutes).toBe(55);
    expect(
      lineupTableRows(rows, "summary", 0, [
        { col: "net_rtg", op: "ge", value: 0 },
      ])[0].off_ppp,
    ).toBe(200);
  });
  it("copies raw Euro counts before replacing rate fields", () => {
    const row = euroLineupRates([
      {
        off_poss: 100,
        def_poss: 80,
        off_pts: 120,
        def_pts: 80,
        off_oreb: 12,
        off_oreb_opp: 40,
        off_tov: 10,
        starters_poss_num: 720,
      },
    ])[0];
    expect(row.off_oreb).toBe(30);
    expect(row.off_oreb_cnt).toBe(12);
    expect(row.off_tov).toBe(10);
    expect(row.num_starters).toBe(4);
  });
  it("preserves R's single-qualified-row difference between leagues", () => {
    const rows = [
      { off_poss: 100, def_poss: 100, off_ppp: 120, def_ppp: 100, net_rtg: 20 },
    ];
    expect(rankLineups(rows, false)[0].pr_ld_net).toBeNull();
    expect(rankLineups(rows, true)[0].pr_ld_net).toBe(0.5);
  });
});
describe("stored Shiny Lineups frames (all numeric fields, rates, ranks, TOTAL)", () => {
  it("has the full oracle, including EuroCup", () =>
    expect(presets.length).toBe(38));
  for (const file of presets)
    it(
      file,
      () => {
        const { g, rows } = loadLineup(file);
        // A missing render is never a passing empty frame.
        expect(g.expected, `${g.id}: Shiny table missing`).not.toBeNull();
        const expected = g.expected!.data;
        const key = (r: Row) =>
          r.is_total === 0 ? "TOTAL" : `${r.team_id}|${r.sub_lineup_hash}`;
        const actual = new Map(rows.map((r) => [key(r), r]));
        expect(actual.size, `${g.id}: duplicate actual row keys`).toBe(
          rows.length,
        );
        expect(rows.length).toBe(expected.length);
        for (const want of expected) {
          const got = actual.get(key(want));
          expect(got, `${g.id} ${key(want)}`).toBeDefined();
          for (const [k, v] of Object.entries(want))
            if (typeof v === "number") {
              // The numeric matcher rejects null/strings; do not coerce them.
              expect(got![k], `${g.id} ${key(want)} ${k}`).toBeCloseTo(v, 7);
            }
        }
        // Triple/quad recordings include tens of MiB and every numeric cell.
        // This is correctness replay; browser/load tests own latency acceptance.
      },
      /_(triple|quad)\.json$/.test(file) ? 15000 : 5000,
    );
});

describe("lineupPage: Tabs 2/10's table for one page state", () => {
  const sel = { on: [], any: [], off: [], count: 1, exact: false };
  const many = Array.from({ length: 200 }, (_, i) => ({
    team_id: i % 2 ? 2 : 3,
    total_poss: i * 5,
    net_rtg: i,
    is_total: 1,
  }));
  it("filters the team first, then takes auto's minimum from that team's units", () => {
    const page = lineupPage(many, { team: "2", selection: sel, view: "summary", min: null, stats: [] });
    const local = localLineups(many, "2", sel);
    expect(page.autoMin).toBe(lineupAutoMin(local));
    expect(page.rows).toEqual(lineupTableRows(local, "summary", page.autoMin ?? 0));
    expect(page.shown).toBe(local.filter((r) => Number(r.total_poss) >= page.min).length);
    expect(page.maxPoss).toBe(1000);
  });
  it("a slider minimum replaces auto; auto over the whole population raises the bar", () => {
    expect(lineupPage(many, { team: "", selection: sel, view: "summary", min: 40, stats: [] }).min).toBe(40);
    const auto = lineupPage(many, { team: "", selection: sel, view: "summary", min: null, stats: [] });
    expect(auto.min).toBe(lineupAutoMin(many));
    expect(auto.min).toBeGreaterThan(0);
  });
});
