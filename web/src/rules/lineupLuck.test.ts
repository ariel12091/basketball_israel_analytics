// Twin pin of app/tests/testthat/test-lineup-3pt-luck.R: the same fixture
// must give the same luck, net without luck and tags as helpers.R
// lineup_3pt_luck().
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { expect, it } from "vitest";
import LineupTable from "@/components/shiny/LineupTable";
import { lineupThreePtLuck } from "./lineups";

const fixture = () => [
  {
    team_id: 1,
    net_rtg: 4,
    off_poss: 100,
    def_poss: 100,
    off_fg3_made: 30,
    off_fg3_att: 60,
    def_fg3_made: 10,
    def_fg3_att: 40,
  },
  {
    team_id: 1,
    net_rtg: 10,
    off_poss: 100,
    def_poss: 100,
    off_fg3_made: 20,
    off_fg3_att: 60,
    def_fg3_made: 20,
    def_fg3_att: 40,
  },
  {
    team_id: 2,
    net_rtg: -3,
    off_poss: 100,
    def_poss: 100,
    off_fg3_made: 20,
    off_fg3_att: 80,
    def_fg3_made: 20,
    def_fg3_att: 50,
  },
];

it("splits our and opponents' shooting at the population's 3P% and tags sign flips", () => {
  const out = lineupThreePtLuck(fixture());
  expect(out[0].luck_3pt_ours).toBeCloseTo(27);
  expect(out[0].luck_3pt_theirs).toBeCloseTo(12);
  expect(out[0].net_without_3pt_luck).toBeCloseTo(4 - 27 - 12);
  expect(out[1].net_without_3pt_luck).toBeCloseTo(31);
  expect(out.map((r) => r.luck_3pt_flag)).toEqual([true, false, true]);
});

it("leaves rows without shooting columns alone and gives no luck without possessions", () => {
  const bare = fixture().map(({ team_id, net_rtg, off_poss, def_poss }) => ({
    team_id,
    net_rtg,
    off_poss,
    def_poss,
  }));
  expect(lineupThreePtLuck(bare)).toBe(bare);
  const zero = fixture();
  zero[0].off_poss = 0;
  const out = lineupThreePtLuck(zero);
  expect(out[0].luck_3pt_ours).toBeNull();
  expect(out[0].luck_3pt_flag).toBe(false);
});

it("the Summary table tags a flagged lineup's Net RTG, and never the TOTAL row", () => {
  const rows = lineupThreePtLuck(fixture()).map((r, i) => ({
    ...r,
    is_total: 1,
    Team: "T",
    Players: `P${i}`,
    sub_lineup_hash: `h${i}`,
  }));
  const html = (rs: typeof rows) =>
    renderToStaticMarkup(
      createElement(LineupTable, {
        rows: rs,
        view: "summary",
        onLineup: () => {},
      }),
    );
  expect(html(rows).match(/onoff-luck-tag/g)).toHaveLength(2);
  expect(html(rows.map((r) => ({ ...r, is_total: 0 })))).not.toContain(
    "onoff-luck-tag",
  );
});
