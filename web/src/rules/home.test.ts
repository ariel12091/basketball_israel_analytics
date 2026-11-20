import { readFileSync } from "node:fs";
import { describe, it, expect } from "vitest";
import {
  homeDashboard,
  homeDefaultTeam,
  homeOrdinal,
  homeFixed,
  type HomeFrames,
  homeLineups,
  homeLineupMinPoss,
} from "./home";
import type { Row } from "./gameLogs";
const cases = JSON.parse(
  readFileSync("test/goldens/home/cases.json", "utf8"),
) as Record<string, { team: string; frames: HomeFrames; expected: unknown }>;
function check(actual: unknown, expected: unknown, path = "") {
  if (typeof expected === "number") {
    expect(actual, path).toBeTypeOf("number");
    expect(actual, path).toBeCloseTo(expected, 9);
  } else if (Array.isArray(expected)) {
    expect(actual, path).toHaveLength(expected.length);
    expected.forEach((v, i) =>
      check((actual as unknown[])[i], v, `${path}.${i}`),
    );
  } else if (expected && typeof expected === "object")
    for (const [k, v] of Object.entries(expected))
      check((actual as Record<string, unknown>)?.[k], v, `${path}.${k}`);
  else expect(actual, path).toEqual(expected);
}
// Where the web's lineup card differs from Shiny's (homeLineups,
// homeLineupMinPoss): one qualifying lineup, or all of them tied, is no
// comparison, so no card (Shiny showed one lineup as both best and worst);
// and a lineup under the team's bar is out -- in missing_values the +5.8
// lineup has 77 possessions (no offensive count), under that team's 100.
const WEB_LINEUPS: Record<string, unknown> = {
  "2027_5": null,
  one_lineup: null,
  ties: null,
  missing_values: { best: { net_rtg: 49.3 }, worst: { net_rtg: 13.9 } },
};
function webExpected(name: string, expected: unknown): unknown {
  if (!(name in WEB_LINEUPS)) return expected;
  return { ...(expected as object), lineups: WEB_LINEUPS[name] };
}
describe("Home original R helpers over recorded dashboard frames", () => {
  for (const [name, g] of Object.entries(cases))
    it(name, () =>
      check(homeDashboard(g.frames, g.team), webExpected(name, g.expected)),
    );
});
it("resolves remembered/leader/first/empty defaults as hub_default_team", () => {
  const teams: Row[] = [{ team_id: 2 }, { team_id: 5 }],
    ratings: Row[] = [
      { team_id: 2, rank_net_rtg: 2 },
      { team_id: 5, rank_net_rtg: 1 },
    ];
  expect(homeDefaultTeam(" 2 ", teams, ratings)).toBe("2");
  expect(homeDefaultTeam("99", teams, ratings)).toBe("5");
  expect(homeDefaultTeam("", teams, [])).toBe("2");
  expect(homeDefaultTeam("", [], ratings)).toBe("");
});
it("keeps ordinal exceptions and signed rounded zero", () => {
  expect([1, 2, 3, 4, 11, 12, 13, 21, 22, 23, 111].map(homeOrdinal)).toEqual([
    "1st",
    "2nd",
    "3rd",
    "4th",
    "11th",
    "12th",
    "13th",
    "21st",
    "22nd",
    "23rd",
    "111th",
  ]);
  expect(homeOrdinal(null)).toBe("");
  expect(homeFixed(-0.01, true)).toBe("-0.0");
  expect(homeFixed(2.25)).toBe("2.2");
});

it("Home lineup card: a bar scaled to the season, and no card without a comparison", () => {
  const row = (net: number, poss = 120) => ({ net_rtg: net, off_poss: poss / 2, def_poss: poss / 2 });
  // 2.5% of the team's possessions, 40 to 100, in steps of 5.
  expect(homeLineupMinPoss(1426)).toBe(40);
  expect(homeLineupMinPoss(2400)).toBe(60);
  expect(homeLineupMinPoss(4400)).toBe(100);
  expect(homeLineupMinPoss(Number.NaN)).toBe(40);
  expect(homeLineups([row(-28)])).toBeNull();
  expect(homeLineups([row(3), row(3)])).toBeNull();
  expect(homeLineups([row(9, 50), row(-4, 45)], 40)).toMatchObject({
    best: { net_rtg: 9 },
    worst: { net_rtg: -4 },
    minPoss: 40,
  });
  expect(homeLineups([row(9, 50), row(-4, 45)], 100)).toBeNull();
});
