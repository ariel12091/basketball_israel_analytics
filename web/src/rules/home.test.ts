import { readFileSync } from "node:fs";
import { describe, it, expect } from "vitest";
import {
  homeDashboard,
  homeDefaultTeam,
  homeOrdinal,
  homeFixed,
  type HomeFrames,
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
describe("Home original R helpers over recorded dashboard frames", () => {
  for (const [name, g] of Object.entries(cases))
    it(name, () => check(homeDashboard(g.frames, g.team), g.expected));
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
