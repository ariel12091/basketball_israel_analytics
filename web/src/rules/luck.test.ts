import { describe, it, expect } from "vitest";
import { luckTag, luckSide, teamLuckTagged } from "./luck";

// docs/shared_definitions.md "Shooting luck (Team Ratings)": luck is what
// shooting added to the Net (net - net without luck).
describe("luck tag", () => {
  it("points the arrow to where the Net would be without luck", () => {
    expect(luckTag(-3.3)).toBe("▲ Unlucky"); // Galil Elion 26-27: +2.8 -> +6.1
    expect(luckTag(4.2)).toBe("▼ Lucky");
  });
  it("says whether the luck-free Net is better or worse than shown", () => {
    expect(luckSide(-3.3)).toBe("better");
    expect(luckSide(4.2)).toBe("worse");
  });
});

describe("teamLuckTagged", () => {
  it("tags luck of half the Net or more, from 1.5 per 100", () => {
    expect(teamLuckTagged(2.8, -3.3)).toBe(true); // 119%
    expect(teamLuckTagged(-11.6, -2.9)).toBe(false); // 25%
    expect(teamLuckTagged(6, 3)).toBe(true); // exactly 50%
    expect(teamLuckTagged(-2.1, 1.3)).toBe(false); // 62%, under the 1.5 floor
  });
  it("always tags a sign flip, however small", () => {
    expect(teamLuckTagged(0.4, 0.6)).toBe(true); // without luck -0.2
    expect(teamLuckTagged(-0.5, -1.0)).toBe(true);
  });
  it("tags a Net of zero only from the floor", () => {
    expect(teamLuckTagged(0, 1.4)).toBe(false);
    expect(teamLuckTagged(0, 1.5)).toBe(true);
  });
  it("never tags missing values", () => {
    expect(teamLuckTagged(null, 3)).toBe(false);
    expect(teamLuckTagged(3, null)).toBe(false);
  });
});
