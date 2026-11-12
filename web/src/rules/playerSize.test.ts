import { describe, it, expect } from "vitest";
import { compareBySize, orderBySize } from "./playerSize";

describe("lineup name order by size (shared by Gameflow and every lineup list)", () => {
  it("runs shortest to tallest, a tie by position then name, no height last", () => {
    const p = (label: string, height: number | null, position: string | null = null) => ({
      label,
      size: { height, position },
    });
    const five = [p("Z", null), p("C1", 2.08, "C"), p("PG1", 1.9, "PG"), p("G1", 1.9, "G"), p("A", 1.9, "G")];
    expect(orderBySize(five, (x) => x.size, (x) => x.label).map((x) => x.label)).toEqual([
      "PG1",
      "A",
      "G1",
      "C1",
      "Z",
    ]);
  });

  it("leaves a lineup in its own order when nobody in it has a height", () => {
    const none = ["C", "A", "B"].map((label) => ({ label }));
    expect(orderBySize(none, () => undefined, (x) => x.label).map((x) => x.label)).toEqual([
      "C",
      "A",
      "B",
    ]);
  });

  it("compares two players directly", () => {
    expect(compareBySize({ height: 1.8, position: null }, { height: 2, position: null }, "b", "a")).toBeLessThan(0);
    expect(compareBySize(undefined, { height: 2, position: null }, "a", "b")).toBeGreaterThan(0);
  });
});
