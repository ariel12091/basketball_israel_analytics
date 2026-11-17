import { expect, test } from "vitest";
import { marginArea, marginPath } from "./gameflow";

test("marginArea closes the margin line down to the zero axis", () => {
  // 0-600 s on 100 units, 40 high from y=10: zero at y=30, +4 at the top.
  const line = marginPath(
    [
      { elapsed: 0, margin: 0 },
      { elapsed: 300, margin: 4 },
      { elapsed: 600, margin: -2 },
    ],
    600,
    100,
    10,
    40,
    0,
    4,
  );
  expect(marginArea(line, 30)).toBe(`${line} V 30.00 H 0.00 Z`);
});

test("marginArea of an empty line is empty", () => {
  expect(marginArea("", 30)).toBe("");
});
