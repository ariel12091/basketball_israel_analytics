import { describe, expect, it } from "vitest";
import { stepValue } from "./stepValue";

describe("stepValue", () => {
  it("rounds a typed value to the nearest step", () => {
    expect(stepValue("123", 0, 2000, 10)).toBe(120);
    expect(stepValue("125", 0, 2000, 10)).toBe(130);
    expect(stepValue(" 40 ", 0, 2000, 10)).toBe(40);
  });
  it("clamps to the range", () => {
    expect(stepValue(-5, 0, 2000, 10)).toBe(0);
    expect(stepValue("3005", 0, 3000, 10)).toBe(3000);
  });
  it("rejects what is not a number", () => {
    expect(stepValue("abc", 0, 2000, 10)).toBeNull();
    expect(stepValue("", 0, 2000, 10)).toBeNull();
  });
});
