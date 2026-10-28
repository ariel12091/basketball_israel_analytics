import { describe, expect, it } from "vitest";
import { mutedAccuracy } from "./palette";

describe("muted shot accuracy text", () => {
  it("is neutral at the league average, green above, red below", () => {
    expect(mutedAccuracy(0)).toBe("#cfc8bb");
    expect(mutedAccuracy(1)).toBe("#7fc79a");
    expect(mutedAccuracy(-1)).toBe("#d98b7e");
  });
});
