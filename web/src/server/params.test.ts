import { describe, it, expect } from "vitest";
import { parseParams } from "./params";
import { ParamError, rawQueryGuard } from "./api";

describe("request validation", () => {
  for (const query of [
    "start=2026-99-01",
    "start=2026-02-30",
    "min_gn=5&max_gn=2",
    "last_n=3&min_gn=1",
    "own_starters=gte:6",
    "season=NaN",
    "stats=%7B%7D",
    "team=abc",
  ]) {
    it(`rejects ${query}`, () =>
      expect(() => parseParams(new URLSearchParams(query), "il")).toThrow(
        ParamError,
      ));
  }
  for (const query of [
    "team=1&%74eam=2",
    "team=%FF",
    "team=%",
    "q=" + "x".repeat(4096),
  ]) {
    it("rejects malformed/ambiguous raw query " + query.slice(0, 30), () =>
      expect(() =>
        rawQueryGuard(new URL("https://example.test/?" + query)),
      ).toThrow(ParamError),
    );
  }
  it("keeps independent own and opponent starter bounds", () => {
    const p = parseParams(
      new URLSearchParams(
        "season=2026&own_starters=gte:3&opp_starters=lte:1&last_n=5&home_away=home",
      ),
      "il",
    );
    expect(p.starters).toEqual({ off_min: 3, def_max: 1 });
    expect(p.gn?.last_n).toBe(5);
    expect(p.home_away).toBe("home");
  });
});
