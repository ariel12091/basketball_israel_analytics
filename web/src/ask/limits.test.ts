import { describe, expect, it } from "vitest";
import { createLimiter } from "./limits";

const HOUR = 3_600_000;

describe("ask rate limit (20 per hour per key)", () => {
  it("21st request in an hour is limited with retryAfter", () => {
    const check = createLimiter(20, HOUR);
    for (let i = 0; i < 20; i++) expect(check(["client:a"], i * 1000)).toEqual({ ok: true });
    expect(check(["client:a"], 30_000)).toEqual({ ok: false, retryAfterSec: 3570 });
  });

  it("window slides", () => {
    const check = createLimiter(20, HOUR);
    for (let i = 0; i < 20; i++) check(["client:a"], 0);
    expect(check(["client:a"], HOUR - 1).ok).toBe(false);
    expect(check(["client:a"], HOUR).ok).toBe(true);
  });

  it("any limited key blocks, and a blocked request is not counted", () => {
    const check = createLimiter(2, HOUR);
    check(["client:a", "ip:1"], 0);
    check(["client:b", "ip:1"], 0);
    expect(check(["client:c", "ip:1"], 0).ok).toBe(false);
    expect(check(["client:c", "ip:2"], 0).ok).toBe(true);
  });
});
