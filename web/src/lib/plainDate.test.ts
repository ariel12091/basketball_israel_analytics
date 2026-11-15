import { describe, expect, it } from "vitest";
import { addDays, formatPlainDate, parsePlainDate } from "./plainDate";

describe("plainDate", () => {
  it("reads a calendar day as UTC midnight and writes it back", () => {
    const d = parsePlainDate("2026-01-05")!;
    expect(d.toISOString()).toBe("2026-01-05T00:00:00.000Z");
    expect(formatPlainDate(d)).toBe("2026-01-05");
  });
  it("rejects malformed and impossible days", () => {
    for (const s of ["", "2026-1-5", "2026-02-30", "2026-13-01", "x"])
      expect(parsePlainDate(s)).toBeNull();
    expect(parsePlainDate("2028-02-29")).not.toBeNull();
  });
  it("adds days across month and year ends", () => {
    expect(addDays("2026-01-31", 1)).toBe("2026-02-01");
    expect(addDays("2026-12-31", 1)).toBe("2027-01-01");
    expect(addDays("2026-03-01", -1)).toBe("2026-02-28");
    expect(addDays("bad", 1)).toBeNull();
  });
});
