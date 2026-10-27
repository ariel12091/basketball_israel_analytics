import { describe, expect, it } from "vitest";
import { noteVersion, withVersion } from "./dataVersion";

describe("withVersion", () => {
  it("adds the league's latest version, shared by EuroLeague and EuroCup", () => {
    expect(withVersion("/api/v1/il/onoff?season=2026")).toBe(
      "/api/v1/il/onoff?season=2026",
    );
    noteVersion("il", "2026-10-25 20:51:00+00");
    noteVersion("euroleague", "2026-10-26T09:34");
    noteVersion("il", "unknown");
    expect(withVersion("/api/v1/il/onoff?season=2026")).toBe(
      "/api/v1/il/onoff?season=2026&v=2026-10-25%2020%3A51%3A00%2B00",
    );
    expect(withVersion("/api/v1/eurocup/lineup-roster")).toBe(
      "/api/v1/eurocup/lineup-roster?v=2026-10-26T09%3A34",
    );
  });
});
