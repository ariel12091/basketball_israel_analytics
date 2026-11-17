import { describe, it, expect } from "vitest";
import { PLAYER_QUERY_DEFAULTS, playerStatsPath } from "./tabQuery";
describe("playerStatsPath", () => {
  it("never sends the column view to the API", () => {
    const path = playerStatsPath("il", {
      ...PLAYER_QUERY_DEFAULTS,
      season: "2026",
      cols: "shooting",
    });
    expect(path).not.toContain("cols=");
    expect(path).toContain("/api/v1/il/player-stats?");
  });
  it("defaults the column view to main", () => {
    expect(PLAYER_QUERY_DEFAULTS.cols).toBe("main");
  });
});
