import { describe, expect, it } from "vitest";
import { toolSchemas } from "./prompt";

describe("tool descriptions", () => {
  const byName = Object.fromEntries(toolSchemas().map((t) => [t.name, t.description]));

  it("table tools say sort is required and list their sort columns", () => {
    expect(byName.lineups).toMatch(/sort is required/i);
    expect(byName.lineups).toMatch(/net_rtg/);
    expect(byName.team_ratings).toMatch(/off_ppp/);
    expect(byName.team_ratings).toMatch(/rank_off_ppp/);
    expect(byName.player_stats).toMatch(/\bpts\b/);
  });

  it("tools without a table do not ask for sort", () => {
    expect(byName.list_teams).not.toMatch(/sort is required/i);
    expect(byName.compare).not.toMatch(/sort is required/i);
  });
});
