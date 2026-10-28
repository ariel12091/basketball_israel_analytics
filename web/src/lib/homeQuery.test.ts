import { expect, it } from "vitest";
import { homeTabHref, homeStoryHref, homeLeagueHref } from "./homeQuery";
it("carries the exact team scopes of Shiny Home navigation", () => {
  expect(homeTabHref("il", "2026", "go_onoff", "2")).toBe(
    "/il/onoff?season=2026&teams=2",
  );
  expect(homeTabHref("il", "2026", "go_lineups", "2")).toBe(
    "/il/lineups?season=2026&team=2&size=5",
  );
  expect(homeTabHref("il", "2026", "go_gamelogs", "2")).toBe(
    "/il/game-logs?season=2026&team=2",
  );
  expect(homeTabHref("il", "2026", "go_team", "2")).toBe(
    "/il/team-ratings?season=2026",
  );
  expect(homeTabHref("il", "2026", "go_playerstats", "2")).toBe(
    "/il/player-stats?season=2026",
  );
  expect(homeTabHref("il", "2026", "go_compare", "2")).toBe(
    "/il/compare?season=2026&preset=starters_bench",
  );
  expect(homeTabHref("eurocup", "2025", "go_euro_gamelogs", "")).toBe(
    "/eurocup/game-logs?season=2025",
  );
});
it("storylines pin both sides and open the selected team detail", () => {
  expect(homeStoryHref("2026", "clutch", "2")).toBe(
    "/il/compare?season=2026&preset=clutch&team_id=2&open_detail=true",
  );
  expect(homeStoryHref("2026", "", "2")).toBe("/il/team-ratings?season=2026");
});
it("keeps seasons only where their numbering is shared", () => {
  expect(homeLeagueHref("eurocup", "euroleague", "2025")).toBe(
    "/eurocup/home?season=2025",
  );
  expect(homeLeagueHref("euroleague", "il", "2026")).toBe(
    "/euroleague/home?season=2026",
  );
});
