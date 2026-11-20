import { expect, it } from "vitest";
import { seasonTypesLabel } from "./TabKit";

// The chip that says what an Israeli page with no Game type set includes.
// Names come from the game_types catalogue, in the Game type menu's order.
it("names the season's game types once each, in menu order", () => {
  // 2026-27 so far: regular season, Winner Cup (codes 10 and 34, one choice).
  expect(seasonTypesLabel(["10", "34", "5"])).toBe("Regular season + Winner Cup");
  expect(seasonTypesLabel(["35", "5", "16"])).toBe("Regular season + PO QF + State Cup");
  expect(seasonTypesLabel([])).toBe("");
});
