import { expect, it } from "vitest";
import { seasonTypeChoices } from "./TabKit";

// The Game type chips an Israeli page shows with no Game type set: the
// season's types, named from the game_types catalogue in the menu's order.
it("names the season's game types once each, in menu order", () => {
  // 2026-27 so far: regular season, Winner Cup (codes 10 and 34, one choice).
  expect(seasonTypeChoices(["10", "34", "5"])).toEqual([
    { value: "5", label: "Regular season" },
    { value: "10,34", label: "Winner Cup" },
  ]);
  expect(seasonTypeChoices(["35", "5", "16"]).map((c) => c.label)).toEqual([
    "Regular season",
    "PO QF",
    "State Cup",
  ]);
  expect(seasonTypeChoices([])).toEqual([]);
});
