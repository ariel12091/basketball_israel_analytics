import { describe, expect, it } from "vitest";
import ui from "@/data/shinyUi.json";
import type { Block } from "./Blocks";
import { keyFromBlocks } from "./TableKey";

const views = (v: Record<string, unknown>) =>
  v as unknown as Record<string, Block[]>;

describe("keyFromBlocks", () => {
  it("draws the on/off rank bar only where Shiny has one", () => {
    // On/Off Four Factors shows on- and off-court ranks.
    expect(keyFromBlocks(views(ui.onoff.il.views).four_factors).rank).toBe(
      true,
    );
    expect(keyFromBlocks(views(ui.onoff.euro.views).four_factors).rank).toBe(
      true,
    );
    // Team Ratings has no on/off: EuroLeague's Four Factors legend is only a
    // colour note (user 2026-11-15: "it doesn't belong there").
    for (const league of ["il", "euro"] as const)
      for (const blocks of Object.values(views(ui.teamRatings[league].views)))
        expect(keyFromBlocks(blocks).rank).toBe(false);
  });
});
