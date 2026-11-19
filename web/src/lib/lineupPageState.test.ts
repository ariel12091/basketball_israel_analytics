import { describe, expect, it } from "vitest";
import { lineupPageOptions } from "./lineupPageState";
import { lineupsPagePath, LINEUP_QUERY_DEFAULTS } from "./tabQuery";
import ui from "../data/shinyUi.json";

const opts = (q: string) =>
  lineupPageOptions(new URLSearchParams(q), "summary");
const pageSearch = (auto: boolean, min = "35") =>
  new URL(
    lineupsPagePath(
      "euroleague",
      { ...LINEUP_QUERY_DEFAULTS, season: "2026", min_poss: min },
      auto,
    ),
    "http://page.local",
  ).searchParams;

describe("lineupPageOptions: the possession minimum", () => {
  it("is automatic when a request sends neither auto nor min_poss", () => {
    expect(opts("season=2026").min).toBeNull();
  });

  it("keeps an explicit min_poss, and auto=true still replaces it", () => {
    expect(opts("min_poss=35").min).toBe(35);
    expect(opts("auto=true&min_poss=35").min).toBeNull();
  });

  it("auto=false without min_poss falls back to the page's default in every league", () => {
    expect(opts("auto=false").min).toBe(Number(ui.lineupDefaults.minPoss));
  });

  it("reads the page's own requests as before: auto, or the hand-set minimum", () => {
    expect(lineupPageOptions(pageSearch(true), "summary").min).toBeNull();
    expect(lineupPageOptions(pageSearch(false), "summary").min).toBe(35);
  });
});
